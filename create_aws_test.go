package dynamolock

import (
	"errors"
	"maps"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestCreateAWS(t *testing.T) {
	client, name := liveTable(t, func(in *dynamodb.CreateTableInput) {
		in.AttributeDefinitions = []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
		}
		in.KeySchema = []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		}
	})
	table := Table{Name: name, PartitionKey: "pk", SortKey: "sk", Attributes: []string{"pending"}}
	strong := ReadOptions{ConsistentRead: true}
	type message struct {
		PK      string `dynamodbav:"pk"`
		SK      string `dynamodbav:"sk"`
		Value   string `dynamodbav:"value"`
		Pending string `dynamodbav:"pending,omitempty"`
	}

	t.Run("compound keys projections and shared writers", func(t *testing.T) {
		key := Key{Partition: "account", Sort: "first"}
		original := message{PK: key.Partition, SK: key.Sort, Value: "created", Pending: "yes"}
		if err := Create(t.Context(), client, table, key, &original); err != nil {
			t.Fatal(err)
		}
		if err := Create(t.Context(), client, table, Key{Partition: key.Partition, Sort: "second"}, &message{Value: "sibling"}); err != nil {
			t.Fatalf("another sort key conflicted: %v", err)
		}
		if err := Create(t.Context(), client, table, key, &original); !errors.Is(err, ErrConflict) {
			t.Fatalf("identical existing data did not conflict: %v", err)
		}
		item, err := getItem(t.Context(), client, table, key)
		if err != nil {
			t.Fatal(err)
		}
		r, err := parseRecord(table, item)
		if err != nil || r.key != key || r.revision.generation == "" || r.revision.version != 1 || r.owner != "" || r.data == nil {
			t.Fatalf("invalid creation: %+v %v", r, err)
		}
		if r.data.Value["pk"] != nil || r.data.Value["sk"] != nil || !reflect.DeepEqual(item["pending"], &types.AttributeValueMemberS{Value: "yes"}) {
			t.Fatalf("incorrect payload keys or projection: %+v", item)
		}
		writer, value, err := ReadCAS[message](t.Context(), client, table, key, strong)
		if err != nil || value == nil || *value != original {
			t.Fatalf("created value: %+v %v", value, err)
		}
		value.Value, value.Pending = "updated", ""
		if err := writer.Update(t.Context(), value); err != nil {
			t.Fatal(err)
		}
		lease, value, err := Lock[message](t.Context(), client, &LockInput{Table: table, Key: key, LeaseDuration: 30 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		cleanupLease(t, lease)
		if value == nil || value.Value != "updated" || value.Pending != "" {
			t.Fatalf("CAS result: %+v", value)
		}
		value.Value = "leased"
		if err := lease.Commit(t.Context(), value); err != nil {
			t.Fatal(err)
		}
		item, err = getItem(t.Context(), client, table, key)
		if err != nil {
			t.Fatal(err)
		}
		updated, err := parseRecord(table, item)
		if err != nil || updated.revision.generation != r.revision.generation || updated.revision.version != 4 || updated.owner != "" || item["pending"] != nil {
			t.Fatalf("shared writes lost revision or projection semantics: %+v %v", updated, err)
		}
	})

	t.Run("every existing item conflicts without mutation", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			fields map[string]types.AttributeValue
		}{
			{name: "keys only"},
			{name: "empty payload", fields: map[string]types.AttributeValue{"data": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{}}}},
			{name: "unversioned payload", fields: map[string]types.AttributeValue{"data": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"value": &types.AttributeValueMemberS{Value: "retained"}}}}},
			{name: "malformed revision", fields: map[string]types.AttributeValue{"generation": &types.AttributeValueMemberS{Value: "partial"}}},
			{name: "live owner", fields: map[string]types.AttributeValue{"owner_token": &types.AttributeValueMemberS{Value: "holder"}, "expires_at": number(time.Now().Add(time.Hour).UnixNano())}},
			{name: "expired owner", fields: map[string]types.AttributeValue{"owner_token": &types.AttributeValueMemberS{Value: "expired"}, "expires_at": number(time.Now().Add(-time.Hour).UnixNano())}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				key := Key{Partition: "existing", Sort: tc.name}
				before := table.key(key)
				maps.Copy(before, tc.fields)
				if _, err := client.PutItem(t.Context(), &dynamodb.PutItemInput{TableName: aws.String(name), Item: before}); err != nil {
					t.Fatal(err)
				}
				if err := Create(t.Context(), client, table, key, &message{Value: "overwrite"}); !errors.Is(err, ErrConflict) {
					t.Fatalf("existing item did not conflict: %v", err)
				}
				after, err := getItem(t.Context(), client, table, key)
				if err != nil || !reflect.DeepEqual(after, before) {
					t.Fatalf("conflict changed the existing item: %+v %v", after, err)
				}
			})
		}
	})

	t.Run("competing creators retain one winner", func(t *testing.T) {
		key := Key{Partition: "race", Sort: "message"}
		start := make(chan struct{})
		type result struct {
			value string
			err   error
		}
		results := make(chan result, 2)
		var wg sync.WaitGroup
		for _, value := range []string{"first", "second"} {
			wg.Go(func() {
				<-start
				err := Create(t.Context(), client, table, key, &message{Value: value})
				results <- result{value, err}
			})
		}
		close(start)
		wg.Wait()
		close(results)
		wins, winner := 0, ""
		for result := range results {
			if result.err == nil {
				wins++
				winner = result.value
			} else if !errors.Is(result.err, ErrConflict) {
				t.Fatal(result.err)
			}
		}
		stored, err := Read[message](t.Context(), client, table, key, strong)
		if wins != 1 || err != nil || stored == nil || stored.Value != winner {
			t.Fatalf("creation race: %d winners, retained %+v, %v", wins, stored, err)
		}
	})

	t.Run("lost creation response remains recoverable", func(t *testing.T) {
		key := Key{Partition: "unknown", Sort: "message"}
		faulty, injected := loseAWSResponse(t, client, func(w wireRequest) bool {
			return w.Target == "DynamoDB_20120810.UpdateItem"
		})
		if err := Create(t.Context(), faulty, table, key, &message{Value: "retained"}); !errors.Is(err, ErrOutcomeUnknown) || injected.Load() != 1 {
			t.Fatalf("lost creation response: %v", err)
		}
		stored, err := Read[message](t.Context(), client, table, key, strong)
		if err != nil || stored == nil || stored.Value != "retained" {
			t.Fatalf("successful but unacknowledged creation was lost: %+v %v", stored, err)
		}
		if err := Create(t.Context(), client, table, key, &message{Value: "retry"}); !errors.Is(err, ErrConflict) {
			t.Fatalf("retry replaced unacknowledged creation: %v", err)
		}
	})
}
