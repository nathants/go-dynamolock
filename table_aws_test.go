package dynamolock

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestTableAWS(t *testing.T) {
	client, name := liveTable(t, func(in *dynamodb.CreateTableInput) {
		in.AttributeDefinitions = []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("pending"), AttributeType: types.ScalarAttributeTypeS},
		}
		in.KeySchema = []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange}}
		in.GlobalSecondaryIndexes = []types.GlobalSecondaryIndex{{IndexName: aws.String("pending-work"), KeySchema: []types.KeySchemaElement{{AttributeName: aws.String("pending"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange}}, Projection: &types.Projection{ProjectionType: types.ProjectionTypeKeysOnly}}}
	})
	table := Table{Name: name, PartitionKey: "pk", SortKey: "sk", Attributes: []string{"pending", "ttl"}}
	type job struct {
		PK      string `dynamodbav:"pk"`
		SK      string `dynamodbav:"sk"`
		Value   string `dynamodbav:"value"`
		Pending string `dynamodbav:"pending,omitempty"`
		TTL     int64  `dynamodbav:"ttl,omitempty"`
	}
	input := func(key Key) *LockInput {
		return &LockInput{Table: table, Key: key, LeaseDuration: 30 * time.Second}
	}
	for _, sort := range []string{"a", "b", "c"} {
		key := Key{Partition: "account", Sort: sort}
		l, _, err := Lock[job](t.Context(), client, input(key))
		cleanupLease(t, l)
		if err != nil {
			t.Fatal(err)
		}
		if l.Key() != key {
			t.Fatal("wrong lease key")
		}
		if err := l.Commit(t.Context(), &job{Value: sort, Pending: "work", TTL: 2000000000}); err != nil {
			t.Fatal(err)
		}
		value, err := Read[job](t.Context(), client, table, key, ReadOptions{ConsistentRead: true})
		if err != nil || value == nil || value.PK != "account" || value.SK != sort || value.Value != sort {
			t.Fatalf("read compound: %+v %v", value, err)
		}
	}
	query := &dynamodb.QueryInput{ConsistentRead: aws.Bool(true), Limit: aws.Int32(1), KeyConditionExpression: aws.String("#pk = :pk"), ExpressionAttributeNames: map[string]string{"#pk": "pk"}, ExpressionAttributeValues: map[string]types.AttributeValue{":pk": &types.AttributeValueMemberS{Value: "account"}}}
	var found []string
	for {
		page, err := Query[job](t.Context(), client, table, query)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range page.Items {
			found = append(found, item.Value.Value)
		}
		if len(page.LastEvaluatedKey) == 0 {
			break
		}
		query.ExclusiveStartKey = page.LastEvaluatedKey
	}
	if !reflect.DeepEqual(found, []string{"a", "b", "c"}) {
		t.Fatalf("range order/pages: %v", found)
	}
	page, err := Scan[job](t.Context(), client, table, &dynamodb.ScanInput{ConsistentRead: aws.Bool(true), Select: types.SelectCount})
	if err != nil || page.Count != 3 {
		t.Fatalf("count: %+v %v", page, err)
	}

	// A typed writer that cannot see ttl still preserves it and its projection.
	type older struct {
		Value   string `dynamodbav:"value"`
		Pending string `dynamodbav:"pending,omitempty"`
	}
	key := Key{Partition: "account", Sort: "b"}
	lease, value, err := Lock[older](t.Context(), client, input(key))
	cleanupLease(t, lease)
	if err != nil {
		t.Fatal(err)
	}
	value.Pending = ""
	value.Value = "changed"
	if err := lease.Update(t.Context(), value); err != nil {
		t.Fatal(err)
	}
	raw, err := getItem(t.Context(), client, table, key)
	if err != nil || raw["pending"] != nil || !reflect.DeepEqual(raw["ttl"], number(2000000000)) {
		t.Fatalf("sparse/hidden projection: %+v %v", raw, err)
	}
	if err := lease.Release(t.Context()); err != nil {
		t.Fatal(err)
	}

	// GSI discovery is eventual. It must converge to only pending jobs, and
	// keys-only projections must never look like complete application documents.
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	index := &dynamodb.QueryInput{IndexName: aws.String("pending-work"), KeyConditionExpression: aws.String("#pending = :pending"), ExpressionAttributeNames: map[string]string{"#pending": "pending"}, ExpressionAttributeValues: map[string]types.AttributeValue{":pending": &types.AttributeValueMemberS{Value: "work"}}}
	for {
		page, err := Query[job](ctx, client, table, index)
		if err != nil {
			t.Fatal(err)
		}
		var keys []string
		for _, item := range page.Items {
			if item.Value != nil {
				t.Fatal("keys-only index fabricated payload")
			}
			keys = append(keys, item.Key.Sort)
		}
		if reflect.DeepEqual(keys, []string{"a", "c"}) {
			break
		}
		if err := wait(ctx, 100*time.Millisecond); err != nil {
			t.Fatalf("index did not converge: %v %v", keys, err)
		}
	}

	// A lost successful Delete is not retried. Release cleanup cannot harm a
	// recreated item, whose new token has a distinct owner.
	faulty, injected := loseAWSResponse(t, client, func(w wireRequest) bool { return w.Target == "DynamoDB_20120810.DeleteItem" })
	old, _, err := Lock[job](t.Context(), faulty, input(key))
	cleanupLease(t, old)
	if err != nil {
		t.Fatal(err)
	}
	if err := old.Delete(t.Context()); !errors.Is(err, ErrOutcomeUnknown) || injected.Load() != 1 {
		t.Fatalf("lost delete: %v", err)
	}
	raw, err = getItem(t.Context(), client, table, key)
	if err != nil || len(raw) != 0 {
		t.Fatalf("delete did not remove item: %+v %v", raw, err)
	}
	successor, _, err := Lock[job](t.Context(), client, input(key))
	cleanupLease(t, successor)
	if err != nil {
		t.Fatal(err)
	}
	if err := successor.Update(t.Context(), &job{Value: "recreated"}); err != nil {
		t.Fatal(err)
	}
	if err := old.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := successor.Delete(t.Context()); err != nil {
		t.Fatalf("old cleanup changed successor: %v", err)
	}
	raw, err = getItem(t.Context(), client, table, key)
	if err != nil || len(raw) != 0 {
		t.Fatalf("confirmed delete: %+v %v", raw, err)
	}
}
