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
	"github.com/aws/smithy-go"
)

func TestQueryIndexAWS(t *testing.T) {
	client, name := liveTable(t, func(in *dynamodb.CreateTableInput) {
		in.AttributeDefinitions = []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("waiting"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("order"), AttributeType: types.ScalarAttributeTypeS},
		}
		in.KeySchema = []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange}}
		for _, projection := range []types.ProjectionType{types.ProjectionTypeKeysOnly, types.ProjectionTypeInclude, types.ProjectionTypeAll} {
			index := types.GlobalSecondaryIndex{
				IndexName:  aws.String(string(projection)),
				KeySchema:  []types.KeySchemaElement{{AttributeName: aws.String("waiting"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("order"), KeyType: types.KeyTypeRange}},
				Projection: &types.Projection{ProjectionType: projection},
			}
			if projection == types.ProjectionTypeInclude {
				// An incomplete ownership pair must not be interpreted as a document.
				index.Projection.NonKeyAttributes = []string{"note", "owner_token"}
			}
			in.GlobalSecondaryIndexes = append(in.GlobalSecondaryIndexes, index)
		}
		in.LocalSecondaryIndexes = []types.LocalSecondaryIndex{{
			IndexName:  aws.String("local-order"),
			KeySchema:  []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("order"), KeyType: types.KeyTypeRange}},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeKeysOnly},
		}}
	})
	table := Table{Name: name, PartitionKey: "pk", SortKey: "sk", Attributes: []string{"waiting", "order", "note", "hidden"}}
	type job struct {
		PK      string `dynamodbav:"pk"`
		SK      string `dynamodbav:"sk"`
		Waiting string `dynamodbav:"waiting"`
		Order   string `dynamodbav:"order"`
		Note    string `dynamodbav:"note"`
		Hidden  string `dynamodbav:"hidden"`
		Secret  string `dynamodbav:"secret"`
	}
	for _, order := range []string{"001", "002", "003"} {
		lease, _, err := Lock[job](t.Context(), client, &LockInput{
			Table: table, Key: Key{Partition: "account", Sort: order},
			HeartbeatMaxAge: 30 * time.Second, HeartbeatInterval: time.Second,
		})
		cleanupLease(t, lease)
		if err != nil {
			t.Fatal(err)
		}
		if err := lease.Update(t.Context(), &job{Waiting: "yes", Order: order, Note: "visible", Hidden: "copy", Secret: "private"}); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("local nonprojected filter", func(t *testing.T) {
		// Index-only selection must reject filters needing parent-table fields,
		// including for counts, rather than silently fetching those fields.
		for _, selection := range []types.Select{"", types.SelectCount} {
			_, err := QueryIndex[job](t.Context(), client, table, &dynamodb.QueryInput{
				IndexName: aws.String("local-order"), ConsistentRead: aws.Bool(true), Select: selection,
				KeyConditionExpression: aws.String("#pk = :pk"), FilterExpression: aws.String("#hidden = :hidden"),
				ExpressionAttributeNames:  map[string]string{"#pk": "pk", "#hidden": "hidden"},
				ExpressionAttributeValues: map[string]types.AttributeValue{":pk": &types.AttributeValueMemberS{Value: "account"}, ":hidden": &types.AttributeValueMemberS{Value: "copy"}},
			})
			var apiErr smithy.APIError
			if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "ValidationException" {
				t.Fatalf("nonprojected filter was not rejected for Select %q: %v", selection, err)
			}
		}
	})
	for _, index := range []string{"KEYS_ONLY", "INCLUDE", "ALL", "local-order"} {
		t.Run(index, func(t *testing.T) {
			partition, value := "waiting", "yes"
			local := index == "local-order"
			if local {
				partition, value = "pk", "account"
			}
			input := &dynamodb.QueryInput{
				IndexName: aws.String(index), ConsistentRead: aws.Bool(local), ScanIndexForward: aws.Bool(false), Limit: aws.Int32(1),
				KeyConditionExpression:   aws.String("#partition = :partition AND begins_with(#order, :prefix)"),
				FilterExpression:         aws.String("#sk <> :skip"),
				ExpressionAttributeNames: map[string]string{"#partition": partition, "#order": "order", "#sk": "sk"},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":partition": &types.AttributeValueMemberS{Value: value}, ":prefix": &types.AttributeValueMemberS{Value: "00"}, ":skip": &types.AttributeValueMemberS{Value: "002"},
				},
			}
			var want []IndexItem[job]
			for _, order := range []string{"003", "001"} {
				entry := job{PK: "account", SK: order, Order: order}
				if !local {
					entry.Waiting = "yes"
				}
				if index == "INCLUDE" || index == "ALL" {
					entry.Note = "visible"
				}
				if index == "ALL" {
					entry.Hidden = "copy"
				}
				want = append(want, IndexItem[job]{Key: Key{Partition: "account", Sort: order}, Value: entry})
			}
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			for {
				input.ExclusiveStartKey = nil
				var found []IndexItem[job]
				var count, scanned int32
				emptyWithCursor := false
				for {
					page, err := QueryIndex[job](ctx, client, table, input)
					if err != nil {
						t.Fatal(err)
					}
					found = append(found, page.Items...)
					count += page.Count
					scanned += page.ScannedCount
					if len(page.LastEvaluatedKey) == 0 {
						break
					}
					if page.LastEvaluatedKey["order"] == nil || page.LastEvaluatedKey["pk"] == nil || page.LastEvaluatedKey["sk"] == nil {
						t.Fatal("incomplete service cursor")
					}
					emptyWithCursor = emptyWithCursor || len(page.Items) == 0
					input.ExclusiveStartKey = page.LastEvaluatedKey
				}
				if reflect.DeepEqual(found, want) && count == 2 && scanned == 3 && emptyWithCursor {
					break
				}
				if err := wait(ctx, 100*time.Millisecond); err != nil {
					t.Fatalf("index did not converge: %+v count=%d scanned=%d emptyWithCursor=%v: %v", found, count, scanned, emptyWithCursor, err)
				}
			}
			input.ExclusiveStartKey, input.Limit, input.Select = nil, nil, types.SelectCount
			page, err := QueryIndex[job](ctx, client, table, input)
			if err != nil || len(page.Items) != 0 || page.Count != 2 || page.ScannedCount != 3 {
				t.Fatalf("count page: %+v %v", page, err)
			}
		})
	}
}
