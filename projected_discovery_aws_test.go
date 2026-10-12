package dynamolock

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestProjectedDiscoveryAWS(t *testing.T) {
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
	table := Table{Name: name, PartitionKey: "pk", SortKey: "sk", Attributes: []string{"secret"}}
	visible := Key{Partition: "account", Sort: "visible"}
	hidden := Key{Partition: "account", Sort: "hidden"}
	skeleton := Key{Partition: "account", Sort: "skeleton"}
	originals := map[Key]map[string]types.AttributeValue{}
	for key, payload := range map[Key]map[string]any{
		visible: {"public": "shown", "secret": "never loaded", "nested": map[string]any{"public": "nested shown", "secret": "never loaded nested"}, "a.b[0]": "literal name"},
		hidden:  {"secret": "unselected whole payload"},
	} {
		item, err := MarshalItem(table, key, payload)
		if err != nil {
			t.Fatal(err)
		}
		originals[key] = item
	}
	originals[skeleton] = table.key(skeleton)
	originals[skeleton]["owner_token"] = &types.AttributeValueMemberS{Value: "expired"}
	originals[skeleton]["expires_at"] = &types.AttributeValueMemberN{Value: "1"}
	for _, item := range originals {
		if _, err := client.PutItem(t.Context(), &dynamodb.PutItemInput{TableName: aws.String(name), Item: item, ConditionExpression: aws.String("attribute_not_exists(pk)")}); err != nil {
			t.Fatal(err)
		}
	}
	fields := [][]string{{"public"}, {"nested", "public"}, {"a.b[0]"}}
	want := map[string]any{"pk": "account", "sk": "visible", "public": "shown", "nested": map[string]any{"public": "nested shown"}, "a.b[0]": "literal name"}
	item, err := ReadProjected[map[string]any](t.Context(), client, table, visible, fields, ReadOptions{ConsistentRead: true})
	if err != nil || item == nil || item.Value == nil || item.HasLeaseMetadata || !reflect.DeepEqual(*item.Value, want) {
		t.Fatalf("real projected read leaked or lost fields: %+v %v", item, err)
	}
	// Eventual reads may initially miss the completed inserts. Check convergence
	// without assuming a delay or upgrading either request to strong consistency.
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	for {
		full, err := Read[map[string]any](ctx, client, table, visible, ReadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		partial, err := ReadProjected[map[string]any](ctx, client, table, visible, fields, ReadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if full != nil && (*full)["secret"] == "never loaded" && partial != nil && partial.Value != nil && !partial.HasLeaseMetadata && reflect.DeepEqual(*partial.Value, want) {
			break
		}
		if err := wait(ctx, 100*time.Millisecond); err != nil {
			t.Fatalf("eventual reads did not converge: %v", err)
		}
	}
	for _, key := range []Key{visible, hidden, skeleton} {
		item, err := ReadProjected[map[string]any](t.Context(), client, table, key, nil, ReadOptions{ConsistentRead: true})
		if err != nil || item == nil || item.Key != key || item.Value != nil || item.HasLeaseMetadata != (key == skeleton) {
			t.Fatalf("real key-only read: %+v %v", item, err)
		}
	}
	missing, err := ReadProjected[map[string]any](t.Context(), client, table, Key{Partition: "account", Sort: "absent"}, fields, ReadOptions{ConsistentRead: true})
	if err != nil || missing != nil {
		t.Fatalf("real absent item: %+v %v", missing, err)
	}
	input := &dynamodb.ScanInput{ConsistentRead: aws.Bool(true), Limit: aws.Int32(1)}
	seen := map[Key]bool{}
	for {
		page, err := ScanProjected[map[string]any](t.Context(), client, table, input, fields)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range page.Items {
			if seen[item.Key] || originals[item.Key] == nil || item.HasLeaseMetadata != (item.Key == skeleton) {
				t.Fatalf("invalid real observation: %+v", item)
			}
			seen[item.Key] = true
			if item.Key == visible {
				if item.Value == nil || !reflect.DeepEqual(*item.Value, want) {
					t.Fatal("real scan loaded private fields or lost selected fields")
				}
			} else if item.Value != nil {
				t.Fatal("unselected data decoded as a complete value")
			}
		}
		if len(page.LastEvaluatedKey) == 0 {
			break
		}
		if len(page.LastEvaluatedKey) != 2 || reflect.DeepEqual(page.LastEvaluatedKey, input.ExclusiveStartKey) {
			t.Fatal("lost or repeated compound cursor")
		}
		input.ExclusiveStartKey = page.LastEvaluatedKey
	}
	if len(seen) != len(originals) {
		t.Fatalf("scan omitted rows: %d", len(seen))
	}
	// DynamoDB evaluates Limit before FilterExpression. Preserve the cursor even
	// when the filter rejects the only evaluated row, instead of ending early.
	input = &dynamodb.ScanInput{
		ConsistentRead: aws.Bool(true), Limit: aws.Int32(1), FilterExpression: aws.String("attribute_not_exists(#p0)"),
		ExpressionAttributeNames: map[string]string{"#p0": "pk"},
	}
	pages, scanned := 0, int32(0)
	for {
		page, err := ScanProjected[map[string]any](t.Context(), client, table, input, fields)
		if err != nil || len(page.Items) != 0 || page.Count != 0 {
			t.Fatalf("real filtered scan: %+v %v", page, err)
		}
		pages++
		scanned += page.ScannedCount
		if len(page.LastEvaluatedKey) == 0 {
			break
		}
		if pages > len(originals)+1 {
			t.Fatal("filtered cursor did not advance")
		}
		input.ExclusiveStartKey = page.LastEvaluatedKey
	}
	if scanned != int32(len(originals)) || pages < len(originals) {
		t.Fatalf("real scan lost limit/count/cursor: pages=%d scanned=%d", pages, scanned)
	}
	for key, original := range originals {
		actual, err := getItem(t.Context(), client, table, key)
		if err != nil || !reflect.DeepEqual(actual, original) {
			t.Fatalf("projected discovery changed native row: %v", err)
		}
	}
}
