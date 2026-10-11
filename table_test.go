package dynamolock

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestCompoundEnvelope(t *testing.T) {
	table := Table{Name: "table", PartitionKey: "pk", SortKey: "sk", Attributes: []string{"pending"}}
	key := Key{Partition: "account", Sort: "purchase"}
	type data struct {
		PK      string
		SK      string
		Pending string `dynamodbav:"pending"`
	}
	item, err := MarshalItem(table, key, data{PK: "account", SK: "purchase", Pending: "work"})
	if err != nil {
		t.Fatal(err)
	}
	value, err := UnmarshalItem[data](table, item)
	if err != nil || value == nil || *value != (data{PK: "account", SK: "purchase", Pending: "work"}) {
		t.Fatalf("round trip: %+v %v", value, err)
	}
	if len(item["data"].(*types.AttributeValueMemberM).Value) != 1 || !reflect.DeepEqual(item["pending"], &types.AttributeValueMemberS{Value: "work"}) {
		t.Fatalf("stored key/projection: %+v", item)
	}
	for _, payload := range []any{data{SK: "wrong"}, map[string]any{"SK": 3}, map[string]any{"pk": "other"}} {
		if _, err := MarshalItem(table, key, payload); !errors.Is(err, ErrInvalidPayload) {
			t.Fatalf("accepted conflicting key: %v", err)
		}
	}
	item["future-index"] = &types.AttributeValueMemberS{Value: "future"}
	if _, err := UnmarshalItem[data](table, item); err != nil {
		t.Fatal(err)
	}
	delete(item, "data")
	if _, err := UnmarshalItem[data](table, item); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("flat row accepted: %v", err)
	}
}

func TestSchemaValidation(t *testing.T) {
	client := protocolClient(func(context.Context, wireRequest) (int, any, error) {
		t.Error("invalid request reached provider")
		return 400, rejectedJSON, nil
	})
	for _, table := range []Table{{}, {Name: "table", PartitionKey: "data"}, {Name: "table", SortKey: "ID"}, {Name: "table", Attributes: []string{"expires_at"}}, {Name: "table", Attributes: []string{""}}, {Name: "table", Attributes: []string{"x", "X"}}} {
		if _, err := Read[struct{}](t.Context(), client, table, Key{Partition: "key"}); err == nil {
			t.Fatalf("accepted schema %+v", table)
		}
	}
	for _, key := range []Key{{}, {Partition: "key", Sort: "unexpected"}} {
		if _, err := Read[struct{}](t.Context(), client, Table{Name: "table"}, key); err == nil {
			t.Fatalf("accepted key %+v", key)
		}
	}
	if _, err := Read[struct{}](t.Context(), client, Table{Name: "table", SortKey: "sk"}, Key{Partition: "key"}); err == nil {
		t.Fatal("accepted missing sort value")
	}
}

func TestProjectedPayloadUsesMergedDataAndCopiesConfig(t *testing.T) {
	input := protocolInput()
	input.Table.Attributes = []string{"hidden", "pending"}
	var writes int
	client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
		if w.ExpressionAttributeValues[":expires"] != nil {
			return acquireReply(w, `{"hidden":{"S":"retained"},"pending":{"S":"work"},"value":{"S":"old"}}`)
		}
		if w.ExpressionAttributeValues[":data"] != nil {
			writes++
			if w.ExpressionAttributeNames["#attr0"] != "hidden" || string(w.ExpressionAttributeValues[":attr0"]) != `{"S":"retained"}` {
				t.Errorf("lost hidden projected value: %+v", w)
			}
			if _, exists := w.ExpressionAttributeValues[":attr1"]; exists {
				t.Error("removed sparse field still projected")
			}
			var data struct{ M map[string]json.RawMessage }
			if err := json.Unmarshal(w.ExpressionAttributeValues[":data"], &data); err != nil {
				t.Error(err)
			}
			if _, exists := data.M["pending"]; exists {
				t.Error("removed field remained in payload")
			}
		}
		return 200, map[string]any{}, nil
	})
	type visible struct {
		Value   string `dynamodbav:"value"`
		Pending string `dynamodbav:"pending,omitempty"`
	}
	lease, value, err := Lock[visible](t.Context(), client, input)
	if err != nil {
		t.Fatal(err)
	}
	cleanupLease(t, lease)
	input.Table.Attributes[0] = "corrupted"
	value.Value = "new"
	value.Pending = ""
	if err := lease.Commit(t.Context(), value); err != nil {
		t.Fatal(err)
	}
	if writes != 1 {
		t.Fatalf("writes %d", writes)
	}
}

func TestDeleteOutcome(t *testing.T) {
	for _, mode := range []string{"success", "ambiguous", "lost", "rejected"} {
		t.Run(mode, func(t *testing.T) {
			deletes := 0
			client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
				if w.ExpressionAttributeValues[":expires"] != nil {
					return acquireReply(w, `{"value":{"S":"old"}}`)
				}
				if w.Target == "DynamoDB_20120810.DeleteItem" {
					deletes++
					switch mode {
					case "ambiguous":
						return 500, serverErrorJSON, nil
					case "lost":
						return 400, conditionalJSON, nil
					case "rejected":
						return 400, rejectedJSON, nil
					}
				}
				return 200, map[string]any{}, nil
			})
			lease, _, err := Lock[keyedData](t.Context(), client, protocolInput())
			if err != nil {
				t.Fatal(err)
			}
			cleanupLease(t, lease)
			err = lease.Delete(t.Context())
			switch mode {
			case "success":
				if err != nil {
					t.Fatal(err)
				}
				await(t, lease.done)
				if !errors.Is(lease.Delete(t.Context()), ErrReleased) {
					t.Error("completed handle reused")
				}
			case "ambiguous":
				if !errors.Is(err, ErrOutcomeUnknown) {
					t.Fatal(err)
				}
				if !errors.Is(lease.Update(t.Context(), &keyedData{}), ErrLeaseLost) {
					t.Error("uncertain handle reused")
				}
			case "lost":
				if !errors.Is(err, ErrLeaseLost) {
					t.Fatal(err)
				}
			case "rejected":
				if err == nil || lease.liveError() != nil {
					t.Fatalf("definitive rejection lost lease: %v", err)
				}
			}
			if deletes != 1 {
				t.Fatalf("delete attempts %d", deletes)
			}
		})
	}
}

func TestDiscoveryPages(t *testing.T) {
	table := Table{Name: "table", PartitionKey: "pk", SortKey: "sk"}
	cursor := map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "p"}, "sk": &types.AttributeValueMemberS{Value: "s"}, "pending": &types.AttributeValueMemberS{Value: "work"}}
	calls := 0
	client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
		calls++
		if w.TableName != "table" {
			t.Error("wrong table")
		}
		switch calls {
		case 1:
			if w.IndexName != "pending-work" || w.Limit != 1 || w.KeyConditionExpression != "#pending = :pending" {
				t.Errorf("lost query options: %+v", w)
			}
			return 200, `{"Items":[],"Count":0,"ScannedCount":1,"LastEvaluatedKey":{"pk":{"S":"p"},"sk":{"S":"s"},"pending":{"S":"work"}}}`, nil
		case 2:
			if len(w.ExclusiveStartKey) != 3 {
				t.Error("lost index cursor")
			}
			return 200, `{"Items":[{"pk":{"S":"p"},"sk":{"S":"s"},"pending":{"S":"work"}}],"Count":1,"ScannedCount":1}`, nil
		case 3:
			return 200, `{"Items":[{"pk":{"S":"p"},"sk":{"S":"s"},"data":{"M":{"value":{"S":"found"}}}}],"Count":1}`, nil
		case 4:
			return 200, `{"Count":7,"ScannedCount":9}`, nil
		default:
			t.Error("unexpected hidden request")
			return 400, rejectedJSON, nil
		}
	})
	in := &dynamodb.QueryInput{IndexName: aws.String("pending-work"), Limit: aws.Int32(1), KeyConditionExpression: aws.String("#pending = :pending"), ExpressionAttributeNames: map[string]string{"#pending": "pending"}, ExpressionAttributeValues: map[string]types.AttributeValue{":pending": &types.AttributeValueMemberS{Value: "work"}}}
	first, err := Query[keyedData](t.Context(), client, table, in)
	if err != nil || len(first.Items) != 0 || !reflect.DeepEqual(first.LastEvaluatedKey, cursor) || first.ScannedCount != 1 {
		t.Fatalf("empty page: %+v %v", first, err)
	}
	if in.TableName != nil {
		t.Error("mutated input")
	}
	in.ExclusiveStartKey = first.LastEvaluatedKey
	second, err := Query[keyedData](t.Context(), client, table, in)
	if err != nil || len(second.Items) != 1 || second.Items[0].Key != (Key{Partition: "p", Sort: "s"}) || second.Items[0].Value != nil {
		t.Fatalf("index discovery: %+v %v", second, err)
	}
	third, err := Scan[keyedData](t.Context(), client, table, &dynamodb.ScanInput{})
	if err != nil || len(third.Items) != 1 || third.Items[0].Value.Value != "found" {
		t.Fatalf("scan: %+v %v", third, err)
	}
	fourth, err := Scan[keyedData](t.Context(), client, table, &dynamodb.ScanInput{Select: types.SelectCount})
	if err != nil || fourth.Count != 7 || fourth.ScannedCount != 9 || len(fourth.Items) != 0 {
		t.Fatalf("count: %+v %v", fourth, err)
	}
}

func TestDiscoveryRejectsPartialAndMalformedBase(t *testing.T) {
	calls := 0
	client := protocolClient(func(context.Context, wireRequest) (int, any, error) {
		calls++
		return 200, `{"Items":[{"id":{"S":"key"},"legacy":{"S":"body"}}]}`, nil
	})
	table := Table{Name: "table"}
	for _, in := range []*dynamodb.ScanInput{{ProjectionExpression: aws.String("id")}, {AttributesToGet: []string{"id"}}, {Select: types.SelectSpecificAttributes}, {TableName: aws.String("other")}} {
		if _, err := Scan[keyedData](t.Context(), client, table, in); err == nil {
			t.Fatal("accepted partial/foreign input")
		}
	}
	if calls != 0 {
		t.Fatal("validation reached service")
	}
	if _, err := Scan[keyedData](t.Context(), client, table, &dynamodb.ScanInput{}); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("flat base accepted: %v", err)
	}
	if _, err := Query[keyedData](t.Context(), client, table, &dynamodb.QueryInput{}); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("flat base query accepted: %v", err)
	}
}
