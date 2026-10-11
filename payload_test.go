package dynamolock

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type aliasChild struct {
	Value string `dynamodbav:"value,omitempty"`
}

type aliasPayload struct {
	Value   string                `dynamodbav:"value,omitempty"`
	Nested  aliasChild            `dynamodbav:"nested"`
	Entries map[string]aliasChild `dynamodbav:"entries"`
	Dynamic any                   `dynamodbav:"dynamic"`
	Members []aliasChild          `dynamodbav:"members"`
}

func TestProtocolPayloadAliases(t *testing.T) {
	original := map[string]any{
		"Value": "before", "future": "retain",
		"Nested": map[string]any{"VALUE": "nested before", "future": "nested future"},
		"Entries": map[string]any{
			"Entry": map[string]any{"Value": "upper before", "future": "upper future"},
			"entry": map[string]any{"Value": "lower before", "future": "lower future"},
		},
		"Dynamic": map[string]any{"Value": "upper", "value": "lower"},
		"Members": []any{map[string]any{"VALUE": "member before"}},
	}
	item, err := MarshalItem(Table{Name: "fixture", PartitionKey: "id"}, Key{Partition: "key"}, original)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := attributevalue.MarshalMapJSON(item["data"].(*types.AttributeValueMemberM).Value)
	if err != nil {
		t.Fatal(err)
	}
	var writes []types.AttributeValue
	client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
		if w.ReturnValues == "ALL_NEW" {
			return acquireReply(w, string(encoded))
		}
		if body := w.ExpressionAttributeValues[":data"]; body != nil {
			payload, err := attributevalue.UnmarshalJSON(body)
			if err != nil {
				return 0, nil, err
			}
			writes = append(writes, payload)
		}
		return 200, `{}`, nil
	})
	lease, data, err := Lock[aliasPayload](t.Context(), client, protocolInput())
	if err != nil {
		t.Fatal(err)
	}
	cleanupLease(t, lease)
	data.Value = "after"
	data.Nested.Value = "nested after"
	data.Entries["Entry"] = aliasChild{Value: "upper after"}
	data.Members[0].Value = "member after"
	if err := lease.Update(t.Context(), data); err != nil {
		t.Fatal(err)
	}
	original["Value"] = "after"
	original["Nested"].(map[string]any)["VALUE"] = "nested after"
	original["Entries"].(map[string]any)["Entry"].(map[string]any)["Value"] = "upper after"
	// List replacement is atomic, so changed complete elements use their encoder's names.
	original["Members"] = []any{map[string]any{"value": "member after"}}
	assertPayload := func() {
		t.Helper()
		want, err := MarshalItem(Table{Name: "fixture", PartitionKey: "id"}, Key{Partition: "key"}, original)
		if err != nil {
			t.Fatal(err)
		}
		if len(writes) == 0 {
			t.Fatal("payload write was not submitted")
		}
		if !reflect.DeepEqual(writes[len(writes)-1], want["data"]) {
			actual, _ := attributevalue.MarshalJSON(writes[len(writes)-1])
			t.Fatalf("aliases or unknown fields changed: %s, want %#v", actual, original)
		}
	}
	assertPayload()
	data.Value, data.Nested.Value = "", ""
	data.Entries["Entry"] = aliasChild{}
	delete(data.Dynamic.(map[string]any), "Value")
	if err := lease.Commit(t.Context(), data); err != nil {
		t.Fatal(err)
	}
	delete(original, "Value")
	delete(original["Nested"].(map[string]any), "VALUE")
	delete(original["Entries"].(map[string]any)["Entry"].(map[string]any), "Value")
	delete(original["Dynamic"].(map[string]any), "Value")
	assertPayload()
}

func TestProtocolPayloadCaseDistinctFields(t *testing.T) {
	type dataType struct {
		Lower string            `dynamodbav:"value,omitempty"`
		Upper string            `dynamodbav:"Value,omitempty"`
		Map   map[string]string `dynamodbav:"map"`
	}
	var written json.RawMessage
	client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
		if w.ReturnValues == "ALL_NEW" {
			return acquireReply(w, `{"value":{"S":"lower"},"Value":{"S":"upper"},"map":{"M":{"value":{"S":"lower"},"Value":{"S":"upper"}}}}`)
		}
		if w.ExpressionAttributeValues[":data"] != nil {
			written = w.ExpressionAttributeValues[":data"]
		}
		return 200, `{}`, nil
	})
	lease, data, err := Lock[dataType](t.Context(), client, protocolInput())
	if err != nil {
		t.Fatal(err)
	}
	cleanupLease(t, lease)
	data.Lower = ""
	data.Upper = "changed"
	delete(data.Map, "Value")
	if err := lease.Commit(t.Context(), data); err != nil {
		t.Fatal(err)
	}
	payload, err := attributevalue.UnmarshalJSON(written)
	if err != nil {
		t.Fatal(err)
	}
	want, err := MarshalItem(Table{Name: "fixture", PartitionKey: "id"}, Key{Partition: "key"}, data)
	if err != nil || !reflect.DeepEqual(payload, want["data"]) {
		t.Fatalf("case-distinct fields merged: %s, %v", written, err)
	}
}

func TestProtocolPayloadAliasCollision(t *testing.T) {
	for _, payload := range []string{
		`{"value":{"S":"canonical"},"Value":{"S":"alias"}}`,
		`{"nested":{"M":{"value":{"S":"canonical"},"VALUE":{"S":"alias"}}}}`,
		`{"entries":{"M":{"key":{"M":{"value":{"S":"canonical"},"Value":{"S":"alias"}}}}}}`,
		`{"members":{"L":[{"M":{"value":{"S":"canonical"},"Value":{"S":"alias"}}}]}}`,
	} {
		t.Run(payload, func(t *testing.T) {
			releases := 0
			client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
				if w.ReturnValues == "ALL_NEW" {
					return acquireReply(w, payload)
				}
				if w.UpdateExpression != "REMOVE #owner, #expires" {
					t.Errorf("collision cleanup changed payload: %#v", w)
				}
				releases++
				return 200, `{}`, nil
			})
			lease, _, err := Lock[aliasPayload](t.Context(), client, protocolInput())
			cleanupLease(t, lease)
			if !errors.Is(err, ErrInvalidRecord) || lease != nil || releases != 1 {
				t.Fatalf("ambiguous aliases accepted or ownership abandoned: lease = %v, releases = %d, err = %v", lease, releases, err)
			}
		})
	}
}

func TestProtocolEmbeddedPayloadAliases(t *testing.T) {
	type untagged struct {
		Value string
	}
	type Tagged struct {
		Preferred string `dynamodbav:"Value,omitempty"`
	}
	type other struct {
		Value string
	}
	type ambiguous struct {
		untagged
		other
		Visible string `dynamodbav:"visible,omitempty"`
	}
	type dataType struct {
		*Tagged
		untagged
		Pointer   *aliasChild `dynamodbav:"pointer"`
		Ambiguous ambiguous   `dynamodbav:"ambiguous"`
		Ignored   string      `dynamodbav:"-"`
	}
	var payload types.AttributeValue
	client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
		if w.ReturnValues == "ALL_NEW" {
			return acquireReply(w, `{"VALUE":{"S":"tagged"},"POINTER":{"M":{"VALUE":{"S":"pointed"},"future":{"S":"retain"}}},"AMBIGUOUS":{"M":{"VALUE":{"S":"hidden"},"VISIBLE":{"S":"visible"}}},"Ignored":{"S":"hidden"}}`)
		}
		if w.ExpressionAttributeValues[":data"] != nil {
			var err error
			payload, err = attributevalue.UnmarshalJSON(w.ExpressionAttributeValues[":data"])
			if err != nil {
				return 0, nil, err
			}
		}
		return 200, `{}`, nil
	})
	lease, data, err := Lock[dataType](t.Context(), client, protocolInput())
	if err != nil {
		t.Fatal(err)
	}
	cleanupLease(t, lease)
	if data.Tagged == nil || data.Preferred != "tagged" || data.Value != "" || data.Pointer.Value != "pointed" || data.Ambiguous.Visible != "visible" {
		t.Fatalf("unexpected SDK projection: %#v", data)
	}
	data.Preferred, data.Pointer.Value, data.Ambiguous.Visible = "", "changed", ""
	if err := lease.Commit(t.Context(), data); err != nil {
		t.Fatal(err)
	}
	want, err := MarshalItem(Table{Name: "fixture", PartitionKey: "id"}, Key{Partition: "key"}, map[string]any{
		"POINTER":   map[string]any{"VALUE": "changed", "future": "retain"},
		"AMBIGUOUS": map[string]any{"VALUE": "hidden"},
		"Ignored":   "hidden",
	})
	if err != nil || !reflect.DeepEqual(payload, want["data"]) {
		encoded, _ := attributevalue.MarshalJSON(payload)
		t.Fatalf("embedded field selection changed: %s, %v", encoded, err)
	}
}

type exactCodec map[string]string

func (value *exactCodec) MarshalDynamoDBAttributeValue() (types.AttributeValue, error) {
	return attributevalue.Marshal(map[string]string(*value))
}

func (value *exactCodec) UnmarshalDynamoDBAttributeValue(av types.AttributeValue) error {
	var decoded map[string]string
	if err := attributevalue.Unmarshal(av, &decoded); err != nil {
		return err
	}
	*value = decoded
	return nil
}

func TestProtocolCustomCodecKeepsExactKeys(t *testing.T) {
	type dataType struct {
		Custom exactCodec `dynamodbav:"custom"`
	}
	var payload types.AttributeValue
	client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
		if w.ReturnValues == "ALL_NEW" {
			return acquireReply(w, `{"CUSTOM":{"M":{"Value":{"S":"upper"},"value":{"S":"lower"}}}}`)
		}
		if w.ExpressionAttributeValues[":data"] != nil {
			var err error
			payload, err = attributevalue.UnmarshalJSON(w.ExpressionAttributeValues[":data"])
			if err != nil {
				return 0, nil, err
			}
		}
		return 200, `{}`, nil
	})
	lease, data, err := Lock[dataType](t.Context(), client, protocolInput())
	if err != nil {
		t.Fatal(err)
	}
	cleanupLease(t, lease)
	delete(data.Custom, "Value")
	data.Custom["value"] = "changed"
	if err := lease.Commit(t.Context(), data); err != nil {
		t.Fatal(err)
	}
	want, err := MarshalItem(Table{Name: "fixture", PartitionKey: "id"}, Key{Partition: "key"}, map[string]any{"CUSTOM": map[string]any{"value": "changed"}})
	if err != nil || !reflect.DeepEqual(payload, want["data"]) {
		encoded, _ := attributevalue.MarshalJSON(payload)
		t.Fatalf("custom codec keys folded: %s, %v", encoded, err)
	}
}
