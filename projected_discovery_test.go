package dynamolock

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Resolve the SDK's actual request aliases, not names chosen by the library.
func projectedWirePaths(t *testing.T, w wireRequest) [][]string {
	t.Helper()
	var paths [][]string
	for path := range strings.SplitSeq(w.ProjectionExpression, ",") {
		var segments []string
		for alias := range strings.SplitSeq(strings.TrimSpace(path), ".") {
			name, ok := w.ExpressionAttributeNames[alias]
			if !ok {
				t.Fatalf("projection uses an unbound alias %q", alias)
			}
			segments = append(segments, name)
		}
		paths = append(paths, segments)
	}
	return paths
}

func TestReadProjectedWire(t *testing.T) {
	table := Table{Name: "table", PartitionKey: "pk", SortKey: "sk"}
	key := Key{Partition: "account", Sort: "job"}
	fields := [][]string{{"public"}, {"nested", "a.b[0]"}}
	calls := 0
	client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
		calls++
		if w.Target != "DynamoDB_20120810.GetItem" || !w.ConsistentRead || w.TableName != table.Name || len(w.Key) != 2 {
			t.Fatalf("wrong read request: %+v", w)
		}
		want := [][]string{{"pk"}, {"sk"}, {"owner_token"}, {"expires_at"}, {"data", "public"}, {"data", "nested", "a.b[0]"}}
		if got := projectedWirePaths(t, w); !reflect.DeepEqual(got, want) {
			t.Fatalf("loaded wrong fields: %v", got)
		}
		return 200, `{"Item":{"pk":{"S":"account"},"sk":{"S":"job"},"owner_token":{"S":"expired"},"expires_at":{"N":"1"},"data":{"M":{"public":{"S":"visible"},"nested":{"M":{"a.b[0]":{"S":"exact"}}}}}}}`, nil
	})
	item, err := ReadProjected[map[string]any](t.Context(), client, table, key, fields)
	if err != nil || item == nil || item.Value == nil || item.Key != key || !item.HasLeaseMetadata {
		t.Fatalf("projected read: %+v %v", item, err)
	}
	want := map[string]any{"pk": "account", "sk": "job", "public": "visible", "nested": map[string]any{"a.b[0]": "exact"}}
	if !reflect.DeepEqual(*item.Value, want) || calls != 1 {
		t.Fatalf("incomplete result or hidden request: %#v calls=%d", *item.Value, calls)
	}
}

func TestReadProjectedAbsentSelectedAndMalformed(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		fields     [][]string
		absent     bool
		value      bool
		leased     bool
		invalid    bool
	}{
		{name: "absent", body: `{}`, absent: true},
		{name: "keys only", body: `{"id":{"S":"key"}}`},
		{name: "no selected field", fields: [][]string{{"missing"}}, body: `{"id":{"S":"key"}}`},
		{name: "empty selected map", fields: [][]string{{"nested"}}, body: `{"id":{"S":"key"},"data":{"M":{"nested":{"M":{}}}}}`, value: true},
		{name: "expired skeleton", body: `{"id":{"S":"key"},"owner_token":{"S":"expired"},"expires_at":{"N":"1"}}`, leased: true},
		{name: "owner only", body: `{"id":{"S":"key"},"owner_token":{"S":"owner"}}`, invalid: true},
		{name: "expiry only", body: `{"id":{"S":"key"},"expires_at":{"N":"1"}}`, invalid: true},
		{name: "bad expiry", body: `{"id":{"S":"key"},"owner_token":{"S":"owner"},"expires_at":{"N":"0"}}`, invalid: true},
		{name: "bad data", fields: [][]string{{"public"}}, body: `{"id":{"S":"key"},"data":{"S":"not a map"}}`, invalid: true},
		{name: "foreign key", body: `{"id":{"S":"foreign"}}`, invalid: true},
		{name: "payload key", fields: [][]string{{"public"}}, body: `{"id":{"S":"key"},"data":{"M":{"ID":{"S":"shadow"}}}}`, invalid: true},
		{name: "duplicate decode aliases", fields: [][]string{{"public"}, {"PUBLIC"}}, body: `{"id":{"S":"key"},"data":{"M":{"public":{"S":"one"},"PUBLIC":{"S":"two"}}}}`, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
				calls++
				if len(tc.fields) == 0 && !reflect.DeepEqual(projectedWirePaths(t, w), [][]string{{"id"}, {"owner_token"}, {"expires_at"}}) {
					t.Fatal("key-only read loaded data")
				}
				return 200, `{"Item":` + tc.body + `}`, nil
			})
			type public struct {
				Public string `dynamodbav:"public"`
			}
			item, err := ReadProjected[public](t.Context(), client, Table{Name: "table"}, Key{Partition: "key"}, tc.fields)
			if tc.invalid {
				if !errors.Is(err, ErrInvalidRecord) {
					t.Fatalf("malformed projected item accepted: %+v %v", item, err)
				}
			} else if err != nil || (item == nil) != tc.absent || item != nil && ((item.Value != nil) != tc.value || item.HasLeaseMetadata != tc.leased) {
				t.Fatalf("wrong observation: %+v %v", item, err)
			}
			if calls != 1 {
				t.Fatalf("unexpected requests: %d", calls)
			}
		})
	}
}

func TestScanProjectedOptionsAndCursor(t *testing.T) {
	names := map[string]string{"#p0": "filter", "#p3": "other"}
	originalNames := maps.Clone(names)
	fields := [][]string{{"public"}}
	input := &dynamodb.ScanInput{
		ConsistentRead: aws.Bool(true), FilterExpression: aws.String("#p0 = :value AND attribute_exists(#p3)"),
		ExpressionAttributeNames: names, ExpressionAttributeValues: map[string]types.AttributeValue{":value": &types.AttributeValueMemberS{Value: "yes"}},
		Limit: aws.Int32(1), Segment: aws.Int32(2), TotalSegments: aws.Int32(3),
	}
	cursor := map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: "next"}}
	calls := 0
	client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
		calls++
		if w.Target != "DynamoDB_20120810.Scan" || !w.ConsistentRead || w.Limit != 1 || w.Segment != 2 || w.TotalSegments != 3 || w.FilterExpression != *input.FilterExpression || len(w.ExpressionAttributeValues) != 1 {
			t.Fatalf("lost scan options: %+v", w)
		}
		for alias, name := range originalNames {
			if w.ExpressionAttributeNames[alias] != name {
				t.Fatal("clobbered filter aliases")
			}
		}
		if got := projectedWirePaths(t, w); !reflect.DeepEqual(got, [][]string{{"id"}, {"owner_token"}, {"expires_at"}, {"data", "public"}}) {
			t.Fatalf("overbroad scan: %v", got)
		}
		if calls == 1 {
			return 200, `{"Items":[],"Count":0,"ScannedCount":1,"LastEvaluatedKey":{"id":{"S":"next"}}}`, nil
		}
		if len(w.ExclusiveStartKey) != 1 || string(w.ExclusiveStartKey["id"]) != `{"S":"next"}` {
			t.Fatal("lost service cursor")
		}
		return 200, `{"Items":[{"id":{"S":"found"},"data":{"M":{"public":{"S":"visible"}}}}],"Count":1,"ScannedCount":1}`, nil
	})
	first, err := ScanProjected[map[string]any](t.Context(), client, Table{Name: "table"}, input, fields)
	if err != nil || len(first.Items) != 0 || first.Count != 0 || first.ScannedCount != 1 || !reflect.DeepEqual(first.LastEvaluatedKey, cursor) {
		t.Fatalf("empty projected page: %+v %v", first, err)
	}
	if input.TableName != nil || input.ProjectionExpression != nil || !maps.Equal(names, originalNames) || !slices.Equal(fields[0], []string{"public"}) {
		t.Fatal("mutated caller input")
	}
	input.ExclusiveStartKey = first.LastEvaluatedKey
	second, err := ScanProjected[map[string]any](t.Context(), client, Table{Name: "table"}, input, fields)
	if err != nil || len(second.Items) != 1 || second.Items[0].Value == nil || (*second.Items[0].Value)["public"] != "visible" || second.Items[0].HasLeaseMetadata || second.Items[0].Key.Partition != "found" || calls != 2 {
		t.Fatalf("projected page: %+v %v calls=%d", second, err, calls)
	}
}

func TestProjectedValidationBeforeRequest(t *testing.T) {
	client := protocolClient(func(context.Context, wireRequest) (int, any, error) {
		t.Fatal("invalid projection reached service")
		return 400, rejectedJSON, nil
	})
	table := Table{Name: "table"}
	for _, fields := range [][][]string{{{}}, {{""}}, {{"nested", ""}}, {{"public"}, {"public"}}, {{"nested"}, {"nested", "secret"}}, {{"nested", "public"}, {"nested"}}, {{"ID"}}} {
		if _, err := ReadProjected[Data](t.Context(), client, table, Key{Partition: "key"}, fields); err == nil {
			t.Errorf("accepted invalid read paths: %v", fields)
		}
		if _, err := ScanProjected[Data](t.Context(), client, table, &dynamodb.ScanInput{}, fields); err == nil {
			t.Errorf("accepted invalid scan paths: %v", fields)
		}
	}
	for _, input := range []*dynamodb.ScanInput{
		nil, {ProjectionExpression: aws.String("id")}, {AttributesToGet: []string{"id"}},
		{Select: types.SelectSpecificAttributes}, {Select: types.SelectCount}, {Select: types.SelectAllAttributes},
		{IndexName: aws.String("keys-only")}, {TableName: aws.String("other")},
	} {
		if _, err := ScanProjected[Data](t.Context(), client, table, input, nil); err == nil {
			t.Errorf("accepted incompatible scan input: %+v", input)
		}
	}
}
