package dynamolock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestQueryIndexPages(t *testing.T) {
	for _, consistent := range []*bool{nil, aws.Bool(false), aws.Bool(true)} {
		t.Run(fmt.Sprintf("consistent=%v/default=%v", aws.ToBool(consistent), consistent == nil), func(t *testing.T) {
			table := Table{Name: "table", PartitionKey: "pk", SortKey: "sk", Attributes: []string{"order", "missing"}}
			input := &dynamodb.QueryInput{
				IndexName: aws.String("waiting"), ConsistentRead: consistent, ScanIndexForward: aws.Bool(false), Limit: aws.Int32(1),
				KeyConditionExpression: aws.String("#order = :order"), FilterExpression: aws.String("attribute_exists(#pk)"),
				ExpressionAttributeNames:  map[string]string{"#order": "order", "#pk": "pk"},
				ExpressionAttributeValues: map[string]types.AttributeValue{":order": &types.AttributeValueMemberN{Value: "9007199254740993"}},
			}
			cursor := map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "account"}, "sk": &types.AttributeValueMemberS{Value: "job"}, "order": &types.AttributeValueMemberN{Value: "9007199254740993"}}
			calls := 0
			client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
				calls++
				if w.Target != "DynamoDB_20120810.Query" || w.TableName != table.Name || w.IndexName != "waiting" || w.ConsistentRead != aws.ToBool(consistent) || w.ScanIndexForward == nil || *w.ScanIndexForward || w.Limit != 1 || w.KeyConditionExpression != *input.KeyConditionExpression || w.FilterExpression != *input.FilterExpression || !reflect.DeepEqual(w.ExpressionAttributeNames, input.ExpressionAttributeNames) || string(w.ExpressionAttributeValues[":order"]) != `{"N":"9007199254740993"}` {
					t.Fatalf("lost query options: %+v", w)
				}
				if w.ProjectionExpression != "" || w.Select != string(types.SelectAllProjectedAttributes) {
					t.Fatalf("query can fetch parent data: %+v", w)
				}
				switch calls {
				case 1:
					return 500, serverErrorJSON, nil
				case 2:
					return 200, `{"Items":[],"Count":0,"ScannedCount":1,"LastEvaluatedKey":{"pk":{"S":"account"},"sk":{"S":"job"},"order":{"N":"9007199254740993"}}}`, nil
				case 3:
					if len(w.ExclusiveStartKey) != 3 || string(w.ExclusiveStartKey["order"]) != `{"N":"9007199254740993"}` {
						t.Fatal("lost full index cursor")
					}
					return 200, `{"Items":[{"pk":{"S":"account"},"sk":{"S":"job"},"order":{"N":"9007199254740993"},"unknown":{"S":"ignored"},"owner_token":{"S":"ignored"},"data":{"M":{"secret":{"S":"ignored"},"order":{"N":"1"}}}}],"Count":1,"ScannedCount":1}`, nil
				default:
					t.Fatal("unexpected hidden request")
					return 400, rejectedJSON, nil
				}
			})
			query := func() IndexPage[map[string]any] {
				t.Helper()
				before, err := json.Marshal(input)
				if err != nil {
					t.Fatal(err)
				}
				page, err := QueryIndex[map[string]any](t.Context(), client, table, input)
				if err != nil {
					t.Fatal(err)
				}
				after, err := json.Marshal(input)
				if err != nil || string(before) != string(after) {
					t.Fatalf("mutated caller input: %s -> %s: %v", before, after, err)
				}
				return page
			}
			first := query()
			if len(first.Items) != 0 || first.Count != 0 || first.ScannedCount != 1 || !reflect.DeepEqual(first.LastEvaluatedKey, cursor) {
				t.Fatalf("empty page: %+v", first)
			}
			input.ExclusiveStartKey = first.LastEvaluatedKey
			input.Select = types.SelectAllProjectedAttributes
			second := query()
			if len(second.Items) != 1 || second.Count != 1 || second.ScannedCount != 1 || len(second.LastEvaluatedKey) != 0 || calls != 3 {
				t.Fatalf("second page: %+v calls=%d", second, calls)
			}
			want := map[string]any{"pk": "account", "sk": "job", "order": attributevalue.Number("9007199254740993")}
			if second.Items[0].Key != (Key{Partition: "account", Sort: "job"}) || !reflect.DeepEqual(second.Items[0].Value, want) {
				t.Fatalf("wrong observation: %+v", second.Items[0])
			}
		})
	}
}

func TestQueryIndexDecode(t *testing.T) {
	type entry struct {
		ID      string
		Order   int64      `dynamodbav:"order"`
		Nested  aliasChild `dynamodbav:"nested"`
		Custom  exactCodec `dynamodbav:"custom"`
		Missing string     `dynamodbav:"missing"`
	}
	for _, tc := range []struct {
		name, body string
		want       entry
		invalid    bool
	}{
		{name: "keys only", body: `{"id":{"S":"key"}}`, want: entry{ID: "key"}},
		{name: "typed copies", body: `{"id":{"S":"key"},"order":{"N":"9007199254740993"},"nested":{"M":{"VALUE":{"S":"nested"}}},"custom":{"M":{"Value":{"S":"upper"},"value":{"S":"lower"}}}}`, want: entry{ID: "key", Order: 9007199254740993, Nested: aliasChild{Value: "nested"}, Custom: exactCodec{"Value": "upper", "value": "lower"}}},
		{name: "exact configured copies", body: `{"id":{"S":"key"},"ORDER":{"N":"7"}}`, want: entry{ID: "key"}},
		{name: "ignore envelope and unknown fields", body: `{"id":{"S":"key"},"order":{"N":"7"},"data":{"S":"not a document"},"owner_token":{"N":"8"},"expires_at":{"S":"invalid"},"ID":{"S":"shadow"}}`, want: entry{ID: "key", Order: 7}},
		{name: "empty entry", body: `{}`, invalid: true},
		{name: "empty key", body: `{"id":{"S":""}}`, invalid: true},
		{name: "wrong key type", body: `{"id":{"N":"1"}}`, invalid: true},
		{name: "duplicate nested aliases", body: `{"id":{"S":"key"},"nested":{"M":{"Value":{"S":"one"},"value":{"S":"two"}}}}`, invalid: true},
		{name: "wrong copy type", body: `{"id":{"S":"key"},"order":{"M":{}}}`, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			client := protocolClient(func(context.Context, wireRequest) (int, any, error) {
				calls++
				return 200, `{"Items":[` + tc.body + `],"Count":1}`, nil
			})
			page, err := QueryIndex[entry](t.Context(), client, Table{Name: "table", Attributes: []string{"order", "nested", "custom", "missing"}}, &dynamodb.QueryInput{IndexName: aws.String("waiting")})
			if tc.invalid {
				if err == nil || len(page.Items) != 0 {
					t.Fatalf("accepted malformed entry: %+v %v", page, err)
				}
			} else if err != nil || len(page.Items) != 1 || !reflect.DeepEqual(page.Items[0].Value, tc.want) {
				t.Fatalf("wrong observation: %+v %v", page, err)
			}
			if calls != 1 {
				t.Fatalf("unexpected requests: %d", calls)
			}
		})
	}
}

func TestQueryIndexCount(t *testing.T) {
	client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
		if w.Select != string(types.SelectCount) {
			t.Fatalf("lost count selection: %+v", w)
		}
		return 200, `{"Count":3,"ScannedCount":5,"LastEvaluatedKey":{"id":{"S":"key"},"order":{"N":"7"}}}`, nil
	})
	page, err := QueryIndex[struct{}](t.Context(), client, Table{Name: "table"}, &dynamodb.QueryInput{IndexName: aws.String("waiting"), Select: types.SelectCount})
	if err != nil || len(page.Items) != 0 || page.Count != 3 || page.ScannedCount != 5 || len(page.LastEvaluatedKey) != 2 {
		t.Fatalf("count page: %+v %v", page, err)
	}
}

func TestQueryIndexValidation(t *testing.T) {
	client := protocolClient(func(context.Context, wireRequest) (int, any, error) {
		t.Fatal("invalid index request reached service")
		return 400, rejectedJSON, nil
	})
	table := Table{Name: "table"}
	for _, in := range []*dynamodb.QueryInput{
		nil, {}, {IndexName: aws.String("")},
		{IndexName: aws.String("waiting"), TableName: aws.String("other")},
		{IndexName: aws.String("waiting"), ProjectionExpression: aws.String("id")},
		{IndexName: aws.String("waiting"), AttributesToGet: []string{"id"}},
		{IndexName: aws.String("waiting"), Select: types.SelectSpecificAttributes},
		{IndexName: aws.String("waiting"), Select: types.SelectAllAttributes},
		{IndexName: aws.String("waiting"), Select: types.Select("invalid")},
	} {
		if _, err := QueryIndex[struct{}](t.Context(), client, table, in); err == nil {
			t.Fatalf("accepted invalid input: %+v", in)
		}
	}
	in := &dynamodb.QueryInput{IndexName: aws.String("waiting")}
	if _, err := QueryIndex[*Data](t.Context(), client, table, in); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("accepted pointer type: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := QueryIndex[struct{}](ctx, client, table, in); !errors.Is(err, context.Canceled) {
		t.Fatalf("accepted canceled context: %v", err)
	}
}

func TestQueryIndexRetryLimits(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		cancel bool
		calls  int
	}{
		{name: "transient", status: 500, body: serverErrorJSON, calls: maxAttempts},
		{name: "permanent", status: 400, body: rejectedJSON, calls: 1},
		{name: "canceled backoff", status: 500, body: serverErrorJSON, cancel: true, calls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls := 0
			client := protocolClient(func(context.Context, wireRequest) (int, any, error) {
				calls++
				if tc.cancel {
					cancel()
				}
				return tc.status, tc.body, nil
			})
			_, err := QueryIndex[struct{}](ctx, client, Table{Name: "table"}, &dynamodb.QueryInput{IndexName: aws.String("waiting")})
			if err == nil || calls != tc.calls || tc.cancel && !errors.Is(err, context.Canceled) {
				t.Fatalf("retry result: calls=%d err=%v", calls, err)
			}
		})
	}
}
