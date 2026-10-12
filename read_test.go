package dynamolock

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func TestEventualReadOutcomes(t *testing.T) {
	for _, operation := range []struct {
		name string
		call func(context.Context, *dynamodb.Client) (*keyedData, error)
	}{
		{"read", func(ctx context.Context, client *dynamodb.Client) (*keyedData, error) {
			return Read[keyedData](ctx, client, Table{Name: "table"}, Key{Partition: "key"}, ReadOptions{})
		}},
		{"projected read", func(ctx context.Context, client *dynamodb.Client) (*keyedData, error) {
			item, err := ReadProjected[keyedData](ctx, client, Table{Name: "table"}, Key{Partition: "key"}, [][]string{{"value"}}, ReadOptions{})
			if err != nil || item == nil {
				return nil, err
			}
			return item.Value, nil
		}},
	} {
		for _, tc := range []struct {
			name    string
			body    string
			value   *keyedData
			invalid bool
		}{
			{name: "absent", body: `{}`},
			{name: "no payload", body: `{"Item":{"id":{"S":"key"}}}`},
			{name: "old payload", body: `{"Item":{"id":{"S":"key"},"data":{"M":{"value":{"S":"old"}}}}}`, value: &keyedData{ID: "key", Value: "old"}},
			{name: "malformed", body: `{"Item":{"id":{"S":"key"},"owner_token":{"S":"owner"}}}`, invalid: true},
		} {
			t.Run(operation.name+"/"+tc.name, func(t *testing.T) {
				calls := 0
				client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
					calls++
					if w.Target != "DynamoDB_20120810.GetItem" || w.ConsistentRead {
						t.Fatalf("eventual read changed consistency: %+v", w)
					}
					return 200, tc.body, nil
				})
				value, err := operation.call(t.Context(), client)
				if tc.invalid {
					if !errors.Is(err, ErrInvalidRecord) {
						t.Fatalf("malformed record accepted: %v", err)
					}
				} else if err != nil || !reflect.DeepEqual(value, tc.value) {
					t.Fatalf("observation changed: %+v %v", value, err)
				}
				if calls != 1 {
					t.Fatalf("read silently retried an observation: %d calls", calls)
				}
			})
		}
	}
}
