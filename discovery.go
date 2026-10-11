package dynamolock

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Item is one discovered primary key and its document. Value is nil when no data
// exists, including a keys-only index projection. Read the key for current state;
// discovery neither acquires ownership nor proves a document is absent.
type Item[T any] struct {
	Key   Key
	Value *T
}

// Page preserves the full service cursor, including index keys. A page can be
// empty and still have a cursor. Count and ScannedCount also support SelectCount.
type Page[T any] struct {
	Items            []Item[T]
	LastEvaluatedKey map[string]types.AttributeValue
	Count            int32
	ScannedCount     int32
}

func validateDiscovery[T any](ctx context.Context, client *dynamodb.Client, table Table, name *string, projection *string, attributes []string, selection types.Select) (Table, error) {
	table, err := table.normalized()
	if err != nil {
		return table, err
	}
	if ctx == nil || client == nil || table.Name == "" {
		return table, fmt.Errorf("context, DynamoDB client, and table are required")
	}
	if err := ctx.Err(); err != nil {
		return table, err
	}
	if name != nil && *name != table.Name {
		return table, fmt.Errorf("discovery table does not match configured table")
	}
	if projection != nil || len(attributes) != 0 || selection == types.SelectSpecificAttributes {
		return table, fmt.Errorf("partial document projections are not supported")
	}
	if err := validateType[T](); err != nil {
		return table, err
	}
	return table, nil
}

func decodePage[T any](table Table, items []map[string]types.AttributeValue, cursor map[string]types.AttributeValue, count, scanned int32, index bool) (Page[T], error) {
	page := Page[T]{LastEvaluatedKey: cursor, Count: count, ScannedCount: scanned}
	for _, item := range items {
		key, err := table.readKey(item)
		if err != nil {
			return Page[T]{}, err
		}
		var value *T
		// An explicit index can omit data; a base table cannot turn a malformed flat
		// legacy record into a successful no-payload observation.
		if _, hasData := item["data"]; hasData || !index {
			value, err = UnmarshalItem[T](table, item)
			if err != nil {
				return Page[T]{}, err
			}
		}
		page.Items = append(page.Items, Item[T]{Key: key, Value: value})
	}
	return page, nil
}

// Query returns one typed page with ordinary SDK query/index/filter options.
// Input is copied, never mutated. No hidden rereads, leases or full collection.
// Do not request partial projections; indexes without data return nil Values.
func Query[T any](ctx context.Context, client *dynamodb.Client, table Table, input *dynamodb.QueryInput) (Page[T], error) {
	if input == nil {
		return Page[T]{}, fmt.Errorf("nil QueryInput")
	}
	in := *input
	var err error
	table, err = validateDiscovery[T](ctx, client, table, in.TableName, in.ProjectionExpression, in.AttributesToGet, in.Select)
	if err != nil {
		return Page[T]{}, err
	}
	in.TableName = aws.String(table.Name)
	for attempt := 0; ; attempt++ {
		out, err := client.Query(ctx, &in, noSDKRetry)
		if err == nil {
			return decodePage[T](table, out.Items, out.LastEvaluatedKey, out.Count, out.ScannedCount, aws.ToString(in.IndexName) != "")
		}
		if !retryable(err) || attempt+1 >= maxAttempts {
			return Page[T]{}, err
		}
		if err := backoff(ctx, attempt); err != nil {
			return Page[T]{}, err
		}
	}
}

// Scan returns one typed page with ordinary SDK scan/filter/segment options.
// It has the same projection and ownership rules as Query.
func Scan[T any](ctx context.Context, client *dynamodb.Client, table Table, input *dynamodb.ScanInput) (Page[T], error) {
	if input == nil {
		return Page[T]{}, fmt.Errorf("nil ScanInput")
	}
	in := *input
	var err error
	table, err = validateDiscovery[T](ctx, client, table, in.TableName, in.ProjectionExpression, in.AttributesToGet, in.Select)
	if err != nil {
		return Page[T]{}, err
	}
	in.TableName = aws.String(table.Name)
	for attempt := 0; ; attempt++ {
		out, err := client.Scan(ctx, &in, noSDKRetry)
		if err == nil {
			return decodePage[T](table, out.Items, out.LastEvaluatedKey, out.Count, out.ScannedCount, aws.ToString(in.IndexName) != "")
		}
		if !retryable(err) || attempt+1 >= maxAttempts {
			return Page[T]{}, err
		}
		if err := backoff(ctx, attempt); err != nil {
			return Page[T]{}, err
		}
	}
}
