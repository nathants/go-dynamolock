package dynamolock

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// IndexItem is a read-only index observation, not a complete document. Value is
// decoded from primary keys and configured Table.Attributes present on the entry,
// even when only keys were returned. Missing copies do not prove that fields are
// absent from the document. No payload envelope or lease metadata is interpreted.
type IndexItem[T any] struct {
	Key   Key
	Value T
}

// IndexPage preserves counts and the complete service cursor for one index page.
// An empty page may still have a cursor. Count-only pages have no Items.
type IndexPage[T any] struct {
	Items            []IndexItem[T]
	LastEvaluatedKey map[string]types.AttributeValue
	Count            int32
	ScannedCount     int32
}

func decodeIndexPage[T any](table Table, out *dynamodb.QueryOutput) (IndexPage[T], error) {
	page := IndexPage[T]{LastEvaluatedKey: out.LastEvaluatedKey, Count: out.Count, ScannedCount: out.ScannedCount}
	for _, raw := range out.Items {
		key, err := table.readKey(raw)
		if err != nil {
			return IndexPage[T]{}, err
		}
		fields := make(map[string]types.AttributeValue, len(table.Attributes))
		for _, name := range table.Attributes {
			if value, ok := raw[name]; ok {
				fields[name] = value
			}
		}
		value, err := unmarshalPayload[T](table, key, &types.AttributeValueMemberM{Value: fields})
		if err != nil {
			return IndexPage[T]{}, err
		}
		page.Items = append(page.Items, IndexItem[T]{Key: key, Value: *value})
	}
	return page, nil
}

// QueryIndex returns one page of typed index observations, decoding only primary
// keys and configured Table.Attributes copies. It ignores data, lease metadata
// and unconfigured attributes, even if the index projects them. It never reads
// the parent document or acquires a lease; lock and recheck before modifying it.
// IndexName is required. Caller projections are rejected; Select may be empty,
// AllProjectedAttributes or Count. Filter, ordering, limit and cursor options are
// preserved. ConsistentRead defaults to eventual; strong reads work only on LSIs.
// Input is copied, never mutated; do not mutate it during a call. Pass the full
// LastEvaluatedKey as ExclusiveStartKey until empty, even after an empty page.
// Neither strong reads nor pagination provide a collection snapshot.
func QueryIndex[T any](ctx context.Context, client *dynamodb.Client, table Table, input *dynamodb.QueryInput) (IndexPage[T], error) {
	if input == nil {
		return IndexPage[T]{}, fmt.Errorf("nil QueryInput")
	}
	in := *input
	var err error
	table, err = validateDiscovery[T](ctx, client, table, in.TableName, in.ProjectionExpression, in.AttributesToGet, in.Select)
	if err != nil {
		return IndexPage[T]{}, err
	}
	if aws.ToString(in.IndexName) == "" {
		return IndexPage[T]{}, fmt.Errorf("index query requires IndexName")
	}
	// ALL_ATTRIBUTES on an LSI can fetch the parent document. Restrict the
	// request to index entries, not just which fields the decoder consumes.
	if in.Select != "" && in.Select != types.SelectAllProjectedAttributes && in.Select != types.SelectCount {
		return IndexPage[T]{}, fmt.Errorf("index query requires SelectAllProjectedAttributes or SelectCount")
	}
	if in.Select == "" {
		in.Select = types.SelectAllProjectedAttributes
	}
	in.TableName = aws.String(table.Name)
	for attempt := 0; ; attempt++ {
		out, err := client.Query(ctx, &in, noSDKRetry)
		if err == nil {
			return decodeIndexPage[T](table, out)
		}
		if !retryable(err) || attempt+1 >= maxAttempts {
			return IndexPage[T]{}, err
		}
		if err := backoff(ctx, attempt); err != nil {
			return IndexPage[T]{}, err
		}
	}
}
