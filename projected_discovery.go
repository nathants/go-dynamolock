package dynamolock

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ProjectedItem is an incomplete, read-only observation, not a full document.
// A present item with nil Value returned none of the selected payload; it does
// not prove the full payload is absent or valid. HasLeaseMetadata reports a valid
// ownership pair, even if expired, and grants no ownership or mutation authority.
type ProjectedItem[T any] struct {
	Key              Key
	Value            *T
	HasLeaseMetadata bool
}

// ProjectedPage preserves the full service cursor and counts for one scan page.
// An empty page may still have a cursor. Items contain only selected payloads.
type ProjectedPage[T any] struct {
	Items            []ProjectedItem[T]
	LastEvaluatedKey map[string]types.AttributeValue
	Count            int32
	ScannedCount     int32
}

// payloadProjection selects primary keys, both ownership fields and explicit
// payload paths. Each segment is an exact stored attribute name, including dots
// or brackets. An empty field list selects keys/metadata only, never all data.
func payloadProjection(table Table, fields [][]string, names map[string]string) (string, map[string]string, error) {
	for i, path := range fields {
		if len(path) == 0 || slices.Contains(path, "") {
			return "", nil, fmt.Errorf("projection paths must contain nonempty attribute names")
		}
		if table.isKey(path[0]) {
			return "", nil, fmt.Errorf("primary keys belong only at the outer level")
		}
		for _, prior := range fields[:i] {
			n := min(len(path), len(prior))
			if slices.Equal(path[:n], prior[:n]) {
				return "", nil, fmt.Errorf("duplicate or overlapping projection paths")
			}
		}
	}
	aliases := maps.Clone(names)
	if aliases == nil {
		aliases = make(map[string]string)
	}
	next := 0
	alias := func(name string) string {
		for {
			key := fmt.Sprintf("#p%d", next)
			next++
			if _, exists := aliases[key]; !exists {
				aliases[key] = name
				return key
			}
		}
	}
	paths := []string{alias(table.PartitionKey)}
	if table.SortKey != "" {
		paths = append(paths, alias(table.SortKey))
	}
	paths = append(paths, alias("owner_token"), alias("expires_at"))
	if len(fields) != 0 {
		data := alias("data")
		for _, field := range fields {
			var path strings.Builder
			path.WriteString(data)
			for _, segment := range field {
				path.WriteByte('.')
				path.WriteString(alias(segment))
			}
			paths = append(paths, path.String())
		}
	}
	return strings.Join(paths, ", "), aliases, nil
}

func decodeProjected[T any](table Table, item map[string]types.AttributeValue) (*ProjectedItem[T], error) {
	if len(item) == 0 {
		return nil, nil
	}
	// The request always includes both ownership fields. Reuse envelope guards
	// on what was returned, without claiming to validate unrequested payload.
	r, err := parseRecord(table, item)
	if err != nil {
		return nil, err
	}
	value, err := UnmarshalItem[T](table, item)
	if err != nil {
		return nil, err
	}
	return &ProjectedItem[T]{Key: r.key, Value: value, HasLeaseMetadata: r.owner != ""}, nil
}

// ReadProjected reads one key with only the requested payload paths. Reads are
// eventually consistent unless ConsistentRead is true; eventual reads can return
// stale data, lease metadata or absence. Paths are exact attribute-name segments
// relative to data, not expressions or list indexes. No fields selects keys/metadata
// only. A nil result means no item was observed; a nil Value on a present result
// means no selected payload was returned. No hidden full read or lease is provided.
func ReadProjected[T any](ctx context.Context, client *dynamodb.Client, table Table, key Key, fields [][]string, options ReadOptions) (*ProjectedItem[T], error) {
	var err error
	table, err = table.normalized()
	if err != nil {
		return nil, err
	}
	if err := validateTarget(ctx, client, table, key); err != nil {
		return nil, err
	}
	if err := validateType[T](); err != nil {
		return nil, err
	}
	projection, names, err := payloadProjection(table, fields, nil)
	if err != nil {
		return nil, err
	}
	raw, err := readItem(ctx, client, &dynamodb.GetItemInput{
		TableName: aws.String(table.Name), Key: table.key(key),
		ConsistentRead:       aws.Bool(options.ConsistentRead),
		ProjectionExpression: aws.String(projection), ExpressionAttributeNames: names,
	})
	if err != nil {
		return nil, err
	}
	item, err := decodeProjected[T](table, raw)
	if err == nil && item != nil && item.Key != key {
		return nil, fmt.Errorf("%w: read returned another primary key", ErrInvalidRecord)
	}
	return item, err
}

// ScanProjected returns one incomplete base-table page, selecting only explicit
// payload paths plus primary keys and both ownership fields. It preserves filter,
// consistency, limit, segment and cursor options; caller inputs are not mutated.
// ConsistentRead defaults to false (eventual); true requests strong reads, not a
// collection snapshot. Pass LastEvaluatedKey as ExclusiveStartKey until the cursor
// is empty, including after pages with no items.
// Index scans and caller projection/Select options are rejected so omitted index
// metadata cannot masquerade as an unleased item. No hidden reads or leases.
func ScanProjected[T any](ctx context.Context, client *dynamodb.Client, table Table, input *dynamodb.ScanInput, fields [][]string) (ProjectedPage[T], error) {
	if input == nil {
		return ProjectedPage[T]{}, fmt.Errorf("nil ScanInput")
	}
	in := *input
	var err error
	table, err = validateDiscovery[T](ctx, client, table, in.TableName, in.ProjectionExpression, in.AttributesToGet, in.Select)
	if err != nil {
		return ProjectedPage[T]{}, err
	}
	if aws.ToString(in.IndexName) != "" || in.Select != "" {
		return ProjectedPage[T]{}, fmt.Errorf("projected scans require a base table and no Select option")
	}
	projection, names, err := payloadProjection(table, fields, in.ExpressionAttributeNames)
	if err != nil {
		return ProjectedPage[T]{}, err
	}
	in.TableName = aws.String(table.Name)
	in.ProjectionExpression = aws.String(projection)
	in.ExpressionAttributeNames = names
	for attempt := 0; ; attempt++ {
		out, err := client.Scan(ctx, &in, noSDKRetry)
		if err == nil {
			page := ProjectedPage[T]{LastEvaluatedKey: out.LastEvaluatedKey, Count: out.Count, ScannedCount: out.ScannedCount}
			for _, raw := range out.Items {
				item, err := decodeProjected[T](table, raw)
				if err != nil {
					return ProjectedPage[T]{}, err
				}
				if item == nil {
					return ProjectedPage[T]{}, fmt.Errorf("%w: empty scan item", ErrInvalidRecord)
				}
				page.Items = append(page.Items, *item)
			}
			return page, nil
		}
		if !retryable(err) || attempt+1 >= maxAttempts {
			return ProjectedPage[T]{}, err
		}
		if err := backoff(ctx, attempt); err != nil {
			return ProjectedPage[T]{}, err
		}
	}
}
