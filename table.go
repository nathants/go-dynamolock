package dynamolock

import (
	"fmt"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Table describes string primary keys and optional top-level projections of data
// fields for indexes or application TTL. PartitionKey defaults to id. Attributes
// are exact stored field names, not paths; lease metadata must never be TTL.
type Table struct {
	Name         string
	PartitionKey string
	SortKey      string
	Attributes   []string
}

// Key identifies an item without concatenating its partition and sort values.
type Key struct {
	Partition string
	Sort      string
}

func (t Table) normalized() (Table, error) {
	if t.Name == "" {
		return t, fmt.Errorf("table name is required")
	}
	if t.PartitionKey == "" {
		t.PartitionKey = "id"
	}
	names := []string{"data", "owner_token", "expires_at"}
	configured := []string{t.PartitionKey}
	if t.SortKey != "" {
		configured = append(configured, t.SortKey)
	}
	configured = append(configured, t.Attributes...)
	for _, name := range configured {
		if name == "" || len(name) > 255 {
			return t, fmt.Errorf("attribute name must contain 1 to 255 bytes")
		}
		for _, prior := range names {
			if strings.EqualFold(prior, name) {
				return t, fmt.Errorf("reserved or overlapping attribute %q", name)
			}
		}
		names = append(names, name)
	}
	t.Attributes = slices.Clone(t.Attributes)
	return t, nil
}

func (t Table) validateKey(k Key) error {
	if len(k.Partition) == 0 || len(k.Partition) > 2048 {
		return fmt.Errorf("partition key must contain 1 to 2048 bytes")
	}
	if t.SortKey == "" && k.Sort != "" {
		return fmt.Errorf("sort value without a sort key")
	}
	if t.SortKey != "" && (len(k.Sort) == 0 || len(k.Sort) > 1024) {
		return fmt.Errorf("sort key must contain 1 to 1024 bytes")
	}
	return nil
}

func (t Table) key(k Key) map[string]types.AttributeValue {
	item := map[string]types.AttributeValue{t.PartitionKey: &types.AttributeValueMemberS{Value: k.Partition}}
	if t.SortKey != "" {
		item[t.SortKey] = &types.AttributeValueMemberS{Value: k.Sort}
	}
	return item
}

func (t Table) readKey(item map[string]types.AttributeValue) (Key, error) {
	var k Key
	p, ok := item[t.PartitionKey].(*types.AttributeValueMemberS)
	if !ok || p == nil {
		return k, fmt.Errorf("%w: missing string partition key", ErrInvalidRecord)
	}
	k.Partition = p.Value
	if t.SortKey != "" {
		s, ok := item[t.SortKey].(*types.AttributeValueMemberS)
		if !ok || s == nil {
			return k, fmt.Errorf("%w: missing string sort key", ErrInvalidRecord)
		}
		k.Sort = s.Value
	}
	if err := t.validateKey(k); err != nil {
		return k, fmt.Errorf("%w: %w", ErrInvalidRecord, err)
	}
	return k, nil
}

func (t Table) isKey(name string) bool {
	return strings.EqualFold(name, t.PartitionKey) || t.SortKey != "" && strings.EqualFold(name, t.SortKey)
}
