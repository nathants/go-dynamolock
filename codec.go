package dynamolock

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ErrInvalidPayload reports a nil, non-map, or conflicting-ID payload.
var ErrInvalidPayload = errors.New("invalid lock payload")

// ErrInvalidRecord reports an item that does not use this package's record format.
var ErrInvalidRecord = errors.New("invalid lock record")

// MarshalItem encodes an unlocked envelope without writing to DynamoDB.
// Nil data, non-map encodings and conflicting keys are rejected. Primary keys
// are stored only outside data. Does not overwrite or acquire an existing item.
func MarshalItem(table Table, key Key, data any) (map[string]types.AttributeValue, error) {
	table, err := table.normalized()
	if err != nil {
		return nil, err
	}
	payload, err := marshalPayload(table, key, data)
	if err != nil {
		return nil, err
	}
	item := table.key(key)
	item["data"] = payload
	for _, name := range table.Attributes {
		if value, ok := payload.Value[name]; ok {
			item[name] = value
		}
	}
	return item, nil
}

// UnmarshalItem decodes a full envelope into a struct or string-keyed map T.
// The input map is unchanged.
// Outer primary keys are injected before decoding; map values must accept strings.
// An absent item or absent data returns nil; empty data returns a non-nil value.
// Interface numbers use attributevalue.Number to preserve precision; typed
// numeric fields keep their declared Go types.
func UnmarshalItem[T any](table Table, item map[string]types.AttributeValue) (*T, error) {
	if err := validateType[T](); err != nil {
		return nil, err
	}
	table, err := table.normalized()
	if err != nil {
		return nil, err
	}
	r, err := parseRecord(table, item)
	if err != nil {
		return nil, err
	}
	if r.data == nil {
		return nil, nil
	}
	return unmarshalPayload[T](table, r.key, r.data)
}

// unmarshalPayload decodes known payload fields with the outer identity. Callers
// validate the schema, type and key, and decide which fields are authoritative.
func unmarshalPayload[T any](table Table, key Key, fields *types.AttributeValueMemberM) (*T, error) {
	if err := validateProjection(fields, reflect.TypeFor[T]()); err != nil {
		return nil, err
	}
	payload := maps.Clone(fields.Value)
	if payload == nil {
		payload = make(map[string]types.AttributeValue)
	}
	maps.Copy(payload, table.key(key))
	var data T
	if err := attributevalue.UnmarshalMapWithOptions(payload, &data, func(options *attributevalue.DecoderOptions) {
		options.UseNumber = true
	}); err != nil {
		return nil, fmt.Errorf("decode lock payload: %w", err)
	}
	return &data, nil
}

func validateType[T any]() error {
	t := reflect.TypeFor[T]()
	if t.Kind() == reflect.Struct || t.Kind() == reflect.Map && t.Key().Kind() == reflect.String {
		return nil
	}
	return fmt.Errorf("%w: T must be a struct or string-keyed map, got %s", ErrInvalidPayload, t)
}

func marshalPayload(table Table, key Key, data any) (*types.AttributeValueMemberM, error) {
	if err := table.validateKey(key); err != nil {
		return nil, err
	}
	// The SDK can encode a pointer to a nil map as an empty map. Reject nil
	// before marshaling so an absent value cannot clear an existing payload.
	value := reflect.ValueOf(data)
	for value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface {
		value = value.Elem()
	}
	if !value.IsValid() || value.Kind() == reflect.Map && value.IsNil() {
		return nil, fmt.Errorf("%w: data must not be nil", ErrInvalidPayload)
	}
	av, err := attributevalue.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidPayload, err)
	}
	payload, ok := av.(*types.AttributeValueMemberM)
	if !ok || payload == nil {
		return nil, fmt.Errorf("%w: data must encode as a map, not %T", ErrInvalidPayload, av)
	}
	// The SDK also matches struct fields case-insensitively. Validate and
	// remove every key alias so it cannot shadow the injected outer identity.
	// A custom marshaler may return its own map; never remove keys in place.
	values := maps.Clone(payload.Value)
	for name, value := range values {
		if !table.isKey(name) {
			continue
		}
		expected := key.Partition
		if strings.EqualFold(name, table.SortKey) {
			expected = key.Sort
		}
		value, ok := value.(*types.AttributeValueMemberS)
		if !ok || value == nil || value.Value != "" && value.Value != expected {
			return nil, fmt.Errorf("%w: data key must be empty or match the lock key", ErrInvalidPayload)
		}
		delete(values, name)
	}
	return &types.AttributeValueMemberM{Value: values}, nil
}

type record struct {
	revision revision
	key      Key
	owner    string
	expires  int64
	data     *types.AttributeValueMemberM
}

func parseRecord(table Table, item map[string]types.AttributeValue) (record, error) {
	var r record
	if len(item) == 0 {
		return r, nil
	}
	var err error
	r.key, err = table.readKey(item)
	if err != nil {
		return r, err
	}
	// Future projected attributes are allowed only on a real envelope. A flat
	// legacy row must never be interpreted as an empty application document.
	if _, hasData := item["data"]; !hasData {
		for name := range item {
			if name != table.PartitionKey && (table.SortKey == "" || name != table.SortKey) && name != "owner_token" && name != "expires_at" && name != "generation" && name != "version" {
				return r, fmt.Errorf("%w: unexpected outer attribute %q without data", ErrInvalidRecord, name)
			}
		}
	}
	r.revision, err = readRevision(item)
	if err != nil {
		return r, err
	}
	var ok bool
	if value, exists := item["data"]; exists {
		r.data, ok = value.(*types.AttributeValueMemberM)
		if !ok || r.data == nil {
			return r, fmt.Errorf("%w: data must be a map", ErrInvalidRecord)
		}
		for name := range r.data.Value {
			if table.isKey(name) {
				return r, fmt.Errorf("%w: primary keys belong only at the outer level", ErrInvalidRecord)
			}
		}
	}
	owner, hasOwner := item["owner_token"]
	expires, hasExpiry := item["expires_at"]
	if !hasOwner && !hasExpiry {
		return r, nil
	}
	o, ownerOK := owner.(*types.AttributeValueMemberS)
	e, expiryOK := expires.(*types.AttributeValueMemberN)
	if !hasOwner || !hasExpiry || !ownerOK || !expiryOK || o == nil || e == nil || o.Value == "" {
		return r, fmt.Errorf("%w: owner_token and expires_at must be present together", ErrInvalidRecord)
	}
	n, err := strconv.ParseInt(e.Value, 10, 64)
	if err != nil || n <= 0 {
		return r, fmt.Errorf("%w: expires_at must be positive Unix nanoseconds", ErrInvalidRecord)
	}
	r.owner, r.expires = o.Value, n
	return r, nil
}
