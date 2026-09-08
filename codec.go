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

// LockKey is the table's string partition key. No sort key is supported.
type LockKey struct {
	ID string `json:"id" dynamodbav:"id"`
}

// MarshalItem encodes an unlocked envelope without writing to DynamoDB.
// Use attribute_not_exists(id) when inserting it with PutItem.
// Nil data and non-map encodings are rejected. A missing or empty data.id uses
// id; a conflicting or non-string data.id is rejected. Only the outer id is stored.
func MarshalItem(id string, data any) (map[string]types.AttributeValue, error) {
	payload, err := marshalPayload(id, data)
	if err != nil {
		return nil, err
	}
	item := key(id)
	item["data"] = payload
	return item, nil
}

// UnmarshalItem decodes an envelope, or a projection including id and data,
// into a struct or string-keyed map T. The input map is unchanged.
// The outer id is injected before decoding, so map values must accept a string.
// An absent item or absent data returns nil; empty data returns a non-nil value.
// Interface numbers use attributevalue.Number to preserve precision; typed
// numeric fields keep their declared Go types.
func UnmarshalItem[T any](item map[string]types.AttributeValue) (*T, error) {
	if err := validateType[T](); err != nil {
		return nil, err
	}
	r, err := parseRecord(item)
	if err != nil {
		return nil, err
	}
	if r.data == nil {
		return nil, nil
	}
	payload := maps.Clone(r.data.Value)
	if payload == nil {
		payload = make(map[string]types.AttributeValue)
	}
	payload["id"] = &types.AttributeValueMemberS{Value: r.id}
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

func validateID(id string) error {
	if len(id) == 0 || len(id) > 2048 {
		return errors.New("id must contain 1 to 2048 bytes")
	}
	return nil
}

func key(id string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: id}}
}

func marshalPayload(id string, data any) (*types.AttributeValueMemberM, error) {
	if err := validateID(id); err != nil {
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
	// remove every alias so data.ID cannot shadow the injected outer id.
	// A custom marshaler may return its own map; never remove its id in place.
	values := maps.Clone(payload.Value)
	for name, value := range values {
		if !strings.EqualFold(name, "id") {
			continue
		}
		value, ok := value.(*types.AttributeValueMemberS)
		if !ok || value == nil || value.Value != "" && value.Value != id {
			return nil, fmt.Errorf("%w: data.id must be empty or match the lock key", ErrInvalidPayload)
		}
		delete(values, name)
	}
	return &types.AttributeValueMemberM{Value: values}, nil
}

type record struct {
	id      string
	owner   string
	expires int64
	data    *types.AttributeValueMemberM
}

func parseRecord(item map[string]types.AttributeValue) (record, error) {
	var r record
	if len(item) == 0 {
		return r, nil
	}
	for name := range item {
		switch name {
		case "id", "owner_token", "expires_at", "data":
		default:
			return r, fmt.Errorf("%w: unexpected outer attribute %q", ErrInvalidRecord, name)
		}
	}
	id, ok := item["id"].(*types.AttributeValueMemberS)
	if !ok || id == nil || validateID(id.Value) != nil {
		return r, fmt.Errorf("%w: missing or invalid id", ErrInvalidRecord)
	}
	r.id = id.Value
	if value, exists := item["data"]; exists {
		r.data, ok = value.(*types.AttributeValueMemberM)
		if !ok || r.data == nil {
			return r, fmt.Errorf("%w: data must be a map", ErrInvalidRecord)
		}
		for name := range r.data.Value {
			if strings.EqualFold(name, "id") {
				return r, fmt.Errorf("%w: id belongs only at the outer level", ErrInvalidRecord)
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
