package dynamolock

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Snapshots must not alias a caller's custom marshaler or decoded containers.
func snapshotPayload(payload *types.AttributeValueMemberM) (*types.AttributeValueMemberM, error) {
	if payload == nil {
		return nil, nil
	}
	encoded, err := attributevalue.MarshalMapJSON(payload.Value)
	if err != nil {
		return nil, err
	}
	value, err := attributevalue.UnmarshalMapJSON(encoded)
	if err != nil {
		return nil, err
	}
	return &types.AttributeValueMemberM{Value: value}, nil
}

// mergePayload retains raw fields that this reader does not understand. The
// exclusive lease makes one bounded SET of the merged data safe; generating one
// expression per nested field would impose DynamoDB's 4 KiB expression limit on
// otherwise valid payloads. No other writer may bypass this lease protocol.
func mergePayload[T any](raw, before, after *types.AttributeValueMemberM) (*types.AttributeValueMemberM, error) {
	if raw == nil {
		return after, nil
	}
	value, err := mergePayloadValue(nil, raw, before, after, reflect.TypeFor[T]())
	if err != nil {
		return nil, err
	}
	return value.(*types.AttributeValueMemberM), nil
}

// mergePayloadValue returns the raw payload after applying changes visible to this reader.
// Maps are patched recursively. Removing a visible key removes its whole subtree.
// Lists are atomic: positional merging could attach unknown fields to the wrong
// member after an insertion/reorder, so a lossy projection cannot replace one.
func mergePayloadValue(path []string, raw, before, after types.AttributeValue, t reflect.Type) (types.AttributeValue, error) {
	if reflect.DeepEqual(before, after) {
		return raw, nil
	}
	if after == nil {
		return nil, nil
	}
	rawMap, rawIsMap := raw.(*types.AttributeValueMemberM)
	afterMap, afterIsMap := after.(*types.AttributeValueMemberM)
	if rawIsMap && afterIsMap {
		names, err := projectionNames(rawMap.Value, t)
		if err != nil {
			return nil, err
		}
		beforeMap, _ := before.(*types.AttributeValueMemberM)
		previous := map[string]types.AttributeValue{}
		if beforeMap != nil {
			previous = beforeMap.Value
		}
		next := maps.Clone(rawMap.Value)
		if next == nil {
			next = make(map[string]types.AttributeValue)
		}
		keys := maps.Clone(previous)
		if keys == nil {
			keys = make(map[string]types.AttributeValue)
		}
		maps.Copy(keys, afterMap.Value)
		for _, name := range slices.Sorted(maps.Keys(keys)) {
			if reflect.DeepEqual(previous[name], afterMap.Value[name]) {
				continue
			}
			stored := name
			if original, exists := names[name]; exists {
				stored = original
			}
			_, childType := projectionChild(t, name)
			value, err := mergePayloadValue(append(path, name), rawMap.Value[stored], previous[name], afterMap.Value[name], childType)
			if err != nil {
				return nil, err
			}
			if value == nil {
				delete(next, stored)
			} else {
				next[stored] = value
			}
		}
		return &types.AttributeValueMemberM{Value: next}, nil
	}
	_, explicitNull := after.(*types.AttributeValueMemberNULL)
	if !explicitNull && hiddenFields(raw, before, t) {
		return nil, fmt.Errorf("%w: replacement at %s would erase fields absent from the decoded value", ErrInvalidPayload, strings.Join(path, "."))
	}
	return after, nil
}

// This detects structural projection loss, not harmless number/time normalization.
func hiddenFields(raw, decoded types.AttributeValue, t reflect.Type) bool {
	switch value := raw.(type) {
	case *types.AttributeValueMemberM:
		known, ok := decoded.(*types.AttributeValueMemberM)
		if !ok {
			return len(value.Value) != 0
		}
		for stored, child := range value.Value {
			name, childType := projectionChild(t, stored)
			other, exists := known.Value[name]
			if !exists || hiddenFields(child, other, childType) {
				return true
			}
		}
	case *types.AttributeValueMemberL:
		known, ok := decoded.(*types.AttributeValueMemberL)
		if !ok || len(value.Value) != len(known.Value) {
			return len(value.Value) != 0
		}
		for i, child := range value.Value {
			if hiddenFields(child, known.Value[i], projectionElement(t)) {
				return true
			}
		}
	}
	return false
}
