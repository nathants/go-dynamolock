package dynamolock

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Attribute names are case-insensitive only at ordinary struct boundaries.
// Maps, interfaces, and custom codecs own their names; folding their keys would
// conflate distinct application fields. Custom codecs must retain field names.
func projectionType(t reflect.Type) reflect.Type {
	for t != nil {
		for _, codec := range []reflect.Type{reflect.TypeFor[attributevalue.Marshaler](), reflect.TypeFor[attributevalue.Unmarshaler]()} {
			if t.Implements(codec) || t.Kind() != reflect.Pointer && reflect.PointerTo(t).Implements(codec) {
				return nil
			}
		}
		if t.Kind() != reflect.Pointer {
			return t
		}
		t = t.Elem()
	}
	return nil
}

type projectionField struct {
	name      string
	typeOf    reflect.Type
	index     []int
	tagged    bool
	ambiguous bool
}

type projectionFields struct {
	exact   map[string]projectionField
	ordered []projectionField
}

var projectionCache sync.Map

// The SDK exposes no field metadata. Match its dynamodbav field selection:
// shallowest embedding wins, a tag breaks a depth tie, ambiguous fields vanish,
// and case-insensitive lookup uses declaration order after exact-name lookup.
func structProjection(t reflect.Type) projectionFields {
	if cached, ok := projectionCache.Load(t); ok {
		return cached.(projectionFields)
	}
	type embedded struct {
		typeOf   reflect.Type
		index    []int
		multiple bool
	}
	level := []embedded{{typeOf: t}}
	visited := make(map[reflect.Type]bool)
	fields := make(map[string]projectionField)
	for len(level) != 0 {
		var next []embedded
		queued := make(map[reflect.Type]int)
		for _, parent := range level {
			if visited[parent.typeOf] {
				continue
			}
			visited[parent.typeOf] = true
			for i := 0; i < parent.typeOf.NumField(); i++ {
				sf := parent.typeOf.Field(i)
				if sf.PkgPath != "" && !sf.Anonymous {
					continue
				}
				name, _, _ := strings.Cut(sf.Tag.Get("dynamodbav"), ",")
				if name == "-" {
					continue
				}
				index := append(slices.Clone(parent.index), i)
				child := sf.Type
				if child.Kind() == reflect.Pointer {
					child = child.Elem()
				}
				if sf.Anonymous && name == "" && child.Kind() == reflect.Struct {
					if position, exists := queued[child]; exists {
						next[position].multiple = true
					} else {
						queued[child] = len(next)
						next = append(next, embedded{typeOf: child, index: index})
					}
					continue
				}
				field := projectionField{name: name, typeOf: sf.Type, index: index, tagged: name != "", ambiguous: parent.multiple}
				if name == "" {
					field.name = sf.Name
				}
				previous, exists := fields[field.name]
				switch {
				case !exists || len(index) < len(previous.index) || len(index) == len(previous.index) && field.tagged && !previous.tagged:
					fields[field.name] = field
				case len(index) == len(previous.index) && field.tagged == previous.tagged:
					previous.ambiguous = true
					fields[field.name] = previous
				}
			}
		}
		level = next
	}
	result := projectionFields{exact: fields}
	for name, field := range fields {
		if field.ambiguous {
			delete(fields, name)
		} else {
			result.ordered = append(result.ordered, field)
		}
	}
	slices.SortFunc(result.ordered, func(a, b projectionField) int { return slices.Compare(a.index, b.index) })
	cached, _ := projectionCache.LoadOrStore(t, result)
	return cached.(projectionFields)
}

func projectionChild(t reflect.Type, name string) (string, reflect.Type) {
	t = projectionType(t)
	if t == nil {
		return name, nil
	}
	if t.Kind() == reflect.Map {
		return name, t.Elem()
	}
	if t.Kind() == reflect.Struct {
		fields := structProjection(t)
		if field, ok := fields.exact[name]; ok {
			return field.name, field.typeOf
		}
		for _, field := range fields.ordered {
			if strings.EqualFold(field.name, name) {
				return field.name, field.typeOf
			}
		}
	}
	return name, nil
}

func projectionElement(t reflect.Type) reflect.Type {
	t = projectionType(t)
	if t != nil && (t.Kind() == reflect.Slice || t.Kind() == reflect.Array) {
		return t.Elem()
	}
	return nil
}

// Map the encoder's names back to the actual stored keys. A duplicate struct
// target is invalid: the SDK otherwise picks its value in random map order.
func projectionNames(raw map[string]types.AttributeValue, t reflect.Type) (map[string]string, error) {
	t = projectionType(t)
	if t == nil || t.Kind() != reflect.Struct {
		return nil, nil
	}
	names := make(map[string]string, len(raw))
	for stored := range raw {
		name, _ := projectionChild(t, stored)
		if previous, exists := names[name]; exists {
			return nil, fmt.Errorf("%w: attributes %q and %q decode into the same field %q", ErrInvalidRecord, previous, stored, name)
		}
		names[name] = stored
	}
	return names, nil
}

func validateProjection(raw types.AttributeValue, t reflect.Type) error {
	if projectionType(t) == nil {
		return nil
	}
	switch value := raw.(type) {
	case *types.AttributeValueMemberM:
		if _, err := projectionNames(value.Value, t); err != nil {
			return err
		}
		for name, child := range value.Value {
			_, childType := projectionChild(t, name)
			if err := validateProjection(child, childType); err != nil {
				return fmt.Errorf("attribute %q: %w", name, err)
			}
		}
	case *types.AttributeValueMemberL:
		for i, child := range value.Value {
			if err := validateProjection(child, projectionElement(t)); err != nil {
				return fmt.Errorf("list element %d: %w", i, err)
			}
		}
	}
	return nil
}
