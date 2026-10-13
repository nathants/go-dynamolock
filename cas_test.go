package dynamolock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func casWireItem(value string) map[string]json.RawMessage {
	return map[string]json.RawMessage{
		"id": json.RawMessage(`{"S":"key"}`), "generation": json.RawMessage(`{"S":"generation"}`),
		"version": json.RawMessage(`{"N":"7"}`), "data": json.RawMessage(fmt.Sprintf(`{"M":{"value":{"S":%q},"future":{"S":"preserve"}}}`, value)),
	}
}

func TestCASReadAndReuse(t *testing.T) {
	for _, strong := range []bool{false, true} {
		t.Run(fmt.Sprint(strong), func(t *testing.T) {
			reads, writes := 0, 0
			client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
				if w.Target == "DynamoDB_20120810.GetItem" {
					reads++
					if w.ConsistentRead != strong {
						t.Fatalf("wrong read consistency: %v", w.ConsistentRead)
					}
					return 200, map[string]any{"Item": casWireItem("before")}, nil
				}
				writes++
				if wireInt(w, ":version") != int64(6+writes) || wireInt(w, ":newVersion") != int64(7+writes) {
					t.Fatalf("snapshot did not advance: %+v", w.ExpressionAttributeValues)
				}
				var payload struct{ M map[string]json.RawMessage }
				if err := json.Unmarshal(w.ExpressionAttributeValues[":data"], &payload); err != nil {
					t.Fatal(err)
				}
				if string(payload.M["future"]) != `{"S":"preserve"}` {
					t.Fatalf("unknown field lost: %s", w.ExpressionAttributeValues[":data"])
				}
				return 200, `{}`, nil
			})
			c, data, err := ReadCAS[Data](t.Context(), client, protocolInput().Table, protocolInput().Key, ReadOptions{ConsistentRead: strong})
			if err != nil || c == nil || data == nil || data.Value != "before" || c.Key() != protocolInput().Key {
				t.Fatalf("read: %v %+v %v", c, data, err)
			}
			if err := c.Update(t.Context(), nil); !errors.Is(err, ErrInvalidPayload) {
				t.Fatalf("nil payload: %v", err)
			}
			data.Value = "after"
			if err := c.Update(t.Context(), data); err != nil {
				t.Fatal(err)
			}
			data.Value = "again"
			if err := c.Update(t.Context(), data); err != nil {
				t.Fatal(err)
			}
			if reads != 1 || writes != 2 {
				t.Fatalf("unexpected IO: %d reads, %d writes", reads, writes)
			}
		})
	}
}

func TestCASBusyRead(t *testing.T) {
	for _, versioned := range []bool{false, true} {
		t.Run(fmt.Sprint(versioned), func(t *testing.T) {
			calls := 0
			item := casWireItem("inspectable")
			if !versioned {
				delete(item, "generation")
				delete(item, "version")
			}
			item["owner_token"] = json.RawMessage(`{"S":"holder"}`)
			item["expires_at"] = json.RawMessage(fmt.Sprintf(`{"N":"%d"}`, time.Now().Add(time.Hour).UnixNano()))
			client := protocolClient(func(context.Context, wireRequest) (int, any, error) {
				calls++
				return 200, map[string]any{"Item": item}, nil
			})
			c, data, err := ReadCAS[Data](t.Context(), client, protocolInput().Table, protocolInput().Key, ReadOptions{})
			if c != nil || data == nil || data.Value != "inspectable" || !errors.Is(err, ErrLockHeld) || calls != 1 {
				t.Fatalf("busy result: %v %+v %v (%d calls)", c, data, err, calls)
			}
		})
	}
}

func TestCASLazyInitializationUsesReturnedPayload(t *testing.T) {
	for _, raced := range []bool{false, true} {
		t.Run(fmt.Sprint(raced), func(t *testing.T) {
			calls := 0
			client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
				calls++
				switch calls {
				case 1:
					item := casWireItem("stale")
					delete(item, "generation")
					delete(item, "version")
					return 200, map[string]any{"Item": item}, nil
				case 2:
					item := casWireItem("current")
					if raced {
						return 400, map[string]any{"__type": "ConditionalCheckFailedException", "Item": item}, nil
					}
					item["generation"], item["version"] = w.ExpressionAttributeValues[":generation"], json.RawMessage(`{"N":"1"}`)
					return 200, map[string]any{"Attributes": item}, nil
				default:
					t.Fatal("unexpected extra request")
					return 500, serverErrorJSON, nil
				}
			})
			c, data, err := ReadCAS[Data](t.Context(), client, protocolInput().Table, protocolInput().Key, ReadOptions{})
			if err != nil || c == nil || data == nil || data.Value != "current" || calls != 2 {
				t.Fatalf("initialization used stale payload: %v %+v %v", c, data, err)
			}
		})
	}
}

func TestCASTerminalOutcomes(t *testing.T) {
	for _, operation := range []string{"update", "delete"} {
		for _, outcome := range []string{"conflict", "ambiguous", "rejected", "success"} {
			t.Run(operation+"/"+outcome, func(t *testing.T) {
				writes := 0
				client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
					if w.Target == "DynamoDB_20120810.GetItem" {
						return 200, map[string]any{"Item": casWireItem("before")}, nil
					}
					writes++
					switch outcome {
					case "conflict":
						return 400, conditionalJSON, nil
					case "ambiguous":
						return 500, serverErrorJSON, nil
					case "rejected":
						return 400, rejectedJSON, nil
					default:
						return 200, `{}`, nil
					}
				})
				c, data, err := ReadCAS[Data](t.Context(), client, protocolInput().Table, protocolInput().Key, ReadOptions{})
				if err != nil {
					t.Fatal(err)
				}
				operationFn := func() error { return c.Update(t.Context(), data) }
				if operation == "delete" {
					operationFn = func() error { return c.Delete(t.Context()) }
				}
				err = operationFn()
				var terminal error
				switch outcome {
				case "conflict":
					terminal = ErrConflict
				case "ambiguous":
					terminal = ErrOutcomeUnknown
				case "success":
					if err != nil {
						t.Fatal(err)
					}
					if operation == "delete" {
						terminal = ErrReleased
					}
				case "rejected":
					if err == nil {
						t.Fatal("accepted rejected write")
					}
				}
				if terminal != nil {
					if operation != "delete" || outcome != "success" {
						if !errors.Is(err, terminal) {
							t.Fatalf("wrong terminal result: %v", err)
						}
					}
					if !errors.Is(c.Update(t.Context(), data), terminal) || !errors.Is(c.Delete(t.Context()), terminal) || writes != 1 {
						t.Fatalf("terminal handle reused: %d", writes)
					}
				} else {
					_ = operationFn()
					if writes != 2 {
						t.Fatalf("usable handle rejected: %d", writes)
					}
				}
			})
		}
	}
}

func TestCASConcurrentHandleAndCancellation(t *testing.T) {
	var versions []int64
	client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
		if w.Target == "DynamoDB_20120810.GetItem" {
			return 200, map[string]any{"Item": casWireItem("before")}, nil
		}
		versions = append(versions, wireInt(w, ":version"))
		return 200, `{}`, nil
	})
	c, data, err := ReadCAS[Data](t.Context(), client, protocolInput().Table, protocolInput().Key, ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if !errors.Is(c.Update(ctx, data), context.Canceled) {
		t.Fatal("canceled operation was submitted")
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := c.Update(t.Context(), data); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if !reflect.DeepEqual(versions, []int64{7, 8, 9, 10, 11, 12, 13, 14}) {
		t.Fatalf("concurrent operations used stale snapshots: %v", versions)
	}
}

func TestRevisionValidation(t *testing.T) {
	for _, metadata := range []map[string]types.AttributeValue{
		{"generation": &types.AttributeValueMemberS{Value: "g"}},
		{"version": versionNumber(1)},
		{"generation": &types.AttributeValueMemberS{}, "version": versionNumber(1)},
		{"generation": &types.AttributeValueMemberS{Value: "g"}, "version": versionNumber(0)},
		{"generation": &types.AttributeValueMemberS{Value: "g"}, "version": &types.AttributeValueMemberN{Value: "1.5"}},
		{"generation": &types.AttributeValueMemberS{Value: "g"}, "version": &types.AttributeValueMemberN{Value: "18446744073709551616"}},
	} {
		metadata["id"] = &types.AttributeValueMemberS{Value: "key"}
		if _, err := UnmarshalItem[Data](Table{}, metadata); !errors.Is(err, ErrInvalidRecord) {
			t.Fatalf("malformed revision accepted: %v %v", metadata, err)
		}
	}
	client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
		if w.Target != "DynamoDB_20120810.GetItem" {
			t.Fatal("overflow reached service")
		}
		item := casWireItem("before")
		item["version"] = json.RawMessage(fmt.Sprintf(`{"N":"%d"}`, uint64(math.MaxUint64)))
		return 200, map[string]any{"Item": item}, nil
	})
	c, data, err := ReadCAS[Data](t.Context(), client, protocolInput().Table, protocolInput().Key, ReadOptions{})
	if err != nil || c == nil {
		t.Fatal(err)
	}
	if !errors.Is(c.Update(t.Context(), data), ErrVersionExhausted) {
		t.Fatal("counter wrapped")
	}
}
