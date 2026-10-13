package dynamolock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"testing/synctest"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
)

const throttledJSON = `{"__type":"ProvisionedThroughputExceededException","message":"try again"}`

func TestCASRetryTransitions(t *testing.T) {
	for _, operation := range []string{"update", "delete", "initialize"} {
		for _, exhaust := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/exhaust=%v", operation, exhaust), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					attempts, failures := 0, 2
					if exhaust {
						failures = maxAttempts
					}
					var requests []wireRequest
					client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
						item := casWireItem("before")
						if w.Target == "DynamoDB_20120810.GetItem" {
							if operation == "initialize" {
								delete(item, "generation")
								delete(item, "version")
							}
							return 200, map[string]any{"Item": item}, nil
						}
						attempts++
						requests = append(requests, w)
						if attempts <= failures {
							return 400, throttledJSON, nil
						}
						if operation == "initialize" {
							item["generation"], item["version"] = w.ExpressionAttributeValues[":generation"], json.RawMessage(`{"N":"1"}`)
							return 200, map[string]any{"Attributes": item}, nil
						}
						return 200, `{}`, nil
					})
					read := func() (*CAS[Data], *Data, error) {
						return ReadCAS[Data](t.Context(), client, protocolInput().Table, protocolInput().Key, ReadOptions{})
					}
					c, data, err := read()
					var raw, visible *types.AttributeValueMemberM
					var before revision
					if operation != "initialize" {
						if err != nil {
							t.Fatal(err)
						}
						before = c.revision
						raw, err = snapshotPayload(c.raw)
						if err != nil {
							t.Fatal(err)
						}
						visible, err = snapshotPayload(c.visible)
						if err != nil {
							t.Fatal(err)
						}
						data.Value = "after"
						if operation == "update" {
							err = c.Update(t.Context(), data)
						} else {
							err = c.Delete(t.Context())
						}
					}
					wantAttempts := failures + 1
					if exhaust {
						wantAttempts = maxAttempts
						if _, ok := errors.AsType[*types.ProvisionedThroughputExceededException](err); !ok {
							t.Fatalf("exhaustion result: %v", err)
						}
						if operation == "initialize" {
							if c != nil || data != nil {
								t.Fatal("failed initialization returned a capability")
							}
						} else if c.terminal != nil || c.revision != before || !reflect.DeepEqual(c.raw, raw) || !reflect.DeepEqual(c.visible, visible) {
							t.Fatal("definitively rejected attempts advanced or invalidated snapshot")
						}
					} else if err != nil {
						t.Fatal(err)
					}
					if attempts != wantAttempts {
						t.Fatalf("attempts = %d, want %d", attempts, wantAttempts)
					}
					for _, req := range requests {
						if operation != "initialize" && wireInt(req, ":version") != 7 {
							t.Fatal("retry changed the expected revision")
						}
						if operation == "update" && wireInt(req, ":newVersion") != 8 {
							t.Fatal("retry incremented more than once")
						}
						if string(req.ExpressionAttributeValues[":generation"]) != string(requests[0].ExpressionAttributeValues[":generation"]) {
							t.Fatal("retry changed generation")
						}
					}
					if exhaust {
						if operation == "initialize" {
							c, data, err = read()
						} else if operation == "update" {
							err = c.Update(t.Context(), data)
						} else {
							err = c.Delete(t.Context())
						}
						if err != nil || c == nil || attempts != wantAttempts+1 {
							t.Fatalf("recovery after exhaustion: %d attempts, %v", attempts, err)
						}
					}
					if operation == "update" && (c.revision.version != 8 || reflect.DeepEqual(c.visible, visible)) {
						t.Fatal("confirmed update did not advance the snapshot exactly once")
					}
					if operation == "delete" && !errors.Is(c.Update(t.Context(), data), ErrReleased) {
						t.Fatal("successful deletion left a usable handle")
					}
				})
			})
		}
	}
}

func TestCASWaitingCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, resume := make(chan struct{}), make(chan struct{})
		var versions []int64
		client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
			if w.Target == "DynamoDB_20120810.GetItem" {
				return 200, map[string]any{"Item": casWireItem("before")}, nil
			}
			versions = append(versions, wireInt(w, ":version"))
			if len(versions) == 1 {
				close(entered)
				<-resume
			}
			return 200, `{}`, nil
		})
		c, data, err := ReadCAS[Data](t.Context(), client, protocolInput().Table, protocolInput().Key, ReadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		first := make(chan error, 1)
		go func() { first <- c.Update(t.Context(), data) }()
		<-entered
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		second := make(chan error, 1)
		go func() { second <- c.Delete(ctx) }()
		synctest.Wait() // First request is in flight; Delete is waiting for the handle.
		cancel()
		err = <-second
		close(resume)
		firstErr := <-first
		if !errors.Is(err, context.Canceled) || errors.Is(err, ErrOutcomeUnknown) || firstErr != nil {
			t.Fatalf("queued cancellation: %v, first operation: %v", err, firstErr)
		}
		if err := c.Update(t.Context(), data); err != nil {
			t.Fatalf("pre-submission cancellation invalidated handle: %v", err)
		}
		if !reflect.DeepEqual(versions, []int64{7, 8}) {
			t.Fatalf("queued cancellation submitted a request or changed snapshot: %v", versions)
		}
	})
}

func TestCASCancellationDuringBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		attempts := 0
		client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
			if w.Target == "DynamoDB_20120810.GetItem" {
				return 200, map[string]any{"Item": casWireItem("before")}, nil
			}
			attempts++
			if attempts == 1 {
				return 400, throttledJSON, nil
			}
			return 200, `{}`, nil
		})
		c, data, err := ReadCAS[Data](t.Context(), client, protocolInput().Table, protocolInput().Key, ReadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- c.Update(ctx, data) }()
		synctest.Wait() // The definitive rejection has arrived; backoff is waiting.
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) || errors.Is(err, ErrOutcomeUnknown) || attempts != 1 {
			t.Fatalf("backoff cancellation: %v (%d requests)", err, attempts)
		}
		if err := c.Update(t.Context(), data); err != nil || attempts != 2 || c.revision.version != 8 {
			t.Fatalf("rejected/canceled attempt poisoned snapshot: %v (%d requests)", err, attempts)
		}
	})
}

func TestCASSubmittedCancellation(t *testing.T) {
	for _, outcome := range []string{"unknown", "success", "rejected"} {
		t.Run(outcome, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				attempts := 0
				entered := make(chan struct{})
				client := protocolClient(func(ctx context.Context, w wireRequest) (int, any, error) {
					if w.Target == "DynamoDB_20120810.GetItem" {
						return 200, map[string]any{"Item": casWireItem("before")}, nil
					}
					attempts++
					if attempts == 1 {
						close(entered)
						<-ctx.Done()
						switch outcome {
						case "unknown":
							return 0, nil, ctx.Err()
						case "rejected":
							return 400, rejectedJSON, nil
						}
					}
					return 200, `{}`, nil
				})
				c, data, err := ReadCAS[Data](t.Context(), client, protocolInput().Table, protocolInput().Key, ReadOptions{})
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- c.Update(ctx, data) }()
				<-entered
				cancel()
				err = <-done
				switch outcome {
				case "unknown":
					if !errors.Is(err, ErrOutcomeUnknown) || !errors.Is(err, context.Canceled) || c.revision.version != 7 {
						t.Fatalf("ambiguous cancellation: %v, revision %+v", err, c.revision)
					}
					if !errors.Is(c.Update(t.Context(), data), ErrOutcomeUnknown) || !errors.Is(c.Delete(t.Context()), ErrOutcomeUnknown) || attempts != 1 {
						t.Fatal("ambiguous cancellation allowed another request")
					}
				case "success", "rejected":
					want := uint64(8)
					if outcome == "rejected" {
						want = 7
						var rejected smithy.APIError
						if !errors.As(err, &rejected) || rejected.ErrorCode() != "ValidationException" {
							t.Fatalf("definitive rejection became ambiguous: %v", err)
						}
					} else if err != nil {
						t.Fatalf("confirmed success became ambiguous: %v", err)
					}
					if c.revision.version != want {
						t.Fatalf("wrong confirmed snapshot: %+v", c.revision)
					}
					if err := c.Update(t.Context(), data); err != nil || attempts != 2 || c.revision.version != want+1 {
						t.Fatalf("known outcome invalidated handle: %v", err)
					}
				}
			})
		})
	}
}
