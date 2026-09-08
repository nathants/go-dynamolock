package dynamolock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestUnarmedLiveGateSkipsBeforeProvider(t *testing.T) {
	const helper = "DYNAMOLOCK_TEST_UNARMED_HELPER"
	if os.Getenv(helper) == "1" {
		checkAccount(t)
		return
	}

	t.Setenv("DYNAMOLOCK_TEST_ACCOUNT", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_DEFAULT_REGION", "us-west-2")
	t.Setenv("AWS_REGION", "us-west-2")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_ENDPOINT_URL", "http://127.0.0.1:1")
	t.Setenv("AWS_ENDPOINT_URL_STS", "http://127.0.0.1:1")
	t.Setenv("AWS_MAX_ATTEMPTS", "1")

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestUnarmedLiveGateSkipsBeforeProvider$")
	cmd.Env = append(os.Environ(), helper+"=1")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("unarmed live test reached the provider: %v\n%s", err, output)
	}
}

type httpFunc func(*http.Request) (*http.Response, error)

func (f httpFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

type wireRequest struct {
	Target                              string `json:"-"`
	TableName                           string
	Key                                 map[string]json.RawMessage
	ConditionExpression                 string
	UpdateExpression                    string
	ReturnValues                        string
	ReturnValuesOnConditionCheckFailure string
	ConsistentRead                      bool
	ExpressionAttributeNames            map[string]string
	ExpressionAttributeValues           map[string]json.RawMessage
}

const conditionalJSON = `{"__type":"ConditionalCheckFailedException","message":"condition failed"}`
const serverErrorJSON = `{"__type":"InternalServerError","message":"response lost"}`
const rejectedJSON = `{"__type":"ValidationException","message":"rejected before applying"}`

func protocolClient(fn func(context.Context, wireRequest) (int, any, error)) *dynamodb.Client {
	return dynamodb.New(dynamodb.Options{
		Region: "us-east-1",
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
		// Deliberately enable SDK retries: the library must override them.
		Retryer: retry.NewStandard(func(o *retry.StandardOptions) {
			o.MaxAttempts = 7
			o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 0, nil })
		}),
		HTTPClient: httpFunc(func(req *http.Request) (*http.Response, error) {
			var w wireRequest
			if err := json.NewDecoder(req.Body).Decode(&w); err != nil {
				return nil, err
			}
			w.Target = req.Header.Get("X-Amz-Target")
			status, body, err := fn(req.Context(), w)
			if err != nil {
				return nil, err
			}
			var encoded []byte
			if text, ok := body.(string); ok {
				encoded = []byte(text)
			} else {
				encoded, err = json.Marshal(body)
				if err != nil {
					return nil, err
				}
			}
			return &http.Response{
				StatusCode: status, Request: req,
				Header: http.Header{
					"Content-Type": {"application/x-amz-json-1.0"},
					"X-Amz-Crc32":  {strconv.FormatUint(uint64(crc32.ChecksumIEEE(encoded)), 10)},
				},
				Body: io.NopCloser(strings.NewReader(string(encoded))),
			}, nil
		}),
	})
}

// These are scripted responses, not a DynamoDB condition evaluator or a second
// implementation of the lock. Live tests validate the actual conditions.
func acquiredItem(w wireRequest, data string) map[string]json.RawMessage {
	item := map[string]json.RawMessage{
		"id": w.Key["id"], "owner_token": w.ExpressionAttributeValues[":owner"],
		"expires_at": w.ExpressionAttributeValues[":expires"],
	}
	if data != "" {
		item["data"] = json.RawMessage(`{"M":` + data + `}`)
	}
	return item
}

func acquireReply(w wireRequest, data string) (int, any, error) {
	return 200, map[string]any{"Attributes": acquiredItem(w, data)}, nil
}

func protocolInput() *LockInput {
	return &LockInput{Table: "protocol-table", ID: "key", HeartbeatMaxAge: 2 * time.Hour, HeartbeatInterval: time.Hour}
}

func wireInt(w wireRequest, name string) int64 {
	var av struct{ N string }
	if json.Unmarshal(w.ExpressionAttributeValues[name], &av) != nil {
		return 0
	}
	n, _ := strconv.ParseInt(av.N, 10, 64)
	return n
}

func await(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for test synchronization")
	}
}

func cleanupLease[T any](t *testing.T, l *Lease[T]) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := l.Release(ctx); err != nil {
			t.Errorf("release test lease: %v", err)
		}
	})
}

type keyedData struct {
	ID    string `dynamodbav:"id"`
	Value string `dynamodbav:"value"`
}

func TestEnvelopeIdentityAndReplacement(t *testing.T) {
	for _, id := range []string{"", "key"} {
		item, err := MarshalItem("key", &keyedData{ID: id, Value: "value"})
		if err != nil {
			t.Fatal(err)
		}
		payload := item["data"].(*types.AttributeValueMemberM)
		if _, duplicated := payload.Value["id"]; duplicated || len(item) != 2 {
			t.Fatalf("identity duplicated in stored payload: %#v", item)
		}
		data, err := UnmarshalItem[keyedData](item)
		if err != nil || data == nil || *data != (keyedData{ID: "key", Value: "value"}) {
			t.Fatalf("identity round trip: %#v %v", data, err)
		}
		if _, mutated := payload.Value["id"]; mutated {
			t.Fatal("decoding mutated the raw payload")
		}
	}
	for _, data := range []any{nil, (*keyedData)(nil), 42, []int{1}, &keyedData{ID: "wrong"}, map[string]any{"id": 42}} {
		if _, err := MarshalItem("key", data); !errors.Is(err, ErrInvalidPayload) {
			t.Errorf("payload %T was not rejected: %v", data, err)
		}
	}
	item, err := MarshalItem("key", map[string]any{"owner_token": "application", "expires_at": 7})
	if err != nil {
		t.Fatal(err)
	}
	data, err := UnmarshalItem[map[string]any](item)
	if err != nil || (*data)["id"] != "key" || (*data)["owner_token"] != "application" {
		t.Fatalf("nested metadata-name collision: %#v %v", data, err)
	}
}

func TestEnvelopeRejectsNilMaps(t *testing.T) {
	type namedMap map[string]any
	var data map[string]any
	var named namedMap
	var pointer *map[string]any
	for _, payload := range []any{data, &data, named, &named, &pointer} {
		if _, err := MarshalItem("key", payload); !errors.Is(err, ErrInvalidPayload) {
			t.Errorf("nil payload %T was not rejected: %v", payload, err)
		}
	}
}

func TestProtocolNilMapWriteDoesNotClearPayload(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(fmt.Sprint(commit), func(t *testing.T) {
			var writes []wireRequest
			client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
				if w.ReturnValues == "ALL_NEW" {
					return acquireReply(w, `{"value":{"S":"retained"}}`)
				}
				if w.ExpressionAttributeValues[":data"] != nil {
					writes = append(writes, w)
				}
				return 200, `{}`, nil
			})
			l, _, err := Lock[map[string]any](t.Context(), client, protocolInput())
			if err != nil {
				t.Fatal(err)
			}
			cleanupLease(t, l)
			write := l.Update
			if commit {
				write = l.Commit
			}
			var data map[string]any
			if err := write(t.Context(), &data); !errors.Is(err, ErrInvalidPayload) || len(writes) != 0 {
				t.Fatalf("nil map could clear the payload: err=%v writes=%d", err, len(writes))
			}
			if l.Context().Err() != nil {
				t.Fatalf("invalid payload lost the lease: %v", context.Cause(l.Context()))
			}
			data = make(map[string]any)
			if err := write(t.Context(), &data); err != nil {
				t.Fatal(err)
			}
			if len(writes) != 1 || string(writes[0].ExpressionAttributeValues[":data"]) != `{"M":{}}` {
				t.Fatalf("explicit empty map was not written: %#v", writes)
			}
		})
	}
}

func TestEnvelopePreservesInterfaceNumbers(t *testing.T) {
	const integer = "9007199254740993"
	const decimal = "0.123456789012345678901234567890"
	item := key("key")
	item["data"] = &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
		"integer": &types.AttributeValueMemberN{Value: integer},
		"nested": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"decimal": &types.AttributeValueMemberN{Value: decimal},
		}},
	}}
	data, err := UnmarshalItem[map[string]any](item)
	if err != nil {
		t.Fatal(err)
	}
	if got := (*data)["integer"]; got != attributevalue.Number(integer) {
		t.Errorf("interface number lost precision or type: %T %v", got, got)
	}
	encoded, err := MarshalItem("key", data)
	if err != nil || !reflect.DeepEqual(encoded, item) {
		t.Errorf("unchanged map round-trip changed numbers: %#v %v", encoded, err)
	}

	// Typed numbers keep their declared Go type; interface fields inside structs
	// receive the same precise number representation as dynamic maps.
	type typedData struct {
		Integer int64 `dynamodbav:"integer"`
		Nested  any   `dynamodbav:"nested"`
	}
	typed, err := UnmarshalItem[typedData](item)
	if err != nil || typed == nil || typed.Integer != 9007199254740993 {
		t.Fatalf("typed integer changed: %#v %v", typed, err)
	}
	encoded, err = MarshalItem("key", typed)
	if err != nil || !reflect.DeepEqual(encoded, item) {
		t.Errorf("struct interface field changed numbers: %#v %v", encoded, err)
	}
}

func TestEnvelopeAbsentVersusEmpty(t *testing.T) {
	for _, item := range []map[string]types.AttributeValue{nil, key("key")} {
		value, err := UnmarshalItem[keyedData](item)
		if err != nil || value != nil {
			t.Fatalf("absent payload: %#v %v", value, err)
		}
	}
	item, err := MarshalItem("key", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	value, err := UnmarshalItem[keyedData](item)
	if err != nil || value == nil || value.ID != "key" {
		t.Fatalf("explicitly empty payload: %#v %v", value, err)
	}
	for _, field := range []string{"uid", "unix", "legacy-data"} {
		bad := key("key")
		bad[field] = &types.AttributeValueMemberS{Value: "legacy"}
		if _, err := UnmarshalItem[keyedData](bad); !errors.Is(err, ErrInvalidRecord) {
			t.Errorf("legacy %q accepted: %v", field, err)
		}
	}
	item["data"].(*types.AttributeValueMemberM).Value = key("key")
	if _, err := UnmarshalItem[keyedData](item); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("duplicate data.id accepted: %v", err)
	}
}

func TestProtocolAtomicAcquireAndCopiedInput(t *testing.T) {
	var calls []wireRequest
	client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
		calls = append(calls, w)
		if w.ReturnValues == "ALL_NEW" {
			return acquireReply(w, `{"value":{"S":"fresh"}}`)
		}
		return 200, `{}`, nil
	})
	in := protocolInput()
	l, data, err := Lock[keyedData](t.Context(), client, in)
	if err != nil {
		t.Fatal(err)
	}
	cleanupLease(t, l)
	if data == nil || data.ID != "key" || data.Value != "fresh" || len(calls) != 1 {
		t.Fatalf("payload not returned by atomic acquire: %#v, requests=%d", data, len(calls))
	}
	in.Table, in.ID, in.HeartbeatInterval = "wrong-table", "wrong-key", time.Nanosecond
	if err := l.Commit(t.Context(), data); err != nil {
		t.Fatal(err)
	}
	for _, w := range calls {
		if w.TableName != "protocol-table" || string(w.Key["id"]) != `{"S":"key"}` || w.Target != "DynamoDB_20120810.UpdateItem" {
			t.Fatalf("handle retargeted or whole item replaced: %#v", w)
		}
	}
	if !errors.Is(l.Update(t.Context(), data), ErrReleased) || !errors.Is(l.Commit(t.Context(), data), ErrReleased) {
		t.Fatal("completed handle permitted another payload write")
	}
}

func TestLockExpirationDoesNotIncludeRoundedBoundary(t *testing.T) {
	var w wireRequest
	client := protocolClient(func(_ context.Context, req wireRequest) (int, any, error) {
		w = req
		return 400, conditionalJSON, nil
	})
	in := protocolInput()
	in.HeartbeatMaxAge, in.HeartbeatInterval = 100*time.Millisecond, 40*time.Millisecond
	before := time.Now()
	_, _, err := Lock[keyedData](t.Context(), client, in)
	after := time.Now()
	if !errors.Is(err, ErrLockHeld) {
		t.Fatal(err)
	}
	if !strings.Contains(w.ConditionExpression, "#expires < :now") || w.ExpressionAttributeNames["#expires"] != "expires_at" {
		t.Fatalf("not comparing the holder's expiry: %s", w.ConditionExpression)
	}
	cutoff, expiry := wireInt(w, ":now"), wireInt(w, ":expires")
	if cutoff < before.UnixNano() || cutoff > after.UnixNano() || expiry-cutoff != int64(in.HeartbeatMaxAge) {
		t.Fatalf("truncated or contender-adjusted timestamps: cutoff=%d expiry=%d", cutoff, expiry)
	}
}

func TestProtocolAcquireAmbiguousOutcome(t *testing.T) {
	for _, own := range []bool{false, true} {
		t.Run(strconv.FormatBool(own), func(t *testing.T) {
			var saved map[string]json.RawMessage
			var acquires, reads int
			client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
				if w.ReturnValues == "ALL_NEW" {
					acquires++
					saved = acquiredItem(w, `{"value":{"S":"retained"}}`)
					return 500, serverErrorJSON, nil
				}
				if w.Target == "DynamoDB_20120810.GetItem" {
					reads++
					if !w.ConsistentRead {
						return 0, nil, errors.New("reconciliation was not strongly consistent")
					}
					if own {
						return 200, map[string]any{"Item": saved}, nil
					}
					return 200, `{}`, nil
				}
				return 200, `{}`, nil
			})
			l, data, err := Lock[keyedData](t.Context(), client, protocolInput())
			if own {
				if err != nil || data == nil || data.Value != "retained" {
					t.Fatalf("own committed acquisition not recognized: %#v %v", data, err)
				}
				cleanupLease(t, l)
			} else if !errors.Is(err, ErrOutcomeUnknown) || errors.Is(err, ErrLockUnavailable) || l != nil {
				t.Fatalf("unconfirmed acquisition misclassified: %v", err)
			}
			if acquires != 1 || reads != 1 {
				t.Fatalf("SDK/library stacked retries: acquires=%d reads=%d", acquires, reads)
			}
		})
	}
}

func TestProtocolDecodeFailureReleasesRawPayload(t *testing.T) {
	var calls []wireRequest
	client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
		calls = append(calls, w)
		if w.ReturnValues == "ALL_NEW" {
			return acquireReply(w, `{"value":{"M":{"unknown":{"N":"12345678901234567890"}}}}`)
		}
		return 200, `{}`, nil
	})
	l, _, err := Lock[keyedData](t.Context(), client, protocolInput())
	if err == nil || l != nil || len(calls) != 2 {
		t.Fatalf("decode failure abandoned ownership: lease=%v err=%v requests=%d", l, err, len(calls))
	}
	if calls[1].UpdateExpression != "REMOVE #owner, #expires" || calls[1].ExpressionAttributeValues[":data"] != nil {
		t.Fatalf("cleanup changed malformed payload: %#v", calls[1])
	}
}

type blockingPayload struct {
	ready, resume chan struct{}
}

func (p blockingPayload) MarshalDynamoDBAttributeValue() (types.AttributeValue, error) {
	close(p.ready)
	<-p.resume
	return &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"value": &types.AttributeValueMemberS{Value: "new"}}}, nil
}

func TestProtocolSlowPayloadCannotOverwriteHeartbeat(t *testing.T) {
	var renewals atomic.Int32
	renewed := make(chan struct{}, 1)
	var payload wireRequest
	client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
		if w.ReturnValues == "ALL_NEW" {
			return acquireReply(w, "")
		}
		if w.ExpressionAttributeValues[":next"] != nil {
			renewals.Add(1)
			select {
			case renewed <- struct{}{}:
			default:
			}
		}
		if w.ExpressionAttributeValues[":data"] != nil {
			payload = w
		}
		return 200, `{}`, nil
	})
	in := protocolInput()
	in.HeartbeatMaxAge, in.HeartbeatInterval = time.Second, 10*time.Millisecond
	l, _, err := Lock[blockingPayload](t.Context(), client, in)
	if err != nil {
		t.Fatal(err)
	}
	cleanupLease(t, l)
	p := &blockingPayload{ready: make(chan struct{}), resume: make(chan struct{})}
	resume := sync.OnceFunc(func() { close(p.resume) })
	t.Cleanup(resume)
	done := make(chan error, 1)
	go func() { done <- l.Update(t.Context(), p) }()
	await(t, p.ready)
	await(t, renewed)
	resume()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if renewals.Load() == 0 || payload.UpdateExpression != "SET #data = :data" || payload.ExpressionAttributeValues[":next"] != nil || payload.ExpressionAttributeValues[":expires"] != nil {
		t.Fatalf("payload write could roll back renewal metadata: %#v", payload)
	}
}

func TestProtocolHeartbeatRetriesUseFreshTimestamps(t *testing.T) {
	var timestamps []int64
	var deadlines []time.Time
	renewed := make(chan struct{})
	client := protocolClient(func(ctx context.Context, w wireRequest) (int, any, error) {
		if w.ReturnValues == "ALL_NEW" {
			return acquireReply(w, "")
		}
		if w.ExpressionAttributeValues[":next"] == nil {
			return 200, `{}`, nil
		}
		timestamps = append(timestamps, wireInt(w, ":next"))
		deadline, ok := ctx.Deadline()
		if !ok || !strings.Contains(w.ConditionExpression, "#expires < :next") {
			return 0, nil, errors.New("renewal lacks deadline or monotonic condition")
		}
		deadlines = append(deadlines, deadline)
		if len(timestamps) == 1 {
			return 500, serverErrorJSON, nil
		}
		if len(timestamps) == 2 {
			close(renewed)
		}
		return 200, `{}`, nil
	})
	in := protocolInput()
	in.HeartbeatMaxAge, in.HeartbeatInterval = time.Second, 100*time.Millisecond
	l, _, err := Lock[keyedData](t.Context(), client, in)
	if err != nil {
		t.Fatal(err)
	}
	cleanupLease(t, l)
	await(t, renewed)
	if err := l.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(timestamps) != 2 || timestamps[1] <= timestamps[0] || !deadlines[0].Equal(deadlines[1]) {
		t.Fatalf("stale timestamps or unconfirmed lease extension: %v %v", timestamps, deadlines)
	}
}

func TestProtocolLeaseDeadlineCancelsBlockedRenewal(t *testing.T) {
	entered, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
		if w.ReturnValues == "ALL_NEW" {
			return acquireReply(w, "")
		}
		if w.ExpressionAttributeValues[":next"] != nil {
			close(entered)
			<-resume // Deliberately delay even after the request deadline.
		}
		return 200, `{}`, nil
	})
	in := protocolInput()
	in.HeartbeatMaxAge, in.HeartbeatInterval = 80*time.Millisecond, 10*time.Millisecond
	l, _, err := Lock[keyedData](t.Context(), client, in)
	if err != nil {
		t.Fatal(err)
	}
	cleanupLease(t, l)
	t.Cleanup(unblock)
	await(t, entered)
	await(t, l.Context().Done())
	if !errors.Is(context.Cause(l.Context()), ErrLeaseLost) {
		t.Fatal(context.Cause(l.Context()))
	}
	if !errors.Is(l.Update(t.Context(), &keyedData{}), ErrLeaseLost) {
		t.Fatal("lost lease allowed update")
	}
	unblock()
	await(t, l.done)
	if !errors.Is(l.Commit(t.Context(), &keyedData{}), ErrLeaseLost) {
		t.Fatal("late renewal revived handle")
	}
}

func TestProtocolDefinitiveLossIsNotRetried(t *testing.T) {
	var renewals atomic.Int32
	client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
		if w.ReturnValues == "ALL_NEW" {
			return acquireReply(w, "")
		}
		if w.ExpressionAttributeValues[":next"] != nil {
			renewals.Add(1)
		}
		return 400, conditionalJSON, nil
	})
	in := protocolInput()
	in.HeartbeatMaxAge, in.HeartbeatInterval = time.Second, time.Millisecond
	l, _, err := Lock[keyedData](t.Context(), client, in)
	if err != nil {
		t.Fatal(err)
	}
	cleanupLease(t, l)
	await(t, l.Context().Done())
	await(t, l.done)
	if renewals.Load() != 1 || !errors.Is(context.Cause(l.Context()), ErrLeaseLost) {
		t.Fatalf("definitive failure retried: %d %v", renewals.Load(), context.Cause(l.Context()))
	}
}

func TestProtocolHeartbeatDuringCommitDoesNotCancelItsResponse(t *testing.T) {
	started, heartbeat := make(chan struct{}), make(chan struct{})
	var heartbeats atomic.Int32
	client := protocolClient(func(ctx context.Context, w wireRequest) (int, any, error) {
		if w.ReturnValues == "ALL_NEW" {
			return acquireReply(w, "")
		}
		if w.ExpressionAttributeValues[":data"] != nil {
			close(started)
			select {
			case <-ctx.Done():
				return 0, nil, ctx.Err()
			case <-heartbeat:
			}
			if ctx.Err() != nil {
				return 0, nil, errors.New("own release canceled commit response")
			}
			return 200, `{}`, nil
		}
		if w.ExpressionAttributeValues[":next"] != nil {
			select {
			case <-started:
			default:
				return 200, `{}`, nil
			}
			if heartbeats.Add(1) == 1 {
				close(heartbeat)
			}
			return 400, conditionalJSON, nil
		}
		return 200, `{}`, nil
	})
	in := protocolInput()
	in.HeartbeatMaxAge, in.HeartbeatInterval = time.Second, 10*time.Millisecond
	l, _, err := Lock[keyedData](t.Context(), client, in)
	if err != nil {
		t.Fatal(err)
	}
	cleanupLease(t, l)
	if err := l.Commit(l.Context(), &keyedData{Value: "committed"}); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(context.Cause(l.Context()), ErrReleased) {
		t.Fatal(context.Cause(l.Context()))
	}
}

func TestProtocolPayloadAmbiguityPermanentlyStopsWrites(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(strconv.FormatBool(commit), func(t *testing.T) {
			var writes atomic.Int32
			client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
				if w.ReturnValues == "ALL_NEW" {
					return acquireReply(w, "")
				}
				if w.ExpressionAttributeValues[":data"] != nil {
					writes.Add(1)
					return 500, serverErrorJSON, nil
				}
				return 400, conditionalJSON, nil
			})
			l, _, err := Lock[keyedData](t.Context(), client, protocolInput())
			if err != nil {
				t.Fatal(err)
			}
			cleanupLease(t, l)
			if commit {
				err = l.Commit(t.Context(), &keyedData{})
			} else {
				err = l.Update(t.Context(), &keyedData{})
			}
			if !errors.Is(err, ErrOutcomeUnknown) || !errors.Is(context.Cause(l.Context()), ErrOutcomeUnknown) {
				t.Fatalf("ambiguous payload not terminal: %v %v", err, context.Cause(l.Context()))
			}
			if !errors.Is(l.Update(t.Context(), &keyedData{}), ErrLeaseLost) || !errors.Is(l.Commit(t.Context(), &keyedData{}), ErrLeaseLost) || writes.Load() != 1 {
				t.Fatalf("ambiguous write retried or handle reused: %d", writes.Load())
			}
		})
	}
}

func TestProtocolReleaseReconcilesAmbiguousSuccess(t *testing.T) {
	var releases atomic.Int32
	client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
		if w.ReturnValues == "ALL_NEW" {
			return acquireReply(w, `{"unknown":{"N":"12345678901234567890"}}`)
		}
		if w.UpdateExpression != "REMOVE #owner, #expires" || w.ExpressionAttributeValues[":data"] != nil {
			return 0, nil, errors.New("release modified payload")
		}
		if releases.Add(1) == 1 {
			return 500, serverErrorJSON, nil
		}
		return 400, conditionalJSON, nil
	})
	l, _, err := Lock[keyedData](t.Context(), client, protocolInput())
	if err != nil {
		t.Fatal(err)
	}
	cleanupLease(t, l)
	if err := l.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	if releases.Load() != 2 {
		t.Fatalf("unexpected release attempts: %d", releases.Load())
	}
	await(t, l.done)
	if err := l.Release(t.Context()); err != nil || releases.Load() != 2 {
		t.Fatal("release was not idempotent")
	}
}

func TestProtocolCanceledLeaseAllowsOnlyRelease(t *testing.T) {
	var writes int
	client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
		if w.ReturnValues == "ALL_NEW" {
			return acquireReply(w, "")
		}
		writes++
		if w.UpdateExpression != "REMOVE #owner, #expires" {
			return 0, nil, errors.New("canceled lease attempted payload write")
		}
		return 200, `{}`, nil
	})
	ctx, cancel := context.WithCancel(t.Context())
	l, _, err := Lock[keyedData](ctx, client, protocolInput())
	if err != nil {
		t.Fatal(err)
	}
	cleanupLease(t, l)
	cancel()
	if !errors.Is(l.Update(t.Context(), &keyedData{}), ErrLeaseLost) || !errors.Is(l.Commit(t.Context(), &keyedData{}), ErrLeaseLost) {
		t.Fatal("canceled lease permitted data writes")
	}
	if err := l.Release(t.Context()); err != nil || writes != 1 {
		t.Fatalf("fresh cleanup failed: %d %v", writes, err)
	}
}

func TestProtocolRejectedPayloadKeepsLease(t *testing.T) {
	client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
		if w.ReturnValues == "ALL_NEW" {
			return acquireReply(w, "")
		}
		if w.ExpressionAttributeValues[":data"] != nil {
			return 400, rejectedJSON, nil
		}
		return 200, `{}`, nil
	})
	l, _, err := Lock[keyedData](t.Context(), client, protocolInput())
	if err != nil {
		t.Fatal(err)
	}
	cleanupLease(t, l)
	for _, op := range []func(context.Context, *keyedData) error{l.Update, l.Commit} {
		if !errors.Is(op(t.Context(), nil), ErrInvalidPayload) {
			t.Fatal("nil write accepted")
		}
		if !errors.Is(op(t.Context(), &keyedData{ID: "other"}), ErrInvalidPayload) {
			t.Fatal("mismatched key accepted")
		}
		if err := op(t.Context(), &keyedData{}); err == nil || errors.Is(err, ErrOutcomeUnknown) {
			t.Fatalf("definitive rejection misclassified: %v", err)
		}
		if l.Context().Err() != nil {
			t.Fatalf("known non-write lost ownership: %v", context.Cause(l.Context()))
		}
	}
}

func TestProtocolValidationDoesNotCallAWS(t *testing.T) {
	client := protocolClient(func(context.Context, wireRequest) (int, any, error) {
		t.Error("invalid input reached AWS")
		return 0, nil, errors.New("unexpected request")
	})
	if _, _, err := Lock[keyedData](t.Context(), client, nil); err == nil {
		t.Fatal("nil input accepted")
	}
	if _, _, err := Lock[int](t.Context(), client, protocolInput()); !errors.Is(err, ErrInvalidPayload) {
		t.Fatal(err)
	}
	for _, change := range []func(*LockInput){
		func(in *LockInput) { in.Table = "" },
		func(in *LockInput) { in.ID = "" },
		func(in *LockInput) { in.ID = strings.Repeat("x", 2049) },
		func(in *LockInput) { in.HeartbeatInterval = in.HeartbeatMaxAge },
		func(in *LockInput) { in.HeartbeatInterval = 0 },
		func(in *LockInput) { in.Retries = -1 },
		func(in *LockInput) { in.RetriesSleep = -1 },
	} {
		in := protocolInput()
		change(in)
		if _, _, err := Lock[keyedData](t.Context(), client, in); err == nil {
			t.Fatalf("invalid input accepted: %#v", in)
		}
	}
}

func TestProtocolReadUnwrapsIdentity(t *testing.T) {
	client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
		if w.Target != "DynamoDB_20120810.GetItem" || !w.ConsistentRead {
			return 0, nil, errors.New("Read must be a strong GetItem")
		}
		return 200, `{"Item":{"id":{"S":"key"},"data":{"M":{"value":{"S":"read"}}}}}`, nil
	})
	data, err := Read[keyedData](t.Context(), client, "table", "key")
	if err != nil || !reflect.DeepEqual(data, &keyedData{ID: "key", Value: "read"}) {
		t.Fatalf("Read: %#v %v", data, err)
	}
}

func TestProtocolRequireExistingAndContention(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprint(missing), func(t *testing.T) {
			calls := 0
			client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
				calls++
				if !strings.Contains(w.ConditionExpression, "attribute_exists(#id)") || w.ReturnValuesOnConditionCheckFailure != "ALL_OLD" {
					return 0, nil, errors.New("required existence is not conditional")
				}
				if missing {
					return 400, conditionalJSON, nil
				}
				return 400, `{"__type":"ConditionalCheckFailedException","Item":{"id":{"S":"key"},"owner_token":{"S":"other"},"expires_at":{"N":"9223372036854775807"}}}`, nil
			})
			in := protocolInput()
			in.RequireExisting, in.Retries, in.RetriesSleep = true, 2, time.Nanosecond
			_, _, err := Lock[keyedData](t.Context(), client, in)
			want, attempts := ErrLockHeld, 3
			if missing {
				want, attempts = ErrLockNotFound, 1
			}
			if !errors.Is(err, want) || !errors.Is(err, ErrLockUnavailable) || calls != attempts {
				t.Fatalf("existence/contention: %v calls=%d", err, calls)
			}
		})
	}
}

func TestProtocolAmbiguousAcquireMalformedPayloadCleansUp(t *testing.T) {
	var saved map[string]json.RawMessage
	var releases int
	client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
		if w.ReturnValues == "ALL_NEW" {
			saved = acquiredItem(w, "")
			saved["data"] = json.RawMessage(`{"S":"not a map"}`)
			return 500, serverErrorJSON, nil
		}
		if w.Target == "DynamoDB_20120810.GetItem" {
			return 200, map[string]any{"Item": saved}, nil
		}
		releases++
		return 200, `{}`, nil
	})
	l, _, err := Lock[keyedData](t.Context(), client, protocolInput())
	if l != nil || !errors.Is(err, ErrInvalidRecord) || releases != 1 {
		t.Fatalf("known ownership with malformed payload was abandoned: lease=%v releases=%d err=%v", l, releases, err)
	}
}

func TestProtocolReleaseFailureKeepsHeartbeat(t *testing.T) {
	var releases atomic.Int32
	renewed := make(chan struct{}, 1)
	client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
		if w.ReturnValues == "ALL_NEW" {
			return acquireReply(w, "")
		}
		if w.ExpressionAttributeValues[":next"] != nil {
			select {
			case renewed <- struct{}{}:
			default:
			}
			return 200, `{}`, nil
		}
		if releases.Add(1) == 1 {
			return 400, rejectedJSON, nil
		}
		return 200, `{}`, nil
	})
	in := protocolInput()
	in.HeartbeatMaxAge, in.HeartbeatInterval = time.Second, 10*time.Millisecond
	l, _, err := Lock[keyedData](t.Context(), client, in)
	if err != nil {
		t.Fatal(err)
	}
	cleanupLease(t, l)
	if err := l.Release(t.Context()); err == nil || errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("rejected release: %v", err)
	}
	await(t, renewed)
	if l.Context().Err() != nil {
		t.Fatal(context.Cause(l.Context()))
	}
	if err := l.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestProtocolContentionRetryTiming(t *testing.T) {
	for _, retries := range []int{0, 1, 2, 3} {
		t.Run(fmt.Sprint(retries), func(t *testing.T) {
			calls := 0
			client := protocolClient(func(context.Context, wireRequest) (int, any, error) { calls++; return 400, conditionalJSON, nil })
			in := protocolInput()
			in.Retries, in.RetriesSleep = retries, 10*time.Millisecond
			started := time.Now()
			_, _, err := Lock[keyedData](t.Context(), client, in)
			if !errors.Is(err, ErrLockHeld) || calls != retries+1 || time.Since(started) < time.Duration(retries)*in.RetriesSleep {
				t.Fatalf("contention retries: err=%v calls=%d elapsed=%s", err, calls, time.Since(started))
			}
		})
	}
	t.Run("default sleep is cancelable", func(t *testing.T) {
		calls := 0
		client := protocolClient(func(context.Context, wireRequest) (int, any, error) { calls++; return 400, conditionalJSON, nil })
		in := protocolInput()
		in.Retries = 1
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		_, _, err := Lock[keyedData](ctx, client, in)
		if !errors.Is(err, context.DeadlineExceeded) || calls != 1 {
			t.Fatalf("default contention delay: %v calls=%d", err, calls)
		}
	})
}

func TestProtocolCanceledCallDoesNotLoseLiveLease(t *testing.T) {
	var calls atomic.Int32
	client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
		calls.Add(1)
		if w.ReturnValues == "ALL_NEW" {
			return acquireReply(w, "")
		}
		return 200, `{}`, nil
	})
	l, _, err := Lock[keyedData](t.Context(), client, protocolInput())
	if err != nil {
		t.Fatal(err)
	}
	cleanupLease(t, l)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for range 100 {
		if err := l.Release(ctx); !errors.Is(err, context.Canceled) || errors.Is(err, ErrOutcomeUnknown) {
			t.Fatalf("already-canceled call was treated as an ambiguous write: %v", err)
		}
		if l.Context().Err() != nil {
			t.Fatalf("canceled cleanup lost a live lease: %v", context.Cause(l.Context()))
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("canceled calls reached HTTP: %d", calls.Load())
	}
}

func TestEnvelopeIdentityCannotBeShadowedByCaseAlias(t *testing.T) {
	for _, alias := range []string{"ID", "Id", "iD"} {
		t.Run(alias, func(t *testing.T) {
			valid, err := MarshalItem("key", map[string]any{alias: "key"})
			if err != nil {
				t.Fatal(err)
			}
			data, err := UnmarshalItem[keyedData](valid)
			if err != nil || data == nil || data.ID != "key" || len(valid["data"].(*types.AttributeValueMemberM).Value) != 0 {
				t.Fatalf("matching alias was not normalized to the outer identity: %#v %v", data, err)
			}
			if _, err := MarshalItem("key", map[string]any{alias: "other"}); !errors.Is(err, ErrInvalidPayload) {
				t.Fatalf("SDK's case-insensitive ID alias bypassed identity validation: %v", err)
			}
			item := key("key")
			item["data"] = &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{alias: &types.AttributeValueMemberS{Value: "other"}}}
			if _, err := UnmarshalItem[keyedData](item); !errors.Is(err, ErrInvalidRecord) {
				t.Fatalf("stored alias could override the injected identity: %v", err)
			}
		})
	}
}
