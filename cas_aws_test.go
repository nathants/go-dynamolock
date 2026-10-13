package dynamolock

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestCASAWS(t *testing.T) {
	client, name := liveTable(t)
	table := Table{Name: name, PartitionKey: "id", Attributes: []string{"pending"}}
	strong := ReadOptions{ConsistentRead: true}
	read := func(t *testing.T, key Key) (*CAS[Data], *Data) {
		t.Helper()
		c, data, err := ReadCAS[Data](t.Context(), client, table, key, strong)
		if err != nil {
			t.Fatal(err)
		}
		return c, data
	}
	stored := func(t *testing.T, key Key) record {
		t.Helper()
		item, err := getItem(t.Context(), client, table, key)
		if err != nil {
			t.Fatal(err)
		}
		r, err := parseRecord(table, item)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}

	t.Run("creation competition reuse and recreation", func(t *testing.T) {
		key := Key{Partition: "creation"}
		a, data := read(t, key)
		b, _ := read(t, key)
		if data != nil {
			t.Fatal("absent record returned data")
		}
		if err := a.Update(t.Context(), &Data{Value: "first"}); err != nil {
			t.Fatal(err)
		}
		original := stored(t, key).revision
		if original.generation == "" || original.version != 1 {
			t.Fatalf("creation revision: %+v", original)
		}
		if err := b.Update(t.Context(), &Data{Value: "loser"}); !errors.Is(err, ErrConflict) {
			t.Fatalf("stale absent read overwrote creation: %v", err)
		}
		stale, _ := read(t, key)
		if err := a.Update(t.Context(), &Data{Value: "second"}); err != nil {
			t.Fatal(err)
		}
		if r := stored(t, key).revision; r.generation != original.generation || r.version != 2 {
			t.Fatalf("update revision: %+v", r)
		}
		if err := a.Delete(t.Context()); err != nil {
			t.Fatal(err)
		}
		if r := stored(t, key); r.key != (Key{}) {
			t.Fatal("delete retained a tombstone")
		}
		recreated, _ := read(t, key)
		if err := recreated.Update(t.Context(), &Data{Value: "new incarnation"}); err != nil {
			t.Fatal(err)
		}
		if r := stored(t, key).revision; r.generation == original.generation || r.version != 1 {
			t.Fatalf("recreation reused revision: %+v", r)
		}
		if err := stale.Delete(t.Context()); !errors.Is(err, ErrConflict) {
			t.Fatalf("old generation deleted recreation: %v", err)
		}
	})

	t.Run("lease and CAS share fencing and counters", func(t *testing.T) {
		key := Key{Partition: "lease"}
		c, _ := read(t, key)
		if err := c.Update(t.Context(), &Data{Value: "initial"}); err != nil {
			t.Fatal(err)
		}
		old := stored(t, key).revision
		in := liveInput(name)
		in.Key, in.Table = key, table
		l, data, err := Lock[Data](t.Context(), client, in)
		if err != nil {
			t.Fatal(err)
		}
		cleanupLease(t, l)
		if r := stored(t, key).revision; r.generation != old.generation || r.version != 2 {
			t.Fatalf("acquisition revision: %+v", r)
		}
		busy, inspect, err := ReadCAS[Data](t.Context(), client, table, key, strong)
		if busy != nil || inspect == nil || inspect.Value != "initial" || !errors.Is(err, ErrLockHeld) {
			t.Fatalf("busy read: %v %+v %v", busy, inspect, err)
		}
		if err := c.Update(t.Context(), &Data{Value: "stale"}); !errors.Is(err, ErrConflict) {
			t.Fatalf("CAS bypassed acquisition: %v", err)
		}
		// Model a writer with the current version (e.g. a snapshot whose observed
		// lease had expired before renewal). Equality alone must not authorize it.
		fenced := &CAS[Data]{client: client, table: table, key: key, op: make(chan struct{}, 1), present: true, revision: stored(t, key).revision}
		if err := fenced.Update(t.Context(), &Data{Value: "bypass"}); !errors.Is(err, ErrConflict) {
			t.Fatalf("matching revision bypassed live lease: %v", err)
		}
		if err := l.renew(); err != nil {
			t.Fatal(err)
		}
		if r := stored(t, key).revision; r.version != 2 {
			t.Fatalf("heartbeat advanced version: %+v", r)
		}
		data.Value = "checkpoint"
		if err := l.Update(t.Context(), data); err != nil {
			t.Fatal(err)
		}
		data.Value = "committed"
		if err := l.Commit(t.Context(), data); err != nil {
			t.Fatal(err)
		}
		if r := stored(t, key); r.owner != "" || r.revision.version != 4 {
			t.Fatalf("commit revision/ownership: %+v", r)
		}
		c, data = read(t, key)
		if data.Value != "committed" {
			t.Fatalf("lease result: %+v", data)
		}
		if err := c.Update(t.Context(), &Data{Value: "CAS again"}); err != nil {
			t.Fatal(err)
		}
		l, _, err = Lock[Data](t.Context(), client, in)
		if err != nil {
			t.Fatal(err)
		}
		cleanupLease(t, l)
		if err := l.Release(t.Context()); err != nil {
			t.Fatal(err)
		}
		if r := stored(t, key).revision; r.version != 6 || r.generation != old.generation {
			t.Fatalf("release changed revision: %+v", r)
		}
		if err := c.Update(t.Context(), &Data{}); !errors.Is(err, ErrConflict) {
			t.Fatalf("acquire/release failed to invalidate CAS: %v", err)
		}
	})

	t.Run("lazy initialization and shared field preservation", func(t *testing.T) {
		key := Key{Partition: "old schema"}
		original := map[string]any{"value": "old", "future": map[string]any{"nested": "retain"}, "pending": "yes"}
		item, err := MarshalItem(table, key, original)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.PutItem(t.Context(), &dynamodb.PutItemInput{TableName: aws.String(name), Item: item}); err != nil {
			t.Fatal(err)
		}
		c, data := read(t, key)
		if data == nil || data.Value != "old" || stored(t, key).revision.version != 1 {
			t.Fatalf("initialization: %+v", data)
		}
		data.Value = "new"
		if err := c.Update(t.Context(), data); err != nil {
			t.Fatal(err)
		}
		actual, err := getItem(t.Context(), client, table, key)
		if err != nil {
			t.Fatal(err)
		}
		expectedPayload := item["data"].(*types.AttributeValueMemberM)
		expectedPayload.Value["value"] = &types.AttributeValueMemberS{Value: "new"}
		if !reflect.DeepEqual(actual["data"], expectedPayload) || !reflect.DeepEqual(actual["pending"], item["pending"]) {
			t.Fatalf("unknown fields or projection lost: %+v", actual)
		}
		full, value, err := ReadCAS[map[string]any](t.Context(), client, table, key, strong)
		if err != nil {
			t.Fatal(err)
		}
		delete(*value, "pending")
		if err := full.Update(t.Context(), value); err != nil {
			t.Fatal(err)
		}
		actual, err = getItem(t.Context(), client, table, key)
		if err != nil || actual["pending"] != nil {
			t.Fatalf("projection removal failed: %+v %v", actual, err)
		}
	})

	t.Run("lazy initializers race without replacing generation", func(t *testing.T) {
		key := Key{Partition: "initialize race"}
		item, err := MarshalItem(table, key, Data{Value: "retained"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.PutItem(t.Context(), &dynamodb.PutItemInput{TableName: aws.String(name), Item: item}); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		results := make(chan revision, 8)
		for range 8 {
			wg.Go(func() {
				out, err := initializeCAS(t.Context(), client, table, key)
				if err != nil {
					t.Error(err)
					return
				}
				r, err := parseRecord(table, out)
				if err != nil {
					t.Error(err)
					return
				}
				results <- r.revision
			})
		}
		wg.Wait()
		close(results)
		actual := stored(t, key).revision
		for r := range results {
			if r != actual || r.version != 1 {
				t.Fatalf("initializer replaced winner: %+v != %+v", r, actual)
			}
		}
	})

	t.Run("expired owner requests reach server after CAS takeover", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		holderCtx, stopHolder := context.WithCancel(ctx)
		defer stopHolder()
		in := liveInput(name)
		in.Table = table
		submitted, resume := make(chan struct{}), make(chan struct{})
		release := sync.OnceFunc(func() { close(resume) })
		var payloadCalls, responseStatus int
		var response struct {
			Type string `json:"__type"`
		}
		base := client.Options().HTTPClient
		delayed := dynamodb.New(client.Options(), func(o *dynamodb.Options) {
			o.HTTPClient = httpFunc(func(req *http.Request) (*http.Response, error) {
				w, err := readAWSRequest(req)
				if err != nil {
					return nil, err
				}
				if w.ExpressionAttributeValues[":data"] == nil {
					return base.Do(req)
				}
				payloadCalls++
				if payloadCalls != 1 {
					return nil, errors.New("stale payload request was retried")
				}
				close(submitted)
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-resume:
				}
				// The submitted request must be canceled before delivery, so this
				// test cannot pass merely by beating renewal or cancellation.
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-req.Context().Done():
				}
				// Independently bound delivery so DynamoDB, not local cancellation,
				// decides the fate of the captured stale request.
				resp, err := base.Do(req.Clone(ctx))
				if err != nil {
					return nil, err
				}
				responseStatus = resp.StatusCode
				body, readErr := io.ReadAll(resp.Body)
				if err := errors.Join(readErr, resp.Body.Close()); err != nil {
					return nil, err
				}
				resp.Body = io.NopCloser(bytes.NewReader(body))
				if err := json.Unmarshal(body, &response); err != nil {
					return nil, err
				}
				return resp, nil
			})
		})
		l, _, err := Lock[Data](holderCtx, delayed, in)
		if err != nil {
			t.Fatal(err)
		}
		cleanupLease(t, l)
		done := make(chan error, 1)
		finished := make(chan struct{})
		go func() {
			defer close(finished)
			done <- l.Update(ctx, &Data{Value: "delayed old owner"})
		}()
		t.Cleanup(func() { cancel(); release(); await(t, finished) })
		await(t, submitted)
		stopHolder()
		await(t, l.done)
		// No renewal can race the forced expiry. Model a request submitted
		// before lease loss but processed by DynamoDB after CAS takeover.
		_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(name), Key: table.key(in.Key), UpdateExpression: aws.String("SET expires_at = :past"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":past": number(time.Now().Add(-time.Minute).UnixNano())},
		})
		if err != nil {
			t.Fatal(err)
		}
		c, _ := read(t, in.Key)
		if err := c.Update(ctx, &Data{Value: "CAS winner"}); err != nil {
			t.Fatal(err)
		}
		before, err := getItem(ctx, client, table, in.Key)
		if err != nil {
			t.Fatal(err)
		}
		release()
		writeErr := <-done
		if payloadCalls != 1 || responseStatus != http.StatusBadRequest || !strings.HasSuffix(response.Type, "ConditionalCheckFailedException") {
			t.Fatalf("DynamoDB did not fence stale write: requests=%d status=%d type=%q error=%v", payloadCalls, responseStatus, response.Type, writeErr)
		}
		if !errors.Is(writeErr, ErrLeaseLost) && !errors.Is(writeErr, ErrOutcomeUnknown) {
			t.Fatalf("unexpected stale write outcome: %v", writeErr)
		}
		after, err := getItem(ctx, client, table, in.Key)
		if err != nil || !reflect.DeepEqual(after, before) {
			t.Fatalf("stale write changed the CAS winner: %v", err)
		}
		_, data := read(t, in.Key)
		if data.Value != "CAS winner" || stored(t, in.Key).owner != "" {
			t.Fatalf("takeover corrupted payload or retained ownership: %+v", data)
		}
	})

	for _, operation := range []string{"update", "delete", "initialize"} {
		t.Run("lost response "+operation, func(t *testing.T) {
			key := Key{Partition: "lost " + operation}
			if operation == "initialize" {
				item, err := MarshalItem(table, key, Data{Value: "retained"})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := client.PutItem(t.Context(), &dynamodb.PutItemInput{TableName: aws.String(name), Item: item}); err != nil {
					t.Fatal(err)
				}
			} else {
				c, _ := read(t, key)
				if err := c.Update(t.Context(), &Data{Value: "initial"}); err != nil {
					t.Fatal(err)
				}
			}
			faulty, injected := loseAWSResponse(t, client, func(w wireRequest) bool {
				return strings.HasSuffix(w.Target, ".UpdateItem") || strings.HasSuffix(w.Target, ".DeleteItem")
			})
			c, data, err := ReadCAS[Data](t.Context(), faulty, table, key, strong)
			if operation == "initialize" {
				if c != nil || !errors.Is(err, ErrOutcomeUnknown) {
					t.Fatalf("ambiguous initialization: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if operation == "delete" {
					err = c.Delete(t.Context())
				} else {
					data.Value = "applied"
					err = c.Update(t.Context(), data)
				}
				if !errors.Is(err, ErrOutcomeUnknown) || !errors.Is(c.Delete(t.Context()), ErrOutcomeUnknown) {
					t.Fatalf("ambiguous write remained usable: %v", err)
				}
			}
			if injected.Load() != 1 {
				t.Fatal("response was not lost")
			}
			r := stored(t, key)
			if operation == "delete" && r.key != (Key{}) || operation == "initialize" && r.revision.version != 1 || operation == "update" && r.revision.version != 2 {
				t.Fatalf("lost response did not apply exactly once: %+v", r)
			}
		})
	}

	t.Run("overflow cannot acquire or update but CAS can delete", func(t *testing.T) {
		key := Key{Partition: "overflow"}
		c, _ := read(t, key)
		if err := c.Update(t.Context(), &Data{}); err != nil {
			t.Fatal(err)
		}
		_, err := client.UpdateItem(t.Context(), &dynamodb.UpdateItemInput{
			TableName: aws.String(name), Key: table.key(key), UpdateExpression: aws.String("SET #v = :max"),
			ExpressionAttributeNames: map[string]string{"#v": "version"}, ExpressionAttributeValues: map[string]types.AttributeValue{":max": versionNumber(math.MaxUint64)},
		})
		if err != nil {
			t.Fatal(err)
		}
		in := liveInput(name)
		in.Key = key
		l, _, err := Lock[Data](t.Context(), client, in)
		cleanupLease(t, l)
		if !errors.Is(err, ErrVersionExhausted) {
			t.Fatalf("overflow acquisition: %v", err)
		}
		c, _ = read(t, key)
		if err := c.Update(t.Context(), &Data{}); !errors.Is(err, ErrVersionExhausted) {
			t.Fatalf("overflow update: %v", err)
		}
		if err := c.Delete(t.Context()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("CAS competitors cannot both publish one revision", func(t *testing.T) {
		key := Key{Partition: "competitors"}
		seed, _ := read(t, key)
		if err := seed.Update(t.Context(), &Data{}); err != nil {
			t.Fatal(err)
		}
		var writers []*CAS[Data]
		for range 8 {
			c, _ := read(t, key)
			writers = append(writers, c)
		}
		var successes atomic.Int32
		var wg sync.WaitGroup
		for i, c := range writers {
			wg.Go(func() {
				err := c.Update(t.Context(), &Data{Value: fmt.Sprint(i)})
				if err == nil {
					successes.Add(1)
				} else if !errors.Is(err, ErrConflict) {
					t.Error(err)
				}
			})
		}
		wg.Wait()
		if successes.Load() != 1 || stored(t, key).revision.version != 2 {
			t.Fatalf("CAS winners: %d", successes.Load())
		}
	})
}
