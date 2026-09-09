package dynamolock

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"maps"
	"math/rand"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

func liveClient(t *testing.T) *dynamodb.Client {
	t.Helper()
	expected := os.Getenv("DYNAMOLOCK_TEST_ACCOUNT")
	if expected == "" {
		t.Skip("DYNAMOLOCK_TEST_ACCOUNT is not set; skipping live AWS test")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		t.Fatal(err)
	}
	if actual := aws.ToString(identity.Account); expected != actual {
		t.Fatalf("AWS account mismatch: expected %s, got %s", expected, actual)
	}
	return dynamodb.NewFromConfig(cfg)
}

type Data struct {
	Value string `json:"value" dynamodbav:"value"`
}

func liveTable(t *testing.T) (*dynamodb.Client, string) {
	t.Helper()
	client := liveClient(t)
	reuse := os.Getenv("REUSE") != ""
	table := "test-go-dynamolock-" + Uid()
	if reuse {
		table = "go-dynamolock"
	}
	setupTable(t, client, table, reuse)
	return client, table
}

// setupTable requires an account-checked client. Keep the captured reuse mode
// for cleanup so later environment changes cannot switch clearing and deletion.
func setupTable(t *testing.T, client *dynamodb.Client, table string, reuse bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	cleanup, creationPending := false, false
	t.Cleanup(func() {
		if !cleanup {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		if err := cleanupTable(ctx, client, table, reuse, creationPending); err != nil {
			t.Errorf("cleanup test table %s: %v", table, err)
		}
	})
	input := &dynamodb.CreateTableInput{
		TableName: aws.String(table), BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash}},
	}
	out, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)})
	var absent *types.ResourceNotFoundException
	switch {
	case errors.As(err, &absent):
		// Record responsibility before creation, including a lost response.
		// Never delete an independently pre-existing disposable table.
		cleanup, creationPending = true, true
		_, err := client.CreateTable(ctx, input, noSDKRetry)
		if err != nil {
			// A definitive rejection, including another creator winning, did
			// not create a table that this fixture is responsible for.
			cleanup = !definitelyRejected(err)
			t.Fatalf("create test table %s: %v", table, err)
		}
	case err != nil:
		t.Fatalf("inspect test table %s: %v", table, err)
	case !reuse:
		t.Fatalf("refusing pre-existing disposable test table %s", table)
	case out.Table == nil || !reflect.DeepEqual(out.Table.KeySchema, input.KeySchema) || !reflect.DeepEqual(out.Table.AttributeDefinitions, input.AttributeDefinitions):
		t.Fatalf("refusing to reuse table %s with a different key schema", table)
	default:
		cleanup = true
	}
	if _, err := waitForTable(ctx, client, table); err != nil {
		t.Fatalf("wait for test table %s: %v", table, err)
	}
	creationPending = false
	if reuse {
		if err := ClearTable(ctx, client, table); err != nil {
			t.Fatalf("clear test table %s: %v", table, err)
		}
	}
}

func cleanupTable(ctx context.Context, client *dynamodb.Client, table string, reuse, creationPending bool) error {
	out, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)})
	var absent *types.ResourceNotFoundException
	if creationPending && errors.As(err, &absent) {
		// DescribeTable is eventually consistent after CreateTable. A missing
		// description cannot prove cleanup is complete while creation is pending.
		out, err = waitForTable(ctx, client, table)
		if err != nil {
			return fmt.Errorf("confirm pending table creation: %w", err)
		}
	}
	if errors.As(err, &absent) {
		return nil
	}
	if err != nil {
		return err
	}
	if out.Table == nil {
		return fmt.Errorf("missing table description")
	}
	if out.Table.TableStatus != types.TableStatusDeleting {
		if out.Table.TableStatus != types.TableStatusActive {
			if _, err := waitForTable(ctx, client, table); err != nil {
				return err
			}
		}
		if reuse {
			return ClearTable(ctx, client, table)
		}
		_, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(table)})
		if errors.As(err, &absent) {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return dynamodb.NewTableNotExistsWaiter(client).Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)}, time.Minute, func(o *dynamodb.TableNotExistsWaiterOptions) {
		o.MinDelay, o.MaxDelay = time.Second, 3*time.Second
	})
}

func ClearTable(ctx context.Context, client *dynamodb.Client, table string) error {
	deleteBatch := func(reqs []types.WriteRequest) error {
		const maxUnprocessedRetries = 20
		for attempt := 0; len(reqs) != 0; attempt++ {
			if attempt >= maxUnprocessedRetries {
				return fmt.Errorf("failed to delete %d unprocessed items from %s after %d retries", len(reqs), table, maxUnprocessedRetries)
			}
			out, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					table: reqs,
				},
			})
			if err != nil {
				return err
			}
			reqs = out.UnprocessedItems[table]
			if len(reqs) == 0 {
				return nil
			}
			delay := time.Duration(attempt+1) * 200 * time.Millisecond
			if delay > 2*time.Second {
				delay = 2 * time.Second
			}
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
		}
		return nil
	}

	pages := dynamodb.NewScanPaginator(client, &dynamodb.ScanInput{
		TableName: aws.String(table), ProjectionExpression: aws.String("id"),
		ConsistentRead: aws.Bool(true), Limit: aws.Int32(128),
	})
	for pages.HasMorePages() {
		out, err := pages.NextPage(ctx)
		if err != nil {
			return err
		}
		var reqs []types.WriteRequest
		for _, item := range out.Items {
			reqs = append(reqs, types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{
					Key: item,
				},
			})
			if len(reqs) == 25 {
				if err := deleteBatch(reqs); err != nil {
					return err
				}
				reqs = nil
			}
		}
		if len(reqs) != 0 {
			if err := deleteBatch(reqs); err != nil {
				return err
			}
		}
	}
	return nil
}

func Uid() string {
	return cryptorand.Text()
}

func waitForTable(ctx context.Context, client *dynamodb.Client, table string) (*dynamodb.DescribeTableOutput, error) {
	return dynamodb.NewTableExistsWaiter(client).WaitForOutput(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)}, 2*time.Minute, func(o *dynamodb.TableExistsWaiterOptions) {
		o.MinDelay, o.MaxDelay = time.Second, 3*time.Second
	})
}

func TestBasic(t *testing.T) {
	ctx := t.Context()
	client, table := liveTable(t)
	id := Uid()
	unlock, data, err := Lock[Data](ctx, client, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   time.Second * 30,
		HeartbeatInterval: time.Second * 1,
	})
	cleanupLease(t, unlock)
	if err != nil {
		t.Fatal(err)
	}
	if data != nil {
		t.Fatalf("data should be nil")
	}
	contender, _, err := Lock[Data](ctx, client, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   time.Second * 30,
		HeartbeatInterval: time.Second * 1,
	})
	cleanupLease(t, contender)
	if err == nil {
		t.Fatal("acquired lock twice")
	}
	err = unlock.Release(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadModifyWrite(t *testing.T) {
	type counter struct {
		Count int `dynamodbav:"count"`
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	client, table := liveTable(t)
	id := Uid()
	max := 50
	var inCriticalSection int32
	done := make(chan error, max)
	var workers sync.WaitGroup
	defer func() {
		cancel()
		workers.Wait()
	}()
	for range max {
		workers.Go(func() {
			for {
				select {
				case <-ctx.Done():
					done <- ctx.Err()
					return
				default:
				}
				lease, data, err := Lock[counter](ctx, client, &LockInput{
					Table:             table,
					ID:                id,
					HeartbeatMaxAge:   time.Second * 5,
					HeartbeatInterval: time.Second * 1,
					Retries:           5,
					RetriesSleep:      1 * time.Second,
				})
				cleanupLease(t, lease)
				if errors.Is(err, ErrLockHeld) {
					continue
				}
				if err != nil {
					done <- err
					return
				}
				if !atomic.CompareAndSwapInt32(&inCriticalSection, 0, 1) {
					done <- fmt.Errorf("lock allowed concurrent critical sections")
					return
				}
				time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
				if data == nil {
					data = &counter{}
				}
				data.Count++
				atomic.StoreInt32(&inCriticalSection, 0)
				done <- lease.Commit(lease.Context(), data)
				return
			}
		})
	}
	for range max {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	// Local exclusion is not enough: the stored counter must include every worker.
	stored, err := Read[counter](ctx, client, table, id)
	if err != nil || stored == nil || stored.Count != max {
		t.Fatalf("persisted counter = %#v, err = %v; want %d", stored, err, max)
	}
}

type testData struct {
	Value string
}

func TestLockReturnsDataFromSuccessfulAcquire(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	client, table := liveTable(t)

	id := Uid()
	unlock, _, err := Lock[Data](ctx, client, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   2 * time.Minute,
		HeartbeatInterval: time.Minute,
	})
	cleanupLease(t, unlock)
	if err != nil {
		t.Fatal(err)
	}
	err = unlock.Commit(ctx, &Data{Value: "old"})
	if err != nil {
		t.Fatal(err)
	}

	seenAcquireAttempt := make(chan struct{}, 1)
	releaseAcquire := make(chan struct{})
	releaseDelayedAcquire := sync.OnceFunc(func() { close(releaseAcquire) })
	delayedClient := dynamodb.New(client.Options(), func(options *dynamodb.Options) {
		options.HTTPClient = lockAcquireDelayClient{
			base:               options.HTTPClient,
			seenAcquireAttempt: seenAcquireAttempt,
			releaseAcquire:     releaseAcquire,
		}
	})

	contenderCtx, cancelContender := context.WithCancel(ctx)
	finished := make(chan struct{})
	defer func() {
		cancelContender()
		releaseDelayedAcquire()
		<-finished
	}()
	lockResult := make(chan struct {
		unlock *Lease[Data]
		data   *Data
		err    error
	}, 1)
	go func() {
		defer close(finished)
		delayedCtx := context.WithValue(contenderCtx, delayAcquireContextKey{}, true)
		unlock, data, err := Lock[Data](delayedCtx, delayedClient, &LockInput{
			Table:             table,
			ID:                id,
			HeartbeatMaxAge:   30 * time.Second,
			HeartbeatInterval: 15 * time.Second,
		})
		cleanupLease(t, unlock)
		lockResult <- struct {
			unlock *Lease[Data]
			data   *Data
			err    error
		}{unlock: unlock, data: data, err: err}
	}()

	select {
	case <-seenAcquireAttempt:
	case <-ctx.Done():
		t.Fatal("timed out waiting for contender's acquire attempt")
	}

	otherUnlock, data, err := Lock[Data](ctx, client, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   2 * time.Minute,
		HeartbeatInterval: time.Minute,
	})
	cleanupLease(t, otherUnlock)
	if err != nil {
		t.Fatal(err)
	}
	if data == nil || data.Value != "old" {
		t.Fatalf("expected other holder to read old data, got %#v", data)
	}
	err = otherUnlock.Commit(ctx, &Data{Value: "fresh"})
	if err != nil {
		t.Fatal(err)
	}
	releaseDelayedAcquire()

	select {
	case result := <-lockResult:
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.data == nil {
			t.Fatal("data is nil")
		}
		if result.data.Value != "fresh" {
			t.Fatalf("expected fresh data from successful acquisition, got %q", result.data.Value)
		}
		err = result.unlock.Commit(ctx, result.data)
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for contender lock result")
	}
}

type delayAcquireContextKey struct{}

type lockAcquireDelayClient struct {
	base               dynamodb.HTTPClient
	seenAcquireAttempt chan<- struct{}
	releaseAcquire     <-chan struct{}
}

func (c lockAcquireDelayClient) Do(req *http.Request) (*http.Response, error) {
	if req.Context().Value(delayAcquireContextKey{}) != true {
		return c.base.Do(req)
	}
	if req.Header.Get("X-Amz-Target") == "DynamoDB_20120810.UpdateItem" {
		select {
		case c.seenAcquireAttempt <- struct{}{}:
		default:
		}
		select {
		case <-c.releaseAcquire:
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	}
	return c.base.Do(req)
}

func TestData(t *testing.T) {
	ctx := t.Context()
	client, table := liveTable(t)
	id := Uid()
	unlock, data, err := Lock[testData](ctx, client, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   time.Second * 30,
		HeartbeatInterval: time.Second * 1,
	})
	cleanupLease(t, unlock)
	if err != nil {
		t.Fatal(err)
	}
	if data != nil {
		t.Fatal("data not nil")
	}
	err = unlock.Commit(ctx, &testData{Value: "asdf"})
	if err != nil {
		t.Fatal(err)
	}
	unlock, data, err = Lock[testData](ctx, client, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   time.Second * 30,
		HeartbeatInterval: time.Second * 1,
	})
	cleanupLease(t, unlock)
	if err != nil {
		t.Fatal(err)
	}
	if data == nil {
		t.Fatal("data is nil")
	} else if data.Value != "asdf" {
		t.Fatal("data mismatch")
	}
	read, err := Read[testData](ctx, client, table, id)
	if err != nil {
		t.Fatal(err)
	}
	if read == nil {
		t.Fatal("read is nil")
	} else if read.Value != "asdf" {
		t.Fatal("read mismatch")
	}
	err = unlock.Commit(ctx, &testData{Value: "123"})
	if err != nil {
		t.Fatal(err)
	}
	unlock, data, err = Lock[testData](ctx, client, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   time.Second * 30,
		HeartbeatInterval: time.Second * 1,
	})
	cleanupLease(t, unlock)
	if err != nil {
		t.Fatal(err)
	}
	if data == nil {
		t.Fatal("data is nil")
	} else if data.Value != "123" {
		t.Fatal("data mismatch")
	}
	err = unlock.Commit(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
}

func TestLockRequireExisting(t *testing.T) {
	ctx := t.Context()
	client, table := liveTable(t)

	id := "require-existing"
	missing, _, err := Lock[Data](ctx, client, &LockInput{
		Table:             table,
		ID:                id,
		RequireExisting:   true,
		HeartbeatMaxAge:   30 * time.Second,
		HeartbeatInterval: time.Second,
		Retries:           2,
		RetriesSleep:      time.Nanosecond,
	})
	cleanupLease(t, missing)
	if !errors.Is(err, ErrLockNotFound) {
		t.Fatalf("expected ErrLockNotFound, got: %v", err)
	}
	if !errors.Is(err, ErrLockUnavailable) {
		t.Fatalf("expected missing item to also match ErrLockUnavailable, got: %v", err)
	}
	if errors.Is(err, ErrLockHeld) {
		t.Fatalf("missing item incorrectly matched ErrLockHeld: %v", err)
	}
	data, err := Read[Data](ctx, client, table, id)
	if err != nil {
		t.Fatal(err)
	}
	if data != nil {
		t.Fatalf("required acquisition created missing item: %#v", data)
	}

	unlock, data, err := Lock[Data](ctx, client, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   30 * time.Second,
		HeartbeatInterval: time.Second,
	})
	cleanupLease(t, unlock)
	if err != nil {
		t.Fatal(err)
	}
	if data != nil {
		t.Fatalf("default acquisition returned data for a missing item: %#v", data)
	}
	if err := unlock.Commit(ctx, &Data{Value: "existing"}); err != nil {
		t.Fatal(err)
	}

	unlock, data, err = Lock[Data](ctx, client, &LockInput{
		Table:             table,
		ID:                id,
		RequireExisting:   true,
		HeartbeatMaxAge:   30 * time.Second,
		HeartbeatInterval: time.Second,
	})
	cleanupLease(t, unlock)
	if err != nil {
		t.Fatal(err)
	}
	if data == nil || data.Value != "existing" {
		t.Fatalf("required acquisition returned wrong existing data: %#v", data)
	}

	contender, _, err := Lock[Data](ctx, client, &LockInput{
		Table:             table,
		ID:                id,
		RequireExisting:   true,
		HeartbeatMaxAge:   30 * time.Second,
		HeartbeatInterval: time.Second,
	})
	cleanupLease(t, contender)
	if !errors.Is(err, ErrLockHeld) {
		t.Fatalf("expected held item to match ErrLockHeld, got: %v", err)
	}
	if !errors.Is(err, ErrLockUnavailable) {
		t.Fatalf("expected held item to also match ErrLockUnavailable, got: %v", err)
	}
	if errors.Is(err, ErrLockNotFound) {
		t.Fatalf("held item incorrectly matched ErrLockNotFound: %v", err)
	}
	if err := unlock.Commit(ctx, data); err != nil {
		t.Fatal(err)
	}
}

type preExistingData struct {
	ID    string `json:"id" dynamodbav:"id"`
	Value string `json:"value" dynamodbav:"value"`
}

func TestPreExistingData(t *testing.T) {
	ctx := t.Context()
	client, table := liveTable(t)
	item, err := MarshalItem("test-id", preExistingData{
		ID:    "test-id",
		Value: "test-value",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item:      item,
	})
	if err != nil {
		t.Fatal(err)
	}
	id := "test-id"
	unlock, data, err := Lock[Data](ctx, client, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   time.Second * 30,
		HeartbeatInterval: time.Second * 1,
	})
	cleanupLease(t, unlock)
	if err != nil {
		t.Fatal(err)
	}
	if data == nil {
		t.Fatal("data is nil")
	} else if data.Value != "test-value" {
		t.Fatal("wrong value")
	}
	contender, _, err := Lock[Data](ctx, client, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   time.Second * 30,
		HeartbeatInterval: time.Second * 1,
	})
	cleanupLease(t, contender)
	if !errors.Is(err, ErrLockHeld) {
		t.Fatalf("contending acquisition = %v, want ErrLockHeld", err)
	}
	err = unlock.Commit(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := Read[preExistingData](ctx, client, table, id)
	if err != nil || stored == nil || stored.ID != id || stored.Value != "test-value" {
		t.Fatalf("commit did not preserve the pre-existing payload: %#v %v", stored, err)
	}
}

func TestWriteWithoutUnlocking(t *testing.T) {
	ctx := t.Context()
	client, table := liveTable(t)
	id := "test-id"
	unlock, data, err := Lock[Data](ctx, client, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   time.Second * 30,
		HeartbeatInterval: time.Second * 1,
	})
	cleanupLease(t, unlock)
	if err != nil {
		t.Fatal(err)
	}
	if data != nil {
		t.Fatalf("data not nil")
	}
	data = &Data{Value: "asdf"}
	time.Sleep(2 * time.Second)
	err = unlock.Update(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	read, err := Read[Data](ctx, client, table, id)
	if err != nil {
		t.Fatal(err)
	}
	if read == nil {
		t.Fatal("read is nil")
	} else if read.Value != "asdf" {
		t.Fatal("wrong value")
	}
	data.Value = "foo"
	time.Sleep(2 * time.Second)
	err = unlock.Update(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	read, err = Read[Data](ctx, client, table, id)
	if err != nil {
		t.Fatal(err)
	}
	if read == nil {
		t.Fatal("read is nil")
	} else if read.Value != "foo" {
		t.Fatal("wrong value")
	}
	data.Value = "bar"
	time.Sleep(2 * time.Second)
	err = unlock.Update(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	err = unlock.Commit(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	read, err = Read[Data](ctx, client, table, id)
	if err != nil {
		t.Fatal(err)
	}
	if read == nil {
		t.Fatal("read is nil")
	} else if read.Value != "bar" {
		t.Fatal("wrong value")
	}
	read, err = Read[Data](ctx, client, table, "404")
	if err != nil {
		t.Fatal(err)
	}
	if read != nil {
		t.Fatal("data should be empty")
	}
}

func TestUnlockTwiceFails(t *testing.T) {
	ctx := t.Context()
	client, table := liveTable(t)
	id := Uid()
	unlock, data, err := Lock[Data](ctx, client, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   10 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})
	cleanupLease(t, unlock)
	if err != nil {
		t.Fatal(err)
	}
	if data != nil {
		t.Fatalf("data should be nil for new item")
	}
	err = unlock.Commit(ctx, &Data{Value: "temp"})
	if err != nil {
		t.Fatal(err)
	}
	err = unlock.Commit(ctx, &Data{Value: "temp"})
	if err == nil {
		t.Fatalf("unlock twice should error")
	}
}

func TestContextCancelBeforeLock(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	client, table := liveTable(t)
	id := Uid()
	unlock, _, err := Lock[Data](ctx, client, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   2 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})
	cleanupLease(t, unlock)
	if err == nil {
		t.Fatal("expected error when context is already canceled")
	}
}

func TestContextCancelExpired(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	client, table := liveTable(t)
	id := Uid()
	firstCtx, cancelFirst := context.WithCancel(ctx)
	defer cancelFirst()
	first, _, err := Lock[Data](firstCtx, client, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   3 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})
	cleanupLease(t, first)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Update(first.Context(), &Data{Value: "original"}); err != nil {
		t.Fatal(err)
	}
	cancelFirst()
	await(t, first.done)
	time.Sleep(5 * time.Second)
	second, data, err := Lock[Data](ctx, client, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   3 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})
	cleanupLease(t, second)
	if err != nil {
		t.Fatalf("context was canceled, lock should have expired: %v", err)
	}
	if data == nil || data.Value != "original" {
		t.Fatalf("takeover changed the stored payload: %#v", data)
	}
	if err := second.Update(second.Context(), &Data{Value: "successor"}); err != nil {
		t.Fatal(err)
	}
	if err := first.Update(ctx, &Data{Value: "stale"}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale Update = %v, want ErrLeaseLost", err)
	}
	if err := first.Commit(ctx, &Data{Value: "stale"}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale Commit = %v, want ErrLeaseLost", err)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatal(err)
	}
	stored, err := Read[Data](ctx, client, table, id)
	if err != nil || stored == nil || stored.Value != "successor" {
		t.Fatalf("stale handle changed the successor's payload: %#v %v", stored, err)
	}
	if err := second.Commit(second.Context(), &Data{Value: "committed"}); err != nil {
		t.Fatalf("stale cleanup disturbed successor ownership: %v", err)
	}
	stored, err = Read[Data](ctx, client, table, id)
	if err != nil || stored == nil || stored.Value != "committed" {
		t.Fatalf("successor commit did not persist: %#v %v", stored, err)
	}
}

func TestCommitEmptyData(t *testing.T) {
	ctx := t.Context()
	client, table := liveTable(t)
	id := Uid()
	unlock, data, err := Lock[Data](ctx, client, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   10 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})
	cleanupLease(t, unlock)
	if err != nil {
		t.Fatal(err)
	}
	if data != nil {
		t.Fatalf("expected nil data for a fresh lock")
	}
	err = unlock.Commit(ctx, &Data{})
	if err != nil {
		t.Fatalf("expected no error committing empty data, got: %v", err)
	}
	read, err := Read[Data](ctx, client, table, id)
	if err != nil {
		t.Fatal(err)
	}
	if read == nil {
		t.Fatalf("expected data")
	}
	if read.Value != "" {
		t.Fatalf("expected zero value")
	}
}

func TestUpdateEmptyData(t *testing.T) {
	ctx := t.Context()
	client, table := liveTable(t)
	id := Uid()
	unlock, data, err := Lock[Data](ctx, client, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   10 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})
	cleanupLease(t, unlock)
	if err != nil {
		t.Fatal(err)
	}
	if data != nil {
		t.Fatalf("expected nil data for a fresh lock")
	}
	err = unlock.Update(ctx, &Data{})
	if err != nil {
		t.Fatalf("expected no error updating with empty data, got: %v", err)
	}
	read, err := Read[Data](ctx, client, table, id)
	if err != nil {
		t.Fatal(err)
	}
	if read == nil {
		t.Fatalf("expected data")
	}
	if read.Value != "" {
		t.Fatalf("expected zero value")
	}
	err = unlock.Release(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestLockSucceedsAfterRetryWhenExpires(t *testing.T) {
	ctx := t.Context()
	client, table := liveTable(t)
	id := Uid()

	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	first, _, err := Lock[Data](cancelCtx, client, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   3 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})
	cleanupLease(t, first)
	if err != nil {
		t.Fatal(err)
	}
	// Stop renewal without releasing: acquisition must wait for the stored expiry.
	cancel()

	unlock2, _, err := Lock[Data](ctx, client, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   3 * time.Second,
		HeartbeatInterval: 1 * time.Second,
		Retries:           10,
		RetriesSleep:      500 * time.Millisecond,
	})
	cleanupLease(t, unlock2)
	if err != nil {
		t.Fatalf("expected lock acquisition to succeed after expiration, got: %v", err)
	}
	if err := unlock2.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func liveInput(table string) *LockInput {
	return &LockInput{Table: table, ID: Uid(), HeartbeatMaxAge: 2 * time.Second, HeartbeatInterval: 200 * time.Millisecond}
}

// Inspect a request for fault injection without consuming the body sent to AWS.
func readAWSRequest(req *http.Request) (wireRequest, error) {
	var w wireRequest
	body, readErr := io.ReadAll(req.Body)
	if err := errors.Join(readErr, req.Body.Close()); err != nil {
		return w, err
	}
	req.Body = io.NopCloser(bytes.NewReader(body))
	if err := json.Unmarshal(body, &w); err != nil {
		return w, err
	}
	w.Target = req.Header.Get("X-Amz-Target")
	return w, nil
}

// loseAWSResponse lets DynamoDB apply one matching request, then substitutes an
// error for the successful response. Failing before the write would not exercise
// reconciliation of an ambiguous result.
func loseAWSResponse(t *testing.T, client *dynamodb.Client, match func(wireRequest) bool) (*dynamodb.Client, *atomic.Int32) {
	t.Helper()
	injected := &atomic.Int32{}
	base := client.Options().HTTPClient
	return dynamodb.New(client.Options(), func(o *dynamodb.Options) {
		o.HTTPClient = httpFunc(func(req *http.Request) (*http.Response, error) {
			w, err := readAWSRequest(req)
			if err != nil {
				return nil, err
			}
			resp, err := base.Do(req)
			if err != nil || resp.StatusCode != http.StatusOK || !match(w) || !injected.CompareAndSwap(0, 1) {
				return resp, err
			}
			if _, err := io.Copy(io.Discard, resp.Body); err != nil {
				_ = resp.Body.Close()
				return nil, err
			}
			if err := resp.Body.Close(); err != nil {
				return nil, err
			}
			return &http.Response{
				StatusCode: 500, Request: req,
				Header: http.Header{"Content-Type": {"application/x-amz-json-1.0"}, "X-Amz-Crc32": {strconv.FormatUint(uint64(crc32.ChecksumIEEE([]byte(serverErrorJSON))), 10)}},
				Body:   io.NopCloser(strings.NewReader(serverErrorJSON)),
			}, nil
		})
	}), injected
}

func TestLeaseAWS(t *testing.T) {
	client, table := liveTable(t)
	t.Run("release preserves raw payload and identity", func(t *testing.T) {
		in := liveInput(table)
		item, err := MarshalItem(in.ID, keyedData{Value: "retained"})
		if err != nil {
			t.Fatal(err)
		}
		item["data"].(*types.AttributeValueMemberM).Value["unknown"] = &types.AttributeValueMemberN{Value: "123456789012345678901234567890"}
		if _, err := client.PutItem(t.Context(), &dynamodb.PutItemInput{TableName: aws.String(table), Item: item, ConditionExpression: aws.String("attribute_not_exists(id)")}); err != nil {
			t.Fatal(err)
		}
		in.RequireExisting = true
		l, data, err := Lock[keyedData](t.Context(), client, in)
		if err != nil {
			t.Fatal(err)
		}
		cleanupLease(t, l)
		if data == nil || data.ID != in.ID || data.Value != "retained" {
			t.Fatalf("acquired payload: %#v", data)
		}
		if err := l.Release(t.Context()); err != nil {
			t.Fatal(err)
		}
		actual, err := getItem(t.Context(), client, table, in.ID)
		if err != nil || !reflect.DeepEqual(actual, item) {
			t.Fatalf("release changed raw payload: %#v %v", actual, err)
		}
		l, data, err = Lock[keyedData](t.Context(), client, in)
		if err != nil {
			t.Fatal(err)
		}
		cleanupLease(t, l)
		data.Value = "checkpoint"
		if err := l.Update(t.Context(), data); err != nil {
			t.Fatal(err)
		}
		read, err := Read[keyedData](t.Context(), client, table, in.ID)
		if err != nil || read == nil || *read != *data {
			t.Fatalf("checkpoint not visible while held: %#v %v", read, err)
		}
		contender := *in
		// Leave enough request budget for a real network round trip while
		// still using a much shorter lease than the current holder.
		contender.HeartbeatMaxAge, contender.HeartbeatInterval = 500*time.Millisecond, time.Millisecond
		if other, _, err := Lock[keyedData](t.Context(), client, &contender); !errors.Is(err, ErrLockHeld) {
			if other != nil {
				cleanupLease(t, other)
			}
			t.Fatalf("contender overrode the holder's expiry: %v", err)
		}
		if err := l.Commit(t.Context(), &keyedData{Value: "committed"}); err != nil {
			t.Fatal(err)
		}
		actual, err = getItem(t.Context(), client, table, in.ID)
		if err != nil {
			t.Fatal(err)
		}
		want, err := MarshalItem(in.ID, keyedData{Value: "committed"})
		if err != nil || !reflect.DeepEqual(actual, want) {
			t.Fatalf("commit failed whole-payload replacement: %#v %v", actual, err)
		}
	})
	t.Run("holder expiry and stale handle", func(t *testing.T) {
		in := liveInput(table)
		in.HeartbeatMaxAge, in.HeartbeatInterval = 350*time.Millisecond, 200*time.Millisecond
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		first, _, err := Lock[keyedData](ctx, client, in)
		if err != nil {
			t.Fatal(err)
		}
		cleanupLease(t, first)
		cancel()
		await(t, first.done)
		time.Sleep(400 * time.Millisecond)
		in.HeartbeatMaxAge, in.HeartbeatInterval = 2*time.Second, time.Second
		second, _, err := Lock[keyedData](t.Context(), client, in)
		if err != nil {
			t.Fatal(err)
		}
		cleanupLease(t, second)
		if !errors.Is(first.Update(t.Context(), &keyedData{Value: "stale"}), ErrLeaseLost) {
			t.Fatal("stale handle wrote data")
		}
		if err := first.Release(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := second.Commit(t.Context(), &keyedData{Value: "successor"}); err != nil {
			t.Fatal(err)
		}
		read, err := Read[keyedData](t.Context(), client, table, in.ID)
		if err != nil || read == nil || read.Value != "successor" {
			t.Fatalf("stale cleanup disturbed successor: %#v %v", read, err)
		}
	})
	for _, operation := range []string{"update", "commit"} {
		t.Run("delayed stale "+operation, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			firstCtx, cancelFirst := context.WithCancel(ctx)
			entered, resumeRequest := make(chan struct{}), make(chan struct{})
			resume := sync.OnceFunc(func() { close(resumeRequest) })
			var workers sync.WaitGroup
			defer func() {
				cancelFirst()
				cancel()
				resume()
				workers.Wait()
			}()

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
					close(entered)
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-resumeRequest:
					}
					// Model a request accepted before cancellation but processed
					// after takeover. A separate bounded context lets DynamoDB,
					// rather than client cancellation, decide this write's fate.
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
			in := liveInput(table)
			first, _, err := Lock[keyedData](firstCtx, delayed, in)
			cleanupLease(t, first)
			if err != nil {
				t.Fatal(err)
			}
			write := first.Update
			if operation == "commit" {
				write = first.Commit
			}
			result := make(chan error, 1)
			workers.Go(func() { result <- write(first.Context(), &keyedData{Value: "stale"}) })
			await(t, entered)
			cancelFirst()
			await(t, first.done)

			successorInput := *in
			successorInput.HeartbeatMaxAge, successorInput.HeartbeatInterval = 10*time.Second, time.Second
			successorInput.RequireExisting, successorInput.Retries, successorInput.RetriesSleep = true, 50, 100*time.Millisecond
			second, _, err := Lock[keyedData](ctx, client, &successorInput)
			cleanupLease(t, second)
			if err != nil {
				t.Fatal(err)
			}
			if err := second.Update(ctx, &keyedData{Value: "successor"}); err != nil {
				t.Fatal(err)
			}
			before, err := getItem(ctx, client, table, in.ID)
			if err != nil {
				t.Fatal(err)
			}
			r, err := parseRecord(before)
			if err != nil || r.owner != second.owner || r.data == nil {
				t.Fatalf("successor did not establish ownership and payload: %v", err)
			}

			resume()
			var writeErr error
			select {
			case writeErr = <-result:
			case <-ctx.Done():
				t.Fatal("timed out waiting for the stale write response")
			}
			// A canceled SDK call alone proves nothing: inspect the real service
			// response even if cancellation masks it in the caller's error.
			if payloadCalls != 1 || responseStatus != http.StatusBadRequest || !strings.HasSuffix(response.Type, "ConditionalCheckFailedException") {
				t.Fatalf("DynamoDB did not fence stale %s: requests=%d status=%d type=%q", operation, payloadCalls, responseStatus, response.Type)
			}
			if !errors.Is(writeErr, ErrLeaseLost) && !errors.Is(writeErr, ErrOutcomeUnknown) {
				t.Fatalf("stale in-flight %s result: %v", operation, writeErr)
			}
			after, err := getItem(ctx, client, table, in.ID)
			if err != nil || !reflect.DeepEqual(after["data"], before["data"]) || !reflect.DeepEqual(after["owner_token"], before["owner_token"]) {
				t.Fatalf("stale %s changed successor payload or ownership: %v", operation, err)
			}
			if err := second.Commit(ctx, &keyedData{Value: "committed"}); err != nil {
				t.Fatalf("successor could not commit after stale %s: %v", operation, err)
			}
		})
	}
	t.Run("renewal after ambiguous applied heartbeat", func(t *testing.T) {
		in := liveInput(table)
		faulty, injected := loseAWSResponse(t, client, func(w wireRequest) bool { return w.ExpressionAttributeValues[":next"] != nil })
		l, _, err := Lock[keyedData](t.Context(), faulty, in)
		if err != nil {
			t.Fatal(err)
		}
		cleanupLease(t, l)
		before, err := getItem(t.Context(), client, table, in.ID)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(2300 * time.Millisecond)
		if l.Context().Err() != nil || injected.Load() != 1 {
			t.Fatalf("renewal stopped: injected=%d cause=%v", injected.Load(), context.Cause(l.Context()))
		}
		after, err := getItem(t.Context(), client, table, in.ID)
		if err != nil {
			t.Fatal(err)
		}
		oldRecord, _ := parseRecord(before)
		newRecord, err := parseRecord(after)
		if err != nil || newRecord.expires <= oldRecord.expires {
			t.Fatalf("expiry did not advance: %d %d %v", oldRecord.expires, newRecord.expires, err)
		}
		if err := l.Release(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("ownership loss cancels work", func(t *testing.T) {
		in := liveInput(table)
		l, _, err := Lock[keyedData](t.Context(), client, in)
		if err != nil {
			t.Fatal(err)
		}
		cleanupLease(t, l)
		_, err = client.UpdateItem(t.Context(), &dynamodb.UpdateItemInput{
			TableName: aws.String(table), Key: key(in.ID),
			UpdateExpression:          aws.String("SET owner_token = :other"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":other": &types.AttributeValueMemberS{Value: "successor"}},
		})
		if err != nil {
			t.Fatal(err)
		}
		await(t, l.Context().Done())
		if !errors.Is(context.Cause(l.Context()), ErrLeaseLost) || !errors.Is(l.Update(t.Context(), &keyedData{}), ErrLeaseLost) {
			t.Fatalf("ownership loss was not terminal: %v", context.Cause(l.Context()))
		}
		if err := l.Release(t.Context()); err != nil {
			t.Fatal(err)
		}
		item, err := getItem(t.Context(), client, table, in.ID)
		if err != nil {
			t.Fatal(err)
		}
		record, err := parseRecord(item)
		if err != nil || record.owner != "successor" {
			t.Fatalf("cleanup cleared the successor token: %v", err)
		}
	})
	for _, operation := range []string{"acquire", "commit", "release"} {
		t.Run("ambiguous "+operation, func(t *testing.T) {
			in := liveInput(table)
			faulty, injected := loseAWSResponse(t, client, func(w wireRequest) bool {
				switch operation {
				case "acquire":
					return w.ReturnValues == "ALL_NEW"
				case "commit":
					return strings.HasPrefix(w.UpdateExpression, "SET #data") && strings.Contains(w.UpdateExpression, "REMOVE")
				case "release":
					return w.UpdateExpression == "REMOVE #owner, #expires"
				default:
					return false
				}
			})
			l, _, err := Lock[keyedData](t.Context(), faulty, in)
			if err != nil {
				t.Fatal(err)
			}
			cleanupLease(t, l)
			if operation == "commit" {
				err = l.Commit(t.Context(), &keyedData{Value: "actually committed"})
				if !errors.Is(err, ErrOutcomeUnknown) {
					t.Fatalf("lost commit response was not ambiguous: %v", err)
				}
				data, err := Read[keyedData](t.Context(), client, table, in.ID)
				if err != nil || data == nil || data.Value != "actually committed" {
					t.Fatalf("fault was not post-commit: %#v %v", data, err)
				}
			}
			if err := l.Release(t.Context()); err != nil {
				t.Fatal(err)
			}
			if injected.Load() != 1 {
				t.Fatal("fault was not injected")
			}
			item, err := getItem(t.Context(), client, table, in.ID)
			if err != nil {
				t.Fatal(err)
			}
			if _, held := item["owner_token"]; held {
				t.Fatal("ownership remained after release")
			}
		})
	}
	t.Run("payload decode cleanup", func(t *testing.T) {
		in := liveInput(table)
		item, err := MarshalItem(in.ID, map[string]any{"value": map[string]any{"nested": "invalid string"}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.PutItem(t.Context(), &dynamodb.PutItemInput{TableName: aws.String(table), Item: item}); err != nil {
			t.Fatal(err)
		}
		l, _, err := Lock[keyedData](t.Context(), client, in)
		if l != nil || err == nil {
			t.Fatalf("malformed payload unexpectedly acquired: %v", err)
		}
		actual, err := getItem(t.Context(), client, table, in.ID)
		if err != nil || !reflect.DeepEqual(actual, maps.Clone(item)) {
			t.Fatalf("decode cleanup failed to preserve record: %#v %v", actual, err)
		}
	})
}
