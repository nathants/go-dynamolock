package dynamolock

import (
	"bytes"
	"context"
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
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gofrs/uuid"
	"github.com/nathants/libaws/lib"
)

// Existing live-test setup is kept here; production code has no global client.
var dynamoDBClient = lib.DynamoDBClient

func checkAccount(t *testing.T) {
	t.Helper()
	_ = rand.Float32
	_ = attributevalue.Marshal
	_ = strings.Replace
	expected := os.Getenv("DYNAMOLOCK_TEST_ACCOUNT")
	if expected == "" {
		t.Skip("DYNAMOLOCK_TEST_ACCOUNT is not set; skipping live AWS test")
	}
	account, err := lib.StsAccount(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if expected != account {
		t.Fatalf("%s != %s", expected, account)
	}
}

type Data struct {
	Value string `json:"value" dynamodbav:"value"`
}

// helper functions for reusing a fixed test table when the REUSE env is set
func getTableName() string {
	if os.Getenv("REUSE") != "" {
		return "go-dynamolock"
	}
	return "test-go-dynamolock-" + uuid.Must(uuid.NewV4()).String()
}

func ClearTable(ctx context.Context, table string) error {
	deleteBatch := func(reqs []types.WriteRequest) error {
		const maxUnprocessedRetries = 20
		for attempt := 0; len(reqs) != 0; attempt++ {
			if attempt >= maxUnprocessedRetries {
				return fmt.Errorf("failed to delete %d unprocessed items from %s after %d retries", len(reqs), table, maxUnprocessedRetries)
			}
			out, err := lib.DynamoDBClient().BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
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

	for {
		out, err := lib.DynamoDBClient().Scan(ctx, &dynamodb.ScanInput{
			TableName:            aws.String(table),
			ProjectionExpression: aws.String("id"),
			Limit:                aws.Int32(128),
		})
		if err != nil {
			return err
		}
		if len(out.Items) == 0 {
			return nil
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
		if len(out.LastEvaluatedKey) == 0 {
			return nil
		}
	}
}

func teardown(table string) {
	ctx := context.Background()
	if os.Getenv("REUSE") != "" {
		_ = ClearTable(ctx, table)
	} else {
		_ = lib.DynamoDBDeleteTable(ctx, table, false, false)
	}
}

func Uid() string {
	return uuid.Must(uuid.NewV4()).String()
}

func setup(t *testing.T, table string) error {
	t.Helper()
	checkAccount(t)
	input := &dynamodb.CreateTableInput{
		TableName:   aws.String(table),
		BillingMode: types.BillingModePayPerRequest,
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled: aws.Bool(false),
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       types.KeyTypeHash,
			},
		},
	}
	err := lib.DynamoDBEnsure(context.Background(), input, nil, false)
	if err != nil {
		return err
	}
	if os.Getenv("REUSE") != "" {
		err := lib.DynamoDBWaitForReady(context.Background(), table)
		if err != nil {
			return err
		}
		return ClearTable(context.Background(), table)
	}
	return nil
}

func TestBasic(t *testing.T) {
	ctx := context.Background()
	table := getTableName()
	err := setup(t, table)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	err = lib.DynamoDBWaitForReady(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	id := Uid()
	unlock, data, err := Lock[Data](ctx, dynamoDBClient(), &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   time.Second * 30,
		HeartbeatInterval: time.Second * 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if data != nil {
		t.Fatalf("data should be nil")
	}
	_, _, err = Lock[Data](ctx, dynamoDBClient(), &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   time.Second * 30,
		HeartbeatInterval: time.Second * 1,
	})
	if err == nil {
		t.Fatal("acquired lock twice")
	}
	err = unlock.Release(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadModifyWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	table := getTableName()
	err := setup(t, table)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	err = lib.DynamoDBWaitForReady(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	id := Uid()
	max := 50
	var sum int32
	var inCriticalSection int32
	done := make(chan error, max)
	for range max {
		go func() {
			for {
				select {
				case <-ctx.Done():
					done <- ctx.Err()
					return
				default:
				}
				unlock, _, err := Lock[Data](ctx, dynamoDBClient(), &LockInput{
					Table:             table,
					ID:                id,
					HeartbeatMaxAge:   time.Second * 5,
					HeartbeatInterval: time.Second * 1,
					Retries:           5,
					RetriesSleep:      1 * time.Second,
				})
				if err != nil {
					continue
				}
				if !atomic.CompareAndSwapInt32(&inCriticalSection, 0, 1) {
					_ = unlock.Release(ctx)
					done <- fmt.Errorf("lock allowed concurrent critical sections")
					return
				}
				time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
				newSum := atomic.AddInt32(&sum, 1)
				lib.Logger.Println("releasing lock, sum:", newSum)
				atomic.StoreInt32(&inCriticalSection, 0)
				err = unlock.Release(ctx)
				if err != nil {
					done <- err
					return
				}
				done <- nil
				return
			}
		}()
	}
	for range max {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if got := atomic.LoadInt32(&sum); got != int32(max) {
		t.Errorf("expected %d, got %d", max, got)
	}
}

type testData struct {
	Value string
}

func TestLockReturnsDataFromSuccessfulAcquire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	table := getTableName()
	err := setup(t, table)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	err = lib.DynamoDBWaitForReady(ctx, table)
	if err != nil {
		t.Fatal(err)
	}

	id := Uid()
	unlock, _, err := Lock[Data](ctx, dynamoDBClient(), &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   2 * time.Minute,
		HeartbeatInterval: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = unlock.Commit(ctx, &Data{Value: "old"})
	if err != nil {
		t.Fatal(err)
	}

	originalClient := dynamoDBClient
	defer func() { dynamoDBClient = originalClient }()

	seenAcquireAttempt := make(chan struct{}, 1)
	releaseAcquire := make(chan struct{})
	releaseDelayedAcquire := sync.OnceFunc(func() { close(releaseAcquire) })
	t.Cleanup(releaseDelayedAcquire)
	dynamoDBClient = func() *dynamodb.Client {
		client := originalClient()
		return dynamodb.New(client.Options(), func(options *dynamodb.Options) {
			options.HTTPClient = lockAcquireDelayClient{
				base:               options.HTTPClient,
				seenAcquireAttempt: seenAcquireAttempt,
				releaseAcquire:     releaseAcquire,
			}
		})
	}

	contenderCtx, cancelContender := context.WithCancel(ctx)
	t.Cleanup(cancelContender)
	lockResult := make(chan struct {
		unlock *Lease[Data]
		data   *Data
		err    error
	}, 1)
	go func() {
		delayedCtx := context.WithValue(contenderCtx, delayAcquireContextKey{}, true)
		unlock, data, err := Lock[Data](delayedCtx, dynamoDBClient(), &LockInput{
			Table:             table,
			ID:                id,
			HeartbeatMaxAge:   30 * time.Second,
			HeartbeatInterval: 15 * time.Second,
		})
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

	otherUnlock, data, err := Lock[Data](ctx, dynamoDBClient(), &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   2 * time.Minute,
		HeartbeatInterval: time.Minute,
	})
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
	ctx := context.Background()
	table := getTableName()
	err := setup(t, table)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	err = lib.DynamoDBWaitForReady(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	id := Uid() // new id means empty data
	unlock, data, err := Lock[testData](ctx, dynamoDBClient(), &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   time.Second * 30,
		HeartbeatInterval: time.Second * 1,
	})
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
	unlock, data, err = Lock[testData](ctx, dynamoDBClient(), &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   time.Second * 30,
		HeartbeatInterval: time.Second * 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if data == nil {
		t.Fatal("data is nil")
	} else if data.Value != "asdf" {
		t.Fatal("data mismatch")
	}
	read, err := Read[testData](ctx, dynamoDBClient(), table, id)
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
	unlock, data, err = Lock[testData](ctx, dynamoDBClient(), &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   time.Second * 30,
		HeartbeatInterval: time.Second * 1,
	})
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
	ctx := context.Background()
	table := getTableName()
	if err := setup(t, table); err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	if err := lib.DynamoDBWaitForReady(ctx, table); err != nil {
		t.Fatal(err)
	}

	id := "require-existing"
	_, _, err := Lock[Data](ctx, dynamoDBClient(), &LockInput{
		Table:             table,
		ID:                id,
		RequireExisting:   true,
		HeartbeatMaxAge:   30 * time.Second,
		HeartbeatInterval: time.Second,
		Retries:           2,
		RetriesSleep:      time.Nanosecond,
	})
	if !errors.Is(err, ErrLockNotFound) {
		t.Fatalf("expected ErrLockNotFound, got: %v", err)
	}
	if !errors.Is(err, ErrLockUnavailable) {
		t.Fatalf("expected missing item to also match ErrLockUnavailable, got: %v", err)
	}
	if errors.Is(err, ErrLockHeld) {
		t.Fatalf("missing item incorrectly matched ErrLockHeld: %v", err)
	}
	data, err := Read[Data](ctx, dynamoDBClient(), table, id)
	if err != nil {
		t.Fatal(err)
	}
	if data != nil {
		t.Fatalf("required acquisition created missing item: %#v", data)
	}

	unlock, data, err := Lock[Data](ctx, dynamoDBClient(), &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   30 * time.Second,
		HeartbeatInterval: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if data != nil {
		t.Fatalf("default acquisition returned data for a missing item: %#v", data)
	}
	if err := unlock.Commit(ctx, &Data{Value: "existing"}); err != nil {
		t.Fatal(err)
	}

	unlock, data, err = Lock[Data](ctx, dynamoDBClient(), &LockInput{
		Table:             table,
		ID:                id,
		RequireExisting:   true,
		HeartbeatMaxAge:   30 * time.Second,
		HeartbeatInterval: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if data == nil || data.Value != "existing" {
		t.Fatalf("required acquisition returned wrong existing data: %#v", data)
	}

	_, _, err = Lock[Data](ctx, dynamoDBClient(), &LockInput{
		Table:             table,
		ID:                id,
		RequireExisting:   true,
		HeartbeatMaxAge:   30 * time.Second,
		HeartbeatInterval: time.Second,
	})
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
	ctx := context.Background()
	table := getTableName()
	err := setup(t, table)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	err = lib.DynamoDBWaitForReady(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	item, err := MarshalItem("test-id", preExistingData{
		ID:    "test-id",
		Value: "test-value",
	})
	if err != nil {
		panic(err)
	}
	_, err = lib.DynamoDBClient().PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item:      item,
	})
	if err != nil {
		panic(err)
	}
	id := "test-id"
	unlock, data, err := Lock[Data](ctx, dynamoDBClient(), &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   time.Second * 30,
		HeartbeatInterval: time.Second * 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if data == nil {
		t.Fatal("data is nil")
	} else if data.Value != "test-value" {
		t.Fatal("wrong value")
	}
	_, _, err = Lock[Data](ctx, dynamoDBClient(), &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   time.Second * 30,
		HeartbeatInterval: time.Second * 1,
	})
	if err == nil {
		t.Fatal("acquired lock twice")
	}
	err = unlock.Commit(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteWithoutUnlocking(t *testing.T) {
	ctx := context.Background()
	table := getTableName()
	err := setup(t, table)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	err = lib.DynamoDBWaitForReady(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	id := "test-id"
	unlock, data, err := Lock[Data](ctx, dynamoDBClient(), &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   time.Second * 30,
		HeartbeatInterval: time.Second * 1,
	})
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
		panic(err)
	}
	read, err := Read[Data](ctx, dynamoDBClient(), table, id)
	if err != nil {
		panic(err)
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
		panic(err)
	}
	read, err = Read[Data](ctx, dynamoDBClient(), table, id)
	if err != nil {
		panic(err)
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
		panic(err)
	}
	err = unlock.Commit(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	read, err = Read[Data](ctx, dynamoDBClient(), table, id)
	if err != nil {
		panic(err)
	}
	if read == nil {
		t.Fatal("read is nil")
	} else if read.Value != "bar" {
		t.Fatal("wrong value")
	}
	read, err = Read[Data](ctx, dynamoDBClient(), table, "404")
	if err != nil {
		panic(err)
	}
	if read != nil {
		t.Fatal("data should be empty")
	}
}

func TestUnlockTwiceFails(t *testing.T) {
	ctx := context.Background()
	table := getTableName()
	err := setup(t, table)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	err = lib.DynamoDBWaitForReady(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	id := Uid()
	unlock, data, err := Lock[Data](ctx, dynamoDBClient(), &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   10 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})
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
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	table := getTableName()
	err := setup(t, table)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	err = lib.DynamoDBWaitForReady(context.Background(), table)
	if err != nil {
		t.Fatal(err)
	}
	id := Uid()
	unlock, _, err := Lock[Data](ctx, dynamoDBClient(), &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   2 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})
	if err == nil {
		_ = unlock.Release(context.Background())
		t.Fatal("expected error when context is already canceled")
	}
}

func TestContextCancelExpired(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	table := getTableName()
	err := setup(t, table)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	err = lib.DynamoDBWaitForReady(context.Background(), table)
	if err != nil {
		t.Fatal(err)
	}
	id := Uid()
	unlock, _, err := Lock[Data](ctx, dynamoDBClient(), &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   3 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unlock.Release(context.Background()) }()
	cancel()
	time.Sleep(5 * time.Second)
	unlock, _, err = Lock[Data](context.Background(), dynamoDBClient(), &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   3 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})
	if err != nil {
		t.Fatalf("context was canceled, lock should have expired")
	}
	_ = unlock.Release(context.Background())
}

func TestCommitEmptyData(t *testing.T) {
	ctx := context.Background()
	table := "test-go-dynamolock-" + uuid.Must(uuid.NewV4()).String()
	err := setup(t, table)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	err = lib.DynamoDBWaitForReady(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	id := Uid()
	unlock, data, err := Lock[Data](ctx, dynamoDBClient(), &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   10 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})
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
	read, err := Read[Data](ctx, dynamoDBClient(), table, id)
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
	ctx := context.Background()
	table := "test-go-dynamolock-" + uuid.Must(uuid.NewV4()).String()
	err := setup(t, table)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	err = lib.DynamoDBWaitForReady(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	id := Uid()
	unlock, data, err := Lock[Data](ctx, dynamoDBClient(), &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   10 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})
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
	read, err := Read[Data](ctx, dynamoDBClient(), table, id)
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
	ctx := context.Background()
	table := getTableName()
	err := setup(t, table)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	err = lib.DynamoDBWaitForReady(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	id := Uid()

	// First acquire lock with short expiration
	cancelCtx, cancel := context.WithCancel(ctx)
	_, _, err = Lock[Data](cancelCtx, dynamoDBClient(), &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   3 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel() // leave lock in use

	// Try to acquire lock with retries, should succeed after expiration
	unlock2, _, err := Lock[Data](ctx, dynamoDBClient(), &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   3 * time.Second,
		HeartbeatInterval: 1 * time.Second,
		Retries:           10,
		RetriesSleep:      500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("expected lock acquisition to succeed after expiration, got: %v", err)
	}
	_ = unlock2.Release(ctx)
}

func leaseAWSTable(t *testing.T) (*dynamodb.Client, string) {
	t.Helper()
	checkAccount(t) // Must precede client/configuration access or mutation.
	client := dynamoDBClient()
	table := "test-go-dynamolock-" + Uid()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	created := false
	t.Cleanup(func() {
		if !created {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if _, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(table)}); err != nil {
			t.Errorf("delete lease test table %s: %v", table, err)
			return
		}
		if err := dynamodb.NewTableNotExistsWaiter(client).Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)}, time.Minute, func(o *dynamodb.TableNotExistsWaiterOptions) {
			o.MinDelay, o.MaxDelay = time.Second, 3*time.Second
		}); err != nil {
			t.Errorf("wait for lease test table deletion %s: %v", table, err)
		}
	})
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(table), BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash}},
	})
	if err != nil {
		t.Fatalf("create lease test table %s: %v", table, err)
	}
	created = true
	if err := dynamodb.NewTableExistsWaiter(client).Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)}, time.Minute, func(o *dynamodb.TableExistsWaiterOptions) {
		o.MinDelay, o.MaxDelay = time.Second, 3*time.Second
	}); err != nil {
		t.Fatal(err)
	}
	return client, table
}

func liveInput(table string) *LockInput {
	return &LockInput{Table: table, ID: Uid(), HeartbeatMaxAge: 2 * time.Second, HeartbeatInterval: 200 * time.Millisecond}
}

// Replace exactly one successful write response, AFTER DynamoDB applied it.
// This tests real service conditions and reconciliation, not a mock state store.
func loseAWSResponse(t *testing.T, client *dynamodb.Client, match func(wireRequest) bool) (*dynamodb.Client, *atomic.Int32) {
	t.Helper()
	injected := &atomic.Int32{}
	base := client.Options().HTTPClient
	return dynamodb.New(client.Options(), func(o *dynamodb.Options) {
		o.HTTPClient = httpFunc(func(req *http.Request) (*http.Response, error) {
			body, err := io.ReadAll(req.Body)
			if err != nil {
				return nil, err
			}
			if err := req.Body.Close(); err != nil {
				return nil, err
			}
			req.Body = io.NopCloser(bytes.NewReader(body))
			var w wireRequest
			if err := json.Unmarshal(body, &w); err != nil {
				return nil, err
			}
			w.Target = req.Header.Get("X-Amz-Target")
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
	client, table := leaseAWSTable(t)
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
