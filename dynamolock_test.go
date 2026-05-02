package dynamolock

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gofrs/uuid"
	"github.com/nathants/libaws/lib"
)

func checkAccount() {
	_ = rand.Float32
	_ = attributevalue.Marshal
	_ = strings.Replace
	account, err := lib.StsAccount(context.Background())
	if err != nil {
		panic(err)
	}
	if os.Getenv("DYNAMOLOCK_TEST_ACCOUNT") != account {
		panic(fmt.Sprintf("%s != %s", os.Getenv("DYNAMOLOCK_TEST_ACCOUNT"), account))
	}
}

type Data struct {
	Value string `json:"value" dynamodbav:"value"`
	// note: you cannot use "id", since it is part of LockKey{}
	// note: you cannot use "uid" or "unix", since those are part of LockData{}
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

func setup(table string) error {
	checkAccount()
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
	err := setup(table)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	err = lib.DynamoDBWaitForReady(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	id := Uid()
	unlock, _, data, err := Lock[Data](ctx, &LockInput{
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
	_, _, _, err = Lock[Data](ctx, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   time.Second * 30,
		HeartbeatInterval: time.Second * 1,
	})
	if err == nil {
		t.Fatal("acquired lock twice")
	}
	err = unlock(data)
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadModifyWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	table := getTableName()
	err := setup(table)
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
				unlock, _, data, err := Lock[Data](ctx, &LockInput{
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
					_ = unlock(data)
					done <- fmt.Errorf("lock allowed concurrent critical sections")
					return
				}
				time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
				newSum := atomic.AddInt32(&sum, 1)
				lib.Logger.Println("releasing lock, sum:", newSum)
				atomic.StoreInt32(&inCriticalSection, 0)
				err = unlock(data)
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
	err := setup(table)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	err = lib.DynamoDBWaitForReady(ctx, table)
	if err != nil {
		t.Fatal(err)
	}

	id := Uid()
	unlock, _, _, err := Lock[Data](ctx, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   2 * time.Minute,
		HeartbeatInterval: 2 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = unlock(&Data{Value: "old"})
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
		unlock UnlockFn[Data]
		data   *Data
		err    error
	}, 1)
	go func() {
		delayedCtx := context.WithValue(contenderCtx, delayAcquireContextKey{}, true)
		unlock, _, data, err := Lock[Data](delayedCtx, &LockInput{
			Table:             table,
			ID:                id,
			HeartbeatMaxAge:   30 * time.Second,
			HeartbeatInterval: 30 * time.Second,
		})
		lockResult <- struct {
			unlock UnlockFn[Data]
			data   *Data
			err    error
		}{unlock: unlock, data: data, err: err}
	}()

	select {
	case <-seenAcquireAttempt:
	case <-ctx.Done():
		t.Fatal("timed out waiting for contender's acquire attempt")
	}

	otherUnlock, _, data, err := Lock[Data](ctx, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   2 * time.Minute,
		HeartbeatInterval: 2 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if data == nil || data.Value != "old" {
		t.Fatalf("expected other holder to read old data, got %#v", data)
	}
	err = otherUnlock(&Data{Value: "fresh"})
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
		err = result.unlock(result.data)
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for contender lock result")
	}
}

type retryCheckClient struct {
	attempts int32
}

func newRetryCheckDynamoDBClient(c *retryCheckClient) func() *dynamodb.Client {
	return func() *dynamodb.Client {
		return dynamodb.New(dynamodb.Options{
			Region: "us-east-1",
			Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
				return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test", Source: "test"}, nil
			}),
			HTTPClient: c,
		})
	}
}

func (c *retryCheckClient) Do(req *http.Request) (*http.Response, error) {
	atomic.AddInt32(&c.attempts, 1)
	body := `{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"conditional failed"}`
	return &http.Response{
		StatusCode: http.StatusBadRequest,
		Status:     "400 Bad Request",
		Header: http.Header{
			"Content-Type":     []string{"application/x-amz-json-1.0"},
			"X-Amzn-Requestid": []string{"test-request-id"},
		},
		Body:    io.NopCloser(strings.NewReader(body)),
		Request: req,
	}, nil
}

func assertRetryAttempts(t *testing.T, retries int, retriesSleep time.Duration) time.Duration {
	t.Helper()
	originalClient := dynamoDBClient
	t.Cleanup(func() { dynamoDBClient = originalClient })
	checkClient := &retryCheckClient{}
	dynamoDBClient = newRetryCheckDynamoDBClient(checkClient)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _, _, err := Lock[Data](ctx, &LockInput{
		Table:             "retry-check",
		ID:                "retry-check",
		HeartbeatMaxAge:   30 * time.Second,
		HeartbeatInterval: 1 * time.Second,
		Retries:           retries,
		RetriesSleep:      retriesSleep,
	})
	duration := time.Since(start)
	if err == nil {
		t.Fatal("expected lock acquisition to fail")
	}
	if !strings.Contains(err.Error(), "lock is held") {
		t.Fatalf("expected 'lock is held' error, got: %v", err)
	}
	want := int32(retries + 1)
	if got := atomic.LoadInt32(&checkClient.attempts); got != want {
		t.Fatalf("expected %d acquire attempts, got %d", want, got)
	}
	return duration
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
	err := setup(table)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	err = lib.DynamoDBWaitForReady(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	id := Uid() // new id means empty data
	unlock, _, data, err := Lock[testData](ctx, &LockInput{
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
	err = unlock(&testData{Value: "asdf"})
	if err != nil {
		t.Fatal(err)
	}
	unlock, _, data, err = Lock[testData](ctx, &LockInput{
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
	read, err := Read[testData](ctx, table, id)
	if err != nil {
		t.Fatal(err)
	}
	if read == nil {
		t.Fatal("read is nil")
	} else if read.Value != "asdf" {
		t.Fatal("read mismatch")
	}
	err = unlock(&testData{Value: "123"})
	if err != nil {
		t.Fatal(err)
	}
	unlock, _, data, err = Lock[testData](ctx, &LockInput{
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
	err = unlock(data)
	if err != nil {
		t.Fatal(err)
	}
}

type preExistingData struct {
	ID    string `json:"id" dynamodbav:"id"`
	Value string `json:"value" dynamodbav:"value"`
	// note you cannot use "uid" or "unix", since those are part of LockData{}
}

func TestPreExistingData(t *testing.T) {
	ctx := context.Background()
	table := getTableName()
	err := setup(table)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	err = lib.DynamoDBWaitForReady(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	item, err := attributevalue.MarshalMap(preExistingData{
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
	unlock, _, data, err := Lock[Data](ctx, &LockInput{
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
	_, _, data, err = Lock[Data](ctx, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   time.Second * 30,
		HeartbeatInterval: time.Second * 1,
	})
	if err == nil {
		t.Fatal("acquired lock twice")
	}
	err = unlock(data)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteWithoutUnlocking(t *testing.T) {
	ctx := context.Background()
	table := getTableName()
	err := setup(table)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	err = lib.DynamoDBWaitForReady(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	id := "test-id"
	unlock, update, data, err := Lock[Data](ctx, &LockInput{
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
	err = update(data)
	if err != nil {
		panic(err)
	}
	read, err := Read[Data](ctx, table, id)
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
	err = update(data)
	if err != nil {
		panic(err)
	}
	read, err = Read[Data](ctx, table, id)
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
	err = update(data)
	if err != nil {
		panic(err)
	}
	err = unlock(data)
	if err != nil {
		t.Fatal(err)
	}
	read, err = Read[Data](ctx, table, id)
	if err != nil {
		panic(err)
	}
	if read == nil {
		t.Fatal("read is nil")
	} else if read.Value != "bar" {
		t.Fatal("wrong value")
	}
	read, err = Read[Data](ctx, table, "404")
	if err != nil {
		panic(err)
	}
	if read != nil {
		t.Fatal("data should be empty")
	}
}

func TestNullValueDoesNotBreakLocking(t *testing.T) {
	ctx := context.Background()
	table := getTableName()
	err := setup(table)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	err = lib.DynamoDBWaitForReady(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	item := map[string]types.AttributeValue{
		"id":  &types.AttributeValueMemberS{Value: "test-uid-null"},
		"uid": &types.AttributeValueMemberNULL{Value: true},
	}
	_, err = lib.DynamoDBClient().PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item:      item,
	})
	if err != nil {
		t.Fatal(err)
	}
	unlock, _, data, err := Lock[Data](ctx, &LockInput{
		Table:             table,
		ID:                "test-uid-null",
		HeartbeatMaxAge:   time.Second * 30,
		HeartbeatInterval: time.Second * 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if data == nil {
		t.Fatalf("data is nil")
	}
	err = unlock(data)
	if err != nil {
		t.Fatal(err)
	}
}

func TestHeartbeatErrorHandling(t *testing.T) {
	ctx := context.Background()
	table := getTableName()
	err := setup(table)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	err = lib.DynamoDBWaitForReady(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	id := Uid()
	var heartbeatErrors int32
	sabotageCtx, cancelSabotage := context.WithCancel(ctx)
	defer cancelSabotage()
	go func() {
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-sabotageCtx.Done():
			return
		}
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			val := LockRecord{
				LockKey: LockKey{
					ID: id,
				},
				LockData: LockData{
					Unix: time.Now().Unix(),
					Uid:  "fake-uid",
				},
			}
			item, err := attributevalue.MarshalMap(val)
			if err != nil {
				panic(err)
			}
			_, err = lib.DynamoDBClient().PutItem(sabotageCtx, &dynamodb.PutItemInput{
				TableName: aws.String(table),
				Item:      item,
			})
			if err != nil {
				if sabotageCtx.Err() != nil || strings.Contains(err.Error(), "ResourceNotFoundException") {
					return
				}
				panic(err)
			}
			select {
			case <-ticker.C:
			case <-sabotageCtx.Done():
				return
			}
		}
	}()
	unlock, _, _, err := Lock[Data](ctx, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   5 * time.Second,
		HeartbeatInterval: 1 * time.Second,
		HeartbeatErrFn: func(err error) {
			atomic.AddInt32(&heartbeatErrors, 1)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Second)
	if atomic.LoadInt32(&heartbeatErrors) != 1 {
		t.Fatalf("expected onHeartbeatErr once, got %d times", heartbeatErrors)
	}
	err = unlock(&Data{Value: "asdf"})
	if err == nil {
		t.Fatalf("should fail, uid changed by sabotage")
	}
}

func TestUnlockTwiceFails(t *testing.T) {
	ctx := context.Background()
	table := getTableName()
	err := setup(table)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	err = lib.DynamoDBWaitForReady(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	id := Uid()
	unlock, _, data, err := Lock[Data](ctx, &LockInput{
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
	err = unlock(&Data{Value: "temp"})
	if err != nil {
		t.Fatal(err)
	}
	err = unlock(&Data{Value: "temp"})
	if err == nil {
		t.Fatalf("unlock twice should error")
	}
}

func TestContextCancelBeforeLock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	table := getTableName()
	err := setup(table)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	err = lib.DynamoDBWaitForReady(context.Background(), table)
	if err != nil {
		t.Fatal(err)
	}
	id := Uid()
	unlock, _, _, err := Lock[Data](ctx, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   2 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})
	if err == nil {
		_ = unlock(nil)
		t.Fatal("expected error when context is already canceled")
	}
}

func TestContextCancelUnlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	table := getTableName()
	err := setup(table)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	err = lib.DynamoDBWaitForReady(context.Background(), table)
	if err != nil {
		t.Fatal(err)
	}
	id := Uid()
	unlock, _, data, err := Lock[Data](ctx, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   5 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if data != nil {
		t.Fatal("data should be nil for a fresh lock")
	}
	cancel()
	err = unlock(&Data{Value: "test-cancel"})
	if err == nil {
		t.Fatal("expected unlock to fail after context cancellation or to effectively no-op")
	}
}

func TestContextCancelExpired(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	table := getTableName()
	err := setup(table)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	err = lib.DynamoDBWaitForReady(context.Background(), table)
	if err != nil {
		t.Fatal(err)
	}
	id := Uid()
	unlock, _, _, err := Lock[Data](ctx, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   3 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unlock(nil) }()
	cancel()
	time.Sleep(5 * time.Second)
	unlock, _, _, err = Lock[Data](context.Background(), &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   3 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})
	if err != nil {
		t.Fatalf("context was canceled, lock should have expired")
	}
	_ = unlock(nil)
}

func TestUnlockWithNil(t *testing.T) {
	ctx := context.Background()
	table := "test-go-dynamolock-" + uuid.Must(uuid.NewV4()).String()
	err := setup(table)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	err = lib.DynamoDBWaitForReady(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	id := Uid()
	unlock, _, data, err := Lock[Data](ctx, &LockInput{
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
	err = unlock(nil)
	if err != nil {
		t.Fatalf("expected no error unlocking with nil data, got: %v", err)
	}
	read, err := Read[Data](ctx, table, id)
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

func TestUpdateWithNil(t *testing.T) {
	ctx := context.Background()
	table := "test-go-dynamolock-" + uuid.Must(uuid.NewV4()).String()
	err := setup(table)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown(table)
	err = lib.DynamoDBWaitForReady(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	id := Uid()
	unlock, update, data, err := Lock[Data](ctx, &LockInput{
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
	err = update(nil)
	if err != nil {
		t.Fatalf("expected no error updating with nil data, got: %v", err)
	}
	read, err := Read[Data](ctx, table, id)
	if err != nil {
		t.Fatal(err)
	}
	if read == nil {
		t.Fatalf("expected data")
	}
	if read.Value != "" {
		t.Fatalf("expected zero value")
	}
	err = unlock(nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestLockRetriesWhenHeldNotExpired(t *testing.T) {
	assertRetryAttempts(t, 2, time.Nanosecond)
}

func TestLockSucceedsAfterRetryWhenExpires(t *testing.T) {
	ctx := context.Background()
	table := getTableName()
	err := setup(table)
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
	_, _, _, err = Lock[Data](cancelCtx, &LockInput{
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
	unlock2, _, _, err := Lock[Data](ctx, &LockInput{
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
	_ = unlock2(nil)
}

func TestLockFailsAfterExhaustingRetries(t *testing.T) {
	assertRetryAttempts(t, 1, time.Nanosecond)
}

func TestLockUsesCustomRetriesSleep(t *testing.T) {
	duration := assertRetryAttempts(t, 3, 10*time.Millisecond)
	if duration < 30*time.Millisecond {
		t.Fatalf("expected custom retry sleep to delay at least 30ms, got %s", duration)
	}
}

func TestLockUsesDefaultSleepWhenNotProvided(t *testing.T) {
	originalClient := dynamoDBClient
	t.Cleanup(func() { dynamoDBClient = originalClient })
	checkClient := &retryCheckClient{}
	dynamoDBClient = newRetryCheckDynamoDBClient(checkClient)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _, _, err := Lock[Data](ctx, &LockInput{
		Table:             "retry-check",
		ID:                "retry-check",
		HeartbeatMaxAge:   30 * time.Second,
		HeartbeatInterval: 1 * time.Second,
		Retries:           1,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline while waiting on default retry sleep, got: %v", err)
	}
	if got := atomic.LoadInt32(&checkClient.attempts); got != 1 {
		t.Fatalf("expected 1 acquire attempt before default retry sleep, got %d", got)
	}
}

func TestLockWithZeroRetriesFailsImmediately(t *testing.T) {
	assertRetryAttempts(t, 0, time.Nanosecond)
}

func TestLockRetryCounterIncrementsCorrectly(t *testing.T) {
	assertRetryAttempts(t, 3, time.Nanosecond)
}
