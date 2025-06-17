package dynamolock

import (
	"context"
	"fmt"
	"strings"
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
		}
		_, err = lib.DynamoDBClient().BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				table: reqs,
			},
		})
		if err != nil {
			return err
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
		_ = lib.DynamoDBDeleteTable(ctx, table, false)
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
	sum := map[string]int{"sum": 0}
	max := 50
	done := make(chan error, max)
	for range max {
		go func() {
			// defer func() {}()
			for {
				unlock, _, data, err := Lock[Data](ctx, &LockInput{
					Table:             table,
					ID:                id,
					HeartbeatMaxAge:   time.Second * 5,
					HeartbeatInterval: time.Second * 1,
					Retries:           5,
					RetriesSleep:      1 * time.Second,
				})
				time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
				if err != nil {
					continue
				}
				sum["sum"]++
				done <- nil
				lib.Logger.Println("releasing lock, sum:", sum)
				err = unlock(data)
				if err != nil {
					panic(err)
				}
				break
			}
		}()
	}
	for range max {
		<-done
	}
	if sum["sum"] != max {
		t.Errorf("expected %d, got %d", max, sum["sum"])
	}
}

type testData struct {
	Value string
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
	go func() {
		time.Sleep(2 * time.Second)
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
			go func() {
				_, err = lib.DynamoDBClient().PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(table),
					Item:      item,
				})
				if err != nil {
					if strings.Contains(err.Error(), "ResourceNotFoundException") {
						return // table deleted
					}
					panic(err)
				}
			}()
			time.Sleep(500 * time.Millisecond)
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

	// First acquire lock
	unlock1, _, _, err := Lock[Data](ctx, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   10 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unlock1(nil) }()

	// Try to acquire lock again with retries
	start := time.Now()
	_, _, _, err = Lock[Data](ctx, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   10 * time.Second,
		HeartbeatInterval: 1 * time.Second,
		Retries:           2,
		RetriesSleep:      500 * time.Millisecond,
	})
	duration := time.Since(start)

	if err == nil {
		t.Fatal("expected error, lock should remain held")
	}
	if !strings.Contains(err.Error(), "lock is held") {
		t.Fatalf("expected 'lock is held' error, got: %v", err)
	}
	// Should have retried 2 times with 500ms sleep each
	if duration < 1*time.Second {
		t.Fatalf("expected at least 1 second duration for 2 retries, got: %v", duration)
	}
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

	// First acquire lock
	unlock1, _, _, err := Lock[Data](ctx, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   30 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unlock1(nil) }()

	// Try with limited retries
	_, _, _, err = Lock[Data](ctx, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   30 * time.Second,
		HeartbeatInterval: 1 * time.Second,
		Retries:           1,
		RetriesSleep:      100 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if !strings.Contains(err.Error(), "lock is held") {
		t.Fatalf("expected 'lock is held' error, got: %v", err)
	}
}

func TestLockUsesCustomRetriesSleep(t *testing.T) {
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

	// First acquire lock
	unlock1, _, _, err := Lock[Data](ctx, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   30 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unlock1(nil) }()

	// Try with custom retry sleep
	start := time.Now()
	_, _, _, err = Lock[Data](ctx, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   30 * time.Second,
		HeartbeatInterval: 1 * time.Second,
		Retries:           3,
		RetriesSleep:      200 * time.Millisecond,
	})
	duration := time.Since(start)

	if err == nil {
		t.Fatal("expected error, lock should remain held")
	}
	// Should have slept 3 times * 200ms = 600ms minimum
	if duration < 600*time.Millisecond {
		t.Fatalf("expected at least 600ms duration for custom sleep, got: %v", duration)
	}
	if duration > 1*time.Second {
		t.Fatalf("expected less than 1s duration for custom sleep, got: %v", duration)
	}
}

func TestLockUsesDefaultSleepWhenNotProvided(t *testing.T) {
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

	// First acquire lock
	unlock1, _, _, err := Lock[Data](ctx, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   30 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unlock1(nil) }()

	// Try without RetriesSleep (should use 1 second default)
	start := time.Now()
	_, _, _, err = Lock[Data](ctx, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   30 * time.Second,
		HeartbeatInterval: 1 * time.Second,
		Retries:           1,
	})
	duration := time.Since(start)

	if err == nil {
		t.Fatal("expected error, lock should remain held")
	}
	// Should have used default 1 second sleep
	if duration < 1*time.Second {
		t.Fatalf("expected at least 1 second duration for default sleep, got: %v", duration)
	}
	if duration > 2*time.Second {
		t.Fatalf("expected less than 2s duration for default sleep, got: %v", duration)
	}
}

func TestLockWithZeroRetriesFailsImmediately(t *testing.T) {
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

	// First acquire lock
	unlock1, _, _, err := Lock[Data](ctx, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   30 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unlock1(nil) }()

	// Try with zero retries
	start := time.Now()
	_, _, _, err = Lock[Data](ctx, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   30 * time.Second,
		HeartbeatInterval: 1 * time.Second,
		Retries:           0,
	})
	duration := time.Since(start)

	if err == nil {
		t.Fatal("expected error with zero retries")
	}
	if !strings.Contains(err.Error(), "lock is held") {
		t.Fatalf("expected 'lock is held' error, got: %v", err)
	}
	// Should fail immediately without any sleep
	if duration > 100*time.Millisecond {
		t.Fatalf("expected immediate failure with zero retries, took: %v", duration)
	}
}

func TestLockRetryCounterIncrementsCorrectly(t *testing.T) {
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

	// First acquire lock
	unlock1, _, _, err := Lock[Data](ctx, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   30 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unlock1(nil) }()

	// Try with specific number of retries and verify timing
	retries := 3
	start := time.Now()
	_, _, _, err = Lock[Data](ctx, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   30 * time.Second,
		HeartbeatInterval: 1 * time.Second,
		Retries:           retries,
		RetriesSleep:      100 * time.Millisecond,
	})
	duration := time.Since(start)

	if err == nil {
		t.Fatal("expected error, lock should remain held")
	}
	// Should have done exactly 3 retries with 100ms sleep each
	expectedMin := time.Duration(retries) * 100 * time.Millisecond
	if duration < expectedMin {
		t.Fatalf("expected at least %v duration for %d retries, got: %v", expectedMin, retries, duration)
	}
	// Allow some buffer for execution time
	expectedMax := expectedMin + 500*time.Millisecond
	if duration > expectedMax {
		t.Fatalf("expected less than %v duration for %d retries, got: %v", expectedMax, retries, duration)
	}
}
