package dynamolock

import (
	"context"
	"fmt"
	"strings"

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

func Uid() string {
	return uuid.Must(uuid.NewV4()).String()
}

func EnsureTable(table string) error {
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
	return lib.DynamoDBEnsure(context.Background(), input, nil, false)
}

func TestLockExpiration(t *testing.T) {
	ctx := context.Background()
	table := "test-go-dynamolock-" + uuid.Must(uuid.NewV4()).String()
	err := EnsureTable(table)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lib.DynamoDBDeleteTable(ctx, table, false) }()
	err = lib.DynamoDBWaitForReady(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	id := Uid()
	unlockA, _, dataA, err := Lock[Data](ctx, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   time.Second * 1,
		HeartbeatInterval: time.Second * 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if dataA != nil {
		t.Fatalf("data should be nil")
	}
	time.Sleep(1500 * time.Millisecond)
	unlockB, _, dataB, err := Lock[Data](ctx, &LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   time.Second * 1,
		HeartbeatInterval: time.Second * 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if dataB == nil {
		t.Fatalf("data should not be nil")
	}
	err = unlockB(dataB)
	if err != nil {
		t.Fatal(err)
	}
	_ = unlockA(dataA) // stop goroutine heartbeating to avoid panic on table cleanup
}

func TestBasic(t *testing.T) {
	ctx := context.Background()
	table := "test-go-dynamolock-" + uuid.Must(uuid.NewV4()).String()
	err := EnsureTable(table)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lib.DynamoDBDeleteTable(ctx, table, false) }()
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
	table := "test-go-dynamolock-" + uuid.Must(uuid.NewV4()).String()
	err := EnsureTable(table)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lib.DynamoDBDeleteTable(ctx, table, false) }()
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
					HeartbeatMaxAge:   time.Second * 30,
					HeartbeatInterval: time.Second * 1,
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
	table := "test-go-dynamolock-" + uuid.Must(uuid.NewV4()).String()
	err := EnsureTable(table)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lib.DynamoDBDeleteTable(ctx, table, false) }()
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
	table := "test-go-dynamolock-" + uuid.Must(uuid.NewV4()).String()
	err := EnsureTable(table)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lib.DynamoDBDeleteTable(ctx, table, false) }()
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
	table := "test-go-dynamolock-" + uuid.Must(uuid.NewV4()).String()
	err := EnsureTable(table)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lib.DynamoDBDeleteTable(ctx, table, false) }()
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
	table := "test-go-dynamolock-" + uuid.Must(uuid.NewV4()).String()
	err := EnsureTable(table)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lib.DynamoDBDeleteTable(ctx, table, false) }()
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
	table := "test-go-dynamolock-" + uuid.Must(uuid.NewV4()).String()
	err := EnsureTable(table)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lib.DynamoDBDeleteTable(ctx, table, false) }()
	err = lib.DynamoDBWaitForReady(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	id := Uid()
	heartbeatErrors := 0
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
			heartbeatErrors++
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Second)
	if heartbeatErrors != 1 {
		t.Fatalf("expected onHeartbeatErr once, got %d times", heartbeatErrors)
	}
	err = unlock(&Data{Value: "asdf"})
	if err == nil {
		t.Fatalf("should fail, uid changed by sabotage")
	}
}

func TestUnlockTwiceFails(t *testing.T) {
	ctx := context.Background()
	table := "test-go-dynamolock-" + uuid.Must(uuid.NewV4()).String()
	err := EnsureTable(table)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lib.DynamoDBDeleteTable(ctx, table, false) }()
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
	table := "test-go-dynamolock-" + uuid.Must(uuid.NewV4()).String()
	err := EnsureTable(table)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = lib.DynamoDBDeleteTable(context.Background(), table, false)
	}()
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
	table := "test-go-dynamolock-" + uuid.Must(uuid.NewV4()).String()
	err := EnsureTable(table)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = lib.DynamoDBDeleteTable(context.Background(), table, false)
	}()
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
	table := "test-go-dynamolock-" + uuid.Must(uuid.NewV4()).String()
	err := EnsureTable(table)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = lib.DynamoDBDeleteTable(context.Background(), table, false)
	}()
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
	err := EnsureTable(table)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lib.DynamoDBDeleteTable(ctx, table, false) }()
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
	err := EnsureTable(table)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lib.DynamoDBDeleteTable(ctx, table, false) }()
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
