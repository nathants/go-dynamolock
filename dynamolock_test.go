package dynamolock

import (
	"context"
	"fmt"
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
	// note you cannot use "uid" or "unix", since those are part of LockData{}
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
	unlockA, _, dataA, err := Lock[Data](ctx, table, id, time.Second*1, time.Second*10)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond)
	unlockB, _, dataB, err := Lock[Data](ctx, table, id, time.Second*1, time.Second*10)
	if err != nil {
		t.Fatal(err)
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
	unlock, _, data, err := Lock[Data](ctx, table, id, time.Second*30, time.Second*1)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, err = Lock[Data](ctx, table, id, time.Second*30, time.Second*1)
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
	max := 25
	done := make(chan error, max)
	for range max {
		go func() {
			// defer func() {}()
			for {
				unlock, _, data, err := Lock[Data](ctx, table, id, time.Second*30, time.Second*1)
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
	for i := 0; i < max; i++ {
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
	id := Uid()
	// lock, new id means empty data
	unlock, _, data, err := Lock[testData](ctx, table, id, time.Second*30, time.Second*1)
	if err != nil {
		t.Fatal(err)
	}
	if data.Value != "" {
		t.Fatal("die1")
	}
	// unlock, passing data
	err = unlock(testData{Value: "asdf"})
	if err != nil {
		t.Fatal(err)
	}
	// lock, data not empty
	unlock, _, data, err = Lock[testData](ctx, table, id, time.Second*30, time.Second*1)
	if err != nil {
		t.Fatal(err)
	}
	if data.Value != "asdf" {
		t.Fatal("die2")
	}
	// read data without locking
	read, err := Read[testData](ctx, table, id)
	if err != nil {
		t.Fatal(err)
	}
	if read.Value != "asdf" {
		t.Fatal("die2")
	}
	// unlock, wiping data
	err = unlock(testData{})
	if err != nil {
		t.Fatal(err)
	}
	// lock, data empty
	unlock, _, data, err = Lock[testData](ctx, table, id, time.Second*30, time.Second*1)
	if err != nil {
		t.Fatal(err)
	}
	if data.Value != "" {
		t.Fatal("die3")
	}
	// unlock
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
	unlock, _, data, err := Lock[Data](ctx, table, id, time.Second*30, time.Second*1)
	if err != nil {
		t.Fatal(err)
	}
	if data.Value != "test-value" {
		t.Fatal("wrong value")
	}
	_, _, data, err = Lock[Data](ctx, table, id, time.Second*30, time.Second*1)
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
	unlock, update, data, err := Lock[Data](ctx, table, id, time.Second*30, time.Second*1)
	if err != nil {
		t.Fatal(err)
	}
	{
		data.Value = "asdf"
		time.Sleep(2 * time.Second)
		err = update(data)
		if err != nil {
			panic(err)
		}
	}
	read, err := Read[Data](ctx, table, id)
	if err != nil {
		panic(err)
	}
	if read.Value != "asdf" {
		t.Fatal(lib.PformatAlways(data))
	}
	{
		data.Value = "foo"
		time.Sleep(2 * time.Second)
		err = update(data)
		if err != nil {
			panic(err)
		}
	}
	read, err = Read[Data](ctx, table, id)
	if err != nil {
		panic(err)
	}
	if read.Value != "foo" {
		t.Fatal(lib.PformatAlways(data))
	}
	{
		data.Value = "bar"
		time.Sleep(2 * time.Second)
		err = update(data)
		if err != nil {
			panic(err)
		}
	}
	err = unlock(data)
	if err != nil {
		t.Fatal(err)
	}
	read, err = Read[Data](ctx, table, id)
	if err != nil {
		panic(err)
	}
	if read.Value != "bar" {
		t.Fatal(lib.PformatAlways(data))
	}
	read, err = Read[Data](ctx, table, "404")
	if err != nil {
		panic(err)
	}
	if read != nil {
		t.Fatal("shouldnt exist")
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
		"id": &types.AttributeValueMemberS{Value: "test-uid-null"},
		"uid": &types.AttributeValueMemberNULL{Value: true},
	}
	_, err = lib.DynamoDBClient().PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item:      item,
	})
	if err != nil {
		t.Fatal(err)
	}

	unlock, _, data, err := Lock[Data](ctx, table, "test-uid-null", time.Second*30, time.Second*1)
	if err != nil {
		t.Fatal(err)
	}

	err = unlock(data)
	if err != nil {
		t.Fatal(err)
	}
}
