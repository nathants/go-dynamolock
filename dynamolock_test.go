package dynamolock

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/gofrs/uuid"
	"github.com/nathants/libaws/lib"
)

func Uid() string {
	return uuid.Must(uuid.NewV4()).String()
}

func EnsureTable(table string) error {
	return lib.DynamoDBEnsure(
		context.Background(),
		&dynamodb.CreateTableInput{
			TableName:           aws.String(table),
			BillingMode:         aws.String("PAY_PER_REQUEST"),
			StreamSpecification: &dynamodb.StreamSpecification{StreamEnabled: aws.Bool(false)},
			AttributeDefinitions: []*dynamodb.AttributeDefinition{{
				AttributeName: aws.String("id"),
				AttributeType: aws.String("S"),
			}},
			KeySchema: []*dynamodb.KeySchemaElement{{
				AttributeName: aws.String("id"),
				KeyType:       aws.String("HASH"),
			}},
		},
		false,
	)
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
	unlockA, _, err := Lock(ctx, table, id, time.Second*1, time.Second*10)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond)
	unlockB, _, err := Lock(ctx, table, id, time.Second*1, time.Second*10)
	if err != nil {
		t.Fatal(err)
	}
	err = unlockB(nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = unlockA(nil) // stop goroutine heartbeating to avoid panic on table cleanup
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
	unlock, _, err := Lock(ctx, table, id, time.Second*30, time.Second*1)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = Lock(ctx, table, id, time.Second*30, time.Second*1)
	if err == nil {
		t.Fatal("acquired lock twice")
	}
	err = unlock(nil)
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
	max := 250
	done := make(chan error, max)
	for i := 0; i < max; i++ {
		go func() {
			// defer func() {}()
			for {
				unlock, _, err := Lock(ctx, table, id, time.Second*30, time.Second*1)
				time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
				if err != nil {
					continue
				}
				sum["sum"]++
				done <- nil
				lib.Logger.Println("releasing lock, sum:", sum)
				err = unlock(nil)
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
	data := &testData{}
	unlock, item, err := Lock(ctx, table, id, time.Second*30, time.Second*1)
	if err != nil {
		t.Fatal(err)
	}
	err = dynamodbattribute.UnmarshalMap(item, data)
	if err != nil {
		t.Fatal(err)
	}
	if data.Value != "" {
		t.Fatal("die1")
	}

	// unlock, passing data
	data = &testData{Value: "asdf"}
	item, err = dynamodbattribute.MarshalMap(data)
	if err != nil {
		t.Fatal(err)
	}
	err = unlock(item)
	if err != nil {
		t.Fatal(err)
	}

	// lock, data not empty
	unlock, item, err = Lock(ctx, table, id, time.Second*30, time.Second*1)
	if err != nil {
		t.Fatal(err)
	}
	data = &testData{}
	err = dynamodbattribute.UnmarshalMap(item, data)
	if err != nil {
		t.Fatal(err)
	}
	if data.Value != "asdf" {
		t.Fatal("die2")
	}

	// read data without locking
	item, err = Read(ctx, table, id)
	if err != nil {
		t.Fatal(err)
	}
	data = &testData{}
	err = dynamodbattribute.UnmarshalMap(item, data)
	if err != nil {
		t.Fatal(err)
	}
	if data.Value != "asdf" {
		t.Fatal("die2")
	}

	// unlock, wiping data
	err = unlock(nil)
	if err != nil {
		t.Fatal(err)
	}

	// lock, data empty
	unlock, item, err = Lock(ctx, table, id, time.Second*30, time.Second*1)
	if err != nil {
		t.Fatal(err)
	}
	data = &testData{}
	err = dynamodbattribute.UnmarshalMap(item, data)
	if err != nil {
		t.Fatal(err)
	}
	if data.Value != "" {
		t.Fatal("die3")
	}

	// unlock
	err = unlock(nil)
	if err != nil {
		t.Fatal(err)
	}

}
