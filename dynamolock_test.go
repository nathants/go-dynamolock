package dynamolock

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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
	releaseLock, err := AcquireLock(ctx, table, id, Uid(), time.Second*30, time.Second*1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = AcquireLock(ctx, table, id, Uid(), time.Second*30, time.Second*1)
	if err == nil {
		t.Fatal("acquired lock twice")
	}
	releaseLock()
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
	done := make(chan error)
	for i := 0; i < max; i++ {
		go func() {
			// defer func() {}()
			for {
				releaseLock, err := AcquireLock(ctx, table, id, Uid(), time.Second*30, time.Second*1)
				time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
				if err != nil {
					continue
				}
				sum["sum"]++
				done <- nil
				lib.Logger.Println("releasing lock, sum:", sum)
				releaseLock()
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
