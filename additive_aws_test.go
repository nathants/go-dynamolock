package dynamolock

import (
	"errors"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func TestLeaseAdditiveAWS(t *testing.T) {
	client, table := liveTable(t)
	type nested struct {
		Known string `dynamodbav:"known"`
	}
	type oldData struct {
		Known  string `dynamodbav:"known"`
		Remove string `dynamodbav:"remove,omitempty"`
		Nested nested `dynamodbav:"nested"`
	}
	original := map[string]any{
		"Known": "before", "Remove": "remove intentionally", "future": "retain",
		"Nested": map[string]any{"KNOWN": "before", "future": "retain nested"},
	}
	item, err := MarshalItem(Table{PartitionKey: "id"}, Key{Partition: "key"}, original)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PutItem(t.Context(), &dynamodb.PutItemInput{
		TableName: aws.String(table), Item: item,
		ConditionExpression: aws.String("attribute_not_exists(id)"),
	})
	if err != nil {
		t.Fatal(err)
	}
	input := liveInput(table)
	input.Key.Partition = "key"
	lease, data, err := Lock[oldData](t.Context(), client, input)
	if err != nil {
		t.Fatal(err)
	}
	cleanupLease(t, lease)
	if data == nil {
		t.Fatal("existing payload was absent")
	}
	check := func(expected map[string]any) {
		t.Helper()
		actual, err := Read[map[string]any](t.Context(), client, Table{Name: table}, Key{Partition: "key"}, ReadOptions{ConsistentRead: true})
		if err != nil {
			t.Fatal(err)
		}
		if actual == nil || !reflect.DeepEqual(*actual, expected) {
			t.Fatalf("additive fields changed: got %#v, want %#v", actual, expected)
		}
	}
	data.Known, data.Remove = "after", ""
	if err := lease.Update(lease.Context(), data); err != nil {
		t.Fatal(err)
	}
	expected := map[string]any{
		"id": "key", "Known": "after", "future": "retain",
		"Nested": map[string]any{"KNOWN": "before", "future": "retain nested"},
	}
	check(expected)
	data.Known, data.Nested.Known = "again", "after"
	if err := lease.Commit(lease.Context(), data); err != nil {
		t.Fatal(err)
	}
	expected["Known"] = "again"
	expected["Nested"] = map[string]any{"KNOWN": "after", "future": "retain nested"}
	check(expected)
}

func TestLeaseOpaqueListAWS(t *testing.T) {
	client, table := liveTable(t)
	type member struct {
		Name string `dynamodbav:"name"`
	}
	type oldData struct {
		Count   int      `dynamodbav:"count"`
		Members []member `dynamodbav:"members"`
	}
	input := liveInput(table)
	original := map[string]any{
		"count":   1,
		"members": []map[string]any{{"name": "before", "future": "retain"}},
	}
	item, err := MarshalItem(Table{PartitionKey: "id"}, Key{Partition: input.Key.Partition}, original)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PutItem(t.Context(), &dynamodb.PutItemInput{
		TableName: aws.String(table), Item: item,
		ConditionExpression: aws.String("attribute_not_exists(id)"),
	}); err != nil {
		t.Fatal(err)
	}
	lease, data, err := Lock[oldData](t.Context(), client, input)
	if err != nil {
		t.Fatal(err)
	}
	cleanupLease(t, lease)
	data.Count = 2
	if err := lease.Update(lease.Context(), data); err != nil {
		t.Fatal(err)
	}
	data.Members[0].Name = "after"
	if err := lease.Update(lease.Context(), data); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("lossy list replacement was not refused: %v", err)
	}
	data.Members[0].Name = "before"
	if err := lease.Commit(lease.Context(), data); err != nil {
		t.Fatal(err)
	}
	original["count"] = 2
	expected, err := MarshalItem(Table{PartitionKey: "id"}, Key{Partition: input.Key.Partition}, original)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := getItem(t.Context(), client, Table{Name: table, PartitionKey: "id"}, input.Key)
	if err != nil || !reflect.DeepEqual(actual, expected) {
		t.Fatalf("opaque list or ownership changed: %#v %v", actual, err)
	}
}

func TestLeaseEmptyMapAWS(t *testing.T) {
	client, table := liveTable(t)
	input := liveInput(table)
	lease, data, err := Lock[map[string]any](t.Context(), client, input)
	if err != nil {
		t.Fatal(err)
	}
	cleanupLease(t, lease)
	if data != nil {
		t.Fatal("fresh data was not absent")
	}
	value := map[string]any{}
	if err := lease.Update(lease.Context(), &value); err != nil {
		t.Fatal(err)
	}
	value["temporary"] = map[string]any{"key": "value"}
	if err := lease.Update(lease.Context(), &value); err != nil {
		t.Fatal(err)
	}
	delete(value, "temporary")
	if err := lease.Commit(lease.Context(), &value); err != nil {
		t.Fatal(err)
	}
	expected, err := MarshalItem(Table{PartitionKey: "id"}, Key{Partition: input.Key.Partition}, value)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := getItem(t.Context(), client, Table{Name: table, PartitionKey: "id"}, input.Key)
	if err != nil || !reflect.DeepEqual(actual, expected) {
		t.Fatalf("explicit map deletion or initialization changed: %#v %v", actual, err)
	}
}
