package dynamolock

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gofrs/uuid"
	"github.com/nathants/libaws/lib"
)

type LockKey struct {
	ID string `json:"id" dynamodbav:"id"` // unique id per lock
}

type LockData struct {
	Unix int64  `json:"unix" dynamodbav:"unix"` // timestamp of lock holder
	Uid  string `json:"uid" dynamodbav:"uid"`   // uuid of lock holder
}

type LockRecord struct {
	LockKey
	LockData
}

type UnlockFn[T any] func(T) error

type UpdateFn[T any] func(T) error

func clearInternalKeys(data map[string]ddbtypes.AttributeValue) {
	delete(data, "uid")
	delete(data, "unix")
}

func Read[T any](ctx context.Context, table, id string) (*T, error) {
	var val T
	key, err := attributevalue.MarshalMap(LockKey{ID: id})
	if err != nil {
		return nil, err
	}
	out, err := lib.DynamoDBClient().GetItem(ctx, &dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(true),
		TableName:      aws.String(table),
		Key:            key,
	})
	if err != nil {
		return nil, err
	}
	if out.Item == nil {
		return nil, nil
	}
	clearInternalKeys(out.Item)
	err = attributevalue.UnmarshalMap(out.Item, &val)
	if err != nil {
		return nil, err
	}
	return &val, nil
}

func Lock[T any](ctx context.Context, table, id string, maxAge, heartbeatInterval time.Duration) (UnlockFn[T], UpdateFn[T], T, error) {
	var val T
	uid := uuid.Must(uuid.NewV4()).String()
	lockKey := LockKey{ID: id}
	key, err := attributevalue.MarshalMap(lockKey)
	if err != nil {
		return nil, nil, val, err
	}
	out, err := lib.DynamoDBClient().GetItem(ctx, &dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(true),
		TableName:      aws.String(table),
		Key:            key,
	})
	if err != nil {
		return nil, nil, val, err
	}
	lock := &LockData{}
	if len(out.Item) != 0 {
		err = attributevalue.UnmarshalMap(out.Item, &lock)
		if err != nil {
			return nil, nil, val, err
		}
	}
	if lock.Unix == 0 {
		// lib.Logger.Printf("lock is vacant: %s %s\n", id, uid)
	} else {
		age := time.Since(time.Unix(int64(lock.Unix), 0))
		if age > maxAge {
			// lib.Logger.Printf("lock is expired: %s %s\n", id, uid)
		} else {
			err = fmt.Errorf("lock is held: %s %s", id, uid)
			return nil, nil, val, err
		}
	}

	uidVal, hasUid := out.Item["uid"]
	var condition expression.ConditionBuilder
	if !hasUid {
		condition = expression.Name("uid").AttributeNotExists() // first put asserts no such key
	} else {
		switch v := uidVal.(type) {
		case *ddbtypes.AttributeValueMemberNULL:
			if v.Value {
				condition = expression.AttributeType(expression.Name("uid"), "NULL") // otherwise value might be null
			}
		default:
			condition = expression.Name("uid").Equal(expression.Value(lock.Uid)) // or a string
		}
	}

	expr, err := expression.NewBuilder().
		WithCondition(condition).
		WithUpdate(expression.
			Set(expression.Name("uid"), expression.Value(uid)).
			Set(expression.Name("unix"), expression.Value(time.Now().Unix()))).
		Build()
	if err != nil {
		return nil, nil, val, err
	}
	_, err = lib.DynamoDBClient().UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(table),
		Key:                       key,
		ConditionExpression:       expr.Condition(),
		UpdateExpression:          expr.Update(),
		ExpressionAttributeValues: expr.Values(),
		ExpressionAttributeNames:  expr.Names(),
	})
	if err != nil {
		return nil, nil, val, fmt.Errorf("failed to acquire the lock: %w", err)
	}

	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	go heartbeatLock(heartbeatCtx, table, id, uid, heartbeatInterval)
	unlock := func(data T) error {
		return releaseLock(ctx, table, id, uid, data, cancelHeartbeat)
	}
	update := func(data T) error {
		return updateLocked(ctx, table, id, uid, data)
	}
	clearInternalKeys(out.Item)
	err = attributevalue.UnmarshalMap(out.Item, &val)
	if err != nil {
		return nil, nil, val, err
	}
	return unlock, update, val, nil
}

func updateLocked[T any](ctx context.Context, table, id, uid string, data T) error {
	expr, err := expression.NewBuilder().
		WithCondition(expression.Name("uid").Equal(expression.Value(uid))).
		Build()
	if err != nil {
		return err
	}
	lock := &LockRecord{
		LockKey: LockKey{
			ID: id,
		},
		LockData: LockData{
			Unix: time.Now().Unix(),
			Uid:  uid,
		},
	}
	item, err := attributevalue.MarshalMap(lock)
	if err != nil {
		return err
	}
	dataMap, err := attributevalue.MarshalMap(data)
	if err != nil {
		return err
	}
	for k, v := range dataMap {
		_, ok := item[k]
		if !ok {
			item[k] = v
		}
	}
	_, err = lib.DynamoDBClient().PutItem(ctx, &dynamodb.PutItemInput{
		Item:                      item,
		TableName:                 aws.String(table),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})
	if err != nil {
		lib.Logger.Printf("failed to update locked: %s %s %s\n", id, uid, err)
		return err
	}
	return nil
}

func releaseLock[T any](ctx context.Context, table, id, uid string, data T, cancelHeartbeat func()) error {
	cancelHeartbeat()
	expr, err := expression.NewBuilder().
		WithCondition(expression.Name("uid").Equal(expression.Value(uid))).
		Build()
	if err != nil {
		return err
	}
	lock := &LockRecord{
		LockKey: LockKey{
			ID: id,
		},
		LockData: LockData{
			Unix: 0,
			Uid:  "",
		},
	}
	item, err := attributevalue.MarshalMap(lock)
	if err != nil {
		return err
	}
	dataMap, err := attributevalue.MarshalMap(data)
	if err != nil {
		return err
	}
	for k, v := range dataMap {
		_, ok := item[k]
		if !ok {
			item[k] = v
		}
	}
	_, err = lib.DynamoDBClient().PutItem(ctx, &dynamodb.PutItemInput{
		Item:                      item,
		TableName:                 aws.String(table),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})
	if err != nil {
		lib.Logger.Printf("failed to release the lock: %s %s %s\n", id, uid, err)
		return err
	}
	return nil
}

func heartbeatLock(ctx context.Context, table, id, uid string, heartbeatInterval time.Duration) {
	defer func() {
		if r := recover(); r != nil {
			stack := string(debug.Stack())
			lib.Logger.Println(r)
			lib.Logger.Println(stack)
			lib.Logger.Flush()
			panic(r)
		}
	}()
	key, err := attributevalue.MarshalMap(LockKey{ID: id})
	if err != nil {
		panic(err)
	}
	for {
		select {
		case <-time.After(heartbeatInterval):
		case <-ctx.Done():
			// lib.Logger.Printf("stop the heartbeat: %s %s\n", id, uid)
			return
		}
		expr, err := expression.NewBuilder().
			WithCondition(
				expression.Name("uid").Equal(expression.Value(uid))).
			WithUpdate(expression.
				Set(expression.Name("unix"), expression.Value(int(time.Now().Unix())))).
			Build()
		if err != nil {
			panic(err)
		}
		_, err = lib.DynamoDBClient().UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName:                 aws.String(table),
			Key:                       key,
			ConditionExpression:       expr.Condition(),
			UpdateExpression:          expr.Update(),
			ExpressionAttributeValues: expr.Values(),
			ExpressionAttributeNames:  expr.Names(),
		})
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				panic("failed to heartbeat the lock: " + err.Error())
			}
		}
	}
}
