package dynamolock

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/gofrs/uuid"
	"github.com/nathants/libaws/lib"
)

type LockKey struct {
	ID string `json:"id"` // unique id per lock
}

type LockData struct {
	Unix int64  `json:"unix"` // timestamp of lock holder
	Uid  string `json:"uid"`  // uuid of lock holder
}

type LockRecord struct {
	LockKey
	LockData
}

type DynamoMap map[string]*dynamodb.AttributeValue

type UnlockFn func(DynamoMap) error

func clearInternalKeys(data DynamoMap) {
	delete(data, "id")
	delete(data, "uid")
	delete(data, "unix")
}

func Read(ctx context.Context, table, id string) (DynamoMap, error) {
	key, err := dynamodbattribute.MarshalMap(LockKey{ID: id})
	if err != nil {
		return nil, err
	}
	out, err := lib.DynamoDBClient().GetItemWithContext(ctx, &dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(true),
		TableName:      aws.String(table),
		Key:            key,
	})
	if err != nil {
		return nil, err
	}
	clearInternalKeys(out.Item)
	return out.Item, nil
}

func Lock(ctx context.Context, table, id string, maxAge, heartbeatInterval time.Duration) (UnlockFn, DynamoMap, error) {
	uid := uuid.Must(uuid.NewV4()).String()
	lockKey := LockKey{ID: id}
	key, err := dynamodbattribute.MarshalMap(lockKey)
	if err != nil {
		return nil, nil, err
	}
	out, err := lib.DynamoDBClient().GetItemWithContext(ctx, &dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(true),
		TableName:      aws.String(table),
		Key:            key,
	})
	if err != nil {
		return nil, nil, err
	}
	lock := &LockData{}
	if len(out.Item) != 0 {
		err = dynamodbattribute.UnmarshalMap(out.Item, &lock)
		if err != nil {
			return nil, nil, err
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
			return nil, nil, err
		}
	}
	condition := expression.Name("uid").AttributeNotExists() // first put to a key uses this condition
	_, hasUid := out.Item["uid"]
	if hasUid {
		condition = expression.Name("uid").Equal(expression.Value(lock.Uid)) // all other puts to a key use this condition
	}
	expr, err := expression.NewBuilder().
		WithCondition(condition).
		WithUpdate(expression.
			Set(expression.Name("uid"), expression.Value(uid)).
			Set(expression.Name("unix"), expression.Value(time.Now().Unix()))).
		Build()
	if err != nil {
		return nil, nil, err
	}
	_, err = lib.DynamoDBClient().UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(table),
		Key:                       key,
		ConditionExpression:       expr.Condition(),
		UpdateExpression:          expr.Update(),
		ExpressionAttributeValues: expr.Values(),
		ExpressionAttributeNames:  expr.Names(),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to acquire the lock: %w", err)
	}
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	go heartbeatLock(heartbeatCtx, table, id, uid, heartbeatInterval)
	unlock := func(data DynamoMap) error { return releaseLock(ctx, table, id, uid, data, cancelHeartbeat) }
	clearInternalKeys(out.Item)
	return unlock, out.Item, nil
}

func releaseLock(ctx context.Context, table, id, uid string, data DynamoMap, cancelHeartbeat func()) error {
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
	item, err := dynamodbattribute.MarshalMap(lock)
	if err != nil {
		lib.Logger.Printf("failed to release the lock: %s %s %s\n", id, uid, err)
		return err
	}
	for k, v := range data {
		_, ok := item[k]
		if ok {
			return fmt.Errorf("you cannot use lock internal dynamodb keys in your data: " + k)
		}
		item[k] = v
	}
	_, err = lib.DynamoDBClient().PutItemWithContext(ctx, &dynamodb.PutItemInput{
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
	// lib.Logger.Printf("released the lock: %s %s\n", id, uid)
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
	key, err := dynamodbattribute.MarshalMap(LockKey{ID: id})
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
		_, err = lib.DynamoDBClient().UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
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
		// lib.Logger.Printf("heartbeat: %s %s\n", id, uid)
	}
}
