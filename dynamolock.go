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
	"github.com/nathants/libaws/lib"
)

type LockKey struct {
	ID string `json:"id"` // unique id per lock
}

type LockData struct {
	Unix int    `json:"unix"` // timestamp of lock holder
	Uid  string `json:"uid"`  // uuid of lock holder
}

type Lock struct {
	LockKey
	LockData
}

func condEqualsUid(uid string) expression.Expression {
	expr, err := expression.NewBuilder().WithCondition(
		expression.Name("uid").Equal(expression.Value(uid)),
	).Build()
	if err != nil {
		panic(err)
	}
	return expr
}

func condNotExistsId() expression.Expression {
	expr, err := expression.NewBuilder().WithCondition(
		expression.Name("id").AttributeNotExists(),
	).Build()
	if err != nil {
		panic(err)
	}
	return expr
}

func maybeInitLock(ctx context.Context, table, id, uid string) error {
	lockKey := LockKey{ID: id}
	key, err := dynamodbattribute.MarshalMap(lockKey)
	if err != nil {
		return err
	}
	out, err := lib.DynamoDBClient().GetItemWithContext(ctx, &dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(true),
		TableName:      aws.String(table),
		Key:            key,
	})
	if err != nil {
		return err
	}
	if len(out.Item) == 0 {
		lock := Lock{
			LockKey: lockKey,
			LockData: LockData{
				Unix: 0,
				Uid:  uid,
			},
		}
		item, err := dynamodbattribute.MarshalMap(lock)
		if err != nil {
			return err
		}
		expr := condNotExistsId()
		_, err = lib.DynamoDBClient().PutItemWithContext(ctx, &dynamodb.PutItemInput{
			Item:                      item,
			TableName:                 aws.String(table),
			ConditionExpression:       expr.Condition(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
		})
		if err != nil {
			err = fmt.Errorf("failed to init-acquire the lock: %w", err)
			return err
		}
		// lib.Logger.Printf("init-acquired the lock: %s %s", id, uid)
	}
	return nil
}

func AcquireLock(ctx context.Context, table string, id string, uid string, maxAge, heartbeatInterval time.Duration) (func(), error) {
	err := maybeInitLock(ctx, table, id, uid)
	if err != nil {
		return nil, err
	}
	lockKey := LockKey{ID: id}
	key, err := dynamodbattribute.MarshalMap(lockKey)
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
	lock := &Lock{
		LockKey: LockKey{ID: id},
	}
	if len(out.Item) != 0 {
		err = dynamodbattribute.UnmarshalMap(out.Item, &lock)
		if err != nil {
			return nil, err
		}
	}
	if lock.Unix == 0 {
		// lib.Logger.Printf("lock is vacant: %s %s", id, uid)
	} else {
		age := time.Since(time.Unix(int64(lock.Unix), 0))
		if age > maxAge {
			// lib.Logger.Printf("lock is expired: %s %s", id, uid)
		} else {
			err = fmt.Errorf("lock is held: %s %s", id, uid)
			return nil, err
		}
	}
	expr := condEqualsUid(lock.Uid)
	lock.Uid = uid
	lock.Unix = int(time.Now().Unix())
	item, err := dynamodbattribute.MarshalMap(lock)
	if err != nil {
		return nil, err
	}
	_, err = lib.DynamoDBClient().PutItemWithContext(ctx, &dynamodb.PutItemInput{
		Item:                      item,
		TableName:                 aws.String(table),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})
	if err != nil {
		err = fmt.Errorf("failed to acquire the lock: %w", err)
		return nil, err
	}
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	go heartbeatLock(heartbeatCtx, table, id, uid, heartbeatInterval)
	return func() { releaseLock(ctx, table, id, uid, cancelHeartbeat) }, nil
}

func releaseLock(ctx context.Context, table, id, uid string, cancelHearbeat func()) {
	cancelHearbeat()
	expr := condEqualsUid(uid)
	lock := &Lock{
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
		lib.Logger.Printf("failed to release the lock: %s %s %s", id, uid, err)
		return
	}
	_, err = lib.DynamoDBClient().PutItemWithContext(ctx, &dynamodb.PutItemInput{
		Item:                      item,
		TableName:                 aws.String(table),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})
	if err != nil {
		lib.Logger.Printf("failed to release the lock: %s %s %s", id, uid, err)
		return
	}
	// lib.Logger.Printf("released the lock: %s %s", id, uid)
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
	lock := Lock{
		LockKey:  LockKey{ID: id},
		LockData: LockData{Uid: uid},
	}
	for {
		select {
		case <-time.After(heartbeatInterval):
		case <-ctx.Done():
			// lib.Logger.Printf("stop the heartbeat: %s %s", id, uid)
			return
		}
		expr := condEqualsUid(lock.Uid)
		lock.Unix = int(time.Now().Unix())
		item, err := dynamodbattribute.MarshalMap(lock)
		if err != nil {
			panic("failed to heartbeat the lock: " + err.Error())
		}
		_, err = lib.DynamoDBClient().PutItemWithContext(ctx, &dynamodb.PutItemInput{
			Item:                      item,
			TableName:                 aws.String(table),
			ConditionExpression:       expr.Condition(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
		})
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				panic("failed to heartbeat the lock: " + err.Error())
			}
		}
		// lib.Logger.Printf("heartbeat: %s %s", id, uid)
	}
}
