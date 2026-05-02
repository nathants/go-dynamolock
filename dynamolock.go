package dynamolock

import (
	"context"
	"errors"
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

type UnlockFn[T any] func(*T) error

type UpdateFn[T any] func(*T) error

var dynamoDBClient = lib.DynamoDBClient

func clearInternalKeys(data map[string]ddbtypes.AttributeValue) {
	delete(data, "uid")
	delete(data, "unix")
}

func buildItem[T any](id string, uid string, unix int64, data *T) (map[string]ddbtypes.AttributeValue, error) {
	lock := &LockRecord{
		LockKey: LockKey{
			ID: id,
		},
		LockData: LockData{
			Unix: unix,
			Uid:  uid,
		},
	}
	item, err := attributevalue.MarshalMap(lock)
	if err != nil {
		return nil, err
	}
	if data == nil {
		var val T
		data = &val
	}
	dataMap, err := attributevalue.MarshalMap(data)
	if err != nil {
		return nil, err
	}
	for k, v := range dataMap {
		if _, ok := item[k]; !ok {
			item[k] = v
		}
	}
	return item, nil
}

type LockInput struct {
	Table             string
	ID                string
	HeartbeatMaxAge   time.Duration
	HeartbeatInterval time.Duration
	HeartbeatErrFn    func(error)
	Retries           int
	RetriesSleep      time.Duration
}

func Read[T any](ctx context.Context, table, id string) (*T, error) {
	var val T
	key, err := attributevalue.MarshalMap(LockKey{ID: id})
	if err != nil {
		return nil, err
	}
	out, err := dynamoDBClient().GetItem(ctx, &dynamodb.GetItemInput{
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

func Lock[T any](ctx context.Context, input *LockInput) (UnlockFn[T], UpdateFn[T], *T, error) {
	var val T
	if input.HeartbeatInterval <= 0 {
		return nil, nil, nil, fmt.Errorf("heartbeat interval should be greater than zero")
	}
	if input.HeartbeatMaxAge <= 0 {
		return nil, nil, nil, fmt.Errorf("heartbeat max age should be greater than zero")
	}
	if input.HeartbeatMaxAge < input.HeartbeatInterval {
		return nil, nil, nil, fmt.Errorf("heartbeat max age should be greater than heartbeat interval")
	}
	if input.ID == "" {
		return nil, nil, nil, fmt.Errorf("id should not be empty string")
	}
	if input.Retries < 0 {
		return nil, nil, nil, fmt.Errorf("retries should not be negative")
	}

	uid := uuid.Must(uuid.NewV4()).String()
	key, err := attributevalue.MarshalMap(LockKey{ID: input.ID})
	if err != nil {
		return nil, nil, nil, err
	}

	var acquiredItem map[string]ddbtypes.AttributeValue
	acquireLock := func() error {
		expiredBefore := time.Now().Add(-input.HeartbeatMaxAge).Unix()
		condition := expression.Name("uid").AttributeNotExists().
			Or(expression.AttributeType(expression.Name("uid"), "NULL")).
			Or(expression.Name("uid").Equal(expression.Value(""))).
			Or(expression.Name("unix").LessThanEqual(expression.Value(expiredBefore)))

		expr, err := expression.NewBuilder().
			WithCondition(condition).
			WithUpdate(expression.
				Set(expression.Name("uid"), expression.Value(uid)).
				Set(expression.Name("unix"), expression.Value(time.Now().Unix()))).
			Build()
		if err != nil {
			return err
		}
		updateOut, err := dynamoDBClient().UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName:                 aws.String(input.Table),
			Key:                       key,
			ConditionExpression:       expr.Condition(),
			UpdateExpression:          expr.Update(),
			ExpressionAttributeValues: expr.Values(),
			ExpressionAttributeNames:  expr.Names(),
			ReturnValues:              ddbtypes.ReturnValueAllOld,
		})
		if err != nil {
			return fmt.Errorf("failed to acquire the lock: %w", err)
		}
		acquiredItem = updateOut.Attributes
		return nil
	}

	retryCount := 0
	for {
		err := acquireLock()
		if err == nil {
			break
		}
		if !isConditionalCheckFailed(err) {
			return nil, nil, nil, err
		}
		if retryCount >= input.Retries {
			err = fmt.Errorf("lock is held: %s %s", input.ID, uid)
			return nil, nil, nil, err
		}
		retryCount++
		sleepDuration := 1 * time.Second
		if input.RetriesSleep > 0 {
			sleepDuration = input.RetriesSleep
		}
		// lib.Logger.Printf("retrying lock: id=%s uid=%s retry=%d/%d sleep=%s\n", input.ID, uid, retryCount, input.Retries, sleepDuration)
		select {
		case <-time.After(sleepDuration):
		case <-ctx.Done():
			return nil, nil, nil, ctx.Err()
		}
	}

	var data *T
	if len(acquiredItem) != 0 {
		clearInternalKeys(acquiredItem)
		err = attributevalue.UnmarshalMap(acquiredItem, &val)
		if err != nil {
			return nil, nil, nil, err
		}
		data = &val
	}

	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	go heartbeatLock(heartbeatCtx, input, uid)

	unlock := func(data *T) error {
		return releaseLock(ctx, input, uid, data, cancelHeartbeat)
	}
	update := func(data *T) error {
		return updateLocked(ctx, input, uid, data)
	}

	return unlock, update, data, nil
}

func isConditionalCheckFailed(err error) bool {
	var condErr *ddbtypes.ConditionalCheckFailedException
	return errors.As(err, &condErr)
}

func updateLocked[T any](ctx context.Context, input *LockInput, uid string, data *T) error {
	expr, err := expression.NewBuilder().
		WithCondition(expression.Name("uid").Equal(expression.Value(uid))).
		Build()
	if err != nil {
		return err
	}
	item, err := buildItem(input.ID, uid, time.Now().Unix(), data)
	if err != nil {
		return err
	}
	_, err = dynamoDBClient().PutItem(ctx, &dynamodb.PutItemInput{
		Item:                      item,
		TableName:                 aws.String(input.Table),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})
	if err != nil {
		lib.Logger.Printf("failed to update locked: %s %s %s\n", input.ID, uid, err)
		return err
	}
	return nil
}

func releaseLock[T any](ctx context.Context, input *LockInput, uid string, data *T, cancelHeartbeat func()) error {
	expr, err := expression.NewBuilder().
		WithCondition(expression.Name("uid").Equal(expression.Value(uid))).
		Build()
	if err != nil {
		return err
	}
	cancelHeartbeat()
	item, err := buildItem(input.ID, "", 0, data)
	if err != nil {
		return err
	}
	_, err = dynamoDBClient().PutItem(ctx, &dynamodb.PutItemInput{
		Item:                      item,
		TableName:                 aws.String(input.Table),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})
	if err != nil {
		lib.Logger.Printf("failed to release the lock: %s %s %s\n", input.ID, uid, err)
		return err
	}
	return nil
}

func heartbeatLock(ctx context.Context, input *LockInput, uid string) {
	defer func() {
		r := recover()
		if r != nil {
			stack := string(debug.Stack())
			lib.Logger.Println(r)
			lib.Logger.Println(stack)
			lib.Logger.Flush()
			panic(r)
		}
	}()
	key, err := attributevalue.MarshalMap(LockKey{ID: input.ID})
	if err != nil {
		if input.HeartbeatErrFn != nil {
			input.HeartbeatErrFn(err)
			return
		}
		panic(err)
	}
	for {
		select {
		case <-time.After(input.HeartbeatInterval):
		case <-ctx.Done():
			// lib.Logger.Printf("stop the heartbeat: %s %s\n", input.ID, uid)
			return
		}
		expr, err := expression.NewBuilder().
			WithCondition(expression.Name("uid").Equal(expression.Value(uid))).
			WithUpdate(expression.
				Set(expression.Name("unix"), expression.Value(time.Now().Unix()))).
			Build()
		if err != nil {
			if input.HeartbeatErrFn != nil {
				input.HeartbeatErrFn(err)
				return
			}
			panic(err)
		}
		attempts := 5 // ~5 seconds of retries
		err = lib.RetryAttempts(ctx, attempts, func() error {
			_, err := dynamoDBClient().UpdateItem(ctx, &dynamodb.UpdateItemInput{
				TableName:                 aws.String(input.Table),
				Key:                       key,
				ConditionExpression:       expr.Condition(),
				UpdateExpression:          expr.Update(),
				ExpressionAttributeValues: expr.Values(),
				ExpressionAttributeNames:  expr.Names(),
			})
			return err
		})
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				if input.HeartbeatErrFn != nil {
					input.HeartbeatErrFn(fmt.Errorf("failed to heartbeat the lock: %w", err))
					return
				}
				panic("failed to heartbeat the lock: " + err.Error())
			}
		}
	}
}
