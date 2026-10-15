package dynamolock

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// CAS is a reusable optimistic writer, not a held lock. Do not copy it.
// Operations serialize; caller-owned values must not be mutated during a write.
// Successful updates advance its snapshot without another read. Conflicts and
// ambiguous writes permanently invalidate it; successful Delete consumes it.
// There is no heartbeat, Release, or background work. Each call owns its context.
type CAS[T any] struct {
	client   *dynamodb.Client
	table    Table
	key      Key
	op       chan struct{}
	terminal error
	present  bool
	revision revision
	raw      *types.AttributeValueMemberM
	visible  *types.AttributeValueMemberM
}

// Create writes data only if the item is physically absent, without reading or
// acquiring a lease. Existing items cause ErrConflict, including those without
// payload or with expired ownership. Encoding, projections and revisions use the
// same path as CAS.Update. ErrOutcomeUnknown means creation may have succeeded;
// reconcile the stored record before retrying.
func Create[T any](ctx context.Context, client *dynamodb.Client, table Table, key Key, data *T) error {
	var err error
	table, err = table.normalized()
	if err != nil {
		return err
	}
	if err := validateTarget(ctx, client, table, key); err != nil {
		return err
	}
	if err := validateType[T](); err != nil {
		return err
	}
	writer := &CAS[T]{client: client, table: table, key: key, op: make(chan struct{}, 1)}
	return writer.Update(ctx, data)
}

// ReadCAS reads a complete snapshot and returns its optimistic writer and value.
// Reads default to eventual consistency. An observed live lease returns the
// inspectable value and ErrLockHeld, but no writer. Every write independently
// checks ownership and revision; stale reads can cause conflicts, not lost updates.
// An absent item returns a creation-capable writer and nil data. Existing records
// without revision metadata are initialized with one conditional write, using its
// returned payload rather than attaching a revision to the potentially stale read.
// All old writers must be stopped before enabling this shared versioned protocol.
func ReadCAS[T any](ctx context.Context, client *dynamodb.Client, table Table, key Key, options ReadOptions) (*CAS[T], *T, error) {
	var err error
	table, err = table.normalized()
	if err != nil {
		return nil, nil, err
	}
	if err := validateTarget(ctx, client, table, key); err != nil {
		return nil, nil, err
	}
	if err := validateType[T](); err != nil {
		return nil, nil, err
	}
	item, err := readItem(ctx, client, &dynamodb.GetItemInput{
		TableName: aws.String(table.Name), Key: table.key(key), ConsistentRead: aws.Bool(options.ConsistentRead),
	})
	if err != nil {
		return nil, nil, err
	}
	r, err := parseRecord(table, item)
	if err != nil {
		return nil, nil, err
	}
	if len(item) != 0 && r.key != key {
		return nil, nil, fmt.Errorf("%w: CAS read returned another key", ErrInvalidRecord)
	}
	if len(item) != 0 && r.revision.generation == "" && !leaseObserved(r) {
		item, err = initializeCAS(ctx, client, table, key)
		if err != nil {
			return nil, nil, err
		}
		r, err = parseRecord(table, item)
		if err != nil {
			return nil, nil, err
		}
		if len(item) != 0 && r.key != key {
			return nil, nil, fmt.Errorf("%w: CAS initialization returned another key", ErrInvalidRecord)
		}
	}
	writer := &CAS[T]{client: client, table: table, key: key, op: make(chan struct{}, 1), present: len(item) != 0, revision: r.revision}
	writer.raw, err = snapshotPayload(r.data)
	if err != nil {
		return nil, nil, err
	}
	data, err := UnmarshalItem[T](table, item)
	if err != nil {
		return nil, nil, err
	}
	if leaseObserved(r) {
		return nil, data, ErrLockHeld
	}
	if writer.present && r.revision.generation == "" {
		// A competing initialization must either establish a revision or expose
		// a live lease. Never authorize a write from an unversioned snapshot.
		return nil, data, ErrConflict
	}
	if data != nil {
		visible, err := marshalPayload(table, key, data)
		if err != nil {
			return nil, nil, err
		}
		writer.visible, err = snapshotPayload(visible)
		if err != nil {
			return nil, nil, err
		}
	}
	return writer, data, nil
}

func leaseObserved(r record) bool {
	return r.owner != "" && r.expires >= time.Now().UnixNano()
}

func initializeCAS(ctx context.Context, client *dynamodb.Client, table Table, key Key) (map[string]types.AttributeValue, error) {
	generation := rand.Text()
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(table.Name), Key: table.key(key),
			ConditionExpression:      aws.String("attribute_exists(#id) AND attribute_not_exists(#generation) AND attribute_not_exists(#version) AND " + availableCondition),
			UpdateExpression:         aws.String("SET #generation = :generation, #version = :one REMOVE #owner, #expires"),
			ExpressionAttributeNames: map[string]string{"#id": table.PartitionKey, "#generation": "generation", "#version": "version", "#owner": "owner_token", "#expires": "expires_at"},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":generation": &types.AttributeValueMemberS{Value: generation}, ":one": versionNumber(1), ":now": number(time.Now().UnixNano()),
			},
			ReturnValues: types.ReturnValueAllNew, ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
		}, noSDKRetry)
		if err == nil {
			r, parseErr := parseRecord(table, out.Attributes)
			if parseErr != nil {
				return nil, parseErr
			}
			if r.key != key || r.revision != (revision{generation: generation, version: 1}) || r.owner != "" {
				return nil, fmt.Errorf("%w: unexpected CAS initialization response", ErrInvalidRecord)
			}
			return out.Attributes, nil
		}
		if failure, ok := conditionalFailure(err); ok {
			// The winning initialization/acquisition (or deletion) supplies the
			// current observation without an extra read or an unbounded retry loop.
			return failure.Item, nil
		}
		if !definitelyRejected(err) {
			return nil, fmt.Errorf("%w: initialize CAS: %w", ErrOutcomeUnknown, err)
		}
		if !retryable(err) || attempt+1 >= maxAttempts {
			return nil, err
		}
		if err := backoff(ctx, attempt); err != nil {
			return nil, err
		}
	}
}

// Key returns the primary key associated with this snapshot.
func (c *CAS[T]) Key() Key { return c.key }

func (c *CAS[T]) begin(ctx context.Context) error {
	if c == nil || c.op == nil || ctx == nil {
		return errors.New("initialized CAS writer and context are required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.op <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-c.op
			return err
		}
		if c.terminal != nil {
			<-c.op
			return c.terminal
		}
		return nil
	}
}

// Update merges caller-visible changes, preserving unknown fields exactly as
// Lease.Update does. It conditionally creates an absent item or advances the
// observed revision. It never acquires a lease, and removes expired ownership.
func (c *CAS[T]) Update(ctx context.Context, data *T) error {
	if err := c.begin(ctx); err != nil {
		return err
	}
	defer func() { <-c.op }()
	if c.revision.version == math.MaxUint64 {
		return ErrVersionExhausted
	}
	payload, err := marshalPayload(c.table, c.key, data)
	if err != nil {
		return err
	}
	payload, err = snapshotPayload(payload)
	if err != nil {
		return err
	}
	next, err := mergePayload[T](c.raw, c.visible, payload)
	if err != nil {
		return err
	}
	nextRevision := revision{generation: c.revision.generation, version: c.revision.version + 1}
	if !c.present {
		nextRevision.generation = rand.Text()
	}
	if err := c.mutate(ctx, next, nextRevision); err != nil {
		return err
	}
	c.present, c.revision, c.raw, c.visible = true, nextRevision, next, payload
	return nil
}

// Delete physically removes the observed generation under the same CAS fence.
// For an absent snapshot it succeeds only if the item is still absent. Success
// consumes the handle; ambiguity invalidates it without retrying the deletion.
func (c *CAS[T]) Delete(ctx context.Context) error {
	if err := c.begin(ctx); err != nil {
		return err
	}
	defer func() { <-c.op }()
	if err := c.mutate(ctx, nil, revision{}); err != nil {
		return err
	}
	c.terminal = ErrReleased
	return nil
}

func (c *CAS[T]) mutate(ctx context.Context, payload *types.AttributeValueMemberM, next revision) error {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		names := map[string]string{}
		values := map[string]types.AttributeValue{}
		var update string
		if payload != nil {
			update, names, values = payloadExpression(c.table, payload, true, next.version)
			names["#generation"] = "generation"
			values[":generation"] = &types.AttributeValueMemberS{Value: next.generation}
		}
		condition := "attribute_not_exists(#id)"
		if c.present {
			c.revision.fence(names, values)
			names["#owner"], names["#expires"] = "owner_token", "expires_at"
			values[":now"] = number(time.Now().UnixNano())
			condition = revisionCondition + " AND " + availableCondition
		} else {
			names["#id"] = c.table.PartitionKey
		}
		if len(values) == 0 {
			values = nil
		}
		var err error
		if payload == nil {
			_, err = c.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
				TableName: aws.String(c.table.Name), Key: c.table.key(c.key),
				ConditionExpression: aws.String(condition), ExpressionAttributeNames: names, ExpressionAttributeValues: values,
			}, noSDKRetry)
		} else {
			_, err = c.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
				TableName: aws.String(c.table.Name), Key: c.table.key(c.key), UpdateExpression: aws.String(update),
				ConditionExpression: aws.String(condition), ExpressionAttributeNames: names, ExpressionAttributeValues: values,
			}, noSDKRetry)
		}
		if err == nil {
			return nil
		}
		if _, ok := conditionalFailure(err); ok {
			c.terminal = errors.Join(ErrConflict, err)
			return c.terminal
		}
		if !definitelyRejected(err) {
			c.terminal = fmt.Errorf("%w: CAS mutation: %w", ErrOutcomeUnknown, err)
			return c.terminal
		}
		if !retryable(err) || attempt+1 >= maxAttempts {
			return err
		}
		if err := backoff(ctx, attempt); err != nil {
			return err
		}
	}
}
