package dynamolock

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Lease owns one acquisition. Do not copy it. Payload operations are serialized;
// renewal is independent, so slow marshaling or writes do not block heartbeats.
// Callers own their payload values and must not mutate them during a write.
// A lost lease never becomes usable again, even if a late renewal succeeds.
type Lease[T any] struct {
	client *dynamodb.Client
	input  LockInput
	owner  string
	ctx    context.Context
	cancel context.CancelCauseFunc
	done   chan struct{}
	op     chan struct{}

	mu        sync.Mutex
	deadline  time.Time // Includes Go's monotonic clock reading.
	expires   int64     // Absolute Unix nanoseconds stored in DynamoDB.
	timer     *time.Timer
	released  bool
	finishing chan struct{}
}

func newLease[T any](ctx context.Context, client *dynamodb.Client, in LockInput, owner string, started time.Time) *Lease[T] {
	leaseCtx, cancel := context.WithCancelCause(ctx)
	l := &Lease[T]{
		client: client, input: in, owner: owner, ctx: leaseCtx, cancel: cancel,
		done: make(chan struct{}), op: make(chan struct{}, 1),
		deadline: started.Add(in.HeartbeatMaxAge), expires: started.Add(in.HeartbeatMaxAge).UnixNano(),
	}
	l.mu.Lock()
	l.armDeadlineLocked()
	l.mu.Unlock()
	go l.heartbeat()
	return l
}

// Context is canceled on parent cancellation, loss, or successful completion.
// context.Cause supplies the reason. Its cancellation cannot undo an external
// effect or a DynamoDB request that was already in flight.
func (l *Lease[T]) Context() context.Context { return l.ctx }

// ID returns the immutable primary key shared by the lease and its payload.
func (l *Lease[T]) ID() string { return l.input.ID }

func (l *Lease[T]) armDeadlineLocked() {
	if l.timer != nil {
		l.timer.Stop()
	}
	deadline := l.deadline
	l.timer = time.AfterFunc(time.Until(deadline), func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.deadline.Equal(deadline) && !l.released {
			l.cancel(fmt.Errorf("%w: confirmation deadline elapsed", ErrLeaseLost))
		}
	})
}

func (l *Lease[T]) lose(cause error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.released {
		l.timer.Stop()
		l.cancel(errors.Join(ErrLeaseLost, cause))
	}
}

func (l *Lease[T]) liveError() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.liveErrorLocked()
}

func (l *Lease[T]) liveErrorLocked() error {
	if l.released {
		return ErrReleased
	}
	if !time.Now().Before(l.deadline) || time.Now().UnixNano() >= l.expires {
		l.cancel(fmt.Errorf("%w: confirmation deadline elapsed", ErrLeaseLost))
	}
	if cause := context.Cause(l.ctx); cause != nil {
		if errors.Is(cause, ErrLeaseLost) {
			return cause
		}
		return errors.Join(ErrLeaseLost, cause)
	}
	return nil
}

func (l *Lease[T]) confirm(started time.Time, expires int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.liveErrorLocked(); err != nil {
		return err
	}
	l.deadline, l.expires = started.Add(l.input.HeartbeatMaxAge), expires
	l.armDeadlineLocked()
	return nil
}

func (l *Lease[T]) heartbeat() {
	defer func() {
		l.mu.Lock()
		l.timer.Stop()
		l.mu.Unlock()
		close(l.done)
	}()
	for {
		if l.liveError() != nil {
			return
		}
		l.mu.Lock()
		next := l.deadline.Add(-l.input.HeartbeatMaxAge + l.input.HeartbeatInterval)
		l.mu.Unlock()
		if wait(l.ctx, time.Until(next)) != nil {
			return
		}
		if err := l.renew(); err != nil {
			l.lose(err)
			return
		}
	}
}

func (l *Lease[T]) renew() error {
	l.mu.Lock()
	deadline, confirmedExpiry := l.deadline, l.expires
	l.mu.Unlock()
	ctx, cancel := context.WithDeadline(l.ctx, deadline)
	defer cancel()
	for attempt := 0; ; attempt++ {
		if err := l.liveError(); err != nil {
			return err
		}
		started := time.Now()
		expires := started.Add(l.input.HeartbeatMaxAge).UnixNano()
		if expires <= confirmedExpiry {
			return errors.New("wall clock moved backward during renewal")
		}
		_, err := l.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(l.input.Table), Key: key(l.input.ID),
			UpdateExpression:         aws.String("SET #expires = :next"),
			ConditionExpression:      aws.String("#owner = :owner AND #expires > :now AND #expires < :next"),
			ExpressionAttributeNames: map[string]string{"#owner": "owner_token", "#expires": "expires_at"},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":owner": &types.AttributeValueMemberS{Value: l.owner},
				":now":   number(started.UnixNano()), ":next": number(expires),
			},
			ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
		}, noSDKRetry)
		if err == nil {
			return l.confirm(started, expires)
		}
		if failure, ok := conditionalFailure(err); ok {
			r, parseErr := parseRecord(failure.Item)
			if parseErr == nil && r.id == l.input.ID && r.owner == l.owner && r.expires >= expires {
				// Another delayed renewal already established at least this expiry.
				return l.confirm(started, expires)
			}
			l.mu.Lock()
			finishing := l.finishing
			l.mu.Unlock()
			if finishing != nil {
				// Our own commit/release may have cleared ownership before its
				// response arrives. Do not cancel that response. The independent
				// confirmation deadline still bounds this wait.
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-finishing:
					return nil
				}
			}
			return errors.Join(err, parseErr)
		}
		if !retryable(err) || attempt+1 >= maxAttempts {
			return err
		}
		if err := backoff(ctx, attempt); err != nil {
			return err
		}
	}
}

func (l *Lease[T]) beginOperation(ctx context.Context) error {
	if l == nil || l.ctx == nil || ctx == nil {
		return errors.New("initialized lease and context are required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case l.op <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-l.op
			return err
		}
		return nil
	}
}

func (l *Lease[T]) beginFinish() chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.finishing = make(chan struct{})
	return l.finishing
}

func (l *Lease[T]) endFinish(finishing chan struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.finishing = nil
	close(finishing)
}

func (l *Lease[T]) complete() {
	l.mu.Lock()
	l.released = true
	l.timer.Stop()
	l.cancel(ErrReleased)
	l.mu.Unlock()
	<-l.done
}

// Update replaces the entire payload without extending or releasing the lease.
// Nil is rejected. Fields omitted by the payload's marshaler are removed.
func (l *Lease[T]) Update(ctx context.Context, data *T) error {
	return l.write(ctx, data, false)
}

// Commit atomically replaces the payload and releases ownership. Nil is
// rejected. A repeated Commit fails with ErrReleased; it is not a new write.
func (l *Lease[T]) Commit(ctx context.Context, data *T) error {
	return l.write(ctx, data, true)
}

func (l *Lease[T]) write(ctx context.Context, data *T, commit bool) error {
	if err := l.beginOperation(ctx); err != nil {
		return err
	}
	defer func() { <-l.op }()
	if err := l.liveError(); err != nil {
		return err
	}
	payload, err := marshalPayload(l.input.ID, data)
	if err != nil {
		return err
	}
	if err := l.liveError(); err != nil {
		return err
	}
	if commit {
		finishing := l.beginFinish()
		defer l.endFinish(finishing)
	}
	callCtx, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(l.ctx, func() { cancel(context.Cause(l.ctx)) })
	defer func() { stop(); cancel(nil) }()
	for attempt := 0; ; attempt++ {
		if err := l.liveError(); err != nil {
			return err
		}
		if err := callCtx.Err(); err != nil {
			return err
		}
		update := "SET #data = :data"
		if commit {
			update += " REMOVE #owner, #expires"
		}
		_, err := l.client.UpdateItem(callCtx, &dynamodb.UpdateItemInput{
			TableName: aws.String(l.input.Table), Key: key(l.input.ID),
			UpdateExpression:         aws.String(update),
			ConditionExpression:      aws.String("#owner = :owner AND #expires > :now"),
			ExpressionAttributeNames: map[string]string{"#owner": "owner_token", "#expires": "expires_at", "#data": "data"},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":owner": &types.AttributeValueMemberS{Value: l.owner},
				":now":   number(time.Now().UnixNano()), ":data": payload,
			},
		}, noSDKRetry)
		if err == nil {
			if commit {
				l.complete()
			}
			return nil
		}
		if _, ok := conditionalFailure(err); ok {
			l.lose(err)
			return errors.Join(ErrLeaseLost, err)
		}
		if !definitelyRejected(err) {
			err = fmt.Errorf("%w: payload write: %w", ErrOutcomeUnknown, err)
			l.lose(err)
			return err
		}
		if !retryable(err) || attempt+1 >= maxAttempts {
			return err
		}
		if err := backoff(callCtx, attempt); err != nil {
			return err
		}
	}
}

// Release clears only this handle's ownership, preserving the raw payload. It
// works after loss or parent cancellation with a fresh context. Release is
// idempotent: an absent/different token or a completed handle is already released.
// Successful completion joins the heartbeat goroutine.
func (l *Lease[T]) Release(ctx context.Context) error {
	if err := l.beginOperation(ctx); err != nil {
		return err
	}
	defer func() { <-l.op }()
	l.mu.Lock()
	released := l.released
	l.mu.Unlock()
	if released {
		return nil
	}
	finishing := l.beginFinish()
	defer l.endFinish(finishing)
	uncertain := false
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil && !uncertain {
			return ctx.Err()
		}
		_, err := l.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(l.input.Table), Key: key(l.input.ID),
			UpdateExpression:          aws.String("REMOVE #owner, #expires"),
			ConditionExpression:       aws.String("#owner = :owner"),
			ExpressionAttributeNames:  map[string]string{"#owner": "owner_token", "#expires": "expires_at"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":owner": &types.AttributeValueMemberS{Value: l.owner}},
		}, noSDKRetry)
		_, notOwned := conditionalFailure(err)
		if err == nil || notOwned {
			l.complete()
			return nil
		}
		uncertain = uncertain || !definitelyRejected(err)
		if retryable(err) && attempt+1 < maxAttempts {
			if waitErr := backoff(ctx, attempt); waitErr == nil {
				continue
			} else {
				err = errors.Join(err, waitErr)
			}
		}
		if uncertain {
			err = fmt.Errorf("%w: release: %w", ErrOutcomeUnknown, err)
			l.lose(err)
		}
		return err
	}
}
