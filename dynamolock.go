// Package dynamolock coordinates leases and atomic payload writes on DynamoDB
// items with a string partition key named id. It does not fence external effects.
package dynamolock

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	mathrand "math/rand/v2"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// ErrLockUnavailable classifies expected acquisition failures.
var ErrLockUnavailable = errors.New("lock unavailable")

// ErrLockNotFound means RequireExisting prevented creation of a missing item.
var ErrLockNotFound = fmt.Errorf("%w: required item is missing", ErrLockUnavailable)

// ErrLockHeld means the configured contention retries were exhausted.
var ErrLockHeld = fmt.Errorf("%w: lock is held", ErrLockUnavailable)

// ErrLeaseLost means the handle can no longer authorize payload writes.
var ErrLeaseLost = errors.New("lease lost")

// ErrReleased means this handle has already been released or committed.
var ErrReleased = errors.New("lease released")

// ErrOutcomeUnknown means a write may have committed. It does not mean rollback.
// An ambiguous payload write permanently invalidates the handle for further
// writes. Release-only cleanup is still allowed with a fresh context.
var ErrOutcomeUnknown = errors.New("write outcome unknown")

// LockInput is copied by Lock. Its table, key, and timing must not be changed
// during that call, but the caller may freely reuse it after Lock returns.
type LockInput struct {
	Table             string
	ID                string
	RequireExisting   bool
	HeartbeatMaxAge   time.Duration // Duration of each lease, established by its holder.
	HeartbeatInterval time.Duration // Measured from the last confirmed request's start.
	Retries           int           // Additional attempts after ordinary contention.
	RetriesSleep      time.Duration // Zero uses one second.
}

// Read returns the latest committed payload without acquiring ownership. Reads
// are strongly consistent, but do not make a subsequent external read atomic.
func Read[T any](ctx context.Context, client *dynamodb.Client, table, id string) (*T, error) {
	if err := validateTarget(ctx, client, table, id); err != nil {
		return nil, err
	}
	if err := validateType[T](); err != nil {
		return nil, err
	}
	item, err := getItem(ctx, client, table, id)
	if err != nil {
		return nil, err
	}
	return UnmarshalItem[T](item)
}

// Lock atomically acquires ownership and reads the payload. The context owns
// the entire lease lifetime, not just acquisition. Work must use Lease.Context
// and stop when it is canceled. Missing data returns nil even for a metadata-only
// existing item; RequireExisting tests the existence of the item, not its data.
//
// The client must honor request contexts. SDK retries are disabled per request;
// this package alone retries, bounded by the caller or confirmed lease lifetime.
func Lock[T any](ctx context.Context, client *dynamodb.Client, input *LockInput) (lease *Lease[T], data *T, err error) {
	if input == nil {
		return nil, nil, errors.New("nil LockInput")
	}
	in := *input
	if err := validateTarget(ctx, client, in.Table, in.ID); err != nil {
		return nil, nil, err
	}
	if err := validateType[T](); err != nil {
		return nil, nil, err
	}
	if in.HeartbeatInterval <= 0 || in.HeartbeatMaxAge <= in.HeartbeatInterval {
		return nil, nil, errors.New("heartbeat max age must exceed a positive heartbeat interval")
	}
	if int64(in.HeartbeatMaxAge) > math.MaxInt64-time.Now().UnixNano() {
		return nil, nil, errors.New("heartbeat max age exceeds the Unix nanosecond range")
	}
	if in.Retries < 0 || in.RetriesSleep < 0 {
		return nil, nil, errors.New("retries and retries sleep must not be negative")
	}
	if in.RetriesSleep == 0 {
		in.RetriesSleep = time.Second
	}
	owner := rand.Text()
	item, started, err := acquire(ctx, client, in, owner)
	if err != nil {
		return nil, nil, err
	}
	l := newLease[T](ctx, client, in, owner, started)
	success := false
	defer func() {
		if !success {
			// Also release on a custom unmarshaler panic, without trying to write
			// the partially decoded value back to DynamoDB.
			l.lose(errors.New("acquired payload could not be returned"))
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			if cleanupErr := l.Release(cleanupCtx); cleanupErr != nil {
				err = errors.Join(err, fmt.Errorf("release after acquisition failure: %w", cleanupErr))
			}
		}
	}()
	r, err := parseRecord(item)
	if err != nil {
		return nil, nil, err
	}
	if r.id != in.ID || r.owner != owner || r.expires != started.Add(in.HeartbeatMaxAge).UnixNano() {
		return nil, nil, fmt.Errorf("%w: acquisition returned unexpected ownership", ErrInvalidRecord)
	}
	data, err = UnmarshalItem[T](item)
	if err != nil {
		return nil, nil, err
	}
	if err := l.liveError(); err != nil {
		return nil, nil, err
	}
	success = true
	return l, data, nil
}

func validateTarget(ctx context.Context, client *dynamodb.Client, table, id string) error {
	if ctx == nil || client == nil || table == "" {
		return errors.New("context, DynamoDB client, and table are required")
	}
	if err := validateID(id); err != nil {
		return err
	}
	return ctx.Err()
}

func number(n int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(n, 10)}
}

func acquire(ctx context.Context, client *dynamodb.Client, in LockInput, owner string) (map[string]types.AttributeValue, time.Time, error) {
	contention, failures := 0, 0
	for {
		started := time.Now()
		expires := started.Add(in.HeartbeatMaxAge)
		attemptCtx, cancel := context.WithDeadline(ctx, expires)
		condition := "((attribute_not_exists(#owner) AND attribute_not_exists(#expires)) OR #expires < :now)"
		if in.RequireExisting {
			condition = "attribute_exists(#id) AND " + condition
		}
		names := map[string]string{"#owner": "owner_token", "#expires": "expires_at"}
		if in.RequireExisting {
			names["#id"] = "id"
		}
		output, err := client.UpdateItem(attemptCtx, &dynamodb.UpdateItemInput{
			TableName: aws.String(in.Table), Key: key(in.ID),
			ConditionExpression:      aws.String(condition),
			UpdateExpression:         aws.String("SET #owner = :owner, #expires = :expires"),
			ExpressionAttributeNames: names,
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":owner": &types.AttributeValueMemberS{Value: owner},
				":now":   number(started.UnixNano()), ":expires": number(expires.UnixNano()),
			},
			ReturnValues:                        types.ReturnValueAllNew,
			ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
		}, noSDKRetry)
		if err == nil {
			cancel()
			return output.Attributes, started, nil
		}
		if failure, ok := conditionalFailure(err); ok {
			cancel()
			if _, parseErr := parseRecord(failure.Item); parseErr != nil {
				return nil, started, parseErr
			}
			if in.RequireExisting && len(failure.Item) == 0 {
				return nil, started, ErrLockNotFound
			}
			if contention == in.Retries {
				return nil, started, ErrLockHeld
			}
			contention++
			if err := wait(ctx, in.RetriesSleep); err != nil {
				return nil, started, err
			}
			continue
		}
		if !definitelyRejected(err) {
			// Do not resend an ambiguous acquire: a delayed create could reclaim
			// an already released key. A strong read of our unique token can
			// confirm the original acquisition without another ownership write.
			item, readErr := getItem(attemptCtx, client, in.Table, in.ID)
			confirmed := readErr == nil && acquisitionConfirmed(item, in.ID, owner, expires.UnixNano())
			cancel()
			if confirmed {
				return item, started, nil
			}
			return nil, started, fmt.Errorf("%w: acquire: %w", ErrOutcomeUnknown, errors.Join(err, readErr))
		}
		cancel()
		failures++
		if !retryable(err) || failures == maxAttempts {
			return nil, started, err
		}
		if err := backoff(ctx, failures-1); err != nil {
			return nil, started, err
		}
	}
}

// Confirm ownership independently of payload decoding, so a corrupt payload
// still takes the ordinary post-acquisition cleanup path.
func acquisitionConfirmed(item map[string]types.AttributeValue, id, owner string, expires int64) bool {
	k, keyOK := item["id"].(*types.AttributeValueMemberS)
	o, ownerOK := item["owner_token"].(*types.AttributeValueMemberS)
	e, expiryOK := item["expires_at"].(*types.AttributeValueMemberN)
	return keyOK && ownerOK && expiryOK && k != nil && o != nil && e != nil &&
		k.Value == id && o.Value == owner && e.Value == strconv.FormatInt(expires, 10)
}

const maxAttempts = 5

func noSDKRetry(options *dynamodb.Options) {
	options.Retryer = aws.NopRetryer{}
	options.RetryMaxAttempts = 1
}

func conditionalFailure(err error) (*types.ConditionalCheckFailedException, bool) {
	var failure *types.ConditionalCheckFailedException
	ok := errors.As(err, &failure)
	return failure, ok
}

func retryable(err error) bool {
	return retry.NewStandard().IsErrorRetryable(err)
}

// A service rejection is different from a transport/server failure: the latter
// may have committed. In particular, never retry an ambiguous payload write.
func definitelyRejected(err error) bool {
	var apiError smithy.APIError
	if !errors.As(err, &apiError) || apiError.ErrorCode() == "RequestTimeout" || apiError.ErrorCode() == "RequestTimeoutException" {
		return false
	}
	if apiError.ErrorFault() == smithy.FaultClient {
		return true
	}
	// Unmodeled service errors (including ValidationException) do not have a
	// generated FaultClient value. Require an actual service rejection response.
	var responseError *smithyhttp.ResponseError
	return errors.As(err, &responseError) && responseError.HTTPStatusCode() >= 400 &&
		responseError.HTTPStatusCode() < 500 && responseError.HTTPStatusCode() != 408
}

func wait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

func backoff(ctx context.Context, attempt int) error {
	ceiling := min(100*time.Millisecond<<attempt, time.Second)
	return wait(ctx, ceiling/2+time.Duration(mathrand.Int64N(int64(ceiling/2))))
}

func getItem(ctx context.Context, client *dynamodb.Client, table, id string) (map[string]types.AttributeValue, error) {
	for attempt := 0; ; attempt++ {
		output, err := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(table), Key: key(id), ConsistentRead: aws.Bool(true),
		}, noSDKRetry)
		if err == nil {
			return output.Item, nil
		}
		if !retryable(err) || attempt+1 == maxAttempts {
			return nil, err
		}
		if err := backoff(ctx, attempt); err != nil {
			return nil, err
		}
	}
}
