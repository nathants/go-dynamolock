# Go-Dynamolock

DynamoDB leases with atomic read-modify-write payloads. The caller supplies an
AWS SDK v2 DynamoDB client; the library does not load credentials or log.

## Record and identity

The table has a string partition key named `id`, and no sort key:

```text
id:          string
owner_token: string                 # present only while owned
expires_at:  number                 # Unix nanoseconds; present with owner_token
data:        map                    # absent until explicitly written
```

`id` identifies both the lock and the payload. It is stored only at the outer
level and injected into decoded application data. A payload's missing or empty
`dynamodbav:"id"` uses the lock key; a nonempty conflicting ID is rejected.
Case aliases such as `ID` follow the same rule, matching the SDK's field lookup.
Application data may otherwise use any attribute names, including `owner_token`
and `expires_at`, inside `data`. Map values must be able to decode the injected
string identity; `map[string]any` is suitable. Interface-valued numbers decode
as `attributevalue.Number`, preserving their decimal precision rather than
rounding through `float64`. Explicitly typed numeric fields keep their Go types.

`MarshalItem(id, data)` produces an unlocked envelope for conditional creation.
`UnmarshalItem[T](item)` decodes an envelope for scans and stream consumers. These
helpers do not authorize bypassing another writer's lease. Flat legacy items
are not supported. All writers of a participating item must follow the same
ownership protocol.

## Operations

1. `Lock[T](ctx, client, input)` returns `(*Lease[T], *T, error)`. Acquisition and
   payload retrieval are one conditional `UpdateItem`, not a read followed by a
   lock. `T` must be a struct or string-keyed map, not a pointer type.
2. `lease.Update(ctx, data)` replaces `data` while retaining ownership. It does
   not write or extend the lease expiry.
3. `lease.Commit(ctx, data)` replaces `data` and clears ownership atomically.
4. `lease.Release(ctx)` clears only this handle's ownership, preserving the raw
   stored payload. It is idempotent and safe for deferred cleanup after Commit.
5. `Read[T](ctx, client, table, id)` reads the payload strongly consistently
   without acquiring a lock. It may observe a checkpoint published by Update.

Update and Commit replace the **whole payload**, not individual fields. Fields
omitted by the marshaler disappear, including fields unknown to an older/narrower
struct. Use Release when no payload change is intended. Nil write payloads are
rejected, including pointers to nil maps. An explicit empty struct or non-nil
empty map deliberately writes empty data. Scalar or list encodings, including
those from custom marshalers, are rejected.

Absent data returns nil, including for a metadata-only existing item. An explicit
empty map returns a non-nil value. `RequireExisting` checks item existence at
acquisition time: it returns `ErrLockNotFound` without creating a missing row. It
does not distinguish deletion/recreation of the same key or application-level
absence, such as a placeholder account lacking credentials.

## Lifetime and failure

The Lock context owns the entire lease lifetime. `lease.Context()` is canceled
on parent cancellation, lease loss, or successful completion. Use it for work
protected by the lease and inspect `context.Cause` for the reason. There is no
background panic or callback. Cancellation does not automatically release the
item; use a fresh, bounded context for release-only cleanup.

The holder establishes its expiry; contenders cannot shorten it using their own
`HeartbeatMaxAge`. Lease times are stored without second rounding, and expiry
comparison is strict. Heartbeat retries generate fresh, increasing expiries and
cannot roll an existing expiry backward. Renewal scheduling includes request
time instead of sleeping a full interval after each response. Attempts are
bounded by the last confirmed lease deadline, with at most five attempts and
jittered backoff. SDK retries are disabled per request to avoid stacked policies.
Acquisition requests are bounded by their proposed lease lifetime. Choose timing
that leaves ample room for network latency and retries.

A separate local deadline cancels work even if renewal stalls. The local lifetime
uses Go's monotonic clock; persisted expiry uses wall-clock time. Hosts must have
synchronized clocks without disruptive clock steps: clock skew can still cause
early takeover. A non-increasing renewal expiry (for example, after a backward
clock step) fails closed rather than extending an uncertain lease. **Do not
configure DynamoDB TTL on `expires_at`:**
lease expiration permits takeover, not deletion of the payload.

Once lost, a handle permanently rejects Update and Commit, even with a fresh
context or a late successful heartbeat. Release-only cleanup remains possible
and never clears a successor's token. Successful Commit/Release joins the
heartbeat goroutine. Concurrent payload operations are serialized independently
of renewal; caller-owned payload values must not be mutated while marshaling.
The supplied client's transport must honor request contexts. Custom codecs must
return promptly; they cannot be forcibly canceled by this package.

Errors can be matched with `errors.Is`:

- `ErrLockHeld`: contention retries exhausted. `Retries` is the number of
  additional attempts; `RetriesSleep` defaults to one second.
- `ErrLockNotFound`: a required item is missing. Both acquisition errors also
  match `ErrLockUnavailable`.
- `ErrLeaseLost`: no further payload writes are permitted.
- `ErrReleased`: this handle has already committed/released; another Commit or
  Update is rejected. Repeated Release succeeds without writing.
- `ErrOutcomeUnknown`: a write may have committed; this is **not rollback**.
  An ambiguous acquisition is reconciled only by strongly reading its unique
  token. Ambiguous payload writes are not retried and invalidate the handle.
  Callers must reconcile application state rather than blindly repeat external
  effects. Release can safely retry its token-conditional, payload-free operation.

A DynamoDB lease fences this library's writes to that item, **not arbitrary S3,
email, payment, process, or cloud operations**. A paused holder can resume after
another owner takes over. External effects need their own identity, idempotency,
or fencing where correctness requires it. Read followed by an external read is
not an atomic snapshot of both systems.

## Usage

```go
package example

import (
    "context"
    "errors"
    "time"

    "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    "github.com/nathants/go-dynamolock"
)

type Data struct {
    ID    string `dynamodbav:"id"`
    Value string `dynamodbav:"value"`
}

func update(ctx context.Context, client *dynamodb.Client) (err error) {
    lease, data, err := dynamolock.Lock[Data](ctx, client, &dynamolock.LockInput{
        Table: "table", ID: "lock1",
        HeartbeatMaxAge: 30 * time.Second,
        HeartbeatInterval: time.Second,
        Retries: 5, RetriesSleep: time.Second,
    })
    if err != nil {
        return err
    }
    defer func() {
        cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
        defer cancel()
        err = errors.Join(err, lease.Release(cleanup))
    }()

    // Pass lease.Context() to any protected work and stop on cancellation.
    if data == nil {
        data = &Data{}
    }
    data.Value = "updated"
    return lease.Commit(lease.Context(), data)
}
```

## Tests

`GOTOOLCHAIN=local go test -race -timeout=2m ./...` runs offline by default. The
protocol tests inspect actual AWS SDK requests and inject responses; they are
not a replacement DynamoDB implementation.

Live tests require `DYNAMOLOCK_TEST_ACCOUNT` to match STS before mutation. Run
`go test -race -run '^TestLeaseAWS$' -count=1 -timeout=5m` for real conditions,
identity/payload preservation, renewal, and deliberately lost write responses.
