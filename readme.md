# Go-Dynamolock

## Why

Locking around DynamoDB should be simple and easy.

## What

A small Go library for locking and atomically updating data in DynamoDB.

## How

Each item uses an ownership token and an expiration time to coordinate callers.
`Lock` acquires an unlocked or expired item and reads its data atomically. A
background heartbeat extends the lease while it is held.

`Commit` writes data and releases the lock. `Update` writes without releasing it.
`Release` releases the lock without changing data. `Read` reads strongly
consistently without locking, including data published by `Update`.

Set `Retries` to the number of additional acquisition attempts on contention.
`RetriesSleep` controls the delay between them and defaults to one second.
Exhausted retries return `ErrLockHeld`.

## Install

Requires Go 1.27 or newer.

```sh
go get github.com/nathants/go-dynamolock
```

## Usage

Create a DynamoDB table with a string partition key named `id` and no sort key.
Pass a configured AWS SDK v2 DynamoDB client:

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
		Table:             "table",
		ID:                "lock1",
		HeartbeatMaxAge:   30 * time.Second,
		HeartbeatInterval: time.Second,
		Retries:           5,
		RetriesSleep:      time.Second,
	})
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		err = errors.Join(err, lease.Release(cleanup))
	}()

	if data == nil {
		data = &Data{}
	}
	data.Value = "updated"
	return lease.Commit(lease.Context(), data)
}
```

The deferred `Release` handles early returns and cancellation. It is harmless
after a successful `Commit`.

## Data

`T` can be a struct or a string-keyed map, not a pointer type. Map values must
accept the decoded string ID; `map[string]any` works. If no data has been written,
`Lock` and `Read` return nil. An explicitly empty payload returns a non-nil value.

By default, `Lock` creates a missing item. Set `RequireExisting` to reject it with
`ErrLockNotFound` instead. An existing item can still have no data. Both
`ErrLockNotFound` and `ErrLockHeld` also match `ErrLockUnavailable` with `errors.Is`.

Application fields are stored under `data`, separately from ownership metadata.
`id` is stored only at the outer level and filled into decoded data. An optional
string field tagged `dynamodbav:"id"` exposes it in your struct. On writes, an
empty or missing ID uses the lock key; a conflicting ID is rejected. ID attribute
names are matched case-insensitively; other application names are unrestricted.

Struct fields use the SDK's exact-name match first, then its case-insensitive
match. Updates and deletions address the original stored attribute name, including
inside nested structs and typed maps. Ordinary map keys remain case-sensitive.
Multiple stored attributes resolving to one struct field are rejected with
`ErrInvalidRecord`, rather than decoding an arbitrary winner.

Numbers decoded into interface values use `attributevalue.Number` to preserve
precision; explicitly typed numeric fields keep their Go types.

`Update` and `Commit` merge changes from the value returned by `Lock` (or the
last successful write) into the original raw payload, then atomically write that
merged payload under the existing ownership and expiry checks. Map fields absent
from your Go type survive, including nested map fields. Removing a previously
visible map key or omitting a previously encoded field removes that subtree.
Unchanged fields retain their stored representation. Both operations reject nil
values, including nil maps; an explicit empty map removes only keys visible to
that map's prior value. Use `Release` to leave all data unchanged.

Lists and scalar values are atomic fields, not collections with inferred identity.
A changed list that would discard fields hidden by typed decoding is rejected with
`ErrInvalidPayload`; use a representation retaining the complete element data before
changing it. No positional merge guesses which record survived an insertion or
reorder. Custom marshalers run when acquiring existing data as well as on writes;
they must represent the decoded value deterministically and retain attribute names,
including case. Custom codecs own their name mapping; the library does not infer
struct aliases inside them. Unknown-field preservation is not permission to bypass
the lease protocol with another writer.

Deploy field-preserving writers everywhere before adding fields older binaries
would erase. Changing an existing field's meaning/type, or deleting a field still
required by a deployed reader, is not an additive schema change. These payload
updates do not make unrelated whole-record or opaque JSON writers compatible.

`MarshalItem` encodes new records. Insert them with a conditional `PutItem` using
`attribute_not_exists(id)` so an existing item is not overwritten. `UnmarshalItem`
decodes records from scans or streams. All writers to a participating item must
follow the same lease protocol.

## Lease lifetime

`HeartbeatMaxAge` is the lease duration. Choose a shorter `HeartbeatInterval`
with ample room for network latency and retries. If a holder stops renewing,
another caller can acquire the item after its expiry passes.

Renewal retries throttling, server errors, timeouts, and network failures,
including DNS lookup failures, until the last confirmed expiry. Each attempt
times out after half of `HeartbeatMaxAge`, so after a stuck request at most half
the lease minus `HeartbeatInterval` remains for retries; backoff and delayed
renewal starts shorten it. Keep the interval well below half the lease. A
response slower than half the lease is abandoned even if it would have arrived
in time. If no renewal is confirmed in time, the lease is lost; `context.Cause`
includes the last renewal failure if an attempt failed before expiry.
Authorization, validation, and TLS certificate errors lose the lease
immediately.

The context passed to `Lock` controls the entire lease lifetime. Use
`lease.Context()` for protected work and stop when it is canceled; `context.Cause`
reports the reason. Cancellation stops renewal but does not release the item.
Release-only cleanup can use a fresh, bounded context, as in the example above.

Use `errors.Is` to handle failures. `ErrLeaseLost` means the handle can no longer
write; `ErrReleased` means it has already committed or released. An ambiguous
write returns `ErrOutcomeUnknown`: it may have succeeded. An ambiguous payload
write also invalidates the handle, leaving only release cleanup available.
Reconcile application state rather than blindly retrying the operation.

## Limits

Keep host clocks synchronized and avoid disruptive clock changes. Clock skew
can allow early takeover. Do not configure DynamoDB TTL on `expires_at`: lease
expiration makes the lock available again, not the application data disposable.

Conditional writes protect this DynamoDB item, not external effects. A paused
holder can resume after another owner takes over. Operations on other systems
need their own idempotency or fencing; holding this lease alone is not enough.

## Tests

Tests require working AWS credentials, an AWS region, and an independently known
scratch-account ID. The test binary verifies that account with STS before running
any test, including selected protocol tests; missing or invalid setup fails the
suite rather than skipping AWS coverage.

```sh
DYNAMOLOCK_TEST_ACCOUNT="${EXPECTED_ACCOUNT:?}" bash test.sh
```

This runs the full suite with the race detector and a twenty-minute timeout.
By default, live tests create and delete disposable DynamoDB tables. Setting
`REUSE` to any nonempty value instead clears every item in the dedicated
`go-dynamolock` test table before and after each test. Never put application data
there or run reusable-table suites concurrently.
