# Go-Dynamolock

## Why

Locking around DynamoDB should be simple and easy.

## What

A small Go library for leases and optimistic conditional updates in DynamoDB.

## How

Each item uses an ownership token and an expiration time to coordinate callers.
`Lock` acquires an unlocked or expired item and reads its data atomically. A
background heartbeat extends the lease while it is held.

`Commit` writes data and releases the lock. `Update` writes without releasing it.
`Release` releases the lock without changing data. `Delete` removes the item under
the live lease. `Read` reads without locking, including data published by `Update`.
Read consistency is configurable; see [Read consistency and cost](#read-consistency-and-cost).
`ReadCAS` provides an alternative optimistic writer without leases or heartbeats.
`Create` conditionally writes a new item without a preliminary read;
see [Optimistic writes (CAS)](#optimistic-writes-cas).

Set `Retries` to the number of additional acquisition attempts on contention.
`RetriesSleep` controls the delay between them and defaults to one second.
Exhausted retries return `ErrLockHeld`.

## Install

Requires Go 1.27 or newer.

```sh
go get github.com/nathants/go-dynamolock
```

## Usage

Create a DynamoDB table with a string partition key (default name `id`) and an
optional string sort key. Pass a configured AWS SDK v2 DynamoDB client:

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
		Table:         dynamolock.Table{Name: "table"},
		Key:           dynamolock.Key{Partition: "lock1"},
		LeaseDuration: 30 * time.Second,
		Retries:       5,
		RetriesSleep:  time.Second,
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
after a successful `Commit` or `Delete`.

## Optimistic writes (CAS)

Use `ReadCAS` for short, repeatable work that does not need exclusive ownership.
It returns a writer handle and the current value, like `Lock`, but creates no
lease or heartbeat. Reads are **eventually consistent by default**; use
`ReadOptions{ConsistentRead: true}` to reduce conflicts caused by stale reads.
The conditional write checks current ownership and revision regardless of the
read mode. Strong reads cannot prevent changes after the read.

```go
func optimistic(ctx context.Context, client *dynamodb.Client) error {
    writer, data, err := dynamolock.ReadCAS[Data](ctx, client,
        dynamolock.Table{Name: "table"}, dynamolock.Key{Partition: "lock1"},
        dynamolock.ReadOptions{})
    if err != nil {
        // ErrLockHeld may include inspectable data, but writer is always nil
        // on error. This observation can be stale with eventual reads.
        return err
    }
    if data == nil {
        data = &Data{}
    }
    data.Value = "updated"
    return writer.Update(ctx, data)
}
```

`Update` preserves unknown fields using the same merge and projection rules as
lease writes. Success advances the handle's private snapshot, so another Update
needs no intervening read. `Delete` conditionally **hard-deletes** the observed
record and consumes the handle. For an absent snapshot, Update requires physical
absence and creates a record; Delete succeeds only while the item remains absent.
Existing records without payload are distinct internally from absent records.
There is no Commit, Release, background goroutine, or lease-lifetime context.
Operations on a handle serialize; do not copy handles or mutate values during calls.
Ordinary, discovery, and projected reads cannot authorize CAS writes.

An observed live lease returns `ErrLockHeld`, the decoded value if any, and a nil
writer. A CAS mutation requires no live lease, and atomically removes expired
ownership when taking over. `ErrConflict` means the generation, version, or
ownership no longer permits the mutation. It invalidates the handle: reread and
recompute before retrying. The library never reruns application work. An ambiguous
mutation returns `ErrOutcomeUnknown` and also invalidates the handle; reconcile
rather than blindly replaying it. Successful Delete makes later operations return
`ErrReleased`. Local payload-validation errors do not invalidate the handle.

CAS is useful when conflicts are uncommon and recomputation is cheap. Work before
publication must tolerate losing the race; irreversible external effects need
independent idempotency or fencing. Choosing CAS versus a lease is the caller's
responsibility. Neither mechanism makes multiple records or external effects atomic.

### Create without a read

Use `Create` when the required prior state is physical absence. It uses the
existing CAS creation path without a preliminary read or lease, and returns an
error rather than a writer handle:

```go
func create(ctx context.Context, client *dynamodb.Client) error {
    return dynamolock.Create(ctx, client,
        dynamolock.Table{Name: "table"}, dynamolock.Key{Partition: "new-item"},
        &Data{Value: "initial"})
}
```

Successful creation normally costs one conditional write. It stores the same
payload envelope and configured projections as other writers, with a fresh
random generation and version 1. Subsequent changes use `ReadCAS` or `Lock`.

Any existing item causes `ErrConflict`, even if its payload is absent or identical,
its lease has expired, or its metadata is unversioned or malformed. The existing
item is left unchanged. On conflict, the caller can read and validate the retained
value according to its own identity and content rules. Duplicate-heavy callers may
prefer reading first instead of paying for a failed conditional write and a read.

`ErrOutcomeUnknown` means creation may have succeeded; reconcile before retrying.
As with other CAS mutations, only definite transient rejections are retried
internally. Ambiguous writes are never blindly replayed.

### Shared revisions and existing tables

**All writers must use the versioned go-dynamolock lease or CAS paths.** Stop and
drain old writers before the cutover; old library implementations cannot safely
coexist with CAS. Older application structs using the new library still preserve
unknown fields. Existing tables do not require a bulk migration:

- First lease acquisition initializes revision metadata within its normal write.
- First CAS access to an existing unversioned record conditionally initializes
  metadata with one extra write, preserving payload and projections. The returned
  snapshot comes from that write, never from the potentially stale preceding read.
  A competing initializer's conditional-failure image can supply the snapshot.
- Ordinary read-only APIs do not initialize metadata. An absent CAS read also
  writes nothing; creation and metadata are applied together on its first Update.

Outer `generation` and `version` are reserved alongside lease metadata. Generation
is an opaque `crypto/rand.Text()` string, assigned on creation/initialization and
retained until physical deletion. Version starts at 1 and increases on acquisition,
lease Update/Commit, and CAS Update; heartbeats and release-only do not increment
it. CAS compares both generation and version, so deletion/recreation cannot make
an old version authorize writes to a new incarnation. Counters are uint64 and never
wrap; `ErrVersionExhausted` refuses increments, while a current CAS handle can
still delete. Do not interpret versions as globally ordered or ordered across
incarnations. Generation uniqueness is probabilistic, with at least 128 random bits.

Both metadata fields absent means an existing uninitialized record. Partial or
malformed metadata is an error, not a signal to overwrite it. Existing outer
attributes or configured keys/projections named `generation` or `version` must be
resolved before cutover. `MarshalItem` remains a pure unversioned envelope codec,
not a way to bypass conditional writers. Hard deletion leaves no tombstones.

Lease acquisition stays a single conditional write. DynamoDB can check a number's
range but not whether it is an integer, so a malformed fractional version can be
incremented before decoding rejects the record and cleanup releases ownership.
No usable lease is returned and the payload is unchanged. Participating writers
never produce fractional versions; there is no pre-read or rollback to repair
externally malformed metadata. CAS reads reject such metadata without changing it.

In the uncontended, already-versioned case, a CAS update costs one read plus one
write. For items up to 1 KB, that is 0.5 read units with eventual consistency (1
with strong) plus 1 write unit. Lock followed by Commit costs two writes, plus
any heartbeats. Each write, including heartbeat and initialization, is billed
against the whole item's size; indexes can add cost. Conflicts and retries add
requests. Intermediate CAS values are published immediately, as with lease Update.

## Data

`T` can be a struct or a string-keyed map, not a pointer type. Map values must
accept the decoded string keys; `map[string]any` works. If no data has been written,
`Lock` and `Read` return nil. An explicitly empty payload returns a non-nil value.

By default, `Lock` creates a missing item. Set `RequireExisting` to reject it with
`ErrLockNotFound` instead. An existing item can still have no data. Both
`ErrLockNotFound` and `ErrLockHeld` also match `ErrLockUnavailable` with `errors.Is`.

Application fields are stored under `data`, separately from ownership metadata.
Primary keys are stored only at the outer level and filled into decoded data.
Optional string fields tagged with their key names expose them in your struct. On
writes, empty or missing key fields use the lease key; conflicting values are
rejected. Key names are matched case-insensitively and cannot overlap each other,
metadata or configured projections. `lease.Key()` returns both key components.

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
reorder. Custom marshalers run when constructing lease/CAS snapshots as well as on writes;
they must represent the decoded value deterministically and retain attribute names,
including case. Custom codecs own their name mapping; the library does not infer
struct aliases inside them. Unknown-field preservation is not permission to bypass
the shared lease/CAS protocol with another writer.

Deploy field-preserving writers everywhere before adding fields older binaries
would erase. Changing an existing field's meaning/type, or deleting a field still
required by a deployed reader, is not an additive schema change. These payload
updates do not make unrelated whole-record or opaque JSON writers compatible.

`MarshalItem(table, key, value)` encodes an unlocked envelope without a write.
`UnmarshalItem[T](table, item)` decodes complete envelopes, for example from streams.
Codecs need only the key schema; `Table{}` uses `id`. Requests require a table name.
Use the lease or CAS path to create/update application documents. All writers to a
participating item must follow the shared revision and ownership protocol.

## Read consistency and cost

All non-locking reads default to **eventually consistent (weak) reads**. They may
return an older value, miss a recent creation, or still return a deleted item.
Choose them for stale-tolerant listings and background discovery. Strong reads
reflect writes completed before the read; use them when read-after-write freshness
matters. Neither mode acquires ownership or prevents a later concurrent change.

| API | Default | Opt into strong reads |
| --- | --- | --- |
| `Read`, `ReadProjected`, `ReadCAS` | Eventual with `ReadOptions{}` | `ReadOptions{ConsistentRead: true}` |
| `Query`, `QueryIndex`, `Scan`, `ScanProjected` | Eventual when SDK `ConsistentRead` is nil or false | `ConsistentRead: aws.Bool(true)` |
| `Lock` | Atomic conditional acquisition and payload read | Always; no weak mode |

`Lock` uses a conditional write, not a separately configurable read. Its internal
reconciliation read always remains strong. Before modifying a document, work
from the value returned by `Lock` or `ReadCAS`, not a previous discovery result.
Even strong scans and queries are not collection-wide or multi-page snapshots.
Global secondary indexes (GSIs) support only eventual reads; base tables and local
secondary indexes support either mode. `ScanProjected` is base-table only.

AWS charges eventual reads **half the read units** of strong reads for the same
operation. A single-item read up to 4 KB consumes 0.5 units eventual versus 1 unit
strong; larger single-item reads round up in 4 KB increments. This halves the
on-demand read-request cost or provisioned read-capacity consumption, not the
whole bill or an already-provisioned capacity charge. Writes, indexes and storage
have their own costs; `Lock` acquisition is billed as a write.

Selecting fewer fields with a projection reduces returned data and decoding work,
**not read-unit consumption**. Filters also do not make evaluated items free: a
scan that returns no matches can still consume capacity and need another page.
See AWS's [read consistency](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html),
[read-unit accounting](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/read-write-operations.html)
and [projection accounting](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ProjectionExpressions.html).

### Single-item reads

Pass a final `ReadOptions` argument to both APIs. When migrating a caller that
relied on the previous always-strong behavior, pass `ConsistentRead: true`.
The following snippets assume `ctx` and a configured DynamoDB `client`, inside a
function returning `error`; `fmt` is used only to display results.

```go
type Job struct {
    Public string `dynamodbav:"public"`
    Secret string `dynamodbav:"secret"`
}
table := dynamolock.Table{Name: "jobs", PartitionKey: "pk", SortKey: "sk"}
key := dynamolock.Key{Partition: "account", Sort: "job"}

// Default: cheaper, potentially stale full-payload read.
value, err := dynamolock.Read[Job](ctx, client, table, key, dynamolock.ReadOptions{})
if err != nil {
    return err
}
fmt.Println(value)

// Opt in when freshness matters.
value, err = dynamolock.Read[Job](ctx, client, table, key,
    dynamolock.ReadOptions{ConsistentRead: true})
if err != nil {
    return err
}
fmt.Println(value)

// Fetch only data.public plus keys and lease metadata; eventual by default.
item, err := dynamolock.ReadProjected[Job](ctx, client, table, key,
    [][]string{{"public"}}, dynamolock.ReadOptions{})
if err != nil {
    return err
}
fmt.Println(item) // Secret was not fetched; this is an incomplete observation.
```

`ReadProjected` accepts the same `ReadOptions{ConsistentRead: true}` for a strong
partial read. A nil result from an eventual read is not proof of current absence;
there is no automatic strong reread when data is missing or stale.

## Keys, indexes and discovery

```go
table := dynamolock.Table{
    Name: "jobs", PartitionKey: "pk", SortKey: "sk",
    Attributes: []string{"pending", "ttl"},
}
```

`Attributes` lists exact top-level payload field names to copy outside `data` for
secondary indexes or application TTL. They are updated or removed atomically with
the merged payload, not supplied as a separate authority. Unknown payload fields
and unconfigured outer attributes survive ordinary writes. Acquisition, renewal
and release do not change projections. Use omission (`omitempty`) for sparse index
fields. DynamoDB still enforces index value types and sizes.

Deploy projection-aware writers everywhere before enabling/backfilling an index
whose fields old writers can change. Preserving an unknown outer attribute is not
enough to keep its value current. Older versions that reject unknown outer fields
also need replacement before adding projections. TTL is for disposable application
records only: deletion is eventual, not expiry enforcement, and can happen while a
lease is held. Never use lease `expires_at` or active money/resource state for TTL.

`Query` and `Scan` return one `Page[T]`, with `Items`, `Count`, `ScannedCount` and
the complete `LastEvaluatedKey`. Pass that cursor as `ExclusiveStartKey` for the
next page, even if this page has no items. SDK filter, index, ordering, consistency
and limit options are preserved. TableName, if supplied, must match `Table.Name`.
Partial projections are rejected rather than decoded into misleading complete
structs; count-only requests are supported. Inputs are copied and not mutated; do
not mutate them during a call.

Each item has a `Key` and `Value`. An explicitly selected index without projected
`data` returns nil Value, not an empty document or proof of absence. Use `Read`
with `ReadOptions{ConsistentRead: true}` on that key for current data. Base-table
discovery retains strict envelope checks.
An index projecting `data` must project the whole map and either both lease fields
or neither, and either both revision fields or neither. Discovery is read-only,
may be eventually consistent, and is not a collection snapshot. Obtain a lease
or CAS snapshot of the actual document before modifying it.

### Typed index entries

Use `QueryIndex[T]` to decode index entries rather than documents. It returns one
`IndexPage[T]` with `IndexItem[T]` entries (`Key` and `Value T`), counts and the
complete service cursor. Every entry has a value decoded only from primary keys
and the exact configured `Table.Attributes` copies present in the index. A small
struct containing the expected index fields makes the incomplete value explicit:

```go
type WaitingEntry struct {
    WaitingOrder string `dynamodbav:"waiting_order"`
}
queues := dynamolock.Table{
    Name: "queues", Attributes: []string{"waiting", "waiting_order"},
}
page, err := dynamolock.QueryIndex[WaitingEntry](ctx, client, queues, &dynamodb.QueryInput{
    IndexName: aws.String("waiting"), Limit: aws.Int32(100),
    KeyConditionExpression: aws.String("#waiting = :waiting"),
    ExpressionAttributeNames: map[string]string{"#waiting": "waiting"},
    ExpressionAttributeValues: map[string]types.AttributeValue{
        ":waiting": &types.AttributeValueMemberS{Value: "yes"},
    },
})
if err != nil {
    return err
}
for _, entry := range page.Items {
    fmt.Println(entry.Key.Partition, entry.Value.WaitingOrder)
}
// Pass page.LastEvaluatedKey as ExclusiveStartKey for the next page.
```

The index must project the desired copies: `KEYS_ONLY` includes index and table
keys, not every configured attribute. `INCLUDE` and `ALL` work with the same
semantics. `data`, lease metadata and unconfigured outer attributes are ignored,
not interpreted or validated as a document. An `ALL` index still returns those
attributes over the wire; this API is not a field-level privacy boundary.
Missing copies do not prove that fields are absent from the current document.

`IndexName` is required. Caller projections and `SelectAllAttributes` are rejected
to prevent parent-table fetching on local indexes. `Select` may be omitted,
`SelectAllProjectedAttributes`, or `SelectCount`. Filters must reference attributes
projected into the index. Filters, ordering, limits, consistency and cursors are
preserved, and inputs are not mutated. Reads default to eventual; only LSIs
support strong reads. There are no automatic document reads or leases. For queue
processing, lock `entry.Key` and recheck eligibility using the acquired value,
not the potentially stale index observation.

### Partial document reads

For read-only paths that must not load private fields, use `ReadProjected[T]` or
`ScanProjected[T]` with explicit payload paths such as `[][]string{{"public"},
{"nested", "status"}}`. Each segment is an exact stored attribute name, not a
Go field name, dotted expression or list index. Empty fields select keys and lease
metadata only; empty paths, duplicate paths and ancestor overlaps are rejected.
The request includes primary keys, both lease fields and only the selected data.
It never fetches the complete document or silently rereads it.

These APIs return distinct `ProjectedItem[T]` / `ProjectedPage[T]` types to mark
incomplete observations. A nil `ReadProjected` item means no item was observed;
with eventual consistency this can be stale. A present item with nil `Value`
returned none of the selected data, not proof the full payload is absent or valid.
`HasLeaseMetadata` reports a valid owner/expiry pair in the observed version, even
if expired or stale, never ownership or live-lease authority.
Malformed returned metadata and payload are errors. `ScanProjected` is base-table
only, preserves SDK filter, consistency, limit, segment and continuation options,
and clones expression aliases. Caller projection and `Select` options are rejected;
indexes might omit lease metadata. The full-object APIs retain their strict
projection/envelope checks. Acquire the complete owner before any mutation.

### Pagination examples

`Query`, `QueryIndex`, `Scan`, and `ScanProjected` each make one logical page
request (with bounded retries on transient failures), not a request for the
whole collection. DynamoDB can stop a page at its 1 MB read limit or your `Limit`;
`Limit` counts items evaluated **before filtering**, not matches returned. `Count`
is the number of matches in that page; `ScannedCount` is the number evaluated
before filtering.

Continue until `len(page.LastEvaluatedKey) == 0`, **not** until `Items` or `Count`
is zero. Copy the entire cursor into the next input's `ExclusiveStartKey`; index
cursors can contain extra attributes, so do not rebuild them from `Item.Key`.
Keep the query/filter/index/consistency settings unchanged while paging. Parallel
scans need a separate cursor for each segment. Concurrent writes mean pages do
not form a snapshot, even with strong consistency.

Using the `Job` and compound-key `table` above, query one account with strong
reads (omit `ConsistentRead`, or set it to false, for half-cost eventual reads):

```go
query := &dynamodb.QueryInput{
    ConsistentRead: aws.Bool(true),
    Limit: aws.Int32(100),
    KeyConditionExpression: aws.String("#pk = :pk"),
    ExpressionAttributeNames: map[string]string{"#pk": "pk"},
    ExpressionAttributeValues: map[string]types.AttributeValue{
        ":pk": &types.AttributeValueMemberS{Value: "account"},
    },
}
for {
    page, err := dynamolock.Query[Job](ctx, client, table, query)
    if err != nil {
        return err
    }
    for _, item := range page.Items {
        fmt.Println(item.Key, item.Value) // Value can be nil; see index rules above.
    }
    if len(page.LastEvaluatedKey) == 0 {
        break
    }
    query.ExclusiveStartKey = page.LastEvaluatedKey
}
```

Scan a table with eventual reads. Set `ConsistentRead: aws.Bool(true)` to opt into
strong reads instead:

```go
scan := &dynamodb.ScanInput{ConsistentRead: aws.Bool(false), Limit: aws.Int32(100)}
for {
    page, err := dynamolock.Scan[Job](ctx, client, table, scan)
    if err != nil {
        return err
    }
    for _, item := range page.Items {
        fmt.Println(item.Key, item.Value)
    }
    if len(page.LastEvaluatedKey) == 0 {
        break
    }
    scan.ExclusiveStartKey = page.LastEvaluatedKey
}
```

A projected scan uses the same cursor pattern and SDK consistency option, but
returns explicitly incomplete observations and never loads unselected payload
fields. This example defaults to eventual reads:

```go
projected := &dynamodb.ScanInput{Limit: aws.Int32(100)}
fields := [][]string{{"public"}}
for {
    page, err := dynamolock.ScanProjected[Job](ctx, client, table, projected, fields)
    if err != nil {
        return err
    }
    for _, item := range page.Items {
        fmt.Println(item.Key, item.Value, item.HasLeaseMetadata)
    }
    if len(page.LastEvaluatedKey) == 0 {
        break
    }
    projected.ExclusiveStartKey = page.LastEvaluatedKey
}
```

## Lease lifetime

`LeaseDuration` sets the lifetime established by acquisition and each renewal.
Heartbeats are automatic: renewal is due every `LeaseDuration / 4`, measured
from the last confirmed request's **start**, not its response. For example, a
30-second lease renews every 7.5 seconds. There is no separate interval setting.
The duration must be at least 4 nanoseconds so the interval is positive; real
workloads need ample time for network latency and retries.

Choose the duration by the acceptable delay before takeover after a holder
crashes, balanced against tolerance for network stalls. It is not a limit on
job duration: a healthy renewing holder can keep a lease longer. If a holder
stops renewing, another caller can acquire the item after its expiry passes.
Longer durations reduce heartbeat traffic but delay crash recovery. A lease
completed before its first heartbeat incurs no heartbeat writes. Update does
not renew or defer heartbeats; Commit writes and releases in one operation.

Renewal retries throttling, server errors, timeouts, and network failures,
including DNS lookup failures, until the last confirmed expiry. Each attempt
times out after half of `LeaseDuration`, leaving roughly a quarter of the lease
for retries after a stuck first renewal. Backoff and delayed renewal starts
shorten that budget. A response slower than half the lease is abandoned even
if it would have arrived in time. If no renewal is confirmed in time, the lease
is lost; `context.Cause` includes the last renewal failure if an attempt failed
before expiry. Authorization, validation, and TLS certificate errors lose the
lease immediately.

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
By default, live tests create and delete disposable DynamoDB tables. Custom-schema
tests always use disposable tables. For the default schema, setting `REUSE` to any
nonempty value instead clears every item in the dedicated `go-dynamolock` test table before and after each test. Never put application data
there or run reusable-table suites concurrently.
