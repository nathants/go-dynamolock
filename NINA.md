# Go-Dynamolock

Read [readme.md](readme.md) before changing the public API or lease protocol.
The README is a human-facing introduction and usage guide, not a place for agent
instructions, review resolutions, or task status. Keep project-wide agent
guidance here and implementation rationale beside the relevant code.

## Layout

- `dynamolock.go`: acquisition, reads, and shared request/retry helpers.
- `lease.go`: lease state, lifetime, renewal, and payload operations.
- `codec.go`: envelope encoding and identity validation.
- `payload.go`, `projection.go`: field-preserving merging and SDK field-name mapping.
- `main_test.go`: mandatory AWS setup and account verification for every test run.
- `dynamolock_test.go`, `payload_test.go`: scripted protocol and fixture tests.
- `dynamolock_aws_test.go`, `additive_aws_test.go`: live AWS tests and table setup/cleanup.

## Protocol invariants

- One item per string partition key `id`; no sort key. The only outer attributes
  are `id`, `owner_token`, `expires_at`, and `data`. Ownership fields are present
  together; payloads live under `data`. There is no flat-record fallback.
- Identity is stored only outside `data`, injected on decode, and validated and
  removed on encode, including case aliases recognized by the SDK. Encoding and
  decoding must not mutate caller-owned attribute maps.
- Struct aliases follow the SDK's exact-name, embedding, tag, and case-folding
  rules; map keys and custom codec names remain exact. Reject duplicate stored
  attributes targeting one struct field before decoding. Custom codecs retain names.
- Payload writes apply the delta between immutable caller-visible encodings while
  preserving unknown map fields recursively. Removing a visible key removes its subtree;
  atomic list replacement must not silently discard fields hidden by typed decoding.
  Reject nil values, including pointers to nil maps. Absent data and explicitly empty
  data remain distinct. Interface numbers decode as `attributevalue.Number`; typed
  numeric fields retain their Go types.
- Acquisition obtains ownership and the payload in one conditional UpdateItem.
  Heartbeats update only `expires_at`; application updates write only the merged
  `data` map. Commit writes that map and removes ownership atomically. Advance
  raw/visible snapshots only after confirmed writes. Release preserves the raw
  payload and is conditional only on this handle's token, even after loss.
- `RequireExisting` checks item existence at acquisition, not payload presence
  or a generation of the key. It does not detect deletion followed by recreation.
- The holder establishes expiry in Unix nanoseconds. Takeover requires
  `expires_at < now`; renewal and payload writes require `expires_at > now`.
  Renewal cannot decrease persisted expiry, and unconfirmed attempts cannot
  extend the local monotonic deadline. Expiry permits takeover, never TTL deletion.
- Lease loss permanently cancels the exposed context, without a background
  panic. Late renewal success cannot revive the handle. Successful Commit or
  Release joins the heartbeat goroutine. Payload operations serialize separately
  from renewal; do not hold that serialization gate in the heartbeat path.
- Production uses the caller's DynamoDB SDK client, without configuration loading
  or logging dependencies. Lock copies its input.
- SDK retries are disabled per request. Acquisition, read, payload-write, and
  release retries are bounded by `maxAttempts` and their contexts; configured
  contention retries are separate. Renewal retries only transient failures, each
  attempt time-bounded, until the last confirmed monotonic deadline; permanent
  errors, loss, release, and parent cancellation end it. Ambiguous acquisition is
  reconciled by reading its token, never by resending acquisition. Ambiguous
  payload writes are not retried.
- Conditional writes protect this item, not external effects. Monotonic local
  timing does not eliminate wall-clock skew between hosts.

## Validation

Run from the repository root with installed Go 1.27 and `GOTOOLCHAIN=local`.

- Gate: `bash bin/check.sh`. It requires `libcheck` on PATH
  (github.com/nathants/libcheck; read its readme before changing lint setup) and
  runs the shared lint policy with `libcheck check`, then `libcheck security`
  (govulncheck; needs network) and the full AWS-required race suite. Lint policy
  lives in libcheck: add no engine configs or output filtering here.
  Suppress a finding only when it is wrong for that line, with
  `//nolint:LINTER // reason`; repository-wide exceptions go in `libcheck.json`.
- Tests alone: `bash test.sh`. This runs uncached race tests with a twenty-minute
  timeout and forwards additional `go test` flags, e.g. `-run '^TestProtocol'`.
  Every invocation requires credentials, a region, and `DYNAMOLOCK_TEST_ACCOUNT`;
  TestMain verifies STS before any test runs. There is no offline or skip mode.
- Scripted protocol tests use the real SDK with controlled HTTP responses, not an
  implementation of DynamoDB conditions. Live tests validate real conditions
  and deliberately lost responses; preserve both kinds of coverage.
- Validate shell-script changes by running the scripts, not by adding Go tests
  for project tooling.

## Live AWS tests

Use an independently known scratch-account ID as `EXPECTED_ACCOUNT`. TestMain
compares it with STS before running tests; STS and DynamoDB share the same SDK
configuration. A missing account guard must fail before loading configuration or
touching credential providers.

```sh
DYNAMOLOCK_TEST_ACCOUNT="${EXPECTED_ACCOUNT:?}" GOTOOLCHAIN=local \
  go test -race -run '^TestLeaseAWS$' -count=1 -timeout=5m
```

All live tests use `liveTable`. By default each creates and deletes a disposable
table. Any nonempty `REUSE` instead selects the dedicated `go-dynamolock` test
table and **clears every item before and after each test**. Never put application
data there or run reusable-table suites concurrently.

Capture cleanup policy at setup and register cleanup before creation can fail.
Cancellation, worker joins, and lease cleanup must precede table cleanup.
Cleanup uses bounded contexts and reports failures, including after partial setup.
After an ambiguous CreateTable, a missing DescribeTable result does not prove
absence: reconcile pending creation within the cleanup deadline.

Stale-owner payload tests must deliver an already-submitted write after takeover
and inspect DynamoDB's conditional failure. Local cancellation alone does not
exercise the server-side ownership condition.

