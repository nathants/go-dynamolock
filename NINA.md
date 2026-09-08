# Go-Dynamolock

Read [readme.md](readme.md) before changing the lease protocol or public API.

## Layout

- `dynamolock.go`: acquisition, reads, and shared request/retry helpers.
- `lease.go`: lease state, lifetime, renewal, and payload operations together.
- `codec.go`: envelope encoding and identity validation.
- `dynamolock_test.go`: offline protocol tests; `dynamolock_aws_test.go`: armed
  live AWS tests and their setup.

## Invariants

- One item per string partition key `id`; no sort key. Identity is stored only
  outside `data`, injected on decode, and validated/removed on encode.
- Acquisition atomically obtains ownership and the payload. Heartbeats update
  only `expires_at`; application updates replace only `data`. Commit replaces
  data and removes ownership atomically; Release preserves the raw payload.
- Payload writes reject nil values, including pointers to nil maps. Interface
  numbers decode as `attributevalue.Number`, not lossy `float64`; explicitly
  typed numeric fields retain their declared Go types.
- The holder establishes expiry in Unix nanoseconds. Renewals cannot decrease
  persisted expiry, and unconfirmed attempts cannot extend the local lease.
- Lease loss is permanent and cancels the exposed context. No background panic.
  Lost handles may only perform token-conditional release cleanup.
- Production uses the caller's DynamoDB SDK client, with no libaws/config/logging
  dependency. SDK retries are disabled per request; library retries are bounded.
- Live fixtures use direct SDK clients: STS checks and DynamoDB operations share
  the same per-fixture configuration. No libaws dependency remains, even in tests.
- All live tests use `liveTable`: disposable tables by default; REUSE selects
  the dedicated `go-dynamolock` test table and clears it before/after each test.
  Never run reusable-table suites concurrently. Cleanup policy is captured at
  setup; test cancellation, worker joins, and lease cleanup precede table cleanup.
  Cleanup is bounded and reported, including after setup failure.
- These guarantees do not fence external effects or eliminate clock-skew risks.

## Validation

Use installed Go 1.27 with `GOTOOLCHAIN=local`.

- Offline tests: `bash test.sh` runs uncached race tests with a two-minute
  timeout and accepts additional `go test` flags. It uses `GOTOOLCHAIN=local`.
- Gate: `bash bin/check.sh`, from the repository root. It requires all tools on
  PATH, never installs them, checks formatting without rewriting files, and runs
  analysis plus explicitly disarmed offline tests. Tool versions are supplied by
  the environment; go-hasdefer/go-hasdefault/golint are labeled advisory.
- Live: `DYNAMOLOCK_TEST_ACCOUNT=EXPECTED_ACCOUNT go test -race -run '^TestLeaseAWS$' -count=1 -timeout=5m`.
  The expected account must be independently known and match STS before mutation.
  The unarmed gate must skip before accessing credentials or AWS providers.
- Protocol tests use the real AWS SDK with scripted HTTP responses, not a second
  implementation of DynamoDB conditions. Live tests exercise real conditions and
  deliberately lost responses. Preserve both kinds of evidence.
- `TestReadModifyWrite` asserts a persisted counter under contention, retries
  only ErrLockHeld, and joins workers before table cleanup.
