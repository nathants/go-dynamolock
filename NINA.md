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
- These guarantees do not fence external effects or eliminate clock-skew risks.

## Validation

Use installed Go 1.27 with `GOTOOLCHAIN=local`.

- Offline: `DYNAMOLOCK_TEST_ACCOUNT= go test -race -count=1 -timeout=2m ./...`.
- Existing gate: `bash bin/check.sh`. Its missing-tool branches install software,
  so first verify every tool is present; do not use it for implicit installation.
  The go-hasdefer/go-hasdefault/golint stages remain advisory.
- Live: `DYNAMOLOCK_TEST_ACCOUNT=EXPECTED_ACCOUNT go test -race -run '^TestLeaseAWS$' -count=1 -timeout=5m`.
  The expected account must be independently known and match STS before mutation.
  The unarmed gate must skip before accessing credentials or AWS providers.
- Protocol tests use the real AWS SDK with scripted HTTP responses, not a second
  implementation of DynamoDB conditions. Live tests exercise real conditions and
  deliberately lost responses. Preserve both kinds of evidence.
- `TestReadModifyWrite` asserts a persisted counter under contention, retries
  only ErrLockHeld, and joins workers before table cleanup.
