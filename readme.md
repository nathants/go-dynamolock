# Go-Dynamolock

## Why

Locking around DynamoDB should be simple and easy.

## What

A minimal Go library for locking around DynamoDB.

Compared [to](https://github.com/cirello-io/dynamolock) [alternatives](https://github.com/Clever/dynamodb-lock-go) it has less code and fewer features.

## How

A record in DynamoDB uses a UUID and a timestamp to coordinate callers.

To lock, a caller finds the UUID missing and adds it.

While locked, the caller heartbeats the timestamp.

To unlock, the caller removes the UUID.

Arbitrary data can be stored atomically in the lock record. It is read via lock, written via unlock, and can be written without unlocking via update.

That data is returned as a struct pointer. If there was no existing data for that key, nil is returned.

Manipulation of external state while the lock is held is subject to concurrent updates depending on `HeartbeatMaxAge`, `HeartbeatInterval`, and caller clock drift.

In practice, a small `HeartbeatInterval`, a large `HeartbeatMaxAge`, and reasonable clock drift should be [safe](https://en.wikipedia.org/wiki/Lease_(computer_science)).

When lock contention is expected you can set `Retries` to automatically retry acquiring the lock. Use `RetriesSleep` to control how long to sleep between attempts. Exhausted contention returns an error matching `ErrLockHeld`.

Set `RequireExisting` when a stale lock request must not create a missing record. A missing required record immediately returns `ErrLockNotFound` without consuming retries and leaves the table unchanged. This checks existence at acquisition time; it does not identify an item incarnation across deletion and recreation.

Both `ErrLockHeld` and `ErrLockNotFound` also match their parent `ErrLockUnavailable`. Match a child when the causes require different handling, or match the parent to handle every expected acquisition failure alike.

Prefer to store data within the lock when possible, since those updates use compare and swap.

You cannot use `"id"`, `"uid"`, or `"unix"` as `dynamodbav` values, since they are used internally by `LockRecord{}`.

## Install

`go get github.com/nathants/go-dynamolock`

## Usage

```go
package main

import (
	"context"
	"time"

	"github.com/nathants/go-dynamolock"
)

type Data struct {
	Value string
}

func main() {
	ctx := context.Background()

	// dynamodb table
	table := "table"

	// dynamodb key
	id := "lock1"

	// after a failure to unlock/heartbeat, this much time must pass since the last heartbeat before the lock is available
	heartbeatMaxAge := time.Second * 30

	// how often to heartbeat the lock
	heartbeatInterval := time.Second * 1

	// lock and read data
	unlock, _, data, err := dynamolock.Lock[Data](ctx, &dynamolock.LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   heartbeatMaxAge,
		HeartbeatInterval: heartbeatInterval,
		Retries:           5,
		RetriesSleep:      1 * time.Second,
	})
	if err != nil {
		// TODO handle lock contention
		panic(err)
	}

	// do work with the lock
	time.Sleep(time.Second * 1)
	if data == nil {
		data = &Data{}
	}
	data.Value = "updated"

	// unlock and write data. Use a fresh context so cleanup can still run if the lock-acquisition context is canceled.
	err = unlock(context.Background(), data)
	if err != nil {
		panic(err)
	}
}

```
