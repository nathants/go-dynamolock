# go-dynamolock

## why

locking around dynamodb should be simple and easy.

## what

a minimal go library for locking around dynamodb.

compared [to](https://github.com/cirello-io/dynamolock) [alternatives](https://github.com/Clever/dynamodb-lock-go) it has less code and fewer features.

## how

a record in dynamodb uses a uuid and a timestamp to coordinate callers.

to lock, a caller finds the uuid missing and adds it.

while locked, the caller heartbeats the timestamp.

to unlock, the caller removes the uuid.

arbitrary data can be stored atomically in the lock record. it is read via lock, written via unlock, and can be written without unlocking via update.

that data is returned as a struct pointer. if there was no existing data for that key, nil is returned.

manipulation of external state while the lock is held is subject to concurrent updates depending on `HeartbeatMaxAge`, `HeartbeatInterval`, and caller clock drift.

in practice, a small `HeartbeatInterval`, a large `HeartbeatMaxAge`, and reasonable clock drift should be [safe](https://en.wikipedia.org/wiki/Lease_(computer_science)).

prefer to store data within the lock when possible, since those updates use compare and swap.

you cannot use `"id"`, `"uid"`, or `"unix"` as `dynamodbav` values, since they are used internally by `LockRecord{}`.

## install

`go get github.com/nathants/go-dynamolock`

## usage

```go
package main

import (
	"context"
	"time"
	"github.com/nathants/go-dynamolock"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
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
	HeartbeatMaxAge := time.Second * 30

	// how often to heartbeat the lock
	heartbeatInterval := time.Second * 1

	// lock and read data
	unlock, _, data, err := dynamolock.Lock[Data](ctx, &dynamolock.LockInput{
		Table:             table,
		ID:                id,
		HeartbeatMaxAge:   HeartbeatMaxAge,
		HeartbeatInterval: HeartbeatInterval,
	})
	if err != nil {
		// TODO handle lock contention
		panic(err)
	}

	// do work with the lock
	time.Sleep(time.Second * 1)
	data.Value = "updated"

	// unlock and write data
	err = unlock(data)
	if err != nil {
		panic(err)
	}
}

```
