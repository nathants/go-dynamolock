# go-dynamolock

## why

locking around dynamodb should be simple and easy.

## what

a minimal go library for locking around dynamodb.

compared [to](https://github.com/cirello-io/dynamolock) [alternatives](https://github.com/Clever/dynamodb-lock-go) it has less code and fewer features.

## how

a record in dynamodb uses a uuid and a timetamp to coordinate callers.

to lock, a caller finds the uuid missing and adds it.

while locked, the caller heartbeats the timestamp.

to unlock, the caller removes the uuid.

arbitrary data can be stored atomically in the lock record. it is read via lock, and written via unlock.

manipulation of external state while the lock is held is subject to concurrent updates depending on maxAge, heartbeatInterval, and caller clock drift.

in practice, a small heartbeatInterval, a large maxAge, and reasonable clock drift should be safe.

prefer to store data within the lock when possible.

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

	// after a failure to unlock/heartbeat, this much time must pass before lock is available
	maxAge := time.Second * 30

	// how often to heartbeat lock timestamp
	heartbeat := time.Second * 1

	// lock and read data
	unlock, item, err := dynamolock.Lock(ctx, table, id, maxAge, heartbeat)
	if err != nil {
		// TODO handle lock contention
		panic(err)
	}
	data := &Data{}
	err = dynamodbattribute.UnmarshalMap(item, data)
	if err != nil {
		panic(err)
	}

	// do work with the lock
	time.Sleep(time.Second * 1)
	data.Value = "updated"

	// unlock and write data
	item, err = dynamodbattribute.MarshalMap(data)
	if err != nil {
		panic(err)
	}
	err = unlock(item)
	if err != nil {
		panic(err)
	}
}

```
