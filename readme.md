# go-dynamolock

## why

locking around dynamodb should be simple and easy.

## what

a minimal go library for locking around dynamodb.

compared [to](https://github.com/cirello-io/dynamolock) [alternatives](https://github.com/Clever/dynamodb-lock-go) it has less code and fewer features.

## install

`go get github.com/nathants/go-dynamolock`

## usage

```go
package main

import (
	"context"
	"time"
	"github.com/gofrs/uuid"
	"github.com/nathants/go-dynamolock"
)

func main() {
	ctx := context.Background()
	table := "table"
	lockId := "lock1"
	lockUid := uuid.Must(uuid.NewV4()).String()
	maxAge := time.Second * 30
	heartbeatInterval := time.Second * 1
	releaseLock, err := dynamolock.AcquireLock(
		ctx,
		table,
		lockId,
		lockUid,
		maxAge,
		heartbeatInterval,
	)
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second * 1) // do work with the lock
	releaseLock()
}
```
