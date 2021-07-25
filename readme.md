# go-dynamolock

`go get github.com/nathants/go-dynamolock`

```go
package main

import (
	"context"
	"time"

	"github.com/satori/go.uuid"
	"github.com/nathants/go-dynamolock"
)

func main() {
	ctx := context.Background()
	table := "a-table-name"
	lockId := "a-lock-name"
	lockUid := uuid.NewV4().String()
	maxAge := time.Second * 30
	heartbeatInterval := time.Second * 1
	releaseLock, err := dynamolock.AcquireLock(ctx, table, lockId, lockUid, maxAge, heartbeatInterval)
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second * 1) // do work with the lock
	releaseLock()
}
```
