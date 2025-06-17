go test ./... -v -race -o ./test -c && ./test -test.v
# go test ./... -v -race -o ./test -c && REUSE=y ./test -test.v
# go test ./... -v -race -o ./test -c && REUSE=y ./test -test.v -test.run ^TestLockExpiration$
