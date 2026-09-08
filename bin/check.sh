#!/bin/bash
set -euo pipefail
export GOTOOLCHAIN=local

for tool in bash go gofmt find grep staticcheck golint ineffassign errcheck bodyclose nargs go-hasdefault go-hasdefer govulncheck; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        printf 'missing required tool: %s\n' "$tool" >&2
        exit 1
    fi
done

mapfile -d '' -t go_files < <(find . -type f -name '*.go' -print0)
wait "$!" # Propagate errors from the process substitution.
if (( ${#go_files[@]} == 0 )); then
    echo 'no Go files found; run this check from the repository root' >&2
    exit 1
fi

echo 'gofmt (check only)'
if ! unformatted=$(gofmt -l "${go_files[@]}") || [[ -n "$unformatted" ]]; then
    printf 'gofmt check failed:\n%s\n' "$unformatted" >&2
    exit 1
fi

echo govulncheck
govulncheck ./...

echo 'go-hasdefer (advisory)'
go-hasdefer "${go_files[@]}" || true

echo 'go-hasdefault (advisory)'
go-hasdefault "${go_files[@]}" || true

echo nargs
nargs ./...

echo bodyclose
go vet -vettool="$(command -v bodyclose)" ./...

echo 'golint (advisory)'
golint ./... | grep -v -e unexported -e "should be" || true

echo staticcheck
staticcheck ./...

echo ineffassign
ineffassign ./...

echo errcheck
errcheck ./...

echo 'go vet'
go vet ./...

echo 'go test (offline)'
DYNAMOLOCK_TEST_ACCOUNT= bash test.sh
