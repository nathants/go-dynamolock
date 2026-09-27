#!/bin/bash
set -euo pipefail
# Every go command this gate runs, including inside tools, must use the
# installed toolchain rather than download one.
export GOTOOLCHAIN=local
cd "$(dirname "$0")/.."

command -v libcheck >/dev/null || { echo 'libcheck not found; install it with: go install github.com/nathants/libcheck@latest' >&2; exit 1; }
libcheck check
libcheck security

echo 'go test (offline)'
DYNAMOLOCK_TEST_ACCOUNT= bash test.sh
