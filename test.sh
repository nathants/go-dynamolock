#!/bin/bash
set -euo pipefail

GOTOOLCHAIN=local exec go test ./... -v -race -count=1 -timeout=2m "$@"
