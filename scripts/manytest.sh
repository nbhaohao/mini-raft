#!/usr/bin/env bash
set -euo pipefail

pattern="${1:-M01}"
count="${2:-10}"

cd "$(dirname "$0")/.."
for i in $(seq 1 "$count"); do
  echo "== run $i/$count: go test -race ./... -run $pattern -count=1"
  go test -race ./... -run "$pattern" -count=1
done
