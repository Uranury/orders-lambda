#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

go mod tidy
go vet ./...

# provided.al2023 expects a Linux binary named "bootstrap".
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -tags lambda.norpc -o bootstrap .
mkdir -p build
zip -j build/function.zip bootstrap
echo "Built build/function.zip"
