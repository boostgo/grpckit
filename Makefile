.DEFAULT_GOAL := help

GO ?= go

.PHONY: help test test-short lint cover vet tidy check

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

test: ## Run all tests with the race detector (includes ~35s keepalive tests)
	$(GO) test -race ./...

test-short: ## Run tests, skipping the slow keepalive tests
	$(GO) test -race -short ./...

lint: ## Run golangci-lint
	golangci-lint run ./...

cover: ## Run tests with coverage and print a per-function summary
	$(GO) test -race -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out

vet: ## Run go vet
	$(GO) vet ./...

tidy: ## Tidy go.mod / go.sum
	$(GO) mod tidy

check: vet lint test ## vet + lint + test
