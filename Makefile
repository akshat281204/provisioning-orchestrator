SHELL := /bin/bash
GO ?= go
BIN := bin/orchestrator
PKG := github.com/akshat281204/provisioning-orchestrator

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build the orchestrator binary
	$(GO) build -trimpath -o $(BIN) ./cmd/orchestrator

.PHONY: run
run: ## Run locally against docker-compose dependencies
	$(GO) run ./cmd/orchestrator

.PHONY: test
test: ## Run unit tests (no external services required)
	$(GO) test -race -count=1 ./...

.PHONY: test-integration
test-integration: ## Run tests that need docker-compose up
	$(GO) test -race -count=1 -tags=integration ./...

.PHONY: vet
vet: ## go vet
	$(GO) vet ./...

.PHONY: fmt
fmt: ## Format all Go source
	$(GO) fmt ./...

.PHONY: fmt-check
fmt-check: ## Fail if any file is not gofmt'd
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "not gofmt'd:"; echo "$$out"; exit 1; fi

.PHONY: verify
verify: fmt-check vet test build ## The gate the agent loop runs after every phase

.PHONY: up
up: ## Start postgres, vault, otel-collector, prometheus
	docker compose up -d

.PHONY: down
down: ## Stop the local stack
	docker compose down -v

.PHONY: logs
logs: ## Tail the local stack
	docker compose logs -f --tail=100
