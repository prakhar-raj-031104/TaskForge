# TaskForge developer task runner.
#
# Every recipe below is a single command with no shell built-ins, pipes or
# redirects, so the same Makefile works under Windows cmd.exe as well as sh.

.DEFAULT_GOAL := help

# Load .env when it exists so `make run` sees exactly the variables the
# binaries expect. `export` pushes them into the environment of each recipe.
ifneq (,$(wildcard ./.env))
include .env
export
endif

# Fallbacks used by targets that need these values even without a .env file.
# `?=` never overrides a value already set by the include above.
DB_USER ?= taskforge
DB_NAME ?= taskforge

BIN_DIR := bin

# Pinned to the same Go version as go.mod. Used by test-race-docker, which is
# how the race detector runs on machines without a C toolchain (notably a
# stock Windows install).
GO_IMAGE := golang:1.26.5

.PHONY: help
help:
	@echo TaskForge make targets:
	@echo   make setup          create .env from .env.example
	@echo   make run            run the API server
	@echo   make run-worker     run a worker process
	@echo   make build          compile both binaries into ./bin
	@echo   make test           run all tests
	@echo   make test-race      run tests under the race detector, needs a C compiler
	@echo   make test-race-docker  run the race detector inside a Go container
	@echo   make test-cover     run tests and write coverage.out
	@echo   make fmt            format all Go source
	@echo   make vet            run go vet
	@echo   make lint           alias for vet until golangci-lint is added
	@echo   make tidy           tidy go.mod and go.sum
	@echo   make docker-db      start PostgreSQL only, wait until healthy
	@echo   make docker-up      build and start the full stack
	@echo   make docker-scale   full stack with 3 worker replicas
	@echo   make docker-down    stop containers, keep data
	@echo   make docker-nuke    stop containers and DELETE the data volume
	@echo   make docker-logs    follow container logs
	@echo   make docker-ps      show container status
	@echo   make psql           open a psql shell inside the container
	@echo   make migrate-up     apply all pending migrations
	@echo   make migrate-down   roll back the most recent migration
	@echo   make migrate-status show the current schema version
	@echo   make migrate-create create a migration, needs name=some_name

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

.PHONY: setup
setup:
	go run ./scripts/setupenv

# ---------------------------------------------------------------------------
# Go
# ---------------------------------------------------------------------------

.PHONY: run
run:
	go run ./cmd/api

.PHONY: run-worker
run-worker:
	go run ./cmd/worker

.PHONY: build
build:
	go build -o $(BIN_DIR)/ ./cmd/api ./cmd/worker

.PHONY: test
test:
	go test ./...

# The race detector is implemented in C, so it requires cgo and therefore a C
# compiler. Linux, macOS and CI have one; a stock Windows install does not.
.PHONY: test-race
test-race:
	go test -race ./...

# Same suite, run inside the official Go image, which ships gcc. Nothing to
# install on the host. The two named volumes cache the module downloads and the
# build cache so repeat runs are fast.
.PHONY: test-race-docker
test-race-docker:
	docker run --rm -v "$(CURDIR):/src" -v taskforge-gocache:/root/.cache/go-build -v taskforge-gomod:/go/pkg/mod -w /src $(GO_IMAGE) go test -race ./...

.PHONY: test-cover
test-cover:
	go test -coverprofile=coverage.out ./...

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: lint
lint: vet

.PHONY: tidy
tidy:
	go mod tidy

# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------

.PHONY: docker-db
docker-db:
	docker compose up -d --wait postgres

.PHONY: docker-up
docker-up:
	docker compose up -d --build

.PHONY: docker-scale
docker-scale:
	docker compose up -d --build --scale worker=3

.PHONY: docker-down
docker-down:
	docker compose down

.PHONY: docker-nuke
docker-nuke:
	docker compose down -v

.PHONY: docker-logs
docker-logs:
	docker compose logs -f

.PHONY: docker-ps
docker-ps:
	docker compose ps

.PHONY: psql
psql:
	docker compose exec postgres psql -U $(DB_USER) -d $(DB_NAME)

# ---------------------------------------------------------------------------
# Migrations (golang-migrate, run as a container so nothing to install)
# ---------------------------------------------------------------------------

.PHONY: migrate-up
migrate-up:
	docker compose run --rm migrate up

.PHONY: migrate-down
migrate-down:
	docker compose run --rm migrate down 1

.PHONY: migrate-status
migrate-status:
	docker compose run --rm migrate version

.PHONY: migrate-create
migrate-create:
	docker compose run --rm --entrypoint migrate migrate create -ext sql -dir /migrations -seq $(name)
