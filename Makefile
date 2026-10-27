MODULES := ./pkg ./services/depositor ./services/publisher ./services/dashboard ./services/gateway
COMPOSE_FILE := deploy/docker-compose.yml

.PHONY: all build test lint tidy up down scale proto

all: lint test build

build:
	@echo "==> Building all modules..."
	go build ./pkg/...
	go build -o bin/depositor ./services/depositor/cmd/depositor
	go build -o bin/publisher ./services/publisher/cmd/publisher
	go build -o bin/gateway ./services/gateway/cmd/gateway

proto:
	@echo "==> Generating Protobuf code..."
	protoc --go_out=. --go_opt=paths=source_relative \
	       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	       pkg/pb/deposit/v1/deposit.proto

test:
	@echo "==> Starting infrastructure services for testing..."
	docker compose -f $(COMPOSE_FILE) up -d postgres nats
	@echo "==> Waiting for PostgreSQL to be ready..."
	@for i in {1..30}; do \
	   if docker compose -f $(COMPOSE_FILE) exec -T postgres pg_isready -U postgres -d deposits_db >/dev/null 2>&1; then \
	      echo "PostgreSQL is ready!"; \
	      break; \
	   fi; \
	   sleep 1; \
	done
	@echo "==> Running tests across all modules..."
	@set -e; \
	for mod in $(MODULES); do \
	   echo "--- Testing $$mod ---"; \
	   (cd $$mod && go test -v -race -count=1 ./...) || { docker compose -f $(COMPOSE_FILE) down; exit 1; }; \
	done
	@echo "==> Stopping infrastructure services..."
	docker compose -f $(COMPOSE_FILE) down

lint:
	@echo "==> Vet & Linting all modules..."
	@for mod in $(MODULES); do \
	   echo "--- Vetting $$mod ---"; \
	   (cd $$mod && go vet ./...) || exit 1; \
	done
	@if command -v golangci-lint >/dev/null 2>&1; then \
	   for mod in $(MODULES); do \
	      (cd $$mod && golangci-lint run ./...) || exit 1; \
	   done; \
	fi

tidy:
	@for mod in $(MODULES); do \
	   (cd $$mod && go mod tidy) || exit 1; \
	done
	go work sync

up:
	docker compose -f $(COMPOSE_FILE) up --build -d

down:
	docker compose -f $(COMPOSE_FILE) down

scale:
	docker compose -f $(COMPOSE_FILE) up --build -d --scale depositor=2