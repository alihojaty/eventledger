MODULES := ./pkg ./services/depositor ./services/publisher
COMPOSE_FILE := deploy/docker-compose.yml

.PHONY: all build test lint tidy up down scale

all: lint test build

build:
	@echo "==> Building all modules..."
	go build ./pkg/...
	go build -o bin/depositor ./services/depositor/cmd/depositor
	go build -o bin/publisher ./services/publisher/cmd/publisher

test:
	@echo "==> Running tests across all modules..."
	@for mod in $(MODULES); do \
		echo "--- Testing $$mod ---"; \
		(cd $$mod && go test -v -race -count=1 ./...) || exit 1; \
	done

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