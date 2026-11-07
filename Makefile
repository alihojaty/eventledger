MODULES := ./pkg ./services/depositor ./services/publisher ./services/dashboard ./services/gateway
COMPOSE_FILE := deploy/docker-compose.yml
STACK_FILE := deploy/stack.yml
STACK_NAME ?= deposit
REGISTRY ?= 127.0.0.1:5000
GIT_SHA := $(shell git rev-parse --short=12 HEAD 2>/dev/null || echo "devbuild")
IMAGE_TAG ?= $(GIT_SHA)
SECRETS_DIR := .secrets

.PHONY: all build test lint tidy up down scale proto loadgen registry images deploy undeploy

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
	       --go-grpc_out=. --go_grpc_opt=paths=source_relative \
	       pkg/pb/deposit/v1/deposit.proto

registry:
	@if ! docker ps --format '{{.Names}}' | grep -q '^local-registry$$'; then \
		echo "==> Starting local Docker registry (registry:2) on $(REGISTRY)..."; \
		docker run -d --restart=always -p 5000:5000 --name local-registry registry:2 >/dev/null 2>&1 || docker start local-registry >/dev/null; \
	fi

images: registry
	@echo "==> Building and pushing images with immutable tag: $(IMAGE_TAG)"
	@for svc in depositor publisher gateway dashboard; do \
		IMG="$(REGISTRY)/reliable-deposit-$$svc:$(IMAGE_TAG)"; \
		echo "--- Building $$IMG ---"; \
		docker build -t "$$IMG" -f "services/$$svc/Dockerfile" .; \
		echo "--- Pushing $$IMG ---"; \
		docker push "$$IMG"; \
	done
	@echo "==> All images built and pushed with tag $(IMAGE_TAG)"

deploy: registry
	@echo "==> Ensuring Docker Swarm is initialized..."
	@if [ "$$(docker info --format '{{.Swarm.LocalNodeState}}')" != "active" ]; then \
		docker swarm init >/dev/null 2>&1 || docker swarm init --advertise-addr 127.0.0.1; \
	fi
	@NODE_ID=$$(docker info --format '{{.Swarm.NodeID}}'); \
	docker node update --label-add stateful=true "$$NODE_ID" >/dev/null
	@mkdir -p $(SECRETS_DIR)
	@if [ ! -s "$(SECRETS_DIR)/api_key" ]; then \
		printf "prod-swarm-api-key-9f8e7d6c5b4a" > "$(SECRETS_DIR)/api_key"; \
		chmod 600 "$(SECRETS_DIR)/api_key"; \
	fi
	@if [ ! -s "$(SECRETS_DIR)/postgres_password" ]; then \
		printf "prod-swarm-pg-pass-3a2b1c9d8e7f" > "$(SECRETS_DIR)/postgres_password"; \
		chmod 600 "$(SECRETS_DIR)/postgres_password"; \
	fi
	@if ! docker secret inspect deposit_api_key >/dev/null 2>&1; then \
		echo "==> Creating Docker secret: deposit_api_key"; \
		docker secret create deposit_api_key "$(SECRETS_DIR)/api_key" >/dev/null; \
	fi
	@if ! docker secret inspect deposit_postgres_password >/dev/null 2>&1; then \
		echo "==> Creating Docker secret: deposit_postgres_password"; \
		docker secret create deposit_postgres_password "$(SECRETS_DIR)/postgres_password" >/dev/null; \
	fi
	@echo "==> Deploying stack '$(STACK_NAME)' with IMAGE_TAG=$(IMAGE_TAG)..."
	REGISTRY=$(REGISTRY) IMAGE_TAG=$(IMAGE_TAG) docker stack deploy -c $(STACK_FILE) $(STACK_NAME)
	@echo "==> Stack '$(STACK_NAME)' deployed. Use 'docker stack services $(STACK_NAME)' to check status."

undeploy:
	docker stack rm $(STACK_NAME)

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
	      if [ "$$mod" = "./pkg" ]; then continue; fi; \
	      echo "--- Linting $$mod ---"; \
	      (cd $$mod && golangci-lint run --modules-download-mode=vendor ./...) || exit 1; \
	   done; \
	   echo "==> All linters passed successfully!"; \
	fi

tidy:
	@for mod in $(MODULES); do \
	   (cd $$mod && go mod tidy) || exit 1; \
	done
	go work sync
	go work vendor
	@for mod in $(MODULES); do \
	   rm -rf $$mod/vendor; \
	   ln -s ../vendor $$mod/vendor; \
	done

up:
	docker compose -f $(COMPOSE_FILE) up --build -d

down:
	docker compose -f $(COMPOSE_FILE) down

scale:
	docker compose -f $(COMPOSE_FILE) up --build -d --scale depositor=2

loadgen:
	go run ./services/gateway/cmd/loadgen -url http://localhost:8081 -api-key-file $(SECRETS_DIR)/api_key -requests 2000 -concurrency 200