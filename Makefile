MODULES := ./pkg ./services/depositor ./services/publisher ./services/dashboard ./services/gateway
COMPOSE_FILE := deploy/docker-compose.yml
STACK_FILE := deploy/stack.yml
STACK_NAME ?= deposit
OVERLAY_NET ?= deposit_overlay
REGISTRY ?= 127.0.0.1:5000
GIT_SHA := $(shell git rev-parse --short=12 HEAD 2>/dev/null || echo "devbuild")
IMAGE_TAG ?= $(GIT_SHA)
SERVICE_VERSION ?= 1.0.0
SECRETS_DIR := .secrets

LDFLAGS := -s -w \
	-X github.com/alihojaty/reliable-deposit-processor/pkg/metricsx.Revision=$(IMAGE_TAG) \
	-X github.com/alihojaty/reliable-deposit-processor/pkg/metricsx.Version=$(SERVICE_VERSION)

.PHONY: all build test lint tidy up down scale proto loadgen registry images deploy undeploy publisher-stats build-loadgen chaos-500k chaos-5m metrics-check

all: lint test build

build:
	@echo "==> Building all modules..."
	go build ./pkg/...
	go build -ldflags="$(LDFLAGS)" -o bin/depositor ./services/depositor/cmd/depositor
	go build -ldflags="$(LDFLAGS)" -o bin/publisher ./services/publisher/cmd/publisher
	go build -ldflags="$(LDFLAGS)" -o bin/gateway ./services/gateway/cmd/gateway
	go build -ldflags="$(LDFLAGS)" -o bin/dashboard ./services/dashboard/cmd/dashboard

proto:
	@echo "==> Generating Protobuf code..."
	protoc --go_out=. --go_opt=paths=source_relative \
	       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
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
	   docker build --build-arg GIT_REVISION=$(IMAGE_TAG) --build-arg SERVICE_VERSION=$(SERVICE_VERSION) -t "$$IMG" -f "services/$$svc/Dockerfile" .; \
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
	  ln -s "$(CURDIR)/vendor" $$mod/vendor; \
	done

up:
	docker compose -f $(COMPOSE_FILE) up --build -d

down:
	docker compose -f $(COMPOSE_FILE) down

scale:
	docker compose -f $(COMPOSE_FILE) up --build -d --scale depositor=2

loadgen:
	go run ./services/gateway/cmd/loadgen -url http://localhost:8081 -api-key-file $(SECRETS_DIR)/api_key -requests 2000 -concurrency 200

publisher-stats:
	@echo "==> Querying /stats on all running publisher containers (port 8090):"
	@for cid in $$(docker ps -qf "name=$(STACK_NAME)_publisher"); do \
	   echo -n "$$cid: "; \
	   docker exec "$$cid" wget -qO- http://127.0.0.1:8090/stats; \
	   echo ""; \
	done

build-loadgen:
	go build -ldflags="$(LDFLAGS)" -o bin/loadgen ./services/gateway/cmd/loadgen

chaos-500k: build-loadgen
	@RUN_ID="500k-$$(date -u +%Y%m%d%H%M%S)"; \
	echo "==> Starting 500,000 request validation run (RUN_ID=$$RUN_ID) with live chaos..."; \
	./scripts/chaos.sh & \
	CHAOS_PID=$$!; \
	./bin/loadgen -url http://localhost:8081 -api-key-file $(SECRETS_DIR)/api_key -run-id "$$RUN_ID" -requests 500000 -concurrency 256; \
	kill "$$CHAOS_PID" >/dev/null 2>&1 || true; \
	./scripts/verify_phase3.sh "$$RUN_ID" 500000

chaos-5m: build-loadgen
	@RUN_ID="5m-$$(date -u +%Y%m%d%H%M%S)"; \
	echo "==> Starting 5,000,000 request production run (RUN_ID=$$RUN_ID) with live chaos..."; \
	./scripts/chaos.sh & \
	CHAOS_PID=$$!; \
	./bin/loadgen -url http://localhost:8081 -api-key-file $(SECRETS_DIR)/api_key -run-id "$$RUN_ID" -requests 5000000 -concurrency 256; \
	kill "$$CHAOS_PID" >/dev/null 2>&1 || true; \
	./scripts/verify_phase3.sh "$$RUN_ID" 5000000

metrics-check:
	@echo "==> Validating /metrics endpoints from inside network $(OVERLAY_NET) via promtool check metrics..."
	@for target in "gateway:9090" "publisher:9090" "depositor:9090" "dashboard:9090"; do \
	   echo "  -> Checking http://$$target/metrics ..."; \
	   docker run --rm --network $(OVERLAY_NET) --entrypoint /bin/sh prom/prometheus:v2.55.1 -c \
	      "wget -qO- http://$$target/metrics | promtool check metrics" || exit 1; \
	   echo "     OK: $$target passed promtool check metrics with 0 warnings!"; \
	done
	@echo "==> Verifying Prometheus recording rules syntax via promtool check rules..."
	@docker run --rm -v "$(CURDIR)/deploy/prometheus/rules:/rules:ro" --entrypoint /bin/promtool prom/prometheus:v2.55.1 \
	   check rules /rules/recording_rules.yml
	@echo "==> ALL METRICS & RULES CHECKS PASSED!"