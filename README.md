# Reliable Deposit Processor — Cloud-Native Financial Ledger

A **cloud-native, horizontally scalable, fault-tolerant financial deposit processing platform** built with **Go 1.25 Workspace Monorepo**, **REST API Gateway**, **gRPC (with mTLS 1.3)**, **NATS JetStream**, **PostgreSQL 16**, and **Docker Swarm**.

This system implements the complete lifecycle of an exchange deposit transaction: from high-throughput HTTP ingestion, exact arbitrary-precision decimal validation (`NUMERIC(36, 18)`), multi-layer idempotency enforcement, and durable JetStream persistence to micro-batched SQL ingestion (`UNNEST`), periodic atomic ledger settlement (`settle_pending_deposits`), dead-letter queue (DLQ) replay, and real-time ledger reconciliation.

---

## Table of Contents

1. [System Architecture & Data Flow](#1-system-architecture--data-flow)
2. [Monorepo Structure (Go 1.25 Workspace)](#2-monorepo-structure-go-125-workspace)
3. [Engineering Highlights & Reliability Guarantees](#3-engineering-highlights--reliability-guarantees)
    * [3.1. REST API Gateway & Resilient gRPC Transport](#31-rest-api-gateway--resilient-grpc-transport)
    * [3.2. Unified Health Manager & 6-Step Ordered Graceful Shutdown](#32-unified-health-manager--6-step-ordered-graceful-shutdown)
    * [3.3. Two-Phase PostgreSQL Ingestion & Atomic Batch Settlement](#33-two-phase-postgresql-ingestion--atomic-batch-settlement)
    * [3.4. Dead-Letter Queue (DLQ) Isolation & Safe Replay](#34-dead-letter-queue-dlq-isolation--safe-replay)
4. [5-Million-Request Load Test under Chaos Engineering](#4-5-million-request-load-test-under-chaos-engineering)
5. [Quick Start & Makefile Reference](#5-quick-start--makefile-reference)
6. [Configuration & Environment Variables](#6-configuration--environment-variables)
7. [System Design & Architectural Trade-offs (Q&A)](#7-system-design--architectural-trade-offs-qa)

---

## 1. System Architecture & Data Flow

```text
+---------------------------------------------------------------------------------------------------+
|                                   EXTERNAL CLIENTS / LOADGEN                                      |
|                     POST /v1/deposits (Headers: X-API-Key, Idempotency-Key)                       |
+-------------------------------------------------+-------------------------------------------------+
                                                  | HTTP/1.1 (JSON, Max 16KB)
                                                  v
+---------------------------------------------------------------------------------------------------+
|                                    1. GATEWAY SERVICE (:8081)                                     |
|  - Constant-Time X-API-Key Auth (SHA-256) & Per-Key Token-Bucket Rate Limiter                     |
|  - Strict JSON Validation & Sharded Commit-Aware Idempotency Payload Guard                        |
|  - Custom Swarm DNS Resolver (swarmdns:///publisher:50051) + Round-Robin gRPC Load Balancing      |
|  - Non-Blocking Readiness Cache (/stats) & Automatic gRPC Retry (Unavailable / ResourceExhausted) |
+-------------------------------------------------+-------------------------------------------------+
                                                  | gRPC Unary RPC (Protobuf v1, Optional mTLS 1.3)
                                                  v
+---------------------------------------------------------------------------------------------------+
|                             2. PUBLISHER SERVICE (:50051 / :8090)                                 |
|  - Unified Single-Source HealthManager (/livez, /readyz, gRPC Health Check/Watch, /stats)         |
|  - 6-Step Ordered Graceful Shutdown (SIGTERM -> Unhealthy -> DRAIN_DELAY -> GracefulStop -> Exit) |
|  - Publishes to NATS JetStream with Deterministic Deduplication Header (Nats-Msg-Id = event_id)   |
+-------------------------------------------------+-------------------------------------------------+
                                                  | NATS JetStream (FileStorage)
                                                  v
+---------------------------------------------------------------------------------------------------+
|                                  NATS JETSTREAM BROKER (:4222)                                    |
|  - Stream "DEPOSITS" (Subject: deposits.events, 10-min Dedup Window)                              |
|  - Stream "DEPOSITS_DLQ" (Subject: deposits.dlq, Poison-Pill Isolation & Replay)                  |
+-------------------------------------------------+-------------------------------------------------+
                                                  | Durable Pull Consumer (deposits_processor)
                                                  v
+---------------------------------------------------------------------------------------------------+
|                                     3. DEPOSITOR SERVICE                                          |
|  - Concurrent Worker Pool (Default 20 Workers/Replica) + Exponential Backoff with Full Jitter     |
|  - Phase A (Ingestion): 4 Concurrent Batch Flushers (Up to 128 events/batch via SQL UNNEST)       |
|  - Phase B (Settlement): Periodic SettlementWorker (Every 5s calls settle_pending_deposits)       |
+-------------------------------------------------+-------------------------------------------------+
                                                  | pgx / database/sql (Connection Pool)
                                                  v
+---------------------------------------------------------------------------------------------------+
|                                    POSTGRESQL 16 (:5432)                                          |
|  - Table "deposits": PK (event_id COLLATE "C"), NUMERIC(36, 18), is_settled flag                  |
|  - Table "balances": PK (user_id, currency), NUMERIC(36, 18), HOT-Update Tuned (fillfactor=70)    |
+---------------------------------------------------------------------------------------------------+
```

---

## 2. Monorepo Structure (Go 1.25 Workspace)

The repository is organized as a multi-module **Go 1.25 Workspace (`go.work`)**, isolating dependencies across shared libraries and four independent microservices:

```text
reliable-deposit-processor/
├── go.work                                      # Go workspace linking pkg and all 4 services
├── Makefile                                     # Build, test, lint, Docker Swarm, and chaos targets
├── chaos.sh                                     # Automated chaos runner (scaling, SIGKILL, rolling updates)
├── verify_phase3.sh                             # End-to-end 5M deposit & ledger reconciliation verifier
├── proto/deposit/v1/deposit.proto               # Protobuf v1 contract for DepositPublisherService
├── deploy/
│   ├── stack.yml                                # Docker Swarm production stack specification
│   ├── Dockerfile.gateway                       # Multi-stage distroless image for Gateway
│   ├── Dockerfile.publisher                     # Multi-stage distroless image for Publisher
│   ├── Dockerfile.depositor                     # Multi-stage distroless image for Depositor + SQL migrations
│   └── Dockerfile.dashboard                     # Multi-stage distroless image for Dashboard
├── pkg/                                         # Shared foundational libraries
│   ├── broker/                                  # NATS JetStream client, stream provisioning & auto-reconnect
│   ├── config/                                  # Environment & Docker Secret (*_FILE) configuration loader
│   ├── dbx/                                     # PostgreSQL connection pool & golang-migrate runner
│   ├── domain/                                  # Domain models & strict Decimal validation (shopspring/decimal)
│   ├── logger/                                  # Structured JSON logger powered by Uber Zap
│   ├── pb/deposit/v1/                           # Generated Protobuf & gRPC Go bindings
│   └── tlscfg/                                  # TLS 1.3 mutual authentication (external certs or secret seed)
└── services/
    ├── gateway/                                 # HTTP ingress, rate limiter, swarmdns resolver, and loadgen CLI
    ├── publisher/                               # gRPC server, unified HealthManager, 6-step graceful shutdown
    ├── depositor/                               # JetStream worker pool, UNNEST batch flusher, PL/pgSQL settlement
    └── dashboard/                               # Real-time cluster control center, DLQ inspector, ledger auditor
```

---

## 3. Engineering Highlights & Reliability Guarantees

### 3.1. REST API Gateway & Resilient gRPC Transport (`services/gateway`)

* **Constant-Time Authentication:** Validates the `X-API-Key` header using `sha256.Sum256` and `subtle.ConstantTimeCompare` to eliminate timing side-channel vulnerabilities.
* **Per-Key Token-Bucket Rate Limiting:** Enforces configurable RPS and burst limits per API key with automatic background eviction of idle limiters after 10 minutes.
* **Commit-Aware Sharded Idempotency Guard:** Tracks `Idempotency-Key` fingerprints (`user_id:currency:amount`) across 32 concurrent shards with two-generation rotation. Reusing an `Idempotency-Key` with mutated parameters immediately returns `409 Conflict`. Once a request is committed (`202 Accepted`), its fingerprint is locked so concurrent transient failures never evict a committed key.
* **Headless Swarm DNS Discovery (`swarmdns:///`):** The `publisher` service runs in `endpoint_mode: dnsrr` (bypassing Docker's single-VIP bottleneck). Gateway's custom gRPC resolver polls `tasks.publisher` every 2 seconds, retains last-known healthy IPs during transient DNS hiccups, and distributes RPCs via `round_robin`.
* **Automatic gRPC Retry Interceptor:** Transparently retries transient upstream errors (`codes.Unavailable`, `codes.ResourceExhausted`) with exponential backoff and jitter across healthy publisher subchannels.

### 3.2. Unified Health Manager & 6-Step Ordered Graceful Shutdown (`services/publisher`)

`healthmgr.Manager` serves as the single atomic source of truth for HTTP `/livez`, HTTP `/readyz`, gRPC Health Checking (`grpc.health.v1.Health/Check` and `Watch`), and `/stats`. Upon receiving `SIGTERM`, the publisher executes an **exact 6-step shutdown sequence**:

1. **Signal Reception (`STEP 1/6`):** Captures `SIGTERM` / `SIGINT` and logs shutdown initiation with configured `DRAIN_DELAY` and `SHUTDOWN_TIMEOUT`.
2. **Immediate Health Flip (`STEP 2/6`):** Atomically transitions to `NOT_SERVING_UNHEALTHY`. HTTP `/livez` and `/readyz` return `503 Service Unavailable`, and active gRPC `Health.Watch` streams broadcast `NOT_SERVING` so Gateway stops routing new RPCs to this instance.
3. **Drain Window (`STEP 3/6 - DRAIN_DELAY`):** Continues accepting and processing in-flight or arriving RPCs normally for `DRAIN_DELAY` (default `5s`) while Swarm DNS and upstream load balancers converge.
4. **Graceful gRPC Stop (`STEP 4/6 - GracefulStop`):** Closes health watcher streams and invokes `grpcSrv.GracefulStop()`, waiting up to `SHUTDOWN_TIMEOUT` (default `10s`) for active `PublishDeposit` RPCs to complete before forcing `Stop()`.
5. **Broker Teardown (`STEP 5/6`):** Flushes pending JetStream acknowledgments and closes the NATS connection cleanly.
6. **Clean Exit (`STEP 6/6`):** Logs final processed request counters and timestamps, then exits with code `0`.

### 3.3. Two-Phase PostgreSQL Ingestion & Atomic Batch Settlement (`services/depositor`)

To sustain **10,000+ deposits/sec** without suffering from PostgreSQL row-level lock contention on hot user wallets, database persistence is decoupled into two specialized phases:

1. **Phase A — Micro-Batched Ingestion (`executeBatch`):**
    * Four concurrent `batchFlusherLoop` goroutines coalesce incoming worker requests into batches of up to 128 events (with a $200\ \mu\text{s}$ linger window) and execute a single parameterized array query:
      ```sql
      INSERT INTO deposits (event_id, user_id, currency, amount, is_settled, created_at)
      SELECT UNNEST($1::text[]), UNNEST($2::int8[])::int4, UNNEST($3::text[]),
             UNNEST($4::text[])::numeric(36, 18), FALSE, UNNEST($5::timestamptz[])
      ON CONFLICT (event_id) DO NOTHING
      RETURNING event_id;
      ```
    * Batch execution uses an isolated batch context (`newBatchQueryContext`) that only cancels if *every* request in the batch has been canceled, preventing a single expiring caller context from aborting 127 healthy siblings.
2. **Phase B — Periodic Atomic Settlement (`SettlementWorker` & `settle_pending_deposits`):**
    * Every 5 seconds, `SettlementWorker` invokes the PL/pgSQL function `settle_pending_deposits(100000)`.
    * Guarded by `pg_advisory_xact_lock(778899)` and `FOR UPDATE SKIP LOCKED`, the function claims unsettled deposits (`is_settled = FALSE`), aggregates net amounts by `(user_id, currency)` in deterministic order (`ORDER BY user_id ASC, currency ASC` to prevent deadlocks), upserts the `balances` table in bulk, and marks all claimed rows `is_settled = TRUE` within a single ACID transaction.

### 3.4. Dead-Letter Queue (DLQ) Isolation & Safe Replay (`services/dashboard`)

* **Poison-Pill Isolation:** Events failing domain validation (`ErrInvalidEvent`) bypass retry loops and are immediately published to `DEPOSITS_DLQ` (`deposits.dlq`) with full provenance metadata (`source_stream`, `stream_sequence`, `error_code`, `reason`, and `original_event`).
* **Bounded Retries with Full Jitter:** Transient infrastructure errors are retried via `msg.NakWithDelay` using exponential backoff and full jitter up to `MaxDeliver = 5` before routing to `DEPOSITS_DLQ`.
* **Authenticated DLQ Replay:** Operators can inspect, edit, revalidate, and replay DLQ messages back to `deposits.events` from the Dashboard UI. All mutating Dashboard endpoints (`POST /api/*`) strictly require either a valid `X-API-Key` header or a cryptographically random `HttpOnly; SameSite=Strict` session cookie.

---

## 4. 5-Million-Request Load Test under Chaos Engineering

The repository includes an automated end-to-end verification suite that drives **5,000,000 deposit requests** through the Gateway while simultaneously injecting infrastructure chaos (`SIGKILL`, random horizontal scaling between 1 and 6 replicas, rolling updates, and depositor restarts).

### Step 1: Build Images & Deploy the Docker Swarm Stack

```bash
make swarm-up
```

### Step 2: Execute the 5M Chaos Benchmark & Ledger Verification

```bash
make phase3-chaos-test
```

What this target does:
1. Compiles the host-native `bin/loadgen` binary to avoid Docker ingress NAT bottlenecks.
2. Launches `chaos.sh` in the background, recording cluster telemetry every 5 seconds to `chaos.log` while ensuring at least 2 `healthy` publisher containers exist before firing `docker kill -s KILL`.
3. Executes `bin/loadgen` for 5,000,000 requests with deterministic user/currency/amount cycling and automatic client-side retry on transient `429`/`5xx` responses.
4. Runs `verify_phase3.sh` to confirm that all 5,000,000 deposits are ingested (`is_settled = TRUE`) and verifies **zero ledger discrepancy** (`discrepancy_count = 0`) between `SUM(deposits.amount)` and `balances.balance` across all 20 users and 3 currencies (`USDT`, `BTC`, `IRT`).

---

## 5. Quick Start & Makefile Reference

| Command | Description |
| :--- | :--- |
| `make build` | Compiles all binaries (`gateway`, `publisher`, `depositor`, `dashboard`, `loadgen`) |
| `make test` | Runs unit and integration tests with `-race` across all 5 workspace modules |
| `make lint` | Runs `golangci-lint` across every module in `go.work` |
| `make swarm-up` | Initializes Docker Swarm, creates secrets, builds images, and deploys the `deposit` stack |
| `make swarm-down` | Removes the `deposit` stack from Docker Swarm |
| `make swarm-status` | Displays live Docker Swarm services and task states |
| `make scale PUB=5 DEP=5` | Horizontally scales `publisher` and `depositor` replicas |
| `make phase3-chaos-test` | Runs the complete 5,000,000-request load test + `chaos.sh` + `verify_phase3.sh` |

* **Cluster Control Dashboard UI:** `http://localhost:8080`
* **Gateway HTTP Endpoint:** `http://localhost:8081`

### Sample Deposit Request (`curl`)

```bash
curl -i -X POST http://localhost:8081/v1/deposits \
  -H "Content-Type: application/json" \
  -H "X-API-Key: super-secret-gateway-key-2026" \
  -H "Idempotency-Key: dep-2026-00001" \
  -d '{
    "user_id": 7,
    "currency": "USDT",
    "amount": "1250.750000000000000000"
  }'
```

---

## 6. Configuration & Environment Variables

All sensitive settings support the Docker Secrets `*_FILE` convention (e.g., `API_KEY_FILE`, `DATABASE_URL_FILE`, `TLS_CA_SEED_FILE`):

| Variable | Default | Service(s) | Description |
| :--- | :--- | :--- | :--- |
| `GATEWAY_PORT` | `8081` | `gateway` | HTTP listen port for the REST API Gateway |
| `API_KEY` / `API_KEY_FILE` | — | `gateway`, `dashboard` | Shared secret key for `X-API-Key` authentication |
| `RATE_LIMIT_RPS` | `500` (`50000` in stack) | `gateway` | Allowed requests per second per API key |
| `RATE_LIMIT_BURST` | `1000` (`50000` in stack) | `gateway` | Token-bucket burst capacity per API key |
| `PUBLISHER_GRPC_ADDR` | `publisher:50051` | `gateway` | Target gRPC address resolved via `swarmdns:///` |
| `PUBLISHER_GRPC_PORT` | `50051` | `publisher` | gRPC server listen port |
| `PUBLISHER_HEALTH_PORT` | `8090` | `publisher`, `dashboard` | HTTP health & telemetry port (`/livez`, `/readyz`, `/stats`) |
| `DRAIN_DELAY` | `5s` | `publisher` | Duration to continue accepting requests after marking health `NOT_SERVING` |
| `SHUTDOWN_TIMEOUT` | `10s` | All | Maximum grace period to finish in-flight work before forced stop |
| `MTLS_ENABLED` | `true` | `gateway`, `publisher` | Enables mutual TLS 1.3 encryption and client cert verification |
| `TLS_CERT_FILE` / `TLS_KEY_FILE` / `TLS_CA_FILE` | `""` | `gateway`, `publisher` | External X.509 PEM certificate paths for mTLS |
| `TLS_CA_SEED` / `TLS_CA_SEED_FILE` | `""` | `gateway`, `publisher` | Secret seed for deterministic internal CA key derivation |
| `NATS_URL` | `nats://localhost:4222` | All | NATS JetStream server connection URL |
| `DATABASE_URL` / `DATABASE_URL_FILE` | — | `depositor`, `dashboard` | PostgreSQL connection DSN |
| `WORKER_CONCURRENCY` | `20` | `depositor` | Number of concurrent JetStream consumer workers per replica |

---

## 7. System Design & Architectural Trade-offs (Q&A)

### 1. Why decouple deposit ingestion (`UNNEST`) from balance updates (`settle_pending_deposits`)?
In high-throughput financial exchanges, thousands of deposits per second often target a small number of hot accounts (e.g., market makers or 20 active test users). Updating `balances` synchronously inside every individual deposit transaction serializes execution on PostgreSQL row-level locks (`FOR NO KEY UPDATE` on `(user_id, currency)`). By ingesting immutable deposit events in 128-row `UNNEST` batches (`ON CONFLICT (event_id) DO NOTHING`) and aggregating unsettled rows periodically via `settle_pending_deposits`, the database performs at most **one balance row update per user/currency per settlement cycle**, increasing ingestion throughput by over an order of magnitude while preserving 100% ACID ledger accuracy.

### 2. How is Exactly-Once financial effect guaranteed across an At-Least-Once network?
Idempotency is enforced across four defense-in-depth layers:
1. **Gateway Layer:** Sharded `Idempotency-Key` payload fingerprint validation rejects conflicting parameter reuse (`409 Conflict`).
2. **Publisher Layer:** Secondary gRPC payload guard plus deterministic `Nats-Msg-Id` header assignment (`jetstream.WithMsgID(eventID)`).
3. **NATS JetStream Layer:** Broker-level deduplication window (`Duplicates: 10m`) discards duplicate message publishes at the storage engine.
4. **PostgreSQL Layer (Source of Truth):** `event_id VARCHAR(255) COLLATE "C" PRIMARY KEY` with `ON CONFLICT (event_id) DO NOTHING` guarantees that even if a message is redelivered outside the NATS deduplication window, it can never be inserted or settled twice.