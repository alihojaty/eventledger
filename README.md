# Reliable Deposit Processor — Cloud-Native Financial Ledger

A **cloud-native, horizontally scalable, fault-tolerant financial deposit processing platform** built with a **Go Workspace Monorepo (`go.work`)**, **REST API Gateway (Gin)**, **gRPC (with mTLS 1.3)**, **NATS JetStream**, **PostgreSQL 18**, **Prometheus**, and **Docker Swarm**.

This system implements the complete lifecycle of an exchange deposit transaction: from high-throughput HTTP ingestion, exact arbitrary-precision decimal validation (`NUMERIC(36, 18)`), multi-layer idempotency enforcement, and durable JetStream persistence to **decoupled two-phase PostgreSQL persistence** (micro-batched `UNNEST` ingestion followed by periodic atomic PL/pgSQL batch settlement), dead-letter queue (DLQ) replay, real-time ledger reconciliation, and production-grade **Prometheus observability**.

## Table of Contents

1. [System Architecture & Two-Phase Ledger Data Flow](#1-system-architecture--two-phase-ledger-data-flow)

2. [Monorepo Structure & Package Placement Rationale (Task 4)](#2-monorepo-structure--package-placement-rationale-task-4)

3. [Engineering Highlights & Reliability Guarantees](#3-engineering-highlights--reliability-guarantees)

4. [REST API, HTTP 202 Semantics & gRPC-to-HTTP Error Mapping (Task 5)](#4-rest-api-http-202-semantics--grpc-to-http-error-mapping-task-5)

5. [Docker Swarm, Compose Differences, Healthchecks & Graceful Shutdown Timeline (Task 6)](#5-docker-swarm-compose-differences-healthchecks--graceful-shutdown-timeline-task-6)

6. [5-Million-Request Load Test & Performance Timeline under Chaos](#6-5-million-request-load-test--performance-timeline-under-chaos)

7. [Prometheus Observability, Service Discovery & Metrics Architecture (Task 8)](#7-prometheus-observability-service-discovery--metrics-architecture-task-8)

8. [Quick Start & Makefile Reference](#8-quick-start--makefile-reference)

9. [Configuration & Environment Variables](#9-configuration--environment-variables)

10. [Comprehensive Architectural Q&A (Tasks 3, 4, 5, 6, 7 & Prometheus)](#10-comprehensive-architectural-qa-tasks-3-4-5-6-7--prometheus)

## 1. System Architecture & Two-Phase Ledger Data Flow

> **Architecture Decision Record (ADR):** The decision to decouple deposit ingestion from balance settlement—along with its performance gains and eventual-consistency trade-offs—is documented in [`docs/adr/0001-decoupled-ingestion-and-periodic-batch-settlement.md`](docs/adr/0001-decoupled-ingestion-and-periodic-batch-settlement.md).

```
+---------------------------------------------------------------------------------------------------+
|                                   EXTERNAL CLIENTS / LOADGEN                                      |
|                     POST /v1/deposits (Headers: X-API-Key, Idempotency-Key)                       |
+-------------------------------------------------+-------------------------------------------------+
                                                  | HTTP/1.1 (JSON, Max 16KB)
                                                  v
+---------------------------------------------------------------------------------------------------+
|                             1. GATEWAY SERVICE (:8081 | Metrics :9090)                            |
|  - Constant-Time X-API-Key Auth (SHA-256) & Per-Key Lock-Free Token-Bucket Rate Limiter           |
|  - Strict JSON Validation & Sharded Commit-Aware Idempotency Payload Guard                        |
|  - Custom Swarm DNS Resolver (swarmdns:///publisher:50051) + Round-Robin gRPC Load Balancing      |
|  - OpenMetrics Exemplars (request_id) & Automatic gRPC Retry (Unavailable)                        |
+-------------------------------------------------+-------------------------------------------------+
                                                  | gRPC Unary RPC (Protobuf v1, Optional mTLS 1.3)
                                                  v
+---------------------------------------------------------------------------------------------------+
|                     2. PUBLISHER SERVICE (:50051 | Health :8090 | Metrics :9090)                  |
|  - Unified Single-Source HealthManager (/livez, /readyz, gRPC Health, /stats, Prometheus Gauges)  |
|  - 6-Step Ordered Graceful Shutdown (SIGTERM -> Unhealthy -> DRAIN_DELAY -> GracefulStop -> Exit) |
|  - Publishes to NATS JetStream with Deterministic Deduplication Header (Nats-Msg-Id = event_id)   |
+-------------------------------------------------+-------------------------------------------------+
                                                  | NATS JetStream (FileStorage)
                                                  v
+---------------------------------------------------------------------------------------------------+
|                        NATS JETSTREAM BROKER (:4222 | NATS Exporter :7777)                        |
|  - Stream "DEPOSITS" (Subject: deposits.events, 10-min Dedup Window)                              |
|  - Stream "DEPOSITS_DLQ" (Subject: deposits.dlq, Poison-Pill Isolation & Replay)                  |
+-------------------------------------------------+-------------------------------------------------+
                                                  | Durable Pull Consumer (deposits_processor)
                                                  v
+---------------------------------------------------------------------------------------------------+
|                                3. DEPOSITOR SERVICE (Metrics :9090)                               |
|  - Concurrent Worker Pool + NakWithDelay on Temporary Infrastructure Failures                     |
|  - Phase A (Fast Ingestion): 4 Batch Flushers coalesce up to 256 events via SQL UNNEST            |
|    into "deposits" with is_settled = FALSE, then immediately send msg.Ack() to NATS               |
|  - Phase B (Periodic Settlement): SettlementWorker runs every 5s (and on shutdown) calling        |
|    PL/pgSQL settle_pending_deposits(100000) in a separate atomic transaction                      |
+-------------------------------------------------+-------------------------------------------------+
                                                  | pgx / database/sql (Connection Pool)
                                                  v
+---------------------------------------------------------------------------------------------------+
|                        POSTGRESQL 18 (:5432 | Postgres Exporter :9187)                            |
|  - Table "deposits": PK (event_id COLLATE "C"), NUMERIC(36, 18), is_settled flag (fillfactor=75)  |
|  - Table "balances": PK (user_id, currency), NUMERIC(36, 18), HOT-Update Tuned (fillfactor=70)    |
|  - Table "ledger_settled_summary": 60-row pre-aggregated summary for O(1) sub-ms reconciliation   |
+---------------------------------------------------------------------------------------------------+

```

## 2. Monorepo Structure & Package Placement Rationale (Task 4)

The repository is structured as a multi-module **Go Workspace (`go.work`)** containing one shared foundational module (`pkg`) and four independent service modules (`services/gateway`, `services/publisher`, `services/depositor`, and `services/dashboard`).

### Why Each Package Lives Where It Does (Line-by-Line Rationale)

* **`pkg/config`**: Placed in `pkg/` because every service (`gateway`, `publisher`, `depositor`) needs unified environment variable parsing, `.env` loading, and Docker Swarm secret file (`*_FILE`) resolution.

* **`pkg/logger`**: Placed in `pkg/` so all microservices emit structured JSON logs with identical Uber Zap encoder schemas, ISO-8601 timestamps, and disabled log sampling.

* **`pkg/metricsx`**: Placed in `pkg/` so every microservice constructs an isolated `*prometheus.Registry` (`prometheus.NewRegistry()`, with zero global state or `init()` registration), registers `GoCollector`, `ProcessCollector`, optional `DBStatsCollector`, and `<service>_build_info{version, revision, go_version}`, and runs a dedicated internal `/metrics` server that shuts down last.

* **`pkg/domain`**: Placed in `pkg/` as the single source of truth for deposit validation (`ValidateEventID`, `ParseAndValidateAmount` via `shopspring/decimal`, `ValidateUserID`, `ValidateCurrency`), shared across Gateway, Publisher, Depositor, and Dashboard without copy-pasting.

* **`pkg/broker`**: Placed in `pkg/` because both `publisher` (producing events) and `depositor` (consuming events and publishing to DLQ) require NATS JetStream connection management, stream provisioning, and auto-reconnect hooks.

* **`pkg/dbx`**: Placed in `pkg/` to provide standardized PostgreSQL connection pooling (`pgx/v5`) and automated `golang-migrate` execution reusable by persistence-backed services.

* **`pkg/pb/deposit/v1`**: Placed in `pkg/` so both the gRPC client (`services/gateway`) and the gRPC server (`services/publisher`) compile against the exact same Protobuf contract (`deposit.proto`) without duplicating `.proto` files.

* **`pkg/tlscfg`**: Placed in `pkg/` so both `gateway` (client) and `publisher` (server) share mutual TLS 1.3 certificate loading and deterministic Ed25519 CA derivation.

* **`services/gateway/internal/{handler,discovery,ratelimit,metrics}`**: Placed in `services/gateway/internal/` because HTTP routing (Gin), `swarmdns:///` gRPC name resolution, per-API-key GCRA rate limiting, and `gateway_*` Prometheus metrics are exclusive to the API Gateway ingress boundary.

* **`services/publisher/internal/{grpcserver,healthmgr,generator,metrics}`**: Placed in `services/publisher/internal/` because gRPC endpoint handling, the unified 4-state Publisher health state machine, the simulated event generator, and `publisher_*` Prometheus metrics belong solely to the Publisher service.

* **`services/depositor/internal/{repository,worker,metrics}`**: Placed in `services/depositor/internal/` because `UNNEST` batch ingestion, PL/pgSQL periodic settlement workers, JetStream pull worker pools, terminal worker control, and `depositor_*` Prometheus metrics are strictly internal to the Depositor consumer.

* **`services/dashboard/internal/{server,metrics}`**: Placed in `services/dashboard/internal/` because cluster telemetry aggregation, DLQ replay UI handlers, CSRF/authentication guards, and `dashboard_*` Prometheus metrics are specific to the monitoring dashboard.

## 3. Engineering Highlights & Reliability Guarantees

### 3.1. REST API Gateway & Resilient gRPC Transport (`services/gateway`)

* **Constant-Time Authentication:** Validates `X-API-Key` using `sha256.Sum256` and `subtle.ConstantTimeCompare` to prevent timing side-channel attacks, never logging the secret key.

* **Lock-Free Per-Key Rate Limiting:** Uses a 32-shard FNV-1a map with atomic nanosecond GCRA (`emptyAtNs.CompareAndSwap`) to enforce per-key RPS and burst limits without mutex contention.

* **Commit-Aware Sharded Idempotency Guard:** Tracks `Idempotency-Key` payload fingerprints (`user_id:currency:amount`) across 32 shards. Reusing the same `Idempotency-Key` with different parameters immediately returns `409 Conflict`.

* **Headless Swarm DNS Discovery (`swarmdns:///`):** Polls `tasks.publisher` every 2 seconds, retains last-known healthy IPs during transient DNS failures, updates the `gateway_publisher_endpoints` gauge, and distributes RPCs across a 4-connection gRPC client pool via `round_robin`.

### 3.2. Unified Health Manager & 6-Step Ordered Graceful Shutdown (`services/publisher`)

`healthmgr.Manager` is the single atomic source of truth (`state atomic.Int32`) for HTTP `/livez`, HTTP `/readyz`, gRPC Health (`Check` & `Watch`), `/stats`, and Prometheus `publisher_ready`, `publisher_draining`, and `publisher_nats_connected` gauges. On `SIGTERM`, Publisher executes:

1. **`STEP 1/6`**: Logs `SIGTERM` arrival timestamp and configured drain/shutdown timeouts.

2. **`STEP 2/6`**: Atomically flips health to `NOT_SERVING_UNHEALTHY` (`503` on `/livez` and `/readyz`, `NOT_SERVING` broadcast to all gRPC `Health.Watch` streams, `publisher_ready=0`, `publisher_draining=1`).

3. **`STEP 3/6`**: Continues accepting and processing arriving RPCs normally for `DRAIN_DELAY` ($6\text{s}$) while Gateway and Swarm DNS converge.

4. **`STEP 4/6`**: Closes health watch streams and invokes `grpcSrv.GracefulStop()` (waiting up to `SHUTDOWN_TIMEOUT` = $10\text{s}$ for in-flight RPCs to receive NATS `PubAck`, then falling back to `grpcSrv.Stop()`).

5. **`STEP 5/6`**: Flushes and closes the NATS JetStream connection.

6. **`STEP 6/6`**: Shuts down the auxiliary HTTP health server, closes the dedicated Prometheus `/metrics` server **last**, and exits cleanly with code `0`.

### 3.3. Two-Phase PostgreSQL Ingestion & Atomic Batch Settlement (`services/depositor`)

Rather than updating `balances` inside the per-message ingestion transaction (which causes severe row-lock contention on hot wallets), Depositor separates ingestion from settlement (see [ADR 0001](docs/adr/0001-decoupled-ingestion-and-periodic-batch-settlement.md)):

1. **Phase A — Micro-Batched Ingestion (`executeBatch`):** 4 concurrent flusher goroutines coalesce up to 256 events ($250\ \mu\text{s}$ linger window) into a single `INSERT INTO deposits ... SELECT UNNEST(...) ON CONFLICT (event_id) DO NOTHING RETURNING event_id` transaction with `is_settled = FALSE`. Once committed to `deposits`, the worker immediately acknowledges NATS (`msg.Ack()`).

2. **Phase B — Periodic Atomic Settlement (`settle_pending_deposits`):** Every 5 seconds (and once via `StopAndFlush` during shutdown), `SettlementWorker` invokes `SELECT settle_pending_deposits(100000)` in its own ACID transaction to claim unsettled rows via `ctid` and `FOR UPDATE SKIP LOCKED`, transition `is_settled = TRUE`, aggregate amounts per `(user_id, currency)` ordered by `user_id ASC, currency ASC`, and atomically upsert `balances` and `ledger_settled_summary`.

## 4. REST API, HTTP 202 Semantics & gRPC-to-HTTP Error Mapping (Task 5)

### Why `POST /v1/deposits` Returns `202 Accepted` Instead of `201 Created`

When the Gateway responds to the client, the deposit event has been validated and durably persisted in the **NATS JetStream** log (`DEPOSITS` stream, confirmed via `PubAck`), **not** yet inserted or settled in PostgreSQL (`deposits` and `balances` tables). Because final ledger persistence and periodic balance settlement happen **asynchronously** downstream in the `depositor` service, returning `201 Created` would falsely imply that the database resource and wallet balance have already been updated. `202 Accepted` accurately communicates that the request has been durably accepted for asynchronous processing.

### gRPC Status Code to HTTP Status Code Mapping Table

| Upstream Condition / Error | gRPC Status Code (`codes.*`) | Gateway HTTP Status | Error JSON `code` | `gateway_deposit_requests_total{result}` | Retryable? | 
| ----- | ----- | ----- | ----- | ----- | ----- | 
| Event persisted in NATS (`PubAck`, fresh) | `OK` (`0`) | `202 Accepted` | — | `accepted` | Yes | 
| Event persisted in NATS (`PubAck`, dedup) | `OK` (`0`) | `202 Accepted` | — | `duplicate` | Yes | 
| Invalid `event_id`, `user_id`, `currency`, or `amount` | `InvalidArgument` (`3`) | `400 Bad Request` | `VALIDATION_ERROR` | `invalid` | No | 
| Same `Idempotency-Key` reused with different payload | `AlreadyExists` (`6`) | `409 Conflict` | `IDEMPOTENCY_KEY_CONFLICT` | `invalid` | No | 
| Per-key rate limit or upstream resource exhausted | `ResourceExhausted` (`8`) | `429 Too Many Requests` | `RATE_LIMITED` | `rate_limited` | Yes (backoff) | 
| Publisher / NATS unreachable or connection dropped | `Unavailable` (`14`) / `Canceled` (`1`) | `503 Service Unavailable` | `SERVICE_UNAVAILABLE` | `upstream_unavailable` | Yes (same key) | 
| Context deadline expired before `PubAck` returned | `DeadlineExceeded` (`4`) | `504 Gateway Timeout` | `GATEWAY_TIMEOUT` | `upstream_timeout` | Yes (same key) | 
| Unexpected panic / unhandled internal error | `Internal` (`13`) / Other | `502 Bad Gateway` | `UPSTREAM_ERROR` | `internal_error` | Yes (same key) | 

## 5. Docker Swarm, Compose Differences, Healthchecks & Graceful Shutdown Timeline (Task 6)

### 5.1. Docker Compose Keys Ignored by `docker stack deploy` & Our Replacements

| Ignored Compose Key | Why Swarm Ignores It | Replacement in `deploy/stack.yml` & Workflow | 
| ----- | ----- | ----- | 
| `build:` | Swarm orchestrates pre-built images across cluster nodes and cannot build source code on the fly. | Built immutable images tagged with Git SHA (`IMAGE_TAG`), pushed to local `registry:2` (`127.0.0.1:5000`) via `make images`. | 
| `depends_on:` | Swarm schedules tasks asynchronously across nodes and does not enforce startup ordering between services. | Every service (`gateway`, `publisher`, `depositor`, `dashboard`) implements exponential backoff retry loops on startup and auto-reconnects to NATS and Postgres independently. | 
| `restart: unless-stopped` | Top-level Compose restart policy is ignored by the Swarm orchestrator. | Replaced with `deploy.restart_policy` (`condition: any`, `delay: 3s`, `max_attempts: 0`, `window: 60s`). | 
| `container_name:` | Swarm manages dynamic task names (`<stack>_<service>.<slot>.<task_id>`) to support multiple replicas. | Services discover peers via Swarm DNS (`tasks.<service>`) and identify themselves via `os.Hostname()` (`instance_id`). | 

### 5.2. Publisher `HEALTHCHECK` Parameters & Swarm Container States

In `deploy/stack.yml` and `services/publisher/Dockerfile`, Publisher's healthcheck is configured as:

* **`interval: 2s`**: Probes `http://127.0.0.1:8090/livez` every $2\text{s}$ so Docker detects an unhealthy/draining container within $2\text{--}4\text{s}$.

* **`timeout: 1s`**: Since `/livez` reads a single in-memory `atomic.Int32` state, it responds in $<1\ \text{ms}$. A $1\text{s}$ timeout prevents hung probes without false positives under CPU load.

* **`retries: 2`**: Requires 2 consecutive failures ($\approx 4\text{s}$) before declaring the container `unhealthy`, avoiding flapping on transient CPU spikes while still reacting within `DRAIN_DELAY` ($6\text{s}$).

* **`start_period: 5s`** ($10\text{s}$ in Dockerfile): Gives the container grace time during initial boot to connect to NATS and provision streams before failed probes count toward `retries`.

**Where Healthcheck Runs & Swarm's Behavior per State:**

* **Execution Context:** The healthcheck command (`wget -qO- http://127.0.0.1:8090/livez`) is executed **inside the container's network/PID namespace** by the local **Docker Engine daemon (`dockerd`)** on the host node running that task.

* **`starting`**: Initial state after container creation. During `start_period`, probe failures do not increment the failure counter. During rolling updates (`order: start-first`), Swarm waits in this state and does **not** stop the old replica until the new task transitions to `healthy`.

* **`healthy`**: Triggered as soon as `/livez` returns HTTP `200`. Swarm registers the container's IP in internal DNS (`tasks.publisher`) and considers the rolling update step successful.

* **`unhealthy`**: Triggered after `retries` (2) consecutive non-zero exits. Swarm immediately removes the container's IP from DNS (`tasks.publisher`), stops routing traffic to it, and—if the container was not intentionally stopping (`SIGTERM`)—terminates and replaces the task (or triggers automatic rollback during an update if `failure_action: rollback` is tripped).

### 5.3. Calculation of `stop_grace_period`

When Docker stops a Publisher container, the total maximum time needed for a clean 6-step exit is:

$$
\text{Total Shutdown Time} = \text{DRAIN\_DELAY} (6\text{s}) + \text{SHUTDOWN\_TIMEOUT} (10\text{s}) + \text{NATS Flush, Health \& Metrics Teardown} (\approx 4\text{s}) = 20\text{s}
$$


Setting **`stop_grace_period: 25s`** in `deploy/stack.yml` provides a $5\text{s}$ **safety buffer** above the $20\text{s}$ worst-case execution path, guaranteeing that Docker never kills the process prematurely with `SIGKILL`. Similarly, `depositor` uses $\text{SHUTDOWN\_TIMEOUT} (15\text{s}) + \text{Final Settlement Flush} (10\text{s}) = 25\text{s}$ (`stop_grace_period: 25s`), and `gateway` uses $\text{SHUTDOWN\_TIMEOUT} (10\text{s}) + 5\text{s buffer} = 15\text{s}$ (`stop_grace_period: 15s`).

### 5.4. How Gateway Coordinates with a Draining Publisher (Zero 5xx on Scale-Down)

Coordination uses a dual-mechanism defense:

1. **Real-Time gRPC `Health.Watch` Subchannel Ejection (Sub-Millisecond):** Gateway configures gRPC with `"healthCheckConfig": {"serviceName": ""}` and imports `google.golang.org/grpc/health`. Each gRPC subchannel maintains an active streaming `Health.Watch` RPC to its Publisher replica. In **Step 2/6** of shutdown (immediately upon `SIGTERM`), Publisher's `HealthManager` broadcasts `NOT_SERVING` to all watcher channels. Gateway's `round_robin` balancer immediately ejects that subchannel from the active picker **before** Publisher stops accepting requests.

2. **Headless DNS Polling + `DRAIN_DELAY` Absorption (**$6\text{s}$**):** Gateway's `swarmdns` resolver re-queries `tasks.publisher` every $2\text{s}$. During the $6\text{s}$ `DRAIN_DELAY` (**Step 3/6**), any RPCs already in flight on the wire are accepted and processed normally by the Publisher. Only after $6\text{s}$—when both `Health.Watch` and DNS have completely drained traffic away—does Publisher call `GracefulStop()` (**Step 4/6**).

### 5.5. Publisher Graceful Shutdown Timeline (Logs & `docker events`)

Below is the observed timeline of a Publisher replica (`instance_id: b9fe42233331`) during a scale-down under active load with `GRPC_MAX_ATTEMPTS=1`:

| Timestamp (UTC) | Relative | Source | Event / Log Milestone | Details | 
| ----- | ----- | ----- | ----- | ----- | 
| `10:37:24.002Z` | `T+0.00s` | `docker events` | `container kill (signal=15)` | Swarm sends `SIGTERM` to container `b9fe42233331` | 
| `10:37:24.003Z` | `T+0.00s` | `publisher` (Zap) | `STEP 1/6: SIGTERM received` | `drain_delay=6s`, `shutdown_timeout=10s` | 
| `10:37:24.003Z` | `T+0.00s` | `publisher` (Zap) | `STEP 2/6: Health status set to UNHEALTHY` | `/livez` & `/readyz` -> `503`; gRPC `Health.Watch` -> `NOT_SERVING` | 
| `10:37:24.003Z` | `T+0.00s` | `publisher` (Zap) | `STEP 3/6: Entering DRAIN_DELAY window` | Continues serving arriving requests for $6\text{s}$ | 
| `10:37:24.018Z` | `T+0.01s` | `publisher` (Zap) | **Last Request Received** | `last_request_received_at="2026-11-08T10:37:24.018412091Z"` | 
| `10:37:24.021Z` | `T+0.02s` | `publisher` (Zap) | **Last Request Completed** | `last_request_completed_at="2026-11-08T10:37:24.021104882Z"` (`in_flight=0`) | 
| `10:37:27.410Z` | `T+3.41s` | `docker events` | `container health_status: unhealthy` | Docker daemon observes 2 failed `/livez` checks (`interval=2s, retries=2`) | 
| `10:37:30.004Z` | `T+6.00s` | `publisher` (Zap) | `STEP 3/6 COMPLETE: DRAIN_DELAY elapsed` | `in_flight_requests=0` | 
| `10:37:30.004Z` | `T+6.00s` | `publisher` (Zap) | `STEP 4/6: Starting gRPC GracefulStop` | Closes `Health.Watch` streams; calls `grpcSrv.GracefulStop()` | 
| `10:37:30.005Z` | `T+6.00s` | `publisher` (Zap) | `STEP 4/6 COMPLETE: gRPC server stopped` | All in-flight RPCs finished cleanly | 
| `10:37:30.009Z` | `T+6.01s` | `publisher` (Zap) | `STEP 5/6 COMPLETE: NATS connection closed` | Async publish queue flushed and NATS socket closed | 
| `10:37:30.011Z` | `T+6.01s` | `publisher` (Zap) | `STEP 6/6: Publisher process exiting cleanly` | Metrics server closed last; `exit_code=0`, `gateway_5xx=0` | 

### 5.6. Publisher Behavior When NATS Is Stopped for 2 Minutes

When NATS is stopped for 2 minutes (`docker service scale deposit_nats=0` or `docker stop`):

1. Within $250\ \text{ms}$, Publisher's background monitor detects `natsBroker.IsConnected() == false` and calls `healthMgr.SetNATSConnected(false)`.

2. `HealthManager` transitions to **`StateDegradedNATS` (`DEGRADED_NATS_DISCONNECTED`)**:

  * **HTTP `/livez` stays `200 OK`**: Because the Publisher process itself is healthy and waiting for NATS, Docker Swarm does **not** kill or restart the Publisher containers (preventing useless container crash loops).

  * **HTTP `/readyz` returns `503 Service Unavailable`**, **gRPC Health returns `NOT_SERVING`**, **`publisher_nats_connected` drops to `0`**, and **`publisher_ready` drops to `0`**: Gateway's `/readyz` immediately reports `503`, and arriving client deposit requests fail fast with `503 Service Unavailable`.

3. When NATS comes back online after 2 minutes, `nats.go` automatically reconnects (`MaxReconnects(-1)`), `handleReconnected()` re-verifies the JetStream streams, `SetNATSConnected(true)` transitions `HealthManager` back to **`StateHealthy` (`SERVING`)** (`publisher_nats_connected=1`, `publisher_ready=1`), and the entire pipeline resumes automatically without restarting a single container.

## 6. 5-Million-Request Load Test & Performance Timeline under Chaos

### 6.1. Why Stream Message Count Can Exceed 5,000,000 Without Corrupting Balances

During chaos injection (`docker kill -s KILL` on Publisher or Depositor restarts), the NATS `DEPOSITS` stream `Last Sequence` / message count may slightly exceed `5,000,000` for two reasons:

1. If manual poison-pill tests or DLQ replay tests were executed on the same stream prior to or during the run.

2. If an event was published outside the 10-minute NATS deduplication window or replayed with a new replay `MsgID`.
   **Why balances are never corrupted:** PostgreSQL enforces `event_id VARCHAR(255) COLLATE "C" PRIMARY KEY` on the `deposits` table with `ON CONFLICT (event_id) DO NOTHING`. Even if the same deposit event were written to the NATS stream multiple times, PostgreSQL atomically discards any duplicate `event_id` during Phase A ingestion, and `settle_pending_deposits` only settles rows in `deposits` where `is_settled = FALSE`. Thus, `COUNT(*)` for `run-<RUN_ID>-%` in `deposits` is strictly `5,000,000` and ledger discrepancy is `0`.

### 6.2. Throughput (RPS), Publisher Replicas & Gateway 5xx Timeline (`chaos.log` & `loadgen-timeline.log`)

| Elapsed Time | Active Publisher Replicas | Chaos Action Injected | Throughput (RPS) | Gateway 5xx (Window) | Notes | 
| ----- | ----- | ----- | ----- | ----- | ----- | 
| `00:00 - 00:25` | `3 -> 6` | `SCALE_PUBLISHER target=6` | `12,450 req/s` | `0` | Seamless scale-up via `swarmdns` | 
| `00:25 - 01:30` | `6` | Steady High-Throughput Load | `14,820 req/s` | `0` | Load balanced across all 6 replicas | 
| `01:31` | `6 -> 5` | `DOCKER_KILL_SIGKILL (1/2)` | `11,910 req/s` | `0` (Retried) | Instant `SIGKILL`; Gateway auto-retries transient `Unavailable` | 
| `01:32 - 02:50` | `5 -> 4` | `SCALE_PUBLISHER target=4` | `11,340 req/s` | `0` | Zero-error 6-step graceful drain on scale-down | 
| `02:55 - 03:50` | `4` | `FORCE_ROLLING_UPDATE` | `10,980 req/s` | `0` | `order: start-first` + `DRAIN_DELAY=6s` -> 0 errors | 
| `05:05` | `4 -> 6` | `RESTART_DEPOSITOR` + Scale `6` | `14,610 req/s` | `0` | JetStream buffers events; Depositor resumes from durable cursor | 
| `06:30` | `6` -> 5 | `DOCKER_KILL_SIGKILL (2/2)` | `13,890 req/s` | `0` (Retried) | Second ungraceful kill absorbed by idempotent retry | 
| `06:45 - 07:10` | `5` | Run Completed (`5,000,000 / 5,000,000`) | `14,120 req/s` | `0` | **Final Failures: 0 | Discrepancy: 0** | 

## 7. Prometheus Observability, Service Discovery & Metrics Architecture (Task 8)

> **Full Metrics Catalog & PromQL Reference:** See [`docs/metrics.md`](docs/metrics.md) for the complete catalog of every metric, label set, unit, histogram bucket rationale, and golden-signal PromQL queries.

### 7.1. Core Observability Design Rules Implemented

1. **Isolated Registries (`prometheus.NewRegistry()`):** Every service creates its own explicit `*prometheus.Registry` in `pkg/metricsx` and injects it into handlers/workers. Global `prometheus.DefaultRegisterer` and `init()` registrations are strictly prohibited.

2. **Dedicated Internal Metrics Port (`:9090`):** Every service exposes `/metrics` on a separate internal HTTP server (`METRICS_PORT=9090`) reachable **only** inside the `deposit_overlay` network. Port `9090` of application containers is never published on the host, and Gateway's public port `:8081` does **not** serve `/metrics` (`404 Not Found` coalesced under `route="unmatched"`).

3. **Single Source of Counting:** All duplicate manual `atomic.Uint64` request/message counters were removed. Auxiliary `/stats` endpoints read counter and gauge values directly from the Prometheus metric objects via `metricsx.ReadCounterValue` and `metricsx.ReadGaugeValue`.

4. **Zero-Query Scrape Path (**$<5\ \text{ms}$**):** No database query or NATS RPC is ever executed inside the `/metrics` HTTP scrape handler. All external states (`publisher_ready`, `depositor_nats_connected`, `depositor_workers_active`, `gateway_publisher_endpoints`) are updated asynchronously in background goroutines or read from in-memory `atomic.Int32` state (`healthmgr`).

5. **Metrics Server Shuts Down Last:** Across all 4 services, `metricsSrv.Shutdown()` is executed as the **final step** of graceful shutdown—after HTTP/gRPC servers, worker pools, final settlement flushes, NATS connections, and DB pools have closed—ensuring Prometheus can scrape final shutdown metrics.

### 7.2. Target Discovery in Docker Swarm: Options, Trade-offs & Security Risks

To scrape each replica of `gateway`, `publisher`, `depositor`, and `dashboard` individually (with new replicas appearing in `<10s` on scale-up and disappearing on scale-down), we evaluated three service discovery mechanisms:

| Discovery Mechanism | How It Works | Pros | Cons & Security Risks | 
| ----- | ----- | ----- | ----- | 