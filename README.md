# Reliable Deposit Event Processing System 🚀

A production-grade, fault-tolerant financial deposit processing monorepo built with **Go**, backed by **PostgreSQL** (ACID & Concurrency Control) and **NATS JetStream** (Persistent Messaging, Deduplication & Consumer Grouping).

---

## 🏛️ Architecture & Core Guarantees

This system is engineered around a strict golden rule for financial systems:
> **"No valid deposit event shall ever be lost, and no valid event shall ever produce a financial effect more than once."**

1. **Multi-Module Monorepo & Separation of Concerns:**
    - `pkg/domain`: Pure business rules, arbitrary-precision financial calculations (`shopspring/decimal`), ledger discrepancy models, and domain errors (`ErrDuplicateEvent`, `InvalidMessageError`).
    - `pkg/broker`: NATS JetStream integration (`FileStorage`), deduplication windows, and Dead Letter Queue (`DEPOSITS_DLQ`) management with deterministic message IDs.
    - `pkg/dbx`: Generic PostgreSQL connection pooling (`pgx`), retry-ping backoff, and automatic database migrations (`golang-migrate`).
    - `pkg/config` & `pkg/logger`: Shared environment configuration loader (`OS Env > .env > defaults`) and structured JSON logging (`zap`).
    - `services/depositor`: Consumer microservice handling PostgreSQL ACID transactions, row-level locking, NATS Consumer Grouping (`PullMaxMessages(1)`), non-blocking retry backoff (`NakWithDelay`), and live telemetry reporting.
    - `services/publisher`: Standalone event generator microservice simulating deposit traffic over NATS JetStream with live remote pause/resume control.
    - `services/dashboard`: Enterprise cluster monitoring and control center providing real-time TPS telemetry, Automated Ledger Reconciliation (`FULL OUTER JOIN`), and an interactive DLQ Inspector & Replay engine.

2. **Zero Lost Updates & Concurrency Safety:**
   Balance updates execute inside the same ACID transaction as the deposit ledger insert using an atomic upsert with PostgreSQL Row-Level Exclusive Locking:
   ```sql
   INSERT INTO balances (user_id, currency, balance, updated_at)
   VALUES ($1, $2, $3, NOW())
   ON CONFLICT (user_id, currency) 
   DO UPDATE SET
       balance = balances.balance + EXCLUDED.balance,
       updated_at = NOW();
   ```
   Even under a storm of 1,000+ concurrent goroutines or multiple scaled `depositor` replicas, race conditions and lost updates are mathematically impossible.

3. **Poison Pill Isolation & Deterministic DLQ:**
   Malformed JSON, negative/zero amounts, or unsupported currencies (e.g., `DOGE`) are safely intercepted and published to the `DEPOSITS_DLQ` stream—enriched with source stream metadata (`source_stream`, `subject`, `stream_sequence`) and a deterministic `Nats-Msg-Id` (`dlq-<stream>-<seq>`)—*before* acknowledging the original message. This prevents queue blockage while guaranteeing zero data loss and preventing duplicate DLQ entries on crash recovery.

4. **Automated Ledger Reconciliation (Invariant I-3):**
   The system continuously verifies accounting integrity by executing a `FULL OUTER JOIN` between the sum of all recorded ledger events (`SUM(deposits.amount)`) and user wallet balances (`balances.balance`), ensuring zero financial discrepancy across all users and currencies.

5. **Graceful Shutdown (Ordered Shutdown):**
   Upon receiving `SIGINT` or `SIGTERM`, each service independently stops intake/generation (`DrainAll()`), waits up to `SHUTDOWN_TIMEOUT` for in-flight operations and database transactions to complete, flushes logs, and cleanly closes NATS and PostgreSQL connections.

---

## 🛠️ Tech Stack
- **Language:** Go 1.26+ (Multi-Module Workspace via `go.work`)
- **Database:** PostgreSQL 18 (with `pgx/v5` & `lib/pq` drivers)
- **Message Broker:** NATS JetStream 2.10 (File Storage)
- **Financial Math:** `shopspring/decimal` & PostgreSQL `NUMERIC(36, 18)` (Zero floating-point rounding errors)
- **Logging:** `go.uber.org/zap` (Structured JSON logs)
- **Observability:** NATS Live Telemetry & Standalone Enterprise Web Dashboard (`:8080`)

---

## 🚀 Quick Start (For Developers)

### Prerequisites
- Docker & Docker Compose installed on your machine.
- Go 1.26+ and `make` (if running locally without Docker).

### Option A: Run Full Stack via Makefile & Docker Compose (Recommended)
1. Clone the repository:
   ```bash
   git clone https://github.com/alihojaty/reliable-deposit-processor.git
   cd reliable-deposit-processor
   ```
2. Spin up the entire stack (PostgreSQL, NATS, Depositor replicas, Publisher, and Dashboard):
   ```bash
   make up
   ```
3. Scale the `depositor` service horizontally to 2 instances:
   ```bash
   make scale
   ```
4. Open the **Enterprise Cluster Control Dashboard** in your browser:
   👉 **`http://localhost:8080`**
5. Stop the stack when finished:
   ```bash
   make down
   ```

### Option B: Run Locally for Development
1. Copy the example environment file:
   ```bash
   cp .env.example .env
   ```
2. Start infrastructure services only (PostgreSQL & NATS):
   ```bash
   docker compose -f deploy/docker-compose.yml up -d postgres nats
   ```
3. Build all modules and binaries:
   ```bash
   make build
   ```
4. Run `depositor`, `publisher`, and `dashboard` in separate terminals:
   ```bash
   go run ./services/depositor/cmd/depositor
   ```
   ```bash
   PUBLISH_ENABLED=true go run ./services/publisher/cmd/publisher
   ```
   ```bash
   DATABASE_URL="postgres://postgres:postgres@localhost:5432/deposits_db?sslmode=disable" go run ./services/dashboard/cmd/dashboard
   ```

---

## 🧪 Running Tests, Linting & Chaos Engineering

All workspace modules (`./pkg`, `./services/depositor`, `./services/publisher`, `./services/dashboard`) are orchestrated via the root `Makefile`:

- **Run all unit, concurrency (1,000 concurrent deposits), ledger reconciliation, and chaos recovery tests across all modules:**
  ```bash
  make test
  ```
- **Run `go vet` and `golangci-lint` across all modules:**
  ```bash
  make lint
  ```
- **Tidy all module `go.mod` files and sync `go.work`:**
  ```bash
  make tidy
  ```

---

## 🎛️ Enterprise Cluster Dashboard Features (`http://localhost:8080`)
- **Cluster-Wide Live Telemetry:** Real-time monitoring of active `depositor` replicas, worker counts, per-instance TPS, global processed events, blocked duplicates, and retry counts via NATS pub/sub telemetry.
- **Automated Ledger Reconciliation Panel:** Live execution of the `FULL OUTER JOIN` audit query comparing `SUM(deposits.amount)` against `balances.balance` across `USDT`, `BTC`, and `IRT`, immediately highlighting any discrepancies.
- **DLQ Inspector & Deterministic Replay:** Inspect poisoned messages stored in `DEPOSITS_DLQ` complete with source metadata (`source_stream`, `subject`, `stream_sequence`, `reason`, and raw JSON payload). Includes a built-in **Edit & Replay** modal to fix invalid payloads (e.g., changing `DOGE` to `USDT`), republish them to the main `DEPOSITS` stream, and remove them from the DLQ.
- **Live Poison Pill Injector & Publisher Toggle:** Inject test invalid events (`Poison Pill`) or pause/resume the `publisher` service in real time with a single click.
- **PostgreSQL Live Wallet Balances & Recent Deposits:** Real-time tables displaying high-precision user wallet balances and the latest processed deposit transactions.

---

## 🧩 Monorepo & Multi-Module Architecture

This repository is structured as a Go **Multi-Module Monorepo** orchestrated via `go.work`:

```text
reliable-deposit-processor/
├── go.work
├── Makefile
├── pkg/                      # Shared independent module
│   ├── go.mod
│   ├── broker/               # NATS JetStream client, streams & deterministic DLQ
│   ├── config/               # Environment configuration loader (OS Env > .env > defaults)
│   ├── dbx/                  # PostgreSQL connection pool & migrations
│   ├── domain/               # DepositEvent, DLQMessage, LedgerDiscrepancy & validation
│   └── logger/               # Structured Zap logger
├── services/
│   ├── depositor/            # Depositor service module (NATS -> PostgreSQL)
│   │   ├── go.mod
│   │   ├── cmd/depositor/main.go
│   │   ├── internal/{repository,worker,metrics,integration}/
│   │   ├── migrations/
│   │   └── Dockerfile
│   ├── publisher/            # Publisher service module (Event Simulator)
│   │   ├── go.mod
│   │   ├── cmd/publisher/main.go
│   │   ├── internal/generator/
│   │   └── Dockerfile
│   └── dashboard/            # Cluster Monitoring, Reconciliation & DLQ Replay UI
│       ├── go.mod
│       ├── cmd/dashboard/main.go
│       ├── internal/server/
│       └── Dockerfile
└── deploy/
    └── docker-compose.yml
```

### Package Placement Decisions (`pkg/` vs `internal/`)

| Package Path | Module | Reason for Placement |
| :--- | :--- | :--- |
| `pkg/config` | `pkg` | Shared environment configuration loader (`env` + `godotenv`) needed by both `depositor` and `publisher`. |
| `pkg/logger` | `pkg` | Standardized structured JSON logger (`zap`) used across services for uniform observability. |
| `pkg/domain` | `pkg` | Contains the shared `DepositEvent`, `DLQMessage`, and `LedgerDiscrepancy` contracts, currency constants, and `shopspring/decimal` validation rules shared between producers and consumers. |
| `pkg/broker` | `pkg` | Shared NATS JetStream client, stream provisioning (`DEPOSITS`, `DEPOSITS_DLQ`), and publish/consume helpers used across services. |
| `pkg/dbx` | `pkg` | Generic PostgreSQL connection pooling (`pgx`), retry-ping backoff, and migration runner reusable by any database-backed service. |
| `services/depositor/internal/repository` | `depositor` | Contains SQL ACID transactions, balance upserts, and ledger reconciliation queries specific to the `deposits` and `balances` schema; encapsulated so no external service can mutate `depositor`'s state directly. |
| `services/depositor/internal/worker` | `depositor` | Consumer group management (`PullMaxMessages(1)`), `NakWithDelay` retry logic, metadata extraction, and terminal draining specific to the deposit consumer pipeline. |
| `services/depositor/internal/metrics` | `depositor` | Lock-free atomic metrics collector and NATS telemetry reporter specific to `depositor` replicas. |
| `services/depositor/internal/integration` | `depositor` | End-to-end chaos engineering and million-scale streaming pipeline tests verifying `depositor`'s resilience. |
| `services/publisher/internal/generator` | `publisher` | Event simulation, NATS control listener, and random deposit generation logic exclusive to the `publisher` simulator service. |
| `services/dashboard/internal/server` | `dashboard` | HTTP server, NATS telemetry aggregator, `FULL OUTER JOIN` reconciliation runner, and DLQ inspection/replay handlers exclusive to the dashboard service. |

---

### Architectural Q&A

#### 1. Should `go.work` be committed to Git? Why or why not?
**Yes, in this project (an Application/Service Monorepo), we commit `go.work` and `go.work.sum` to Git.**
* **Reasoning:** While single-module Go libraries typically ignore `go.work` so individual developers can maintain local workspaces, in a **Multi-Service Application Monorepo** containing interdependent modules (`pkg`, `depositor`, `publisher`, `dashboard`), committing `go.work` guarantees a reproducible developer and CI/CD experience. Anyone who clones the repository—as well as Docker multi-stage builds—immediately resolves local cross-module changes in `./pkg` without requiring manual `go work init` commands or publishing intermediate tags. Furthermore, each service's `go.mod` also includes a relative `replace` directive (`=> ../../pkg`) so standalone module operations (`go mod tidy`) work seamlessly.

#### 2. If a new microservice (e.g., `notifier` or `withdrawer`) is added tomorrow, what changes and what stays untouched?
* **What stays untouched (Zero-Touch):**
    * All source code and `Dockerfile`s inside `services/depositor`, `services/publisher`, and `services/dashboard` remain 100% untouched.
    * Existing infrastructure packages in `pkg/` (`logger`, `dbx`, `config`, `broker`) are reused as-is without modification (unless a new shared domain event contract is introduced).
* **What is added or updated:**
    1. Create a new directory `services/notifier` with its own independent `go.mod`, `cmd/notifier/main.go`, `internal/` packages, and `Dockerfile`.
    2. Register `./services/notifier` in `go.work` (`go work use ./services/notifier`) and append it to the `MODULES` list in the `Makefile` and CI/CD matrix.
    3. Add the container definition to `deploy/docker-compose.yml`.

#### 3. Why are both `pkg/` and `internal/` necessary? Give a concrete example of each from this project.
* **`pkg/` for Controlled Code Reuse & Shared Contracts:** Prevents code duplication (DRY) across microservices. **Project Example:** `pkg/domain` defines the `DepositEvent` struct and `shopspring/decimal` validation rules. Both `publisher` (to generate valid events) and `depositor` (to parse and validate incoming events) must adhere to the exact same domain contract.
* **`internal/` for Strict Service Boundaries & Compiler-Enforced Encapsulation:** The Go compiler actively forbids any package outside a module's tree from importing its `internal/` packages (`import of internal package not allowed`). This prevents tight coupling between microservices. **Project Example:** `services/depositor/internal/repository` contains the SQL transactions that mutate the `deposits` and `balances` tables. The `publisher` service must never bypass NATS to call `depositor`'s database logic directly, and placing `repository` inside `services/depositor/internal/` enforces this architectural boundary at compile time.

#### 4. What happens if a database transaction commits, but the `depositor` crashes right before sending `Ack` to NATS?
* Because NATS JetStream operates with **At-Least-Once Delivery** (`AckExplicitPolicy`), when the `AckWait` timer expires without receiving an `Ack`, JetStream redelivers the message to another active `depositor` replica (or the same instance upon restart).
* When `ProcessDeposit` executes for the redelivered message, the query `INSERT INTO deposits (event_id, ...) VALUES (...) ON CONFLICT (event_id) DO NOTHING` detects that the primary key `event_id` already exists from the previously committed transaction and returns `RowsAffected == 0`.
* The repository immediately returns `domain.ErrDuplicateEvent` without executing the `balances` update query, rolls back the no-op transaction, logs the duplicate detection, and sends `msg.Ack()` to NATS.
* Consequently, **Exactly-Once Financial Effect** is preserved, and double-crediting (double spend) is impossible.

#### 5. What happens if PostgreSQL goes completely offline for 10 minutes?
* The `depositor` service **does not crash, panic, or exit**, nor does it falsely route valid messages to `DEPOSITS_DLQ` (since infrastructure unavailability is distinct from an invalid payload).
* For every failed database transaction attempt, workers return the message to JetStream using `msg.NakWithDelay(cfg.RetryInterval)` (default `5s`), preventing CPU busy-loops or log flooding.
* Because the `DEPOSITS` stream uses persistent `FileStorage` without premature message expiration and the durable consumer is configured with `MaxDeliver: -1` (unlimited redeliveries for transient infrastructure outages), all incoming events generated by `publisher` remain safely persisted on disk in NATS JetStream.
* The moment PostgreSQL comes back online, `sql.DB` connection pooling automatically re-establishes healthy connections and `depositor` drains the entire 10-minute backlog to completion with zero data loss.

#### 6. Why is using `float32` or `float64` strictly forbidden for financial amounts?
* Standard IEEE 754 floating-point types (`float32`/`float64`) use base-2 (binary) representation, which cannot accurately represent common base-10 decimal fractions such as `0.1` or `0.2` (for example, in binary floating-point arithmetic, `0.1 + 0.2` evaluates to `0.30000000000000004`).
* Across thousands of concurrent financial transactions, these binary rounding errors accumulate, causing severe accounting drift where user wallet balances no longer equal the sum of the deposit ledger (`SUM(deposits.amount) != balances.balance`).
* Therefore, throughout this entire system, monetary amounts are serialized as exact `string` values in JSON, processed using arbitrary-precision base-10 arithmetic via `shopspring/decimal` in Go, and stored as exact `NUMERIC(36, 18)` columns in PostgreSQL.

#### 7. Service Lifecycle Independence & Comparing `COUNT(deposits)` with Stream `LastSequence`
* **Independent Service Lifecycles:** Because `publisher` and `depositor` run as separate OS processes/containers, each manages its own graceful shutdown lifecycle independently. Stopping `depositor` drains active workers and closes its NATS/DB connections without killing `publisher`, while stopping `publisher` halts event generation and flushes in-flight publishes.
* **When `COUNT(deposits) == stream.LastSequence` Holds True:** Equality between the PostgreSQL `deposits` row count and the NATS `DEPOSITS` stream `LastSequence` holds true only in a clean environment where every published message is valid, has a unique `event_id`, and the consumer backlog is fully drained. Whenever poison messages (routed to `DEPOSITS_DLQ`), replayed messages, or duplicate `event_id` messages published outside NATS's deduplication window occur, `stream.LastSequence` will naturally be greater than `COUNT(deposits)`, which is the expected and mathematically sound behavior.