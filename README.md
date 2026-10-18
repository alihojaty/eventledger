# Reliable Deposit Event Processing System 🚀

A production-grade, fault-tolerant financial deposit processing microservice built with **Go**, backed by **PostgreSQL** (ACID & Concurrency Control) and **NATS JetStream** (Persistent Messaging & Consumer Grouping).

---

## 🏛️ Architecture & Core Guarantees

This system is engineered around a strict golden rule for financial systems:
> **"No valid deposit event shall ever be lost, and no valid event shall ever produce a financial effect more than once."**

1. **Clean Architecture & Separation of Concerns:**
   - `internal/domain`: Pure business rules, arbitrary-precision financial calculations (`shopspring/decimal`), and domain errors (`ErrDuplicateEvent`, `InvalidMessageError`).
   - `internal/repository`: PostgreSQL ACID transactions, automatic migrations (`golang-migrate`), and row-level locking.
   - `internal/broker`: NATS JetStream integration (`FileStorage`), deduplication windows, and Dead Letter Queue management.
   - `internal/worker`: NATS Consumer Grouping (`PullMaxMessages(1)`), non-blocking retry backoff (`NakWithDelay`), and interactive terminal draining.

2. **Zero Lost Updates & Concurrency Safety:**
   Balance updates use an atomic upsert with PostgreSQL Row-Level Exclusive Locking:
   ```sql
   INSERT INTO balances ... ON CONFLICT (user_id, currency) 
   DO UPDATE SET balance = balances.balance + EXCLUDED.balance;
   ```
   Even under a storm of 1,000+ concurrent goroutines, race conditions and lost updates are mathematically impossible.

3. **Poison Pill Isolation (DLQ):**
   Malformed JSON, negative amounts, or unsupported currencies (e.g., `DOGE`) are safely intercepted and published to the `DEPOSITS_DLQ` stream *before* acknowledging the original message, preventing queue blockage while ensuring zero data loss.

4. **Graceful Shutdown (8-Stage Ordered Shutdown):**
   Upon receiving `SIGINT` or `SIGTERM`, the application stops consuming new messages (`Drain()`), waits up to `SHUTDOWN_TIMEOUT` for in-flight DB transactions to commit, safely stops the publisher and brokers, and cleanly closes database connections.

---

## 🛠️ Tech Stack
- **Language:** Go 1.23+
- **Database:** PostgreSQL 16 (with `pgx/v5` driver)
- **Message Broker:** NATS JetStream (File Storage)
- **Financial Math:** `shopspring/decimal` (Zero floating-point rounding errors)
- **Logging:** `go.uber.org/zap` (Structured JSON logs)
- **Observability:** Custom Lock-Free Atomic Metrics & Embedded Live Web Dashboard (`:8080`)

---

## 🚀 Quick Start (For Developers)

### Prerequisites
- Docker & Docker Compose installed on your machine.
- Go 1.23+ (if running locally without Docker).

### Option A: Run Full Stack via Docker Compose (Recommended)
1. Clone the repository and copy environment variables:
   ```bash
   git clone https://github.com/your-username/reliable-deposit-processor.git
   cd reliable-deposit-processor
   cp .env.example .env
   ```
2. Spin up the entire stack (PostgreSQL, NATS, and Go Processor):
   ```bash
   docker compose --profile full up --build
   ```
3. Open the **Live Control Dashboard** in your browser:
   👉 **`http://localhost:8080`**

### Option B: Run Locally for Development
1. Start infrastructure services only:
   ```bash
   docker compose up -d postgres nats
   ```
2. Run tests with the Race Detector:
   ```bash
   go test -v -race -count=1 ./...
   ```
3. Run the application locally:
   ```bash
   go run ./cmd/processor
   ```

---

## 🧪 Running Tests & Chaos Engineering

To execute unit tests, concurrency tests (1,000 concurrent deposits), and chaos recovery tests (simulating database/NATS crashes):
```bash
go test -v -race -count=1 ./...
```

---

## 🎛️ Live Web Dashboard Features
By navigating to `http://localhost:8080`, you get:
- **Real-Time TPS & Metrics Cards:** Live counts for received, processed, duplicate, invalid, DLQ, and retried messages.
- **Worker Pool Controller:** Visually inspect worker statuses and click **"Drain"** to gracefully stop individual workers.
- **Live Event & Chaos Injector:** Test valid deposits, duplicates, invalid currencies, or burst traffic with a single click.
- **PostgreSQL Live Wallet Balances:** Real-time table showing user balances across `USDT`, `BTC`, and `IRT`.