# ADR 0001: Decoupled Deposit Ingestion and Periodic PL/pgSQL Batch Settlement

* **Status:** Accepted

* **Date:** 2026-11-14

* **Components:** `services/depositor`, PostgreSQL (`deposits`, `balances`, `ledger_settled_summary`)

## 1. Context and Problem Statement

In our initial synchronous design (Task 3), each incoming deposit message from NATS JetStream was processed inside a single database transaction that performed two operations simultaneously:

1. Inserted the immutable deposit event into the `deposits` table (`ON CONFLICT (event_id) DO NOTHING`).

2. Updated the user's wallet balance in the `balances` table (`INSERT ... ON CONFLICT (user_id, currency) DO UPDATE SET balance = balances.balance + EXCLUDED.balance`).

While straightforward at low throughput, this coupled design creates a severe scalability bottleneck under high concurrency (e.g., $5,000,000$ deposits distributed across $20$ active users and $3$ currencies, i.e., only $60$ distinct `(user_id, currency)` wallet rows):

* **Row-Level Lock Contention:** Every concurrent worker attempting to credit `(user_id = 7, currency = 'USDT')` must acquire a row-level exclusive lock (`FOR NO KEY UPDATE`) on that single row in `balances`. Across $4$ depositor replicas running $32$ workers each, hundreds of transactions queue behind the same $60$ hot wallet rows.

* **WAL & Tuple Bloat Amplification:** Updating the same `balances` row $10,000$ times per second generates $10,000$ dead tuple versions per second in PostgreSQL's MVCC heap, overwhelming `autovacuum` and degrading write latency ($p_{99}$).

* **Per-Message Round-Trip Overhead:** Executing individual transactions per event limits maximum ingestion throughput due to network round-trip latency and `fsync` commits.

## 2. Decision

We decoupled deposit persistence into a **Two-Phase Asynchronous Ledger Architecture** inside PostgreSQL and `services/depositor`:

### Phase A: Micro-Batched Ingestion (`executeBatch`)

* Depositor workers coalesce incoming validated events into micro-batches of up to `256` events (with a $250\ \mu\text{s}$ linger window) and execute a single parameterized `UNNEST` query:

  ```
  INSERT INTO deposits (event_id, user_id, currency, amount, is_settled, created_at)
  SELECT UNNEST($1::text[]), UNNEST($2::int8[])::int4, UNNEST($3::text[]),
         UNNEST($4::text[])::numeric(36, 18), FALSE, UNNEST($5::timestamptz[])
  ON CONFLICT (event_id) DO NOTHING
  RETURNING event_id;
  
  ```

* As soon as the `deposits` row is durably committed with `is_settled = FALSE`, the NATS JetStream message is acknowledged (`msg.Ack()`).

* **Zero locks are acquired on the `balances` table during message ingestion.**

### Phase B: Periodic Atomic Batch Settlement (`settle_pending_deposits`)

* A background `SettlementWorker` in `services/depositor` runs every `SETTLEMENT_INTERVAL` (default $5\text{s}$, plus a final `StopAndFlush` pass on `SIGTERM`) and invokes the PL/pgSQL function `settle_pending_deposits(100000)`.

* Protected by a transaction-level advisory lock (`pg_advisory_xact_lock(778899)`) and a partial index (`WHERE is_settled = FALSE`), the function claims unsettled deposits via physical tuple ID (`ctid`) with `FOR UPDATE SKIP LOCKED`, transitions `is_settled = TRUE`, aggregates total amounts per `(user_id, currency)` sorted by `ORDER BY user_id ASC, currency ASC` (eliminating deadlock risk), and performs **at most one `UPSERT` per `(user_id, currency)` per settlement cycle** into `balances` and `ledger_settled_summary`.

## 3. Consequences

### What We Gained (Benefits)

1. **Order-of-Magnitude Write Throughput (**$10,000+$ **TPS):** Ingestion only appends to `deposits` (`fillfactor = 75`) without touching `balances`. Up to $256$ events commit in a single network round-trip and WAL flush.

2. **Elimination of Hot-Wallet Lock Contention:** Even if User #7 receives $50,000$ deposits within a $5\text{s}$ window, PostgreSQL updates User #7's row in `balances` **only once** during that $5\text{s}$ settlement cycle instead of $50,000$ times.

3. **Preservation of Zero Data Loss & Exactly-Once Financial Effect:**

   * Durability is guaranteed before `msg.Ack()` because the event is committed to `deposits`.

   * Deduplication is still enforced by the `deposits` primary key (`event_id`).

   * Settlement atomicity is guaranteed because `UPDATE deposits SET is_settled = TRUE` and `INSERT INTO balances ... DO UPDATE` execute inside a single CTE statement within a single ACID transaction.

4. **High-Efficiency HOT Updates:** Tuning `fillfactor = 75` on `deposits` and `fillfactor = 70` on `balances` allows both the `is_settled` (`FALSE` to `TRUE`) transition and the periodic balance update to occur as Heap-Only Tuple (HOT) updates on the same $8\text{KB}$ disk page.

### What We Traded Off (Costs & Limitations)

1. **Eventual Consistency of Spendable Wallet Balances (Settlement Lag):** A deposit that has been ingested (`202 Accepted` and committed in `deposits` with `is_settled = FALSE`) is not reflected in the `balances` table until the next `SettlementWorker` cycle completes (up to `SETTLEMENT_INTERVAL` = $5\text{s}$ later).

2. **Operational & Monitoring Complexity:** We must now monitor two separate stages—ingestion (`depositor_messages_total`, `depositor_ingest_batch_duration_seconds`) and settlement (`depositor_settlement_runs_total`, `depositor_settlement_last_success_timestamp_seconds`, and `pg_deposits_unsettled_pending_count`). If the settlement worker stalls, deposits accumulate in `is_settled = FALSE` state even though ingestion continues normally.

3. **Reconciliation Semantics:** Ledger reconciliation queries must compare `balances.balance` against settled deposits (`WHERE is_settled = TRUE`) rather than all rows in `deposits`, while separately tracking pending unsettled rows (`WHERE is_settled = FALSE`).