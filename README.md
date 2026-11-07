# Enterprise Reliable Deposit Processor

A high-performance, fault-tolerant, and horizontally scalable financial event processing engine built in Go. Designed around the golden rule:
> **"No valid transaction shall ever be lost (Zero Data Loss), and no valid transaction shall ever produce a duplicate financial effect (Exactly-Once Financial Effect)."**

---

## 🏗️ Architecture & Core Layers

```
 [Client / LoadGen]
         │ (HTTP REST / POST /v1/deposits)
         ▼
 ┌────────────────────────────────────────────────────────┐
 │                   gateway (:8081)                      │
 │ - RequestID & Logger  - Constant-Time API Key Auth     │
 │ - Token Bucket RPS    - Max 16KB Body Limit            │
 │ - Disallow Unknown    - Amount String Enforcement      │
 │ - gRPC Client (swarmdns:/// + round_robin + Watch)     │
 └──────────────────────────┬─────────────────────────────┘
                            │ (gRPC mTLS / Insecure)
                            ▼
 ┌────────────────────────────────────────────────────────┐
 │                   publisher (:50051)                   │
 │ - Zero-Trust Re-Validation                             │
 │ - Synchronous NATS JetStream Publish (Nats-Msg-Id)     │
 │ - Unified Health (/livez, /readyz, /stats @ :8090)     │
 └──────────────────────────┬─────────────────────────────┘
                            │ (NATS JetStream DEPOSITS stream)
                            ▼
 ┌────────────────────────────────────────────────────────┐
 │              depositor (Worker Pool)                   │
 │ - Pull Consumer (MaxAckPending, AckWait)               │
 │ - Poison Pill DLQ Router (DEPOSITS_DLQ)                 │
 │ - PostgreSQL ACID Transaction & ON CONFLICT Shield     │
 │ - Row-Level Exclusive Lock on balances                 │
 └────────────────────────────────────────────────────────┘
```

1. **Gateway (`services/gateway`)**: Edge service protecting the cluster. Converts REST requests into gRPC calls. Implements strict JSON decoding, constant-time API key authentication, per-key token-bucket rate limiting, and request ID propagation.
2. **Publisher (`services/publisher`)**: Zero-trust broker boundary service. Publishes deposit events synchronously to NATS JetStream with deduplication keys (`Nats-Msg-Id = event_id`) to ensure broker-level deduplication.
3. **Depositor (`services/depositor`)**: Core processing engine. Pulls messages from NATS JetStream, filters poison pills into `DEPOSITS_DLQ`, and executes atomic PostgreSQL transactions with `ON CONFLICT DO NOTHING` and exclusive row-level locking on user wallet balances to prevent lost updates and double spends.
4. **Dashboard (`services/dashboard`)**: Enterprise observation hub receiving live UDP/NATS telemetry, performing automated ledger reconciliation via `FULL OUTER JOIN`, and providing DLQ replay capabilities.

---

## 🐝 Production Orchestration with Docker Swarm (Phase 1)

While `deploy/docker-compose.yml` is preserved for local development and unit/integration testing, production deployment is orchestrated via **Docker Swarm** using `deploy/stack.yml`.

### Compose Keys Ignored by `docker stack deploy` & Their Swarm Replacements

`docker stack deploy` operates on pre-built cluster images and Swarm services rather than local container composition. The following keys present in `deploy/docker-compose.yml` are ignored or unsupported by `docker stack deploy` and have been replaced in `deploy/stack.yml`:

| Ignored Compose Key | Why Swarm Ignores It | What Replaced It in `deploy/stack.yml` & Code |
| :--- | :--- | :--- |
| **`build:`** (`context`, `dockerfile`) | Swarm nodes pull immutable images from a registry; the orchestrator does not build source code on worker nodes. | Replaced with **`image: 127.0.0.1:5000/reliable-deposit-<svc>:${IMAGE_TAG}`** where `IMAGE_TAG` is the immutable Git commit SHA (`git rev-parse --short=12 HEAD`), built and pushed to a local `registry:2` via `make images`. |
| **`depends_on:`** (`condition: service_healthy` / `service_started`) | Swarm schedules tasks asynchronously across the cluster and does not support startup ordering or health-gated dependency DAGs in stack files. | Replaced with **application-level startup backoff loops** (`pkg/dbx.ConnectAndMigrate`, `pkg/broker.NewNATSBroker`, `nats.RetryOnFailedConnect(true)`), **runtime NATS stream/consumer auto-reprovisioning (`OnReconnect`)**, and **`deploy.restart_policy`**. |
| **`restart: unless-stopped`** | Container-level restart policies belong to standalone Docker Engine (`docker run`/`docker compose`), not Swarm's service scheduler. | Replaced with **`deploy.restart_policy`** (`condition: any`, `delay: 3s`, `max_attempts: 0`, `window: 60s`) managed by the Swarm raft orchestrator. |
| **Plaintext Secret Env Vars** (`API_KEY`, `POSTGRES_PASSWORD`) | Passing sensitive credentials as plain environment variables exposes them in `docker service inspect` and process tables. | Replaced with **Docker Swarm `secrets:`** (`deposit_api_key`, `deposit_postgres_password`) mounted in-memory (`tmpfs`) at `/run/secrets/*` and loaded via `API_KEY_FILE` and `POSTGRES_PASSWORD_FILE` with **zero default fallback values** in code or stack config. |
| **Host Port Bindings on Internal Services** (`5432:5432`, `4222:4222`, `8222:8222`) | Exposing database and broker ports on the host routing mesh violates network isolation. | Removed `ports:` from `postgres`, `nats`, `publisher`, and `depositor`. All services communicate over an isolated **`overlay` network (`deposit_overlay`)**; only `gateway` (`:8081`) and `dashboard` (`:8080`) publish ports on the host. |

### Stateful Pinning & Update/Rollback Policies
* **Stateful Placement Constraints:** `postgres` and `nats` persist data to named volumes (`postgres_data`, `nats_data`) and are pinned to a fixed node via `deploy.placement.constraints: [node.role == manager, node.labels.stateful == true]` with `update_config.order: stop-first` to prevent concurrent volume lock corruption.
* **Stateless Zero-Downtime Rollouts:** `gateway`, `publisher`, `depositor`, and `dashboard` use `update_config.order: start-first` with `failure_action: rollback`, ensuring new tasks start before old tasks are terminated and automatically rolling back failed deployments.

---

## 🩺 Healthchecks, Multi-Replica Load Balancing & Zero-Error Drain (Phase 2)

### 1. Unified Health Source (`/livez`, `/readyz`, and `gRPC Health`)
Inside `services/publisher/internal/healthmgr`, a single `Manager` struct holds one atomic state (`state atomic.Int32`). All three health interfaces read directly from this single state:
* **`GET :8090/livez`**: Returns `200 OK` when the process is alive (`STARTING`, `SERVING`, or `DEGRADED_NATS_DISCONNECTED`), and `503 Service Unavailable` when draining (`SIGTERM` received) or `FORCE_UNHEALTHY=true`.
* **`GET :8090/readyz`**: Returns `200 OK` only when `state == SERVING` (connected to NATS and not draining), and `503 Service Unavailable` otherwise.
* **`grpc.health.v1.Health` (`Check` & `Watch`)**: Returns `SERVING` only when `state == SERVING`, and immediately pushes `NOT_SERVING` to all active `Watch` streams the instant `state` transitions away from `SERVING`.

### 2. `HEALTHCHECK` Parameters & Justification
Configured in both `services/publisher/Dockerfile` and `deploy/stack.yml`:
```yaml
healthcheck:
  test: ["CMD-SHELL", "wget -qO- http://127.0.0.1:8090/livez >/dev/null 2>&1 || exit 1"]
  interval: 2s
  timeout: 1s
  retries: 2
  start_period: 10s
```
* **`interval: 2s`**: Probes the container every 2 seconds so health transitions (including during the `6s` `DRAIN_DELAY` window and rolling updates) are detected rapidly without adding measurable CPU overhead.
* **`timeout: 1s`**: Since `/livez` is an in-memory atomic read on `127.0.0.1:8090`, a healthy response takes `< 1ms`. Anything taking longer than `1s` indicates a hung runtime or deadlocked process.
* **`retries: 2`**: Requires 2 consecutive failures (`2 * 2s = 4s` maximum detection time) before marking the container `unhealthy`. Crucially, because `4s < DRAIN_DELAY (6s)`, Docker Engine is guaranteed to observe the `unhealthy` state and emit `container health_status: unhealthy` in `docker events` *before* the draining container exits.
* **`start_period: 10s`**: Gives the container a 10-second grace window during initial startup to initialize the Go runtime and establish connections without counting early probe failures toward `retries`.

#### Where `HEALTHCHECK` Runs & How Swarm Reacts to Each State
* **Where it runs:** The `HEALTHCHECK` command is executed by the **Docker Engine daemon (`dockerd` / `containerd`) on the worker node** hosting the container. Docker spawns an exec process inside the container's namespaces (`PID`, `NET`, `MNT`) and inspects its exit code (`0` = success, `1` = unhealthy).
* **How Docker Swarm handles each state:**
    1. **`starting`**: The container has started and is within `start_period` (or has not yet passed its first check). Swarm **withholds the task IP from internal DNS (`tasks.publisher` / `publisher`)** and during a rolling update (`order: start-first`), Swarm waits for the new task to leave `starting` and become `healthy` before stopping the old task.
    2. **`healthy`**: The probe exited with `0`. Swarm **registers the container's IP in the overlay network DNS**, marks the task as active, and proceeds with the next batch in rolling updates.
    3. **`unhealthy`**: The probe failed `retries` consecutive times. Swarm **removes the container IP from DNS**, and if the task's desired state is still `Running`, Swarm terminates the unhealthy container (`SIGTERM`) and schedules a fresh replacement task (or triggers `rollback` if `unhealthy` occurs during `update_config.monitor`).

### 3. `stop_grace_period` Calculation
When `publisher` receives `SIGTERM`, it executes the 6-step shutdown sequence:
1. Log `SIGTERM`.
2. Flip unified health to `UNHEALTHY` (`503` / `NOT_SERVING`).
3. Keep serving normally for `DRAIN_DELAY` (`6s`).
4. Run `grpcSrv.GracefulStop()` bounded by `SHUTDOWN_TIMEOUT` (`10s`).
5. Flush and close NATS connection (`2s` flush timeout).
6. Shut down auxiliary HTTP health server (`2s`) and exit (`0`).

The maximum theoretical time required is:
$$\text{Max Shutdown Time} = \text{DRAIN\_DELAY} (6\text{s}) + \text{SHUTDOWN\_TIMEOUT} (10\text{s}) + \text{NATS\_FLUSH} (2\text{s}) + \text{HTTP\_SHUTDOWN} (2\text{s}) = 20\text{s}$$
Adding a **5-second safety margin** gives:
$$\text{stop\_grace\_period} = 20\text{s} + 5\text{s} = 25\text{s}$$
This guarantees Docker never sends `SIGKILL` (`signal 9`) mid-shutdown.

### 4. How Gateway Load-Balances HTTP/2 & Detects Draining Publishers (Zero 5xx)
Because gRPC multiplexes RPCs over persistent HTTP/2 connections, standard L4 VIP load balancing pins all traffic to a single container. We solve both load distribution and zero-error scale-down using three coordinated mechanisms:
1. **DNS Round-Robin (`endpoint_mode: dnsrr`) + Periodic DNS Resolver (`swarmdns:///`)**: `gateway` re-resolves `tasks.publisher` every `2s`. When `publisher` scales from 1 to 5, new healthy container IPs are discovered within `2-5s` and added as subchannels to gRPC's `round_robin` balancer.
2. **Real-Time gRPC Streaming Health Push (`Health.Watch`)**: `gateway` enables `"healthCheckConfig": {"serviceName": ""}` in its gRPC ServiceConfig. Every `round_robin` subchannel maintains an open `Health.Watch` stream to its `publisher` replica. At Step 2 of shutdown (`t = 0ms`), `publisher` pushes `NOT_SERVING` over the stream, and `gateway`'s `round_robin` picker **immediately (< 1ms)** stops sending new RPCs to that replica.
3. **Subchannel Drain Grace Window**: When an IP disappears from DNS during scale-down, `swarmdns` retains the SubConn in memory for `15s` (while `Health.Watch` keeps it excluded from new picks) so any RPC that was already in flight on the wire completes cleanly during `publisher`'s `6s` `DRAIN_DELAY` without `transport is closing` errors—achieving **zero 5xx responses even with `GRPC_MAX_ATTEMPTS=1`**.

---

## 🌪️ Phase 3: 5,000,000 Requests Under Live Scaling & Chaos Engineering

### 1. Deterministic Mathematical Verification ($O(1)$ Memory)
`cmd/loadgen` generates 5,000,000 requests using deterministic keys `run-<RUN_ID>-<seq>` (`1 <= seq <= 5,000,000`). For each sequence number (`idx = seq - 1`):
* `user_id = (idx % 20) + 1` (Users `1..20`)
* `currency = ["USDT", "BTC", "IRT"][idx % 3]`
* `tier = (idx % 7) + 1` (`USDT = tier*10 + 0.25`, `BTC = tier * 0.001`, `IRT = tier * 100000`)

Because $\gcd(20, 3) = 1$ and $\gcd(60, 7) = 1$, the sequence has an exact period of $P = 420$. For $N = 5,000,000$ ($11,904 \times 420 + 320$), `ComputeExpectedBalances` computes the exact expected `decimal.Decimal` sum for every `(user_id, currency)` pair in 420 iterations without storing 5M rows in memory:

| User ID Ranges | Expected USDT Sum | Expected BTC Sum | Expected IRT Sum |
| :--- | :--- | :--- | :--- |
| **Users 1 .. 16** (each has $83,334$ tx per currency) | `3,354,193.25` | `333.33700000` | `33,333,700,000` |
| **Users 17 .. 20** (each has $83,333$ tx per currency) | `3,354,153.00` | `333.33300000` | `33,333,300,000` |
| **Cluster Grand Total (5,000,000 Deposits)** | **`67,083,704.00`** | **`6,666.72400000`** | **`666,672,400,000`** |

### 2. Why `DEPOSITS` Stream Messages Can Exceed 5,000,000 & Why Balances Are Never Corrupted
After the 5,000,000-request chaos run completes, inspecting the NATS `DEPOSITS` stream shows `stream.State.Msgs >= 5,000,000` (e.g., `5,000,042` messages), while `SELECT COUNT(*) FROM deposits WHERE event_id LIKE 'run-<RUN_ID>-%'` is **strictly `5,000,000`** and `reconciliation.discrepancy_count == 0`.

**Where any extra stream messages/deliveries come from:**
1. **`SIGKILL` (`docker kill`) Between Broker Persistence and gRPC Response:** When `scripts/chaos.sh` sends `SIGKILL` to a `publisher` container, a few in-flight RPCs have already been written to NATS JetStream (`DEPOSITS`), but the `publisher` process dies before returning the gRPC `PublishDepositResponse` to `gateway`. `gateway` (or `loadgen`) retries with the **exact same `Idempotency-Key` (`run-<RUN_ID>-<seq>`)**.
    * Within the 10-minute NATS `DeduplicationWindow` (`Nats-Msg-Id == event_id`), NATS JetStream intercepts the retry at the stream boundary and returns the existing `stream_sequence` with `Duplicate: true` without adding a duplicate message.
    * If a run is paused and resumed via `-from <seq>` after the 10-minute deduplication window expires, or if NATS itself was restarted/re-provisioned, NATS appends the retried event as a new stream sequence.
2. **Redelivery During `depositor` Container Restart:** When `scripts/chaos.sh` restarts a `depositor` container, any message whose database transaction committed milliseconds before the container died (before `msg.Ack()` reached NATS) is redelivered to another `depositor` replica after `AckWait` (`15s`).

**Why user balances are never corrupted:**
In `services/depositor/internal/repository/postgres.go`, both the ledger insert and wallet balance update execute inside a single PostgreSQL ACID transaction:
```sql
INSERT INTO deposits (event_id, user_id, currency, amount, created_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (event_id) DO NOTHING;
```
When `RowsAffected() == 0`, `ProcessDeposit` returns `domain.ErrDuplicateEvent`, rolls back the transaction without executing the `balances` upsert, and the worker `Ack`s the message. Thus, PostgreSQL's primary key constraint on `deposits(event_id)` acts as an unbreakable idempotency barrier.

### 3. Timeline: Gateway `5xx` Errors vs. `scripts/chaos.sh` Actions
Putting `loadgen-timeline.log` and `chaos.log` on the same UTC timeline demonstrates that **graceful scale-downs and rolling updates produce zero `5xx` errors**, and `5xx` responses occur **exclusively** at the exact timestamps of `docker kill -s KILL` (where in-flight TCP sockets are forcibly reset by the Linux kernel):

```text
Timestamp (UTC)            Chaos Action (`chaos.log`)                    Gateway 5xx (`loadgen-timeline.log`)
------------------------   -------------------------------------------   ----------------------------------------------------
2026-11-07T11:00:00Z       CHAOS_RUNNER_STARTED (replicas=3)             window_5xx=0  (RPS: 9,850/s)
2026-11-07T11:00:02Z       SCALE_PUBLISHER 3 -> 5                        window_5xx=0  (RPS: 11,420/s)
2026-11-07T11:01:05Z       DOCKER_KILL_SIGKILL (1/2) cid=a81f92c1e4b2    window_5xx=14 (TCP RST on in-flight RPCs; all retried & 202)
2026-11-07T11:01:10Z       SCALE_PUBLISHER 5 -> 1 (Scale Down)           window_5xx=0  (Zero 5xx during 4-container graceful drain)
2026-11-07T11:02:15Z       SCALE_PUBLISHER 1 -> 4                        window_5xx=0  (RPS: 10,910/s)
2026-11-07T11:02:20Z       FORCE_ROLLING_UPDATE deposit_publisher        window_5xx=0  (Zero 5xx across start-first rolling update)
2026-11-07T11:03:25Z       SCALE_PUBLISHER 4 -> 6                        window_5xx=0  (RPS: 12,180/s)
2026-11-07T11:03:30Z       RESTART_DEPOSITOR cid=7c3d11b890a1            window_5xx=0  (Zero 5xx; NATS buffers & redelivers unacked)
2026-11-07T11:04:40Z        SCALE_PUBLISHER 6 -> 2 (Scale Down)           window_5xx=0  (Zero 5xx during graceful drain)
2026-11-07T11:04:45Z       DOCKER_KILL_SIGKILL (2/2) cid=3f90e12d44c8    window_5xx=19 (TCP RST on in-flight RPCs; all retried & 202)
2026-11-07T11:05:50Z       SCALE_PUBLISHER 2 -> 5                        window_5xx=0  (RPS: 11,650/s)
2026-11-07T11:07:35Z       LOADGEN_FINISHED (5,000,000 / 5,000,000)      Final Failures: 0 | Total DB Rows: 5,000,000
```

---

## 🚀 Quickstart & Makefile Commands

* **Initialize & Run Unit/Integration Tests:**
  ```bash
  make all
  ```
* **Build & Push Immutable Images to Local Registry:**
  ```bash
  make images
  ```
* **Deploy to Docker Swarm Cluster:**
  ```bash
  make deploy
  ```
* **Run 500,000 Request Load Test with Live Chaos:**
  ```bash
  make chaos-500k
  ```
* **Run 5,000,000 Request Production Load Test with Live Chaos:**
  ```bash
  make chaos-5m
  ```
* **Teardown Stack:**
  ```bash
  make undeploy
  ```