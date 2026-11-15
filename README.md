# Reliable Deposit Processor

A cloud-native deposit processing platform with asynchronous ingestion, durable messaging, and atomic batch settlement.

Built with **Go**, **Gin**, **gRPC**, **NATS JetStream**, **PostgreSQL 18**, **Prometheus**, and **Docker Swarm**, the platform separates request acceptance from ledger settlement to support high-throughput workloads while reducing contention on frequently updated balances.

The repository uses a multi-module **Go Workspace (`go.work`)**, with shared infrastructure packages and independently deployable services.

> **Processing model:** An HTTP `202 Accepted` response confirms acknowledgment by NATS JetStream. PostgreSQL ingestion and balance settlement happen asynchronously.
>
> **Consistency model:** Message delivery is at least once. PostgreSQL event uniqueness and atomic settlement protect against repeated balance updates for the same persisted event.

## Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Repository Structure](#repository-structure)
- [Processing and Consistency](#processing-and-consistency)
- [REST API](#rest-api)
- [Security](#security)
- [Deployment and Operations](#deployment-and-operations)
- [Observability](#observability)
- [Performance and Resilience Testing](#performance-and-resilience-testing)
- [Documentation](#documentation)

## Overview

Reliable Deposit Processor handles the deposit lifecycle from HTTP ingestion to balance updates and ledger reconciliation.

Key capabilities include:

- **Asynchronous processing:** Requests are validated and published to JetStream before downstream database processing.
- **Decimal-safe amounts:** Shared validation uses `shopspring/decimal`, with monetary values stored as PostgreSQL `NUMERIC(36, 18)`.
- **Layered idempotency:** Gateway payload checks, JetStream deduplication, and PostgreSQL uniqueness address retries at different stages.
- **Two-phase persistence:** Micro-batched ingestion is separated from periodic atomic balance settlement.
- **Failure recovery:** Durable consumers, delayed retries, and dead-letter handling support recovery from processing failures.
- **Service discovery:** A custom Swarm DNS resolver and gRPC round-robin balancing distribute requests across Publisher replicas.
- **Coordinated shutdown:** Health transitions, drain windows, and bounded shutdown timeouts reduce disruption during deployments.
- **Operational visibility:** Prometheus metrics, structured logs, reconciliation, and a monitoring dashboard expose pipeline health.

## Architecture

```text
                         External Clients / Load Generator
                                         |
                           POST /v1/deposits
                       X-API-Key + Idempotency-Key
                                         |
                                         v
                              +---------------------+
                              |       Gateway       |
                              | HTTP :8081          |
                              | Metrics :9090       |
                              |                     |
                              | Authentication      |
                              | Validation          |
                              | Rate limiting       |
                              | Payload guard       |
                              +----------+----------+
                                         |
                              gRPC / optional mTLS
                                         |
                                         v
                              +---------------------+
                              |      Publisher      |
                              | gRPC :50051         |
                              | Health :8090        |
                              | Metrics :9090       |
                              |                     |
                              | Health management   |
                              | JetStream publishing|
                              +----------+----------+
                                         |
                                  JetStream PubAck
                                         |
                                         v
                              +---------------------+
                              |   NATS JetStream    |
                              | Client :4222        |
                              |                     |
                              | DEPOSITS            |
                              | DEPOSITS_DLQ        |
                              +----------+----------+
                                         |
                                Durable pull consumer
                                         |
                                         v
                              +---------------------+
                              |      Depositor      |
                              | Metrics :9090       |
                              |                     |
                              | Worker pool         |
                              | Batched ingestion   |
                              | Periodic settlement |
                              | Retry / DLQ handling|
                              +----------+----------+
                                         |
                                         v
                              +---------------------+
                              |    PostgreSQL 18    |
                              | :5432               |
                              |                     |
                              | deposits            |
                              | balances            |
                              | ledger_settled_     |
                              | summary             |
                              +---------------------+
```

The Dashboard provides cluster telemetry and DLQ replay controls. Prometheus collects application metrics and infrastructure metrics from the NATS and PostgreSQL exporters.

### Deposit Lifecycle

1. **Validate:** Gateway authenticates the client, applies rate limits, validates the JSON payload, and checks idempotency-key reuse.
2. **Publish:** Publisher sends the event to JetStream using `Nats-Msg-Id = event_id`.
3. **Accept:** Gateway returns `202 Accepted` after the publish acknowledgment.
4. **Persist:** Depositor inserts events into PostgreSQL in micro-batches and acknowledges their NATS messages after the database commit.
5. **Settle:** A periodic worker atomically updates balances, marks deposits as settled, and updates the reconciliation summary.
6. **Recover:** Transient failures are retried; messages requiring isolation are routed to the dead-letter stream.

The ingestion and settlement design is documented in [ADR 0001: Decoupled Ingestion and Periodic Batch Settlement](docs/adr/0001-decoupled-ingestion-and-periodic-batch-settlement.md).

## Repository Structure

The workspace contains one shared module and four service modules.

```text
.
├── go.work
├── pkg/
│   ├── broker/
│   ├── config/
│   ├── dbx/
│   ├── domain/
│   ├── logger/
│   ├── metricsx/
│   ├── pb/deposit/v1/
│   └── tlscfg/
├── services/
│   ├── gateway/
│   ├── publisher/
│   ├── depositor/
│   └── dashboard/
├── deploy/
│   └── stack.yml
└── docs/
    ├── adr/
    │   └── 0001-decoupled-ingestion-and-periodic-batch-settlement.md
    └── metrics.md
```

### Shared Packages

| Package | Responsibility |
| --- | --- |
| `pkg/broker` | NATS connectivity, JetStream stream provisioning, and reconnection handling. |
| `pkg/config` | Environment parsing, `.env` loading, and supported `*_FILE` secret resolution. |
| `pkg/dbx` | PostgreSQL connection pooling through `pgx/v5` and migration execution through `golang-migrate`. |
| `pkg/domain` | Shared validation for event IDs, user IDs, currencies, and decimal amounts. |
| `pkg/logger` | Consistent structured JSON logging with Zap and ISO-8601 timestamps. |
| `pkg/metricsx` | Per-service Prometheus registries, common collectors, and dedicated metrics servers. |
| `pkg/pb/deposit/v1` | Shared versioned Protobuf contract and generated gRPC bindings. |
| `pkg/tlscfg` | Shared TLS configuration and certificate loading for Gateway and Publisher. |

### Service Boundaries

Service-specific implementation details remain under each module's `internal/` directory.

| Service | Internal Responsibilities |
| --- | --- |
| `gateway` | HTTP handlers, Swarm DNS discovery, per-key rate limiting, and Gateway metrics. |
| `publisher` | gRPC endpoints, health management, event generation, and Publisher metrics. |
| `depositor` | Persistence, consumer workers, batch ingestion, settlement, and Depositor metrics. |
| `dashboard` | Telemetry aggregation, DLQ replay handlers, authentication, CSRF protection, and Dashboard metrics. |

Shared packages contain reusable contracts and infrastructure. Service-local behavior stays internal to avoid unnecessary coupling.

## Processing and Consistency

### Decimal Handling

Amounts are parsed and validated with `shopspring/decimal` and stored as `NUMERIC(36, 18)`.

This avoids binary floating-point arithmetic for monetary values while enforcing the database's configured precision and scale.

### Layered Idempotency

| Layer | Mechanism | Scope |
| --- | --- | --- |
| Gateway | Sharded payload fingerprints for `Idempotency-Key`. | Detects conflicting payloads within the guard's tracked state. |
| JetStream | Deterministic `Nats-Msg-Id` derived from `event_id`. | Suppresses duplicate publications within the configured 10-minute deduplication window. |
| PostgreSQL | Primary key on `deposits.event_id` and `ON CONFLICT DO NOTHING`. | Prevents duplicate event rows while the original record is retained. |
| Settlement | Atomic processing of rows with `is_settled = FALSE`. | Prevents a persisted deposit from being applied repeatedly to balances. |

The Gateway's in-memory guard is not a substitute for durable, cross-replica idempotency. PostgreSQL is the final persistence boundary for event uniqueness.

Clients must reuse the same idempotency key and payload when retrying an uncertain request. A new logical deposit must use a new identity.

### Phase A: Micro-Batched Ingestion

Depositor groups incoming events before writing to PostgreSQL:

- Four concurrent batch flushers.
- Up to 256 events per batch.
- A 250 µs batching linger window.
- Bulk insertion through SQL `UNNEST`.
- Duplicate handling through `ON CONFLICT (event_id) DO NOTHING`.
- New deposits stored with `is_settled = FALSE`.

NATS messages are acknowledged only after the ingestion transaction commits.

If Depositor exits after commit but before acknowledgment, JetStream may redeliver the message. The database uniqueness constraint prevents a second row from being inserted for that event.

### Phase B: Atomic Batch Settlement

`SettlementWorker` runs every five seconds and performs a final settlement attempt during graceful shutdown.

Each invocation calls:

```sql
SELECT settle_pending_deposits(100000);
```

The function processes pending deposits in one transaction:

1. Claims unsettled rows using `FOR UPDATE SKIP LOCKED`.
2. Aggregates amounts by `(user_id, currency)`.
3. Applies balance updates in a consistent key order.
4. Marks the selected deposits as settled.
5. Updates `ledger_settled_summary`.

Because these changes share a transaction, a failed settlement does not leave a committed balance update without its corresponding settlement state.

### Consistency Boundaries

The pipeline has three distinct completion points:

| State | Meaning |
| --- | --- |
| Accepted | JetStream has acknowledged the event. |
| Persisted | The deposit is committed to PostgreSQL. |
| Settled | The deposit has been applied to the balance and settlement summary. |

Balances are eventually consistent with accepted requests. The five-second settlement interval is a scheduling interval, not an upper bound on settlement latency; backlog, database load, and infrastructure failures can increase the delay.

JetStream acknowledgments provide the guarantees of the configured broker deployment. Durability and availability still depend on stream replication, storage configuration, and infrastructure.

### Dead-Letter Handling

The `DEPOSITS_DLQ` stream isolates messages that cannot complete normal processing. Dashboard replay controls allow operators to reintroduce eligible events after investigating the underlying failure.

Replay may create another stream entry, but retaining the original `event_id` preserves database-level deduplication.

## REST API

### Submit a Deposit

```http
POST /v1/deposits
Content-Type: application/json
X-API-Key: <api-key>
Idempotency-Key: <stable-request-key>
```

The Gateway enforces strict JSON validation and a maximum request body size of 16 KB.

### Acceptance Semantics

A successful submission returns:

```http
202 Accepted
```

This response confirms that JetStream has acknowledged the event. It does not confirm PostgreSQL ingestion or balance settlement.

`202 Accepted` reflects the asynchronous API contract without implying that downstream ledger processing is complete.

### Response and Error Mapping

| Condition | gRPC Status | HTTP Status | Error Code |
| --- | --- | --- | --- |
| Publish acknowledged, including broker-detected duplicates | `OK` | `202 Accepted` | — |
| Invalid event ID, user ID, currency, or amount | `InvalidArgument` | `400 Bad Request` | `VALIDATION_ERROR` |
| Idempotency key reused with a different payload | `AlreadyExists` | `409 Conflict` | `IDEMPOTENCY_KEY_CONFLICT` |
| Rate limit or upstream resource limit reached | `ResourceExhausted` | `429 Too Many Requests` | `RATE_LIMITED` |
| Upstream unavailable or canceled | `Unavailable` / `Canceled` | `503 Service Unavailable` | `SERVICE_UNAVAILABLE` |
| Upstream deadline exceeded | `DeadlineExceeded` | `504 Gateway Timeout` | `GATEWAY_TIMEOUT` |
| Other upstream failure | `Internal` / other | `502 Bad Gateway` | `UPSTREAM_ERROR` |

Some validation, conflict, and rate-limit responses originate directly in Gateway without an upstream RPC.

### Client Retry Guidance

- Do not retry successful `202` responses merely because settlement is asynchronous.
- Correct validation errors before retrying.
- Resolve `409 Conflict` responses rather than changing the payload under an existing key.
- Use backoff for `429` and transient `5xx` responses.
- Preserve the original idempotency key and payload when retrying.
- Treat timeouts as ambiguous outcomes: the event may have been accepted even if the response was not received.

## Security

### API Authentication

Gateway hashes API keys with SHA-256 and compares the resulting values using `subtle.ConstantTimeCompare`. Secret API keys are not logged.

### Rate Limiting

Per-key rate limiting uses a sharded lookup structure and atomic GCRA state to enforce configured request rates and burst limits.

### Service-to-Service Transport

Gateway-to-Publisher gRPC connections support mutual TLS with TLS 1.3.

Production deployments should enable mTLS and manage certificate issuance, trust, and rotation explicitly. Optional TLS support should not be interpreted as transport encryption being enabled in every environment.

### Secrets and Internal Endpoints

- Shared configuration supports Docker Swarm secret files through supported `*_FILE` settings.
- Application metrics use dedicated internal listeners.
- The public Gateway listener does not expose `/metrics`.
- Dashboard replay operations are protected by authentication and CSRF controls.

## Deployment and Operations

Docker Swarm deployment configuration is maintained in [`deploy/stack.yml`](deploy/stack.yml).

### Image Workflow

Swarm deploys prebuilt images rather than building application source on cluster nodes.

The provided workflow uses:

- `make images` to build and publish images.
- `IMAGE_TAG` for Git SHA-based image versioning.
- A local registry at `127.0.0.1:5000` in the local deployment setup.

For multi-node deployments, configure a registry endpoint that every node can reach; do not assume the local loopback address refers to the same registry on all hosts.

### Compose and Swarm Differences

| Compose Setting | Swarm Approach |
| --- | --- |
| `build` | Build and publish images before deploying the stack. |
| `depends_on` | Use startup retries and dependency reconnection rather than startup ordering. |
| `restart` | Configure `deploy.restart_policy`. |
| `container_name` | Use service discovery and dynamically assigned task identities. |

Services use retry and reconnection logic to tolerate dependencies starting later or becoming temporarily unavailable.

### Publisher Discovery

Gateway uses a custom `swarmdns:///publisher:50051` resolver that:

- Polls `tasks.publisher` every two seconds.
- Retains the last known endpoint set during transient DNS failures.
- Updates `gateway_publisher_endpoints`.
- Distributes RPCs through a four-connection gRPC client pool using `round_robin`.

DNS discovery identifies candidate endpoints; gRPC health checks help determine whether those endpoints should receive traffic.

### Health Model

Publisher exposes a shared health state through:

- HTTP `/livez`
- HTTP `/readyz`
- gRPC Health `Check` and `Watch`
- `/stats`
- Prometheus health gauges

| Publisher State | `/livez` | `/readyz` | gRPC Health |
| --- | --- | --- | --- |
| Healthy | `200` | `200` | `SERVING` |
| NATS disconnected | `200` | `503` | `NOT_SERVING` |
| Draining | `503` | `503` | `NOT_SERVING` |

Keeping liveness successful during a NATS outage avoids restarting otherwise healthy Publisher processes solely because a dependency is unavailable.

### Container Health Checks

The stack configures Publisher health checks against:

```text
http://127.0.0.1:8090/livez
```

| Setting | Stack Value |
| --- | --- |
| Interval | `2s` |
| Timeout | `1s` |
| Retries | `2` |
| Start period | `5s` |

The Publisher Dockerfile defines a `10s` start period; the stack supplies its deployment-specific override.

Health probes run inside the container and are executed by the Docker Engine on the task's host. Detection, task replacement, and DNS convergence are asynchronous, so probe intervals should not be treated as exact traffic-removal deadlines.

### Graceful Shutdown

Publisher follows an ordered shutdown sequence:

1. Record the shutdown signal and configured timeouts.
2. Mark the service as draining and broadcast `NOT_SERVING`.
3. Continue serving arriving RPCs during `DRAIN_DELAY`.
4. Close health watch streams and begin bounded gRPC graceful shutdown.
5. Flush and close the NATS connection.
6. Stop auxiliary HTTP listeners and close the metrics server last.

The drain window allows health-aware clients and DNS discovery to react before request handling stops.

| Service | Shutdown Settings | `stop_grace_period` |
| --- | --- | --- |
| Publisher | `6s` drain delay, `10s` gRPC shutdown timeout, plus teardown allowance | `25s` |
| Depositor | `15s` shutdown timeout and `10s` final settlement allowance | `25s` |
| Gateway | `10s` shutdown timeout plus buffer | `15s` |

These values are operational budgets, not guarantees against forced termination. In particular, the Depositor budget leaves no additional margin if both configured allowances are fully consumed.

Review shutdown timing under realistic backlog and infrastructure conditions before production rollout.

### Behavior During a NATS Outage

When Publisher detects a disconnected NATS connection:

- `/livez` remains successful.
- `/readyz` returns `503`.
- gRPC Health reports `NOT_SERVING`.
- `publisher_nats_connected` and `publisher_ready` become `0`.
- New deposits cannot be accepted until a usable publishing path is restored.

When NATS becomes available, the client reconnects, stream configuration is rechecked, and readiness is restored.

Recovery time depends on connection detection, reconnection timing, and broker readiness.

## Observability

See the [Metrics Catalog](docs/metrics.md) for metric definitions, labels, units, histogram buckets, and PromQL examples.

### Application Metrics

Each service creates an isolated `prometheus.Registry` through `pkg/metricsx`.

Common collectors include:

- Go runtime metrics.
- Process metrics.
- Database pool statistics where applicable.
- `<service>_build_info{version, revision, go_version}`.

Application metrics are served on a dedicated internal listener:

```text
:9090/metrics
```

In the provided stack, application metrics are reachable through `deposit_overlay` and are not published as host ports.

### Scrape Design

The metrics path avoids database queries and NATS RPCs during scraping.

External state is collected asynchronously or represented by in-memory counters and gauges. Auxiliary `/stats` endpoints read the same metric objects to avoid maintaining competing sources of operational counts.

Metrics servers shut down after request handlers, workers, and dependencies so that shutdown state remains observable for as long as possible.

### Operational Signals

Useful signals include:

| Signal | Purpose |
| --- | --- |
| `gateway_deposit_requests_total{result}` | Accepted, duplicate, rejected, rate-limited, and failed requests. |
| `gateway_publisher_endpoints` | Publisher endpoints known to Gateway discovery. |
| `publisher_ready` | Publisher readiness. |
| `publisher_draining` | Publisher shutdown state. |
| `publisher_nats_connected` | Publisher broker connectivity. |
| `depositor_nats_connected` | Depositor broker connectivity. |
| `depositor_workers_active` | Active processing workers. |

NATS and PostgreSQL exporters provide additional broker and database telemetry.

Monitoring should distinguish ingress availability from downstream progress. A healthy acceptance rate does not by itself demonstrate that the settlement backlog is draining.

## Performance and Resilience Testing

The project reports a five-million-request load test with concurrent failure injection.

The test scenario included:

- Publisher scale-up and scale-down.
- Abrupt Publisher termination with `SIGKILL`.
- Rolling Publisher updates.
- Depositor restarts.
- Continued request submission during recovery.

### Reported Results

| Measure | Reported Result |
| --- | --- |
| Logical deposit requests | `5,000,000` |
| Throughput in listed observation windows | Approximately `10,980–14,820 req/s` |
| Final request failures | `0` |
| Gateway `5xx` in listed windows | `0`, with retries enabled where noted |
| Final ledger discrepancy | `0` |

These results describe a specific test run, not a service-level guarantee. The listed throughput values are observation-window measurements rather than an independently established full-run average.

For reproducible comparisons, retain the associated `chaos.log` and `loadgen-timeline.log` alongside:

- Git revision and image tags.
- Hardware and cluster topology.
- Service replica counts and resource limits.
- Load-generator concurrency and retry configuration.
- JetStream storage and replication settings.
- PostgreSQL configuration.
- Request latency percentiles.
- Settlement backlog and drain time.
- Final database and reconciliation checks.

### Interpreting JetStream Counts

JetStream sequence numbers, retained message counts, and unique deposit counts measure different things.

A stream's last sequence number is not its current retained message count. Neither necessarily equals the number of unique logical deposits.

Additional stream entries can result from:

- Earlier tests sharing the same stream.
- DLQ replay.
- Republishing after the deduplication window expires.
- Replay-specific message IDs.

Validate a completed run against its expected unique event IDs in PostgreSQL, confirm that no run-specific deposits remain unsettled, and verify ledger reconciliation.

A larger stream sequence or message count alone does not demonstrate duplicate balance application.

## Documentation

- [Architecture Decision: Decoupled Ingestion and Periodic Batch Settlement](docs/adr/0001-decoupled-ingestion-and-periodic-batch-settlement.md)
- [Metrics Catalog and PromQL Reference](docs/metrics.md)
- [Docker Swarm Stack Configuration](deploy/stack.yml)
