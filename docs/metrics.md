# Prometheus Metrics Catalog & PromQL Reference

This document is the authoritative catalog for all Prometheus metrics exposed by the **Reliable Deposit Processor** microservices (`gateway`, `publisher`, `depositor`, and `dashboard`) as well as the infrastructure exporters (`prometheus-nats-exporter` and `postgres_exporter`).

---

## 1. Global Conventions & Cardinality Rules

1. **Isolated Per-Service Registries:** Every service constructs its own `*prometheus.Registry` via `metricsx.NewRegistry()` with zero reliance on `prometheus.DefaultRegisterer` or `init()`.
2. **Dedicated Internal Scrape Port (`:9090`):** Every application container serves `/metrics` on port `9090` exclusively inside the `deposit_overlay` network. No application `/metrics` port is published on the host, and `/metrics` is not registered on Gateway's public `:8081` router.
3. **Common Target Labels:** Every scraped series automatically carries:
   * `job`: The Prometheus job name (`gateway`, `publisher`, `depositor`, `dashboard`, `nats-exporter`, `postgres-exporter`).
   * `service`: Normalized service identifier (`gateway`, `publisher`, `depositor`, `dashboard`, `nats`, `postgres`).
   * `instance`: The container's overlay IP and metrics port (`<container_ip>:9090`), uniquely identifying each replica.
   * `cluster`: External label set to `"deposit-swarm"`.
4. **Strict Label Cardinality Guard:** High-cardinality or unbounded identifiers (`event_id`, `request_id`, `user_id`, `Idempotency-Key`, raw URL paths, and raw error strings) are **never** placed in Prometheus labels. Unknown HTTP routes are coalesced into `route="unmatched"`, and `request_id` is attached exclusively as an **OpenMetrics Exemplar** on `gateway_http_request_duration_seconds`.
5. **Standard Runtime & Process Collectors:** Every Go service registers `collectors.NewGoCollector()` (`go_*`) and `collectors.NewProcessCollector()` (`process_*`).

---

## 2. Build Info Metric (All Services)

| Metric Name | Type | Labels & Allowed Values | Unit | Meaning & Operational Question Answered |
| :--- | :--- | :--- | :--- | :--- |
| `gateway_build_info`<br>`publisher_build_info`<br>`depositor_build_info`<br>`dashboard_build_info` | `Gauge` | `version` (e.g., `"1.0.0"`),<br>`revision` (12-char Git SHA / `IMAGE_TAG`),<br>`go_version` (e.g., `"go1.26.0"`) | Constant `1` | **Meaning:** Exposes immutable build metadata as labels with a constant gauge value of `1`.<br>**Question Answered:** *"Which exact Git commit (`revision`) and Go runtime version is currently running on each replica during a rolling update?"* |

---

## 3. Service 1: `gateway` Metrics Catalog

### 3.1. Metric Definitions

| Metric Name | Type | Labels & Allowed Values | Unit | Buckets (if Histogram) | Meaning & Operational Question Answered |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `gateway_http_requests_total` | `Counter` | `method`: `"GET"`, `"POST"`, `"PUT"`, `"DELETE"`, `"OPTIONS"`, `"HEAD"`<br>`route`: `"/v1/deposits"`, `"/healthz"`, `"/readyz"`, `"/stats"`, `"unmatched"`<br>`code`: HTTP status code (`"200"`, `"202"`, `"400"`, `"401"`, `"404"`, `"409"`, `"413"`, `"429"`, `"502"`, `"503"`, `"504"`) | `requests` | — | **Meaning:** Total HTTP requests processed by the Gin router.<br>**Question Answered:** *"How many HTTP requests per second is the Gateway receiving per route and HTTP status code?"* |
| `gateway_http_request_duration_seconds` | `Histogram` (Classic + Native + Exemplars) | `method`: HTTP method<br>`route`: Gin route template or `"unmatched"`<br>*(Exemplar: `request_id`)* | `seconds` | `0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0` | **Meaning:** End-to-end HTTP request handling latency at the Gateway boundary.<br>**Question Answered:** *"What are the $p_{50}$, $p_{95}$, and $p_{99}$ response times seen by external HTTP clients, and which `request_id` exemplar corresponds to a slow spike?"* |
| `gateway_http_requests_in_flight` | `Gauge` | *(none)* | `requests` | — | **Meaning:** Number of HTTP requests currently being processed concurrently by the Gateway.<br>**Question Answered:** *"How saturated are the Gateway HTTP handlers right now?"* |
| `gateway_deposit_requests_total` | `Counter` | `result`: `"accepted"`, `"duplicate"`, `"invalid"`, `"unauthorized"`, `"rate_limited"`, `"payload_too_large"`, `"upstream_unavailable"`, `"upstream_timeout"`, `"internal_error"` | `requests` | — | **Meaning:** Total `/v1/deposits` requests classified by business and infrastructure outcome.<br>**Question Answered:** *"Out of all deposit attempts, how many were newly accepted, deduplicated, rejected by validation/auth/rate-limit, or failed due to upstream gRPC issues?"* |
| `grpc_client_handled_total` | `Counter` | `grpc_service`: `"deposit.v1.DepositPublisherService"`<br>`grpc_method`: `"PublishDeposit"`<br>`grpc_type`: `"unary"`<br>`grpc_code`: `"OK"`, `"InvalidArgument"`, `"AlreadyExists"`, `"DeadlineExceeded"`, `"Unavailable"`, `"Canceled"`, `"ResourceExhausted"`, `"Internal"` | `rpcs` | — | **Meaning:** Standard `go-grpc-middleware` counter of completed client-side gRPC calls.<br>**Question Answered:** *"What is the success and error rate of outbound gRPC calls from Gateway to Publisher?"* |
| `grpc_client_handling_seconds` | `Histogram` | `grpc_service`, `grpc_method`, `grpc_type` | `seconds` | `0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0` | **Meaning:** Standard `go-grpc-middleware` latency histogram of outbound gRPC calls.<br>**Question Answered:** *"How much of Gateway's latency is spent waiting on the gRPC hop to Publisher?"* |
| `gateway_grpc_retries_total` | `Counter` | *(none)* | `retries` | — | **Meaning:** Total automatic client-side gRPC retries triggered when a Publisher replica returns `Unavailable`.<br>**Question Answered:** *"How often is Gateway masking transient Publisher disconnects or `SIGKILL` chaos events via automatic retry?"* |
| `gateway_publisher_endpoints` | `Gauge` | *(none)* | `endpoints` | — | **Meaning:** Number of active Publisher IP addresses currently resolved by the `swarmdns:///` resolver.<br>**Question Answered:** *"Does every Gateway replica see the expected number of healthy Publisher replicas in DNS?"* |

### 3.2. Rationale for `gateway` Histogram Buckets
`HTTPDurationBuckets` (`0.0005` to `5.0` seconds):
* In-memory rejections (`401 Unauthorized`, `429 Rate Limited`, `400 Bad Request`) complete in $0.2\ \text{ms}$ to $1\ \text{ms}$, captured by the fine-grained lower buckets (`0.0005`, `0.001`, `0.0025`).
* Normal `202 Accepted` requests (involving gRPC + NATS JetStream `PubAck` flush) complete between $1.5\ \text{ms}$ and $25\ \text{ms}$ under load, covered by `0.0025`, `0.005`, `0.01`, `0.025`, and `0.05`.
* Retry backoffs (`50ms`) and `REQUEST_TIMEOUT` (`3s`) are bounded by the upper buckets (`0.1` through `5.0`).

---

## 4. Service 2: `publisher` Metrics Catalog

### 4.1. Metric Definitions

| Metric Name | Type | Labels & Allowed Values | Unit | Buckets (if Histogram) | Meaning & Operational Question Answered |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `grpc_server_started_total` | `Counter` | `grpc_service`, `grpc_method`, `grpc_type` | `rpcs` | — | **Meaning:** Total gRPC requests received by the Publisher server.<br>**Question Answered:** *"How many gRPC requests have arrived at each Publisher replica?"* |
| `grpc_server_handled_total` | `Counter` | `grpc_service`, `grpc_method`, `grpc_type`, `grpc_code` | `rpcs` | — | **Meaning:** Total gRPC requests completed by the Publisher server partitioned by status code.<br>**Question Answered:** *"How is gRPC traffic distributed across Publisher replicas, and what is the per-code completion rate?"* |
| `grpc_server_handling_seconds` | `Histogram` | `grpc_service`, `grpc_method`, `grpc_type` | `seconds` | `0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0` | **Meaning:** Server-side gRPC execution time on Publisher.<br>**Question Answered:** *"What is the $p_{99}$ server handling latency of `PublishDeposit`?"* |
| `publisher_jetstream_publish_total` | `Counter` | `result`: `"ok"`, `"duplicate"`, `"error"` | `events` | — | **Meaning:** Total events published to the NATS JetStream `DEPOSITS` stream.<br>**Question Answered:** *"How many events were freshly persisted (`ok`), caught by NATS 10-minute deduplication (`duplicate`), or failed (`error`)?"* |
| `publisher_jetstream_publish_duration_seconds` | `Histogram` (Classic + Native) | *(none)* | `seconds` | `0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0` | **Meaning:** Time spent waiting for NATS JetStream `PublishAsync` to return a disk-backed `PubAck`.<br>**Question Answered:** *"How fast is NATS JetStream acknowledging disk writes (`PubAck`)?"* |
| `publisher_requests_in_flight` | `Gauge` | *(none)* | `requests` | — | **Meaning:** Number of `PublishDeposit` RPCs currently executing inside Publisher.<br>**Question Answered:** *"How many in-flight RPCs are pending `PubAck` during normal load or during the `DRAIN_DELAY` shutdown window?"* |
| `publisher_ready` | `Gauge` (`GaugeFunc`) | *(none)* | `0` or `1` | — | **Meaning:** Reads `healthmgr.IsReady()` (`1` when `StateHealthy`, `0` otherwise).<br>**Question Answered:** *"Is this Publisher replica currently marked `SERVING` in the unified health manager?"* |
| `publisher_draining` | `Gauge` (`GaugeFunc`) | *(none)* | `0` or `1` | — | **Meaning:** Reads `healthmgr.IsDraining()` (`1` after `SIGTERM`, `0` during normal operation).<br>**Question Answered:** *"Is this Publisher replica currently in its 6-step graceful shutdown drain phase?"* |
| `publisher_nats_connected` | `Gauge` (`GaugeFunc`) | *(none)* | `0` or `1` | — | **Meaning:** Reads `healthmgr.IsNATSConnected()` (`1` when connected to NATS, `0` when disconnected).<br>**Question Answered:** *"Does this Publisher replica have an active connection to NATS?"* |
| `publisher_idempotency_conflicts_total` | `Counter` | *(none)* | `conflicts` | — | **Meaning:** Total requests rejected at the Publisher boundary because the same `event_id` was reused with different deposit parameters.<br>**Question Answered:** *"Are clients attempting to mutate payload parameters while reusing an existing `Idempotency-Key`?"* |

### 4.2. Rationale for `publisher` Histogram Buckets
`JetStreamPublishBuckets` starts at `0.00025` ($250\ \mu\text{s}$) because pipelined `PublishAsync` futures over a local overlay network often resolve in $250\ \mu\text{s}$ to $2\ \text{ms}$, while disk `fsync` stalls or broker reconnects can push tail latency into the `0.05` to `2.5` second range.

---

## 5. Service 3: `depositor` Metrics Catalog

### 5.1. Metric Definitions

| Metric Name | Type | Labels & Allowed Values | Unit | Buckets (if Histogram) | Meaning & Operational Question Answered |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `depositor_messages_total` | `Counter` | `result`: `"processed"`, `"duplicate"`, `"dlq"`, `"retry"`, `"unacked_shutdown"` | `messages` | — | **Meaning:** Total JetStream messages handled by Depositor workers.<br>**Question Answered:** *"How many messages were newly ingested (`processed`), deduplicated by Postgres PK (`duplicate`), isolated to `DEPOSITS_DLQ` (`dlq`), retried via `NakWithDelay` (`retry`), or left unacked on shutdown timeout (`unacked_shutdown`)?"* |
| `depositor_message_processing_duration_seconds` | `Histogram` (Classic + Native) | *(none)* | `seconds` | `0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0` | **Meaning:** Duration from the moment a worker receives a message from JetStream until `msg.Ack()` or `msg.NakWithDelay()` finishes.<br>**Question Answered:** *"How long does a Depositor worker take to validate, batch-insert into Postgres, and ACK a message?"* |
| `depositor_event_age_seconds` | `Histogram` (Classic + Native) | *(none)* | `seconds` | `0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0` | **Meaning:** End-to-end pipeline lag measured from `event.CreatedAt` (stamped at Gateway ingress) to PostgreSQL commit completion.<br>**Question Answered:** *"What is the true end-to-end latency from client HTTP request arrival at Gateway to durable database persistence?"* |
| `depositor_ingest_batch_size` | `Histogram` | *(none)* | `events` | `1, 2, 4, 8, 16, 32, 64, 128, 192, 256` | **Meaning:** Number of deposit events coalesced into a single SQL `UNNEST` insert batch.<br>**Question Answered:** *"How effectively are the 4 batch flushers coalescing concurrent events under low vs. high load?"* |
| `depositor_ingest_batch_duration_seconds` | `Histogram` (Classic + Native) | *(none)* | `seconds` | `0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0` | **Meaning:** Execution time of the `INSERT INTO deposits ... SELECT UNNEST(...)` query in PostgreSQL.<br>**Question Answered:** *"What is the database write latency per micro-batch?"* |
| `depositor_ingest_batch_errors_total` | `Counter` | *(none)* | `errors` | — | **Meaning:** Total SQL `UNNEST` batch insert queries that failed with a database error.<br>**Question Answered:** *"Is PostgreSQL rejecting or timing out ingestion batches?"* |
| `depositor_settlement_runs_total` | `Counter` | `result`: `"success"`, `"error"` | `runs` | — | **Meaning:** Total executions of the periodic PL/pgSQL `settle_pending_deposits` worker.<br>**Question Answered:** *"Are periodic 5-second batch settlement cycles succeeding reliably?"* |
| `depositor_settlement_duration_seconds` | `Histogram` (Classic + Native) | *(none)* | `seconds` | `0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 15.0` | **Meaning:** Execution time of `SELECT settle_pending_deposits(100000)`.<br>**Question Answered:** *"How long does PostgreSQL take to settle up to 100,000 pending deposits and update user balances?"* |
| `depositor_settled_deposits_total` | `Counter` | *(none)* | `deposits` | — | **Meaning:** Cumulative number of deposit rows transitioned from `is_settled = FALSE` to `TRUE` by this Depositor replica.<br>**Question Answered:** *"How many deposits per second are being settled into spendable user balances?"* |
| `depositor_settlement_last_success_timestamp_seconds` | `Gauge` | *(none)* | `unix_seconds` | — | **Meaning:** Unix epoch timestamp (`seconds`) of the most recent successful `settle_pending_deposits` run.<br>**Question Answered:** *"How many seconds have elapsed since the last successful balance settlement?"* |
| `depositor_workers_active` | `Gauge` | *(none)* | `workers` | — | **Meaning:** Number of active pull-consumer worker goroutines on this replica.<br>**Question Answered:** *"How many workers are currently pulling messages from NATS across the cluster?"* |
| `depositor_nats_connected` | `Gauge` | *(none)* | `0` or `1` | — | **Meaning:** `1` when connected to NATS, `0` when disconnected.<br>**Question Answered:** *"Is this Depositor replica connected to NATS?"* |
| `go_sql_max_open_connections`<br>`go_sql_open_connections`<br>`go_sql_in_use`<br>`go_sql_idle`<br>`go_sql_wait_count_total`<br>`go_sql_wait_duration_seconds_total` | `Gauge` / `Counter` | `db_name`: `"deposits_db"` | `connections` / `seconds` | — | **Meaning:** Standard `collectors.NewDBStatsCollector` metrics for the PostgreSQL connection pool.<br>**Question Answered:** *"Are database connections exhausted (`go_sql_in_use == go_sql_max_open_connections`) or queuing (`rate(go_sql_wait_count_total[1m]) > 0`)?"* |

### 5.2. Rationale for `depositor` Histogram Buckets
* **`EventAgeBuckets` (`0.002` to `300.0` seconds):** Under normal operation, end-to-end lag from Gateway to PostgreSQL commit is $2\ \text{ms}$ to $50\ \text{ms}$. However, if Depositor replicas are restarted or PostgreSQL experiences temporary contention, JetStream buffers messages and `event_age_seconds` can grow to several seconds or minutes (`10.0`, `30.0`, `60.0`, `300.0`).
* **`IngestBatchSizeBuckets` (`1, 2, 4, 8, 16, 32, 64, 128, 192, 256`):** Matches the exact powers-of-two and saturation thresholds (`64` fast-path threshold and `256` `maxBatchSize`) in `collectNextBatch`.
* **`SettlementDurationBuckets` (`0.001` to `15.0` seconds):** An empty settlement tick completes in $\approx 1\ \text{ms}$, while settling a full batch of `100,000` rows takes $0.1\ \text{s}$ to $2.5\ \text{s}$, bounded by the $15\ \text{s}$ query timeout.

---

## 6. Service 4: `dashboard` & Infrastructure Exporter Metrics

### 6.1. `dashboard` Metrics

| Metric Name | Type | Labels & Allowed Values | Unit | Buckets | Meaning & Operational Question Answered |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `dashboard_http_requests_total` | `Counter` | `method`: `"GET"`, `"POST"`<br>`route`: `"/"`, `"/healthz"`, `"/api/stats"`, `"/api/publisher/toggle"`, `"/api/settlement/trigger"`, `"/api/dlq/replay"`, `"/api/dlq/inject"`, `"/api/gateway/send"`, `"/api/gateway/loadgen"`, `"unmatched"`<br>`code`: HTTP status code | `requests` | — | **Meaning:** Total HTTP requests served by the Dashboard.<br>**Question Answered:** *"What is the request rate and auth rejection rate (`401`/`403`) on the Dashboard?"* |
| `dashboard_http_request_duration_seconds` | `Histogram` (Classic + Native) | `method`, `route` | `seconds` | `0.0005` .. `5.0` | **Meaning:** Latency distribution of Dashboard HTTP endpoints.<br>**Question Answered:** *"How fast is `/api/stats` responding during cluster load?"* |
| `dashboard_http_requests_in_flight` | `Gauge` | *(none)* | `requests` | — | **Meaning:** Concurrent HTTP requests in flight on the Dashboard.<br>**Question Answered:** *"Are Dashboard requests piling up?"* |
| `go_sql_*` | `Gauge` / `Counter` | `db_name`: `"deposits_db"` | `connections` | — | **Meaning:** Standard `DBStatsCollector` metrics for Dashboard's PostgreSQL pool.<br>**Question Answered:** *"How many DB connections is the Dashboard holding open?"* |

### 6.2. Infrastructure Metrics (`prometheus-nats-exporter` & `postgres_exporter`)

| Metric Name | Source Exporter | Type | Key Labels | Meaning & Operational Question Answered |
| :--- | :--- | :--- | :--- | :--- |
| `jetstream_consumer_num_pending` | `nats-exporter` | `Gauge` | `stream_name="DEPOSITS"`, `consumer_name="deposits_processor"` | **Meaning:** Number of messages in the `DEPOSITS` stream that have not yet been delivered to Depositor workers.<br>**Question Answered:** *"How many messages are waiting in the stream queue?"* |
| `jetstream_consumer_num_ack_pending` | `nats-exporter` | `Gauge` | `stream_name="DEPOSITS"`, `consumer_name="deposits_processor"` | **Meaning:** Number of messages delivered to Depositor workers that are currently awaiting `Ack`.<br>**Question Answered:** *"How many messages are currently in flight across all Depositor consumers?"* |
| `jetstream_consumer_num_redelivered` | `nats-exporter` | `Gauge` | `stream_name="DEPOSITS"`, `consumer_name="deposits_processor"` | **Meaning:** Number of messages redelivered by JetStream due to `NakWithDelay` or `AckWait` expiration.<br>**Question Answered:** *"How many message redeliveries have occurred?"* |
| `pg_deposits_unsettled_pending_count` | `postgres-exporter` | `Gauge` | `service="postgres"` | **Meaning:** Custom query count of rows in `deposits` where `is_settled = FALSE`.<br>**Question Answered:** *"How many ingested deposits are currently waiting in PostgreSQL for the next 5s settlement cycle?"* |
| `pg_deposits_settled_total_count` | `postgres-exporter` | `Gauge` | `service="postgres"` | **Meaning:** Custom query sum of `settled_count` from `ledger_settled_summary`.<br>**Question Answered:** *"How many total deposits have been settled into user balances in PostgreSQL?"* |
| `pg_deposits_ledger_discrepancy_count` | `postgres-exporter` | `Gauge` | `service="postgres"` | **Meaning:** Custom query count of `(user_id, currency)` wallets where settled deposit sum differs from `balances.balance`.<br>**Question Answered:** *"Is the financial ledger 100% balanced (`0` discrepancies)?"* |

---

## 7. Golden Signal PromQL Queries (Tested on Live Cluster)

### 7.1. Service: `gateway`

* **Rate (Requests per second by route):**
  ```promql
  sum by (route) (rate(gateway_http_requests_total[1m]))
  ```
* **Errors (Ratio of 5xx responses to total HTTP requests):**
  ```promql
  sum(rate(gateway_http_requests_total{code=~"5.."}[1m]))
  /
  clamp_min(sum(rate(gateway_http_requests_total[1m])), 1e-9)
  ```
* **Duration ($p_{50}$, $p_{95}$, and $p_{99}$ latency on `POST /v1/deposits`):**
  ```promql
  # p50
  histogram_quantile(0.50, sum by (le) (rate(gateway_http_request_duration_seconds_bucket{route="/v1/deposits"}[1m])))

  # p95
  histogram_quantile(0.95, sum by (le) (rate(gateway_http_request_duration_seconds_bucket{route="/v1/deposits"}[1m])))

  # p99
  histogram_quantile(0.99, sum by (le) (rate(gateway_http_request_duration_seconds_bucket{route="/v1/deposits"}[1m])))
  ```
* **Saturation (Total in-flight HTTP requests across all Gateway replicas):**
  ```promql
  sum(gateway_http_requests_in_flight)
  ```

---

### 7.2. Service: `publisher`

* **Rate (gRPC requests per second handled across cluster):**
  ```promql
  sum(rate(grpc_server_handled_total{service="publisher",grpc_method="PublishDeposit"}[1m]))
  ```
* **Errors (Ratio of non-OK / non-AlreadyExists gRPC responses or JetStream publish errors):**
  ```promql
  sum(rate(publisher_jetstream_publish_total{result="error"}[1m]))
  /
  clamp_min(sum(rate(publisher_jetstream_publish_total[1m])), 1e-9)
  ```
* **Duration ($p_{50}$, $p_{95}$, and $p_{99}$ NATS JetStream `PubAck` latency):**
  ```promql
  # p50
  histogram_quantile(0.50, sum by (le) (rate(publisher_jetstream_publish_duration_seconds_bucket[1m])))

  # p95
  histogram_quantile(0.95, sum by (le) (rate(publisher_jetstream_publish_duration_seconds_bucket[1m])))

  # p99
  histogram_quantile(0.99, sum by (le) (rate(publisher_jetstream_publish_duration_seconds_bucket[1m])))
  ```
* **Saturation (Concurrent in-flight gRPC requests per Publisher replica):**
  ```promql
  sum by (instance) (publisher_requests_in_flight)
  ```

---

### 7.3. Service: `depositor`

* **Rate (Messages processed per second by outcome):**
  ```promql
  sum by (result) (rate(depositor_messages_total[1m]))
  ```
* **Errors (Ratio of temporary retry/batch errors to total messages handled):**
  ```promql
  sum(rate(depositor_messages_total{result="retry"}[1m]))
  /
  clamp_min(sum(rate(depositor_messages_total[1m])), 1e-9)
  ```
* **Duration ($p_{50}$, $p_{95}$, and $p_{99}$ worker message processing time):**
  ```promql
  # p50
  histogram_quantile(0.50, sum by (le) (rate(depositor_message_processing_duration_seconds_bucket[1m])))

  # p95
  histogram_quantile(0.95, sum by (le) (rate(depositor_message_processing_duration_seconds_bucket[1m])))

  # p99
  histogram_quantile(0.99, sum by (le) (rate(depositor_message_processing_duration_seconds_bucket[1m])))
  ```
* **Saturation (PostgreSQL connection pool utilization ratio per Depositor replica):**
  ```promql
  go_sql_in_use{service="depositor"} / clamp_min(go_sql_max_open_connections{service="depositor"}, 1)
  ```

---

### 7.4. Service: `dashboard`

* **Rate (HTTP requests per second by route):**
  ```promql
  sum by (route) (rate(dashboard_http_requests_total[1m]))
  ```
* **Errors (Ratio of 5xx responses to total HTTP requests):**
  ```promql
  sum(rate(dashboard_http_requests_total{code=~"5.."}[1m]))
  /
  clamp_min(sum(rate(dashboard_http_requests_total[1m])), 1e-9)
  ```
* **Duration ($p_{50}$, $p_{95}$, and $p_{99}$ latency on `/api/stats`):**
  ```promql
  # p50
  histogram_quantile(0.50, sum by (le) (rate(dashboard_http_request_duration_seconds_bucket{route="/api/stats"}[1m])))

  # p95
  histogram_quantile(0.95, sum by (le) (rate(dashboard_http_request_duration_seconds_bucket{route="/api/stats"}[1m])))

  # p99
  histogram_quantile(0.99, sum by (le) (rate(dashboard_http_request_duration_seconds_bucket{route="/api/stats"}[1m])))
  ```
* **Saturation (In-flight requests and DB connections in use on Dashboard):**
  ```promql
  sum(dashboard_http_requests_in_flight) + sum(go_sql_in_use{service="dashboard"})
  ```

---

## 8. Key End-to-End Pipeline & Infrastructure PromQL Queries

1. **End-to-End Pipeline Latency ($p_{99}$ of `depositor_event_age_seconds`):**
   ```promql
   histogram_quantile(0.99, sum by (le) (rate(depositor_event_age_seconds_bucket[1m])))
   ```
2. **Depositor Consumer Lag (Unprocessed + In-Flight Messages in NATS JetStream):**
   ```promql
   sum(jetstream_consumer_num_pending{consumer_name="deposits_processor"})
   +
   sum(jetstream_consumer_num_ack_pending{consumer_name="deposits_processor"})
   ```
3. **Time Elapsed Since Last Successful Batch Settlement (in Seconds):**
   ```promql
   time() - max(depositor_settlement_last_success_timestamp_seconds)
   ```
4. **Per-Instance Request Rate of Each `publisher` Replica:**
   ```promql
   sum by (instance) (rate(grpc_server_handled_total{service="publisher",grpc_method="PublishDeposit"}[1m]))
   ```