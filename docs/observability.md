# Observability: metrics, traces, readiness, and shutdown

V1.4 wires the roadmap's staged-observability layer onto the existing serving
core. Three properties are load-bearing:

- **Prometheus stays the metrics surface.** All series are registered in the
  shared `internal/metrics` registry; the OpenTelemetry work adds only
  tracing and never a second metrics pipeline.
- **OTLP is independently disable-able.** `GATEWAY_OTLP_ENABLED=false` (the
  default) leaves the no-op OpenTelemetry provider installed: instrumentation
  exists in the code but nothing is created, sampled, or exported. Serving,
  Prometheus, async workers, and every other feature behave identically with
  the switch in either position. Disabling OTLP is the documented rollback.
- **Telemetry is metadata only.** No span, metric label, or log field ever
  carries prompt/completion/tool-argument content, credentials, upstream
  URLs, or raw SQL. Tests pin this (`internal/httpapi/trace_linkage_test.go`,
  `internal/metrics/metrics_test.go`).

## Metrics (`/metrics`, Prometheus exposition)

Existing families are unchanged. The V1.4 observability additions:

| Metric | Type | Meaning |
|---|---|---|
| `gateway_async_queue_depth` | Gauge | Queued jobs, sampled at scrape time by a custom collector (one DB sample per scrape; no per-job labels; a failed sample emits nothing rather than a fabricated zero) |
| `gateway_async_queue_oldest_seconds` | Gauge | Age of the oldest queued job (scrape-time sample) |
| `gateway_async_queue_wait_seconds` | Histogram | Enqueue → first-claim latency observed per claimed job |
| `gateway_async_jobs_total{status}` | Counter | Terminal outcomes: `completed`, `failed`, `cancelled`, `expired` |
| `gateway_async_lease_expired_total` | Counter | Worker leases the recovery sweep reclaimed (requeued or terminal-failed) |
| `gateway_async_results_expired_total` | Counter | Terminal results dropped after their retention TTL |
| `gateway_async_worker_healthy` | Gauge | 1 while the pool accepts jobs and the sweep heartbeat is fresh; 0 during/after shutdown |
| `gateway_async_worker_inflight` | Gauge | Jobs currently executing in this process |
| `gateway_ledger_settlement_failures_total` | Counter | Settlements that failed and remain retryable (`reserved` ledger rows kept) |
| `gateway_ledger_settlement_retries_total` | Counter | Settlement attempts re-driven after a failure (the worker performs one bounded in-process retry; both settle halves are idempotent) |
| `gateway_ledger_cost_unknown_total` | Counter | Settlements whose cost stayed unknown (unknown usage or no applicable price) — never fabricated as zero |
| `gateway_budget_denials_total{scope}` | Counter | Monetary budget denials, `scope` ∈ {`subject`, `tenant`} |
| `gateway_lifecycle_rows_archived_total{table}` / `..._deleted_total{table}` / `gateway_lifecycle_exports_total` | Counters | Retention/archive/export activity |

Label discipline: no metric carries subject, tenant, job, or request
dimensions. The only labels anywhere are the bounded vocabularies above
(`status`, `scope`, `table`, plus the serving labels `model`/`provider`/
`class`/`kind` that predate V1.4 and are bounded by the catalog).

## Traces (OTLP/HTTP)

Span topology — one background job produces one continuous trace across the
process boundary:

```
http.responses            (server root; child of the caller's traceparent when supplied)
└─ http.admission         shared admission pipeline (model, protocol, denial class)
   └─ async.enqueue       job row persisted; W3C identity normalized onto the row
      ┆  (job row: trace_id, parent_span_id, trace_sampled)
async.claim               queue poll (own root; the winner observes queue wait)
└─ async.worker.execute   child of the PERSISTED enqueue span (remote parent)
   ├─ provider.attempt    per attempt: provider name, attempt number, outcome class
   ├─ async.commit        terminal CAS (won/lost)
   └─ ledger.settle       reservation settlement (settled / retrying / failed)
```

- Synchronous requests produce `http.<protocol>` → `http.admission` →
  `provider.attempt` → `ledger.settle`/`ledger.release` traces.
- `async.claim` roots its own trace per poll (job-less polls are not caller
  work); the claimed job's execution joins the enqueue trace through the
  persisted context, which is exactly what makes one trace link enqueue →
  worker → provider → settlement.
- The sampled bit of the caller's decision is persisted with the job so
  `ParentBased` sampling keeps worker spans inside the caller's sampled
  trace (a dropped span would silently truncate the trace).
- Normalization (`internal/tracing`): only exact-length lowercase-hex W3C
  pairs persist; anything else — uppercase, short, script-shaped, oversized —
  collapses to empty before the row is written. Baggage is never read, so it
  can never reach a row or a span.
- Configuration and kill switch: `GATEWAY_OTLP_ENABLED` (default `false`),
  `GATEWAY_OTLP_ENDPOINT` (default `localhost:4318`), `GATEWAY_OTLP_INSECURE`,
  `GATEWAY_OTLP_SAMPLING_RATIO` (default `1.0`), `GATEWAY_OTLP_TIMEOUT`
  (default `10s`).
- Export failure never breaks serving: the batch processor drops spans after
  its bounded queue, export errors surface only in the OTel error log, and
  the request path never blocks on export. Pinned by
  `TestExporterFailureNeverBreaksServing`.

Sampling advice: keep the ratio at 1 until dashboards stabilize, then drop to
0.1–0.25. Async traces are the cheap ones to keep at 1 (one trace per job,
not per request), so a per-route sampling split — synchronous down first,
background last — preserves the accounting evidence longest.

## Readiness and liveness

`/readyz` runs named checks, each under its own 2-second deadline, and
answers:

- `200 {"status":"ready","checks":{"database":"ok",...}}` when every required
  check passes;
- `503 {"status":"unavailable","checks":{...,"redis":"failed"}}` when a
  required check fails — the check name in the body is the degraded
  dependency;
- a non-required failing check reports `"degraded"` while staying ready
  (degradation is visible without a client-facing denial).

Checks register only while their feature is enabled, so a rollback switch
removes its check instead of turning readiness permanently red:

| Check | Condition | Signal |
|---|---|---|
| `database` | `GATEWAY_DATABASE_URL` set | Pool ping |
| `redis` | `GATEWAY_LIMITS_MODE=redis` | PING |
| `catalog` | always | At least one enabled model |
| `queue` | `GATEWAY_ASYNC_ENABLED=true` | A real query against the job table the claimer scans |
| `worker` | `GATEWAY_ASYNC_ENABLED=true` | Pool accepting jobs + sweep heartbeat fresh within 3 poll intervals |
| `settlement` | database mode (ledger wired) | Reserved ledger rows older than 10 minutes below `GATEWAY_SETTLEMENT_BACKLOG_MAX` (default 100). An unmeasurable backlog fails closed. |

`/healthz` stays process-only by contract: it never runs dependency probes,
so a dependency outage cannot confuse restart orchestration.

## Graceful shutdown

Order (roadmap §7), all observable on the worker gauge and in the logs:

1. HTTP listeners stop serving intake; the pool closes intake
   (`async: shutdown: stopping job intake`, `gateway_async_worker_healthy` → 0).
2. A bounded drain window (`GATEWAY_ASYNC_DRAIN_TIMEOUT`, default 10s) lets
   in-flight executions commit (`async: shutdown: drain complete`).
3. If the window elapses, in-flight executions are aborted and their leases
   return to the queue (`async: shutdown: drain window elapsed; aborting
   in-flight executions`, with the in-flight count). Aborts requeue without
   consuming an attempt; terminal commits always run on the execution
   context, so they land inside the drain window or fall to lease recovery —
   never duplicated.

## Suggested alerts

| Alert | Expression sketch | Notes |
|---|---|---|
| Queue starving | `gateway_async_queue_depth > 0 for 5m and gateway_async_worker_healthy == 1 and rate(gateway_async_jobs_total{status="completed"}[5m]) == 0` | Workers live but nothing completes |
| Queue backlog growth | `increase(gateway_async_queue_depth[10m]) > 0 and max_over_time(gateway_async_queue_oldest_seconds[10m]) > 300` | Oldest queued job aging |
| Lease churn | `rate(gateway_async_lease_expired_total[5m]) > 0` | Workers dying mid-job or leases too short for `GATEWAY_ASYNC_JOB_TIMEOUT` |
| Settlement failures | `increase(gateway_ledger_settlement_failures_total[10m]) > 0` | `reserved` rows are accumulating; re-drive before budgets distort |
| Unknown-cost rate | `rate(gateway_ledger_cost_unknown_total[1h]) / rate(gateway_requests_total[1h])` | Missing prices for active models (approximate denominator: all served requests; scoped deployments can narrow by model via `gateway_tokens_total`) |
| Budget pressure | `increase(gateway_budget_denials_total[5m]) > 0` | Per-scope denials correlate with tenant complaints |
| Result expiry surprises | `increase(gateway_async_results_expired_total[1h]) > 0` | Clients losing retrievable results; check TTLs |
| Readiness flapping | probe `/readyz` externally; alert on any 503 body naming a check | The body names the failing dependency |

## Troubleshooting queries

```sql
-- Jobs currently queued, oldest first (matches the depth/age gauges)
SELECT job_id, public_model, created_at, attempt_count
FROM async_jobs WHERE status = 'queued' ORDER BY created_at LIMIT 20;

-- Where settlement failures concentrate (these are the re-drivable rows)
SELECT request_id, job_id, public_model, created_at
FROM usage_ledger WHERE settle_status = 'reserved' ORDER BY created_at LIMIT 50;

-- Trace a job end to end from its row
SELECT job_id, status, trace_id, parent_span_id, final_request_id
FROM async_jobs WHERE job_id = 'resp_...';

-- Unknown-cost settlements: which models lack a price for the window
SELECT public_model, count(*) FROM usage_ledger
WHERE settle_status = 'settled' AND cost_micros IS NULL
  AND created_at > now() - interval '1 day'
GROUP BY public_model;
```

Operational notes:

- Settled cost is recomputable from tokens × price version: the ledger row
  stores the price version it used, so a dashboard can re-price history after
  a catalog fix.
- `trace_id` on `async_jobs`/`llm_requests` joins rows to traces in the
  tracing backend; query by `gw.job_id`/`gw.request_id` span attributes.
- The scrape-time queue collector adds one lightweight aggregate query per
  `/metrics` scrape — safe at standard scrape intervals; do not scrape
  multiple times per second against a saturated database.
