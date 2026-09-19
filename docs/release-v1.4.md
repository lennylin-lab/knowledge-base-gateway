# Release checklist: V1.4 async operations and cost governance

Operator-facing release gate for the V1.4 rollout. V1.4 adds background
Responses jobs, versioned pricing with a settlement ledger and monetary
budgets, scoped admin identities, a data lifecycle, and OTLP tracing on top of
the V1.3 model control plane. Everything new is additive and flag-gated; the
V1.3 wire contracts are frozen by golden fixtures.

Deeper references: the [V1.4 roadmap](v1.4-operations-and-async-roadmap.md),
[admin RBAC](admin-rbac.md), [data lifecycle](data-lifecycle.md),
[observability](observability.md), [client contract](gateway-client-contract.md),
and [API versioning](api-versioning.md).

## 1. What ships

| Feature | Gate | Default | Rollback switch |
| --- | --- | --- | --- |
| Background Responses (jobs, query, cancel, idempotency, worker pool) | `GATEWAY_ASYNC_ENABLED` | `false` | `false` → `background:true` answered with the stable 503 `job_queue_unavailable`; job routes unregistered; queue/worker readiness checks disappear. Rows and results are retained. |
| Monetary budget enforcement | `GATEWAY_BUDGETS_ENABLED` | `false` | `false` → no request is ever denied for money; ledger capture continues (cost evidence retained). |
| Usage ledger + prices | database mode | always on in database mode | No flag: ledger rows are written whenever `GATEWAY_DATABASE_URL` is set (the dark-write stage). Budget tables stay inert until policies are configured. |
| Scoped admin identities | database mode | on in database mode | `GATEWAY_ADMIN_TOKEN` keeps working as the platform-admin bootstrap; scoped credentials coexist. Removing the env token after production verification is the deprecation step. |
| Data lifecycle (retention, archive, export) | `GATEWAY_LIFECYCLE_ENABLED` | `true` | `false` → admin lifecycle endpoints 404 and `cmd/maintain` refuses to run. Inert until `retention_policies` rows exist (absence = keep forever). |
| OTLP trace export | `GATEWAY_OTLP_ENABLED` | `false` | `false` → no-op tracer; serving, Prometheus, workers unchanged. |
| Readiness settlement/queue/worker checks | ride the features above | — | Each check exists only while its feature is enabled; a disabled feature can never fail readiness. |

## 2. Validation matrix (all gates green before enabling tenants)

Run against real PostgreSQL (`TEST_DATABASE_URL`) and real Redis
(`TEST_REDIS_ADDR`) with **zero skipped tests** — a `--- SKIP` in any
env-gated suite fails the gate:

```bash
GOCACHE=/tmp/kb-gateway-go-cache go test ./...
GOCACHE=/tmp/kb-gateway-go-cache go test -race ./... \
  # with TEST_DATABASE_URL / TEST_REDIS_ADDR exported
GOCACHE=/tmp/kb-gateway-go-cache go vet ./...
TEST_DATABASE_URL=... go test ./internal/store/pg/      # migrations up/down walk, every boundary
TEST_REDIS_ADDR=...   go test ./internal/limiter/ ./internal/quota/ ./internal/accounting/
TEST_DATABASE_URL=... go test ./internal/e2e/           # HTTP + PostgreSQL + worker rehearsal (below)
go run ./cmd/replay
scripts/smoke.sh
```

Evidence recorded for this release:

- **Suite**: `go test ./...`, `-race ./...`, `go vet ./...` — all green with
  the environment variables set (no skips).
- **Migrations**: up → version 12 → `steps -1` walk of every down boundary →
  re-up on a real database; V1.3 (version 5) upgrade path test; schema
  invariants (closed state sets, uniqueness, owner-scoped FKs, money checks)
  proven with DB-level rejection tests.
- **Budget/fault suite (Redis)**: multi-instance budget no-oversell, outage
  fail-closed classification (an infrastructure outage is never a 429),
  concurrency leases, quota atomicity — zero skips on real Redis.
- **Golden wire fixtures**: byte-identical (chat non-stream/stream, responses,
  embeddings, error envelopes, rate-limit envelope).
- **Replay tool**: `go run ./cmd/replay` completes.
- **Smoke**: `scripts/smoke.sh` passes (compose one-shot migrate → gateway →
  per-phase health checks).

## 3. End-to-end rollout rehearsal (HTTP + PostgreSQL + worker)

`internal/e2e/async_pg_test.go` runs the staged rehearsal over a real HTTP
server, real PostgreSQL (job queue, results, idempotency keys, ledger, budget
policies) and a real worker pool:

1. **Flags off (V1.3-identical).** `background:true` → 503
   `job_queue_unavailable`, job routes 404, queue empty, sync Responses
   envelope unchanged, `/readyz` shows only database/catalog/settlement (no
   queue/worker checks). Golden fixtures pin the byte-level V1.3 wire.
2. **Async on.** 202 queued envelope → idempotent replay returns the original
   job, digest mismatch → 409 `idempotency_conflict` (real mapping table) →
   worker claims/executes/commits → GET completed with the unified envelope
   and reported usage → exactly one settled ledger row with the price version
   and computed cost → terminal audit record → `/readyz` green including
   queue/worker/settlement.
3. **Cancel.** Cancel while running propagates into the provider context;
   job lands `cancelled` with the lease cleared; cross-subject GET/cancel
   stay non-leaky 404; repeat cancel is an idempotent 200; billing evidence
   ends released-or-settled exactly once.
4. **Legacy admin token.** The bootstrap token authenticates as platform
   admin on the query surface (visibility) and is denied on cancel
   (visibility is not control) — the RBAC rollback identity over the real
   store.
5. **Budgets on.** A configured 1-micro budget denies at admission with 429
   `budget_exceeded` before any job row or provider call; subjects without a
   policy are unaffected.
6. **Budgets rollback.** Same budget, enforcement off: no denial, and the
   completed job still settles exactly one priced ledger row (capture
   retained while enforcement is off).
7. **Async rollback (drain).** Stop intake → in-flight execution commits
   inside the drain window → result and settlement durable, nothing lost.
8. **Race re-verified against the merged whole.** Cancel-versus-commit
   across a 16-step deterministic interleaving matrix (0–250 ms offsets over
   a 40 ms execution): every terminal outcome kept exactly-one terminal
   state, a released lease, at most one settled ledger row, and no reserved
   residue — including the cancel-after-output settlement handoff — on the
   real database, not the in-memory mirror.

Load/capacity profiles (queue depth/latency, lease churn, budget contention,
admin limits, export/partition maintenance) publish per-stage thresholds in
the metrics families documented in [observability.md](observability.md); the
rehearsal above establishes the functional baselines those dashboards alarm
against.

## 4. Staged rollout plan (with stop/rollback thresholds per stage)

| Stage | Action | Watch | Stop / roll back when |
| --- | --- | --- | --- |
| 0. Dark writes | Deploy with all flags off (database mode). Ledger rows and metrics flow; no client-visible change. | Ledger write latency, error rates, settlement backlog, V1.3 goldens. | Any V1.3 regression: revert binary (flags-off deployment must behave as V1.3). |
| 1. Selected models | `GATEWAY_ASYNC_ENABLED=true`; keep the model allowlist narrow (capability matrix). | `gateway_async_queue_depth`, `queue_wait_seconds`, worker health, `jobs_total{status=failed}`. | Queue depth rising monotonically, worker readiness flapping, or failed-rate above baseline: set the flag false (drain preserves in-flight work). |
| 2. Internal tenant | Enable background for the internal subject(s) (policy allowlist). | Cancel rate, idempotency conflicts, settlement failures, `cost_unknown` rate. | Settlement retries climbing or backlog threshold tripping `/readyz`: pause acceptance, keep workers draining. |
| 3. Budgets on selected subjects | `GATEWAY_BUDGETS_ENABLED=true` with policies on pilot subjects only. | `budget_denials_total`, `pricing_unavailable` 500s, budget usage views. | `pricing_unavailable` appearing: publish the missing price or turn enforcement off (evidence is retained either way). |
| 4. Progressive tenants | Widen model/tenant allowlists one cohort at a time. | Per-tenant usage, ledger growth, archive runs, admin audit volume. | Cohort-level anomaly: narrow that cohort's allowlist; feature flags stay on. |
| 5. Defaults | Async + budgets default-on for new configuration; lifecycle policies scheduled via `cmd/maintain`; OTLP enabled on the collectors' schedule. | Full observability.md dashboard set. | Standard per-feature rollback switches. |

## 5. Rollback runbook (order matters)

1. **Stop new async admission** — `GATEWAY_ASYNC_ENABLED=false`, restart (or
   remove the flag wiring). Creation answers 503 `job_queue_unavailable`;
   job query/cancel routes disappear; the worker pool stops intake.
2. **Drain** — the pool stops claiming, gives in-flight jobs the bounded
   drain window to commit, then aborts the remainder and hands leases back;
   a dead process's leases are recovered by any surviving worker's sweep.
   No committed job is lost; a killed job is requeued, never double-billed
   (terminal commits are lease-conditional CAS updates).
3. **Disable monetary enforcement** — `GATEWAY_BUDGETS_ENABLED=false`:
   denials stop immediately; ledger capture continues so cost evidence and
   the readiness settlement check stay live.
4. **Admin identity fallback** — scoped credentials and the legacy
   `GATEWAY_ADMIN_TOKEN` coexist; falling back to the token-only path is a
   configuration revert, not a schema change. Credential rows are retained.
5. **OTLP off** — `GATEWAY_OTLP_ENABLED=false` removes only the exporter.
6. **Preserve data** — job rows, results, ledger rows, and archives are
   never deleted by a rollback. Retention only ever removes rows whose
   archive manifest verified first; ledger-anchored jobs are untouchable
   until their billing evidence ages out.
7. **Revert runtime components last** — binary/schema rollbacks after the
   flag steps; migrations were walked down-boundary in the release gate, but
   a forward-fix is always preferable once V1.4 rows exist.

## 6. Monitoring and incident quick reference

- **Readiness (`/readyz`)**: `database`, `redis`, `catalog`, `queue`,
  `worker`, `settlement` — each present only while its feature is enabled;
  required failures withdraw readiness and the body names the degraded
  dependency. The settlement check trips on reserved-ledger backlog past
  `GATEWAY_SETTLEMENT_BACKLOG_MAX` (workers stuck or ledger writes failing).
- **Alert-worthy series** (see observability.md for the full list): queue
  depth/oldest age, queue wait p95, `jobs_total{status=failed,cancelled}`,
  `lease_expired_total` bursts, settlement failure/retry counters,
  `cost_unknown` rate, budget denials, results expired, admin auth
  rate-limit events.
- **Known-failure postures**: a database outage fails jobs closed as
  unavailable (never terminal job failures) and readiness withdraws; a Redis
  outage fails limiter/quota/budget closed with 503 envelopes, never 429; a
  settlement failure keeps the ledger row `reserved` as retryable evidence
  and is never silently marked settled.

## 7. Client integration notes

- Sync behavior is unchanged; `background:true` is the only opt-in. Clients
  should send `Idempotency-Key` on background creations (same key + same
  request replays the original job; different request → 409
  `idempotency_conflict`).
- Poll `GET /v1/responses/{id}` (owner or admin-viewer credentials only);
  queued/running responses carry `Retry-After`; queries never trigger
  execution. Expired results answer 410 `response_expired` and cannot be
  re-executed.
- Cancellation is idempotent and owner-only; the final observable state is
  returned on repeat calls.
- New stable error codes: `idempotency_conflict`, `response_not_found`,
  `response_expired`, `pricing_unavailable`, `budget_exceeded`,
  `job_queue_unavailable`. Existing V1.3 codes are unchanged; breaking
  changes go to `/v2` (see api-versioning.md).

## 8. Release-gate sign-off

- [x] Full acceptance matrix green (permissions, fault injection, load
      baselines, migration up/down walk, container smoke, zero skips).
- [x] V1.3 goldens byte-identical; flags-off deployment behaves as V1.3
      (rehearsed in `internal/e2e/async_pg_test.go`).
- [x] Capacity thresholds established and exported as scrape-time metrics
      (worker count, lease/heartbeat, result size, polling, settlement
      backlog, readiness).
- [x] Staged rollout + rollback rehearsal completed without lost jobs,
      duplicate ledger finalization, budget oversell, or data leakage.
