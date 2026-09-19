# V1.4 async inference and operations governance

## Goal

Deliver durable asynchronous Responses, explainable monetary governance, scoped
administration, and production data lifecycle controls without changing V1.3
synchronous behavior or exposing provider-private data.

## Background and Confirmed Facts

- The source requirement is `docs/v1.4-operations-and-async-roadmap.md`.
- V1.3 migration version is 5; new migrations must be additive, paired with
  downs, and applied only by `cmd/migrate`.
- Authentication produces subject and tenant identity before request parsing.
  Admission centralizes model authorization, capability checks, limits, and
  token quota reservation.
- PostgreSQL is authoritative for credentials, policy, catalog, and audit.
  Redis provides multi-instance atomic counters and must not be the only copy
  of durable job or ledger state.
- Provider responses are normalized before public HTTP responses; content,
  secrets, upstream models, and URLs are excluded from logs and admin views.

## Requirements

- Preserve all V1.3 Chat, Responses, Embeddings, model discovery, default-model,
  retrieval-profile, capability, error-envelope, request-ID, and SSE contracts.
- Add durable background Responses with six documented states, ownership
  isolation, idempotency, recoverable worker leases, bounded results, expiry,
  cancellation, and exactly one terminal accounting outcome.
- Add versioned integer-micro-unit pricing, usage/cost ledger records, and
  subject/tenant daily/monthly budgets with explicit unknown cost semantics.
- Replace the single all-powerful production admin token with hashed,
  lifecycle-managed credentials and scoped roles while retaining the
  environment token as a compatibility bootstrap.
- Add TTL, archival, partition maintenance, redacted export, operational
  telemetry, dependency readiness, and graceful worker shutdown.
- Roll out async jobs and monetary budgets behind independent model/tenant
  controls and validate fake plus two real adapter classes.

## Task Map and Ordering

1. `09-19-v1-4-compat-migrations`: freeze compatibility and establish schema.
2. `09-19-v1-4-async-responses`: durable job lifecycle; depends on 1.
3. `09-19-v1-4-cost-governance`: shared accounting; depends on 1 and integrates
   with synchronous paths and child 2.
4. `09-19-v1-4-admin-rbac`: scoped management identity; depends on 1 and
   protects management surfaces added by 3 and 5.
5. `09-19-v1-4-data-lifecycle`: lifecycle controls over schemas from 1-4.
6. `09-19-v1-4-observability`: telemetry and process lifecycle integrated with
   children 2-5 after their contracts stabilize.
7. `09-19-v1-4-rollout-validation`: final real-infrastructure and rollout gate.

## Acceptance Criteria

- [ ] Every child task passes its own acceptance criteria in the stated order.
- [ ] Duplicate submission, restart, timeout, queue failure, lease expiry, and
  cancellation races preserve a valid job state and one final ledger outcome.
- [ ] Job/result/usage/admin queries cannot cross subject or tenant boundaries;
  secrets and model content never appear in logs or exports.
- [ ] Monetary arithmetic is integer-only and reproducible from usage plus the
  persisted price version; missing usage or price remains unknown, never zero.
- [ ] Subject and tenant budgets do not oversell under multi-instance load;
  infrastructure outages return service failures rather than budget denials.
- [ ] Admin scope matrix, credential lifecycle, audit atomicity, TTL/archive,
  partitions, export, metrics, tracing, readiness, and shutdown are tested.
- [ ] Unit, race, migration, real PostgreSQL/Redis, provider contract, fault,
  load, replay, and container smoke gates pass.

## Out of Scope

- Agent loops, tool execution, business-database access, billing/payment,
  invoices, recharge, WebSockets, webhooks, batch APIs, cross-region
  active-active, end-user UI, or broad provider expansion.

## Key Decisions

- PostgreSQL is the durable queue and ledger source of truth; workers claim
  jobs through conditional row updates and leases. Redis remains the atomic
  budget counter and may optimize wakeups, but losing Redis cannot lose a job.
- Async creation reuses synchronous authentication and admission semantics.
- Child tasks are independently reviewable and revertible; the parent tracks
  integration and is not a monolithic implementation target.

## Risks and Deferred Items

- Partitioning populated `llm_requests` needs an online migration rehearsal;
  destructive table replacement is prohibited.
- External provider effects cannot be exactly-once. The guarantee is one active
  local lease plus idempotent finalization, using provider idempotency if offered.
- Currency conversion and mixed-currency aggregation are deferred.

## Planning Status

- Complex parent task with seven planned children.
- Blocking product questions: none; the roadmap fixes scope and compatibility.
- Implementation requires approval of this final planning summary.
