# Journal - lenny (Part 1)

> AI development session journal
> Started: 2026-09-14

---



## Session 1: Implement Go LLM gateway MVP
<!-- trellis-session: v=2 fp=1aa32764ee76dce8 -->

**Date**: 2026-09-14
**Task**: Implement Go LLM gateway MVP
**Branch**: `master`

### Summary

Implemented the first-phase Go LLM gateway MVP per docs/agent-start.md: OpenAI-compatible chat + SSE endpoints, salted-hash API key auth, catalog/policy authorization with non-leaky 403, bounded pre-output retries under a total request deadline, per-subject rate/concurrency limits, metadata-only audit, health/ready/metrics endpoints, and initial SQL schema. trellis-check found and fixed an unapplied request timeout (unbounded upstream hangs); specs updated with deadline and error-mapping conventions. Deferred: PostgreSQL/Redis store implementations, readyz store checks, Dockerfile, migration test harness.

### Git Commits

| Hash | Message |
|------|---------|
| `3810cf5` | feat: implement Go LLM gateway MVP |
| `833f5ab` | docs: capture gateway error-mapping and deadline conventions in backend specs |
| `fcb36ae` | chore: add llm-gateway-mvp task artifacts and zcode config |

### Status

[OK] **Completed**


## Session 2: V1.1 production hardening and infra library adoption
<!-- trellis-session: v=2 fp=e28594fa8820c8e1 -->

**Date**: 2026-09-15
**Task**: V1.1 production hardening and infra library adoption
**Branch**: `master`

### Summary

Two tasks completed. (1) gateway-v1-1-production: PostgreSQL key/catalog/policy/audit persistence with admin key lifecycle API, Anthropic adapter with SSRF-validated base URLs, primary/backup routing with circuit breaker and pre-output failover, Redis rate/concurrency leases with readiness gating, trace-ID propagation, e2e client contract tests; check pass fixed Redis-outage-mapped-to-429 by widening limiter.Gate with ErrUnavailable -> 503 limiter_unavailable. (2) replace-duplicated-infrastructure: adopted prometheus/client_golang (custom registry + duration histogram), cenkalti/backoff v5 (bounded exponential + jitter), failsafe-go behind the router breaker adapter, atomic ZREMRANGEBYSCALE stale-lease pruning in the Redis Lua script, and golang-migrate v4 via cmd/migrate CLI with versioned up/down files and advisory locking; also fixed latent migration-test path bug. Deferred: CI run of migration/Redis tests against real services (TEST_DATABASE_URL/TEST_REDIS_ADDR), docker container smoke test, daily/monthly token quota enforcement.

### Git Commits

| Hash | Message |
|------|---------|
| `74814da` | feat: add v1.1 production persistence, failover, and admin lifecycle |
| `3b58d9f` | feat: wire v1.1 routing, limits, and config into gateway core |
| `1edd313` | docs: capture limiter outage and failover conventions in backend specs |
| `8804840` | chore: add gateway-v1-1-production task artifacts |
| `822a4ba` | refactor: adopt prometheus client, backoff, failsafe breaker, and redis lease pruning |
| `9c0c3f1` | feat: add golang-migrate CLI and versioned migration files |
| `16d3339` | docs: document adopted infrastructure dependencies in specs and README |
| `d990d7e` | chore: add replace-duplicated-infrastructure task artifacts |
| `016f396` | test: add metrics registry and histogram tests |

### Status

[OK] **Completed**


## Session 3: Real-service integration verification
<!-- trellis-session: v=2 fp=36e04055a300646f -->

**Date**: 2026-09-15
**Task**: Real-service integration verification
**Branch**: `master`

### Summary

Ran the previously-skipped integration tests against the real PostgreSQL/Redis from ../knowledge-base-server docker compose (dedicated kb_gateway_test database, app data untouched). Found and fixed two latent bugs: (1) pg ResolveAuth returned a zero Principal with nil error for unknown keys (rows.Err() is nil on zero matches) — now returns auth.ErrInvalid, and digest comparison switched to subtle.ConstantTimeCompare; (2) the 0002 down migration hit FK violations rolling back because api_keys/llm_requests created against the seed subject outlived the filtered deletes — down script now removes dependents before seed parents, preserving non-seed audit data. Verified: pg up/down/store test, real-Redis limiter test, cmd/migrate CLI up/version/steps/up, full go build/vet/test green. Lessons captured in backend specs; kb_gateway_test database left in place for future env-gated runs.

### Git Commits

| Hash | Message |
|------|---------|
| `c1abae4` | fix: return ErrInvalid for unknown keys in pg authenticator |
| `2688f0c` | fix: remove dependent rows before seed parents in 0002 down migration |
| `4f78361` | docs: capture real-service integration test lessons in backend specs |

### Status

[OK] **Completed**
