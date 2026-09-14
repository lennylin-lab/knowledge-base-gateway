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


## Session 4: Gateway production readiness: config boundary, CI, container smoke
<!-- trellis-session: v=2 fp=2915f9730b0d027e -->

**Date**: 2026-09-15
**Task**: Gateway production readiness: config boundary, CI, container smoke
**Branch**: `master`

### Summary

Completed the gateway-production-readiness parent and all three children. (1) test fix: real-Redis limiter test now uses a per-run namespace and releases its lease, verified with -count=2 back-to-back runs. (2) gateway-production-config: GATEWAY_DATABASE_URL is the config-mode boundary (database mode no longer requires dev GATEWAY_API_KEYS/GATEWAY_MODELS, local mode unchanged); provider secrets load before validation (fixes Anthropic validate-before-load bug); per-kind registry credential checks run pre-listener with non-leaky errors; also fixed fake-provider internal:// URLs being rejected against seed data. Check caught a critical dropped-default regression (MaxConcurrent=0 would 429 all traffic) with regression test added. (3) gateway-ci-real-services: single verify workflow with postgres:17/redis:7.4 health-gated services, add-mask, gofmt/build/vet/test -race, disposable-database migration up/version/down/up with output assertions, env-gated integration tests with a hard skip gate; every run block executed locally against real services. (4) gateway-container-smoke: multi-stage non-root image (USER 10001, empty Env), one-shot migrate service with service_completed_successfully, loopback-only ports, bounded smoke script with outage/recovery phases — full smoke ran green including dependency outage. Parent integration review passed all six acceptance criteria. Deferred observations: database connect error may echo non-password DSN parts; listener goroutine still uses os.Exit(1); smoke does not exercise the chat path.

### Git Commits

| Hash | Message |
|------|---------|
| `1a51a0f` | test: namespace real-Redis limiter test per run |
| `df7d03a` | fix: make database mode the config boundary and validate provider credentials at startup |
| `a1b2222` | test: serialize pg migration test with advisory lock |
| `2c194f7` | docs: document config mode boundary and provider credential rules |
| `446db0a` | docs: capture dropped-default and fake-URL validation lessons in backend specs |
| `d954053` | ci: add real-service workflow with migration lifecycle and skip gate |
| `c84d150` | docs: capture CI migration and env-gated test contract in backend specs |
| `5a663f1` | feat: add non-root container stack with one-shot migration and smoke script |
| `f497ae8` | docs: document container stack and smoke path in README |
| `606be25` | docs: capture container migration conventions in backend specs |
| `d30c51a` | docs: qualify GATEWAY_PROVIDER as local-mode-only in v1.1 table |

### Status

[OK] **Completed**
