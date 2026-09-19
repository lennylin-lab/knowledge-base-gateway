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


## Session 5: Startup hardening and chat-path smoke coverage
<!-- trellis-session: v=2 fp=8dc2e01a98aad0ef -->

**Date**: 2026-09-15
**Task**: Startup hardening and chat-path smoke coverage
**Branch**: `master`

### Summary

Closed the three deferred observations from production-readiness. (1) Database connect errors are now classified (credentials rejected / unreachable / invalid string / other) and never wrap the pgx error, so no DSN/password/host/user/db substring reaches logs — pinned by hermetic and real-credential tests, with live container runs verified by grep. Supporting fix: pgstore.Connect now pings eagerly (pgx pools are lazy, so the unsanitized error previously surfaced at first query); the lazy-pool lesson was captured in database-guidelines. (2) Listener goroutines no longer call os.Exit: both listeners bind synchronously and serve pre-bound sockets, a serve failure cancels runCtx and converges on one graceful shutdown, exiting non-zero through run() — test-pinned for the port-in-use case. (3) The container smoke now covers the full request path: probes the admin API with a throwaway token, mints a key, asserts a non-streaming chat.completion envelope from the seeded fake provider (gateway-echo), revokes the key; new exit codes 10/11; zero token/key leakage in output or logs. Enabling fix found en route: database mode never loaded access_policies grants, so every DB-mode chat returned 403 — LoadGrants added and pinned end-to-end. All gates green: -race suite, env-gated tests zero-skip against real PostgreSQL/Redis, full smoke exit 0. New deferred observation: cmd/migrate fatal() can echo DSN-derived connect-error text; recommend the same classification approach in a follow-up.

### Git Commits

| Hash | Message |
|------|---------|
| `bb3816a` | fix: sanitize database connect errors and structure listener shutdown |
| `25ea579` | feat: exercise admin key lifecycle and chat path in container smoke |
| `821f36e` | docs: update smoke docs and capture lazy-pool lesson in backend specs |

### Status

[OK] **Completed**


## Session 6: Migrate CLI DSN echo sanitization
<!-- trellis-session: v=2 fp=e6cdf00ea91036c9 -->

**Date**: 2026-09-15
**Task**: Migrate CLI DSN echo sanitization
**Branch**: `master`

### Summary

Closed the last deferred observation: cmd/migrate's fatal("connect: %v") could echo DSN-derived error text. The connect-failure classifier moved from the gateway binary into the shared internal/dberr package (with leak-prevention unit tests covering pgconn's DSN-embedding message shapes), cmd/migrate now classifies at the connect fatal, and newMigrator pings eagerly with a 10s bound so lazy database/sql failures funnel into the sanitized path instead of surfacing from Up/Version with DSN-bearing text. Verified live: bad-host DSN -> unreachable classification, wrong-password DSN -> credentials classification, zero DSN components in either output; healthy path (version 2) unchanged; gofmt/vet/full suite green. database-guidelines updated to note that both pgstore.Connect and the migrate CLI follow the eager-ping-plus-classifier pattern.

### Git Commits

| Hash | Message |
|------|---------|
| `e6b371c` | fix: classify migrate connect errors through shared dberr package |

### Status

[OK] **Completed**


## Session 7: V1.2 unified protocol and developer platform
<!-- trellis-session: v=2 fp=f6477cfde0a1b307 -->

**Date**: 2026-09-15
**Task**: V1.2 unified protocol and developer platform
**Branch**: `v1.2`

### Summary

Implemented the V1.2 developer platform on branch v1.2 (base master): unified provider-neutral domain layer (internal/model) and provider boundary with an offline shared contract suite for both adapters plus a deterministic fake mock provider; POST /v1/responses with SSE event protocol and gateway-owned IDs; GET /v1/models discovery with caller-filtered capability/limit metadata; admin management endpoints (audit query with P50/P95 usage aggregates, model/provider/policy views, management-audited enable/disable) backed by internal/mgmt and PostgreSQL; migration 0003 (llm_requests.protocol, capability declarations, admin_audit). V1/V1.1 wire compatibility locked by golden fixtures. Review found and the follow-up fixed an auth-after-decode ordering regression (401 must precede any body read) — auth is now structurally outside the shared admission pipeline and pinned by auth_order_test.go; check also fixed non-compiling Python SDK examples and a missing config-default assertion. Validation: 15/15 packages pass under -race with env-gated tests zero-skip against real PostgreSQL/Redis (migration to version 3 up/down verified), container smoke green including /v1/models and /v1/responses phases. Deferred: Anthropic structured output stays catalog-gated until a translation exists; SDK compatibility pass with a real openai-python install; streaming usage not yet settled into quotas (conservative reservation retained).

### Git Commits

| Hash | Message |
|------|---------|
| `af0d60a` | feat: add unified model protocol, discovery, and management platform |
| `a09111b` | docs: add v1.2 quickstart, api versioning, examples, and smoke phases |
| `556b5e7` | docs: capture v1.2 pipeline and fixture conventions in backend specs |

### Status

[OK] **Completed**


## Session 8: Resolve issue #1 v1.2 follow-up gaps
<!-- trellis-session: v=2 fp=7973181a5d810680 -->

**Date**: 2026-09-16
**Task**: Resolve issue #1 v1.2 follow-up gaps
**Branch**: `master`

### Summary

Completed the resolve-v1-2-followup parent and all three children, closing every one of the 12 issue #1 requirements with zero deferrals. (1) protocol-streaming: tool bounds enforced at shared admission for both protocols; /v1/responses accepts exactly one JSON document; OpenAI clean-EOF-without-[DONE] is a transport truncation with no completed events; Anthropic absent usage stays unknown (never fabricated zeros); streamed final-output validation emits response.failed for responses and audit-visible schema_validation_failed for chat — check caught that built-in adapters wrap emit errors as ClassInternal, so the encoder now records the validation failure for audit classification, pinned with real-adapter tests. (2) quota-provider-safety: input estimate checked against subject max_input_tokens / model context ceiling before rate/quota/provider; SSRF blocklist covers loopback/private/link-local/cloud-metadata IPs with IPv4-mapped-IPv6 canonicalization and a narrow dev allowance; router permits are now taken lazily one per attempt (AdmitRoute/EnabledRoutes) after check found half-open permit starvation wedged backup recovery forever. (3) management-replay-finish: SetModelEnabledWithAudit commits mutation+audit in one transaction with runtime refresh boundary (update_failed vs refresh_failed semantics); /admin/providers gains breaker state/health/24h error summary, /admin/usage gains error_rate plus staged null cost/first-token fields; offline cmd/replay with 12 deterministic fixtures; archived PRD TBD cleanup. Issue #1 updated with two public comments mapping all items to commits; parent validation passed all repo gates (race suite zero-skip, container smoke, replay 12/12, secret scan clean). Deferred notes: DNS-name resolution outside SSRF validation scope (documented boundary); wire replay into a CI step; openai-python SDK compatibility pass.

### Git Commits

| Hash | Message |
|------|---------|
| `947e12f` | fix: close v1.2 protocol admission, decoding, and stream correctness gaps |
| `0f2dd2c` | docs: capture stream audit classification and decoding lessons in backend specs |
| `ab5b291` | chore: add v1-2-protocol-streaming task artifacts |
| `8d4755d` | fix: enforce input token ceilings and harden provider URL safety |
| `6221be5` | fix: take route breaker permits lazily per attempt to prevent probe starvation |
| `e9c3540` | docs: document input ceilings and per-attempt routing in README and specs |
| `a06da87` | chore: add v1-2-quota-provider-safety task artifacts |
| `81f31e2` | feat: make admin model toggles atomic, audited, and live-applying |
| `5a4cce6` | feat: add offline replay command with deterministic provider fixtures |
| `fe0a359` | docs: document management refresh semantics and staged metrics |
| `3ca7f66` | docs: capture atomic management mutation conventions in backend specs |
| `84b7d8e` | chore: clean stale placeholder from archived v1.2 prd and add task artifacts |

### Status

[OK] **Completed**


## Session 9: Streaming pipeline completion and SDK/CI verification
<!-- trellis-session: v=2 fp=7f4a376d26329bd7 -->

**Date**: 2026-09-16
**Task**: Streaming pipeline completion and SDK/CI verification
**Branch**: `master`

### Summary

Closed the remaining staged gaps in two tasks. (1) streaming-structured-output-pipeline: Anthropic structured output translated via the synthesized forced-tool pattern (schema as input_schema, forced tool_choice), unwrapped to protocol-independent text with per-model capability gating; streaming usage settles quotas exactly once (OpenAI streams request stream_options.include_usage, terminal usage chunk parsed; Anthropic message_delta flows through; unknown stream usage keeps the conservative reservation, never fabricated); first_token_millis recorded via additive migration 0004 with true percentiles in /admin/usage PostgreSQL mode; cost stays staged null per product decision (pricing columns deferred until real price data exists). Check pass added the missing chunk-usage sentence to the client contract and a test pinning the outbound include_usage. (2) ci-replay-sdk-verification: replay fixtures now an explicit CI gate step; real openai-python 3.5.0 compatibility matrix (7 scenarios, all PASS) run against a live dev gateway from the sibling venv read-only — two native-shape incompatibilities (flat Responses tool object, text.format) fixed additively in /v1/responses decoding with inner strict decoding preserved (unknown fields, trailing JSON, conflicting spellings all still 400, zero provider calls on rejection); SDK version and required client config documented in quickstart/api-versioning; decoder-compatibility convention captured in specs. All gates green: -race suite, env-gated zero-skip against real PostgreSQL/Redis, migration lifecycle to version 4, replay 12/12, actionlint clean, golden fixtures byte-identical. Remaining deliberate staging: cost pricing columns await real pricing data.

### Git Commits

| Hash | Message |
|------|---------|
| `5c851a9` | feat: translate Anthropic structured output via forced-tool pattern |
| `bef7faf` | feat: settle streaming usage into quotas exactly once |
| `a120c2f` | feat: record first-token latency with additive migration 0004 |
| `a2ee970` | docs: document stream usage settlement and structured output |
| `2bc8582` | docs: capture stream settle and structured-output conventions in specs |
| `fa37a52` | chore: add streaming-structured-output-pipeline task artifacts |
| `de4ba86` | ci: run offline replay fixtures as a workflow gate |
| `d97693d` | feat: accept OpenAI SDK native tool and text.format shapes in /v1/responses |
| `736edbb` | docs: record openai-python 3.5.0 compatibility matrix and native shapes |
| `8a7ce37` | docs: capture additive decoder compatibility convention in specs |
| `03bfd4d` | chore: add ci-replay-sdk-verification task artifacts |

### Status

[OK] **Completed**


## Session 10: Model control plane, integrations docs, and issue triage
<!-- trellis-session: v=2 fp=f11dfa88bc8fa9e5 -->

**Date**: 2026-09-16
**Task**: Model control plane, integrations docs, and issue triage
**Branch**: `master`

### Summary

Shipped the model-control-plane task (issue #4 features 1-3) and the per-provider-credentials task (issue #5), plus integration docs and two integration-issue verifications. (1) model-control-plane: POST /v1/embeddings OpenAI-compatible proxy with capability-gated embeddings/embedding_dim (directory attribute; dim mismatch fails loud 500 embedding_dim_mismatch), shared admission pipeline with same-pool quota settle-to-usage, GATEWAY_EMBEDDINGS_ENABLED rollback flag, deterministic fake embeddings + openai adapter in the contract suite; migration 0005 (access_policies.default_model + default_embedding_model nullable FKs, model_catalog.retrieval_profile JSONB) with protocol-aware default backfill in admission and admin default-model mutation through the atomic mgmt-audit path; retrieval_profile surfaced in /v1/models/{model}; user decisions: dual slots day one, shared quota pool, model-level profiles. Check pass: zero defects. (2) Integration docs written into knowledge-base-server/docs: gateway-v1.3-integration.md (new) and a v1.3 pointer in gateway-v1.2-integration.md. (3) Issue triage: #2 retest feedback fixed (tool-call identity only on the opening stream fragment; encoder no longer repeats id/name; two-way regression pins) and #3 fixed (fake stream shards on rune boundaries; multi-byte UTF-8 regression tests; spec now requires multi-byte fixture content). (4) per-provider-credentials: credentialEnvName/resolveCredential resolve <KIND>_API_KEY__<PROVIDER_NAME> with kind-level fallback for openai+anthropic; both-missing startup failure names variables without values; double-openai-upstream deployment unblocked. Verified: closed issues #2/#3/#5 with evidence comments. Open follow-ups from the latest triage (NOT yet fixed): issue #6 embeddings drops the dimensions parameter (fix: inject catalog embedding_dim upstream) and issue #7 LoadLimits row collapsing makes any NULL default_model on the winning row kill subject backfill (fix: first-non-empty-wins; ceilings collapsing semantics flagged for a decision).

### Git Commits

| Hash | Message |
|------|---------|
| `1137abc` | feat: add embeddings proxy, subject default models, and retrieval profiles |
| `03d8bd8` | docs: document embeddings, default models, and retrieval profiles |
| `03d0fc6` | docs: capture capability-gating principle in backend specs |
| `4841408` | chore: add model-control-plane task artifacts |
| `16a37cd` | feat: resolve provider credentials per provider with kind-level fallback |
| `7c48eae` | docs: document per-provider credential env convention |
| `33f0eb0` | chore: add per-provider-credentials task artifacts |

### Status

[OK] **Completed**


## Session 11: Control plane follow-ups: dimensions, defaults, ceilings
<!-- trellis-session: v=2 fp=efd8deb2c921fe79 -->

**Date**: 2026-09-17
**Task**: Control plane follow-ups: dimensions, defaults, ceilings
**Branch**: `master`

### Summary

Closed the three v1.3 rollout issues in one task. (1) issue #6: the gateway now injects the catalog-declared embedding_dim as the dimensions parameter into upstream openai embeddings requests (client-passed dimensions is structurally ignored, never forwarded); MRL upstreams (e.g. Qwen3) return the declared width -> 200, dimensions-ignoring upstreams still fail loud 500 embedding_dim_mismatch; injection and CheckEmbeddingDim share one catalog lookup so they cannot drift. (2) issue #7: LoadLimits folds multi-row subject policies through the shared policy.Limits.FoldPolicyRow — default_model/default_embedding_model take the first non-empty value in id order (slots independent), so a NULL on one row no longer erases defaults declared on another; pg and memory modes share the fold function. (3) issue #8 (server decision: short-term option 1, mid-term option 4): ceilings changed from last-row-wins to min-of-declared — NOT NULL columns min over all rows, nullable caps constrain only when declared, order-independent, single-row identity, adding a row can never raise a quota; /admin/policies rows gained an effective_limits block computed via LoadLimits (view/enforcement no drift, metadata-only); README documents the upgrade note that existing multi-row subjects' quotas may tighten. Spec: multi-row folding convention recorded in database-guidelines; folding decision point isolated in FoldPolicyRow for the future option-4 migration. Verified: issues #6/#7/#8 closed with evidence comments; gofmt/vet clean; -race suite with real PostgreSQL/Redis zero skips; replay 12/12; golden fixtures byte-identical.

### Git Commits

| Hash | Message |
|------|---------|
| `054454a` | fix: inject catalog embedding_dim into upstream embeddings requests |
| `c6ade45` | fix: fold multi-row subject policies with first-non-empty defaults |
| `e2ad49d` | docs: record multi-row policy folding semantics in backend specs |
| `4236be5` | chore: add control-plane-followups task artifacts |
| `86a3bed` | feat: fold policy ceilings to min-of-declared with effective view |
| `284f2ec` | docs: document min-of-declared folding and upgrade note |
| `5adbed5` | chore: record ceilings decision in task prd |

### Status

[OK] **Completed**


## Session 12: V1.4 async operations roadmap: seven children delivered
<!-- trellis-session: v=2 fp=c7d2420f2c770533 -->

**Date**: 2026-09-19
**Task**: V1.4 async operations roadmap: seven children delivered
**Branch**: `master`

### Summary

Delivered the complete V1.4 async-operations roadmap through seven checked children under the parent task. (1) compat-migrations: V1.3 wire frozen behind extended golden fixtures; schema foundation 0006-0009 (async jobs/results/idempotency, pricing/ledger/budgets, admin credentials/scopes, lifecycle metadata) with constraint-encoded invariants proven by DB-level rejection tests, every down boundary walked, version-5 upgrade path proven; spec gained the migration-batch lifecycle convention. (2) async-responses: POST background jobs with a closed 6-state CAS machine, lease/heartbeat/recovery workers, subject-scoped idempotency hashing, exactly-once local state with documented at-least-once upstream effects, GATEWAY_ASYNC_ENABLED rollback posture; check-pass fixes pinned terminal audit correlation IDs, the poison-snapshot path, and requeue backoff/attempt exhaustion (migration 0011). (3) cost-governance: integer-micros pricing catalog, usage ledger with exactly-once settlement, monetary budgets enforced atomically across instances (same-pool decision), enforcement-off retaining ledger capture, and the child-2 handoff resolved — cancel-after-output settles known usage on the loser path. (4) admin-rbac: scoped admin identities (hashed credentials, plaintext once), a single route-to-scope matrix, subjects.tenant_id as sole tenant authority, legacy token as bootstrap platform-admin; check fixed a real tenant-boundary gap in key creation. (5) data-lifecycle: verify-before-delete archive sweeps, legal holds, tenant-scoped exports with management-log platform-scope, KeyTTL enforcement closing the child-2 debt, cmd/maintain; plus a pinned race regression in pg Create. (6) observability: OTLP with independent kill switch, trace context persisted through jobs, dependency readiness checks; check fixed an always-200 span status fabrication. (7) rollout-validation: real HTTP+PG+worker staged rehearsals found two production-only defects (jobs sweep never converged against the ledger FK; result-expiry sweep SQL rejected by PostgreSQL) — both fixed with bite-proven regressions; 16-step cancel-vs-commit matrix clean; release checklist in docs/release-v1.4.md. All gates green throughout: -race suites with real PostgreSQL/Redis zero skips, migration lifecycle to version 12, replay 12/12, golden fixtures byte-identical, container smoke passing.

### Git Commits

| Hash | Message |
|------|---------|
| `181c524` | test: freeze v1.3 wire behavior behind golden fixtures |
| `f034816` | feat: add v1.4 schema foundation with constraint-encoded invariants |
| `2b9cf06` | docs: record migration batch lifecycle conventions in backend specs |
| `80e357a` | feat: add async responses job lifecycle with lease-based workers |
| `c9bae23` | feat: gate async operations behind flags and config knobs |
| `05ee89d` | feat: add async job payload and requeue backoff migrations |
| `e2a1f6a` | docs: record async exactly-once state conventions in backend specs |
| `78d91dc` | feat: add integer-micros accounting with pricing and budget gates |
| `f209237` | feat: integrate money reservation and settle into all serving paths |
| `8045fe3` | docs: document ledger settle-on-cancel exception and budget operations |
| `fe07b4d` | feat: merge ledger cost into admin usage view |
| `3409a9e` | feat: add scoped admin identities with hashed credential lifecycle |
| `843eea3` | feat: enforce admin scope matrix and tenant boundaries across admin routes |
| `94ccd43` | docs: add admin RBAC reference and update operations guide |
| `c2f6903` | docs: capture admin RBAC matrix and tenant authority conventions in specs |
| `c254df6` | feat: add retention sweeper with verify-before-delete archive lifecycle |
| `3386778` | feat: enforce idempotency key TTL and expose lifecycle admin endpoints |
| `b32f8be` | docs: add data lifecycle runbook and record sweep discipline in specs |
| `3de9e9e` | feat: add opentelemetry tracing with independent kill switch |
| `096ccef` | feat: propagate trace context through async jobs and serving spans |
| `3ac0a40` | feat: add dependency readiness checks and async/cost telemetry |
| `37b347c` | docs: add observability reference and record span status convention |
| `21c85ff` | fix: make lifecycle sweeps converge against ledger-anchored jobs |
| `4c6f152` | fix: correct async result-expiry sweep SQL for postgres |
| `66a9424` | test: add staged rollout and rollback rehearsal over real services |
| `6027535` | docs: add v1.4 release checklist |
| `aec717a` | docs: record release rehearsal conventions in backend specs |

### Status

[OK] **Completed**
