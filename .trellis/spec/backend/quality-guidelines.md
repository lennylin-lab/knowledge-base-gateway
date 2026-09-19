# Backend Quality Guidelines

This is an early Go gateway with no implementation or CI yet. New code must preserve `docs/agent-start.md` and keep the service startable and testable.

Run `gofmt` and `go test ./...`; add static analysis when the Go module is introduced. Test request validation, authentication, policy decisions, error mapping, retry/deadline behavior, SSE termination, cancellation, and redaction. Use interfaces at provider, store, clock and limiter boundaries.

Do not add heavyweight frameworks without a documented need, expose provider SDK types through HTTP, accept client-supplied upstream URLs, store plaintext keys, or use unbounded retries/goroutines. Do not modify `../knowledge-base-server` for gateway-only work.

Review auth-before-provider ordering, stable error envelopes, IDs, bounded resources, stream cancellation, migration coverage, and provider failure paths.

### Convention: Mature infrastructure behind local interfaces

**What**: Standard infrastructure behavior is delegated to small, pinned
libraries — Prometheus exposition to `prometheus/client_golang` (custom
registry in `internal/metrics`), retry delay generation to
`cenkalti/backoff/v5` (bounded exponential + jitter), and breaker state
transitions to `failsafe-go/circuitbreaker` (`TryAcquirePermit` /
`RecordSuccess` / `RecordFailure` in `internal/router`). Redis limiter tests
use `alicebob/miniredis/v2` (hermetic, supports EVAL/Lua) for fault injection.
Domain boundaries (provider adapters, repositories, route ordering, error
classification) stay hand-written in this repo behind local interfaces.

**Why**: Hand-written infrastructure accrues subtle bugs (label escaping,
backoff timing, half-open races); swapping adapters behind local interfaces
keeps domain semantics pinned by tests while letting implementations revert
independently.

**Boundary**: Never expose library types through HTTP handlers or provider
interfaces; adapters translate at package edges. Retry classification
(`provider.RetryEligible`), the total request deadline, and the streaming
no-switch rule remain owned by `internal/gateway`.

### Common Mistake: Struct-literal refactor drops a config default

**Symptom**: Everything returns 429 from the first request after an unrelated config refactor; unit tests that never assert the default don't catch it.

**Cause**: Rewriting `FromEnv` to a struct literal silently deleted `MaxConcurrent: 8`; the zero value made the concurrency check deny on first admission. Found only by the check pass, not by tests.

**Fix / Prevention**: When refactoring config construction, assert every field with a meaningful default is non-zero in the happy-path test (see `TestFromEnvValid`); zero values that mean "deny all" or "unbounded" must never be reachable from `FromEnv`.

### Convention: CI migration and env-gated test contract

**What**: `.github/workflows/ci.yml` runs quality gates, a migration
lifecycle (up → version → down → up) against a per-run disposable database
(`kb_gateway_ci_test`, never shared/developer/production), and the env-gated
integration tests with a hard gate: any `--- SKIP` in the integration step
fails the job.

**Why**: Destructive `down` must never reach a shared database; the skip-gate
prevents env-gated tests from silently rotting into always-skipped.

**Boundary**: Adding a migration changes the expected version asserted in the
lifecycle step; adding a new env-gated test requires adding its package to the
integration step's package list. Provider secrets are masked (`add-mask` runs
first) and never echoed; service credentials are job-local.

### Convention: Container stack runs migrations one-shot, never the process

**What**: `docker-compose.yml` runs the schema migration as a dedicated
one-shot `migrate` service (`cmd/migrate up`, gated by
`service_completed_successfully`) before the gateway starts; the gateway
container never touches schema. Host ports bind to `127.0.0.1` only; the
runtime image is non-root with a numeric `USER` and an empty `Env` (DSN and
credentials are compose-run config only, never baked into layers).

**Why**: Startup schema mutation hides drift (see database guidelines); baked
DSNs leak into image history; loopback binding keeps dev stacks off shared
networks.

**Boundary**: `scripts/smoke.sh` is the acceptance path — keep per-phase
`--timeout` budgets separate (a shared deadline lets a slow migration starve
the health-check phase into a false failure) and keep exit codes documented.
The smoke never echoes the DSN, including in failure log dumps.

### Convention: Provider URL validation skips the fake provider

**What**: `provider.ValidateBaseURL` (https-only, no userinfo) applies to `openai`/`anthropic` base URLs in both modes, but `fake` rows are exempt — the migration seed data uses `internal://` URLs and the fake provider never dials its base URL.

**Why**: Startup would otherwise reject the seeded database (config incompatible with its own seed data); the exemption is safe because fake never opens a connection.

### Common Mistake: Config timeout loaded but never applied

**Symptom**: `go vet` and tests pass, but a hanging upstream request never returns — the client's context is the only deadline.

**Cause**: `config.RequestTimeout` was read into `Service.Timeout` but `Complete`/`Stream` never called `context.WithTimeout`, so the value was inert.

**Fix**: Every outbound provider path must wrap the request context with the configured total deadline:

```go
ctx, cancel := context.WithTimeout(ctx, s.Timeout)
defer cancel()
```

**Prevention**: Add a test that a handler configured with a tiny timeout returns within it against a deliberately slow provider (see `internal/gateway/service_test.go`).

### Convention: Dev-only stores behind interfaces

**What**: PostgreSQL/Redis are not wired yet; limiter state, key/catalog stores, and audit sinks are in-memory dev substitutes behind `internal/store` interfaces.

**Why**: HTTP/provider logic is testable now; swapping in real repositories later is a drop-in change with no handler edits.

**Boundary**: `/readyz` currently validates config only — it must gain store checks when real `store` implementations land. As of v1.1, `/readyz` pings PostgreSQL and Redis when their modes are enabled; keep those checks mandatory when adding new backends.

### Common Mistake: Zero-match query loop falls through to nil error

**Symptom**: An unknown API key authenticates "successfully" — the handler gets a zero `Principal` with no error and rejects with 403 instead of 401.

**Cause**: `DB.ResolveAuth` returned `rows.Err()` after the scan loop; with zero matching rows `rows.Err()` is `nil`, so not-found returned `(Principal{}, nil)`. Hidden until the env-gated test first ran against real PostgreSQL.

**Fix / Prevention**: After a search loop, return the explicit domain error (`auth.ErrInvalid`) on fall-through and check `rows.Err()` separately. DB-backed authenticators must be exercised against a real database (tests gated on `TEST_DATABASE_URL`/`TEST_REDIS_ADDR` that always skip hide this class of bug), and digest comparisons must use `subtle.ConstantTimeCompare`, not hand-rolled byte loops.

### Convention: Infra failure vs denial in boundary interfaces

**What**: Interfaces with pass/fail semantics (e.g. `limiter.Gate.Allow`) return a distinguishable error (sentinel `limiter.ErrUnavailable`) for infrastructure failure, separate from a plain `false` denial.

**Why**: A Redis outage that fails closed looked identical to a rate limit and was reported to clients as 429 — wrong status, wrong Retry-After semantics, wrong metrics.

**Example**: `ok, retryAfter, release, err := gate.Allow(...)`; check `err` first, map to 503 `limiter_unavailable`; only `ok=false, err=nil` is a 429.

### Common Mistake: dec.More() misses trailing bracket bytes

**Symptom**: A "strict single JSON document" decoder accepts `{"a":1}]` or
other non-whitespace trailing bytes.

**Cause**: `json.Decoder.More()` only reports whether another JSON value
might follow; it does not detect a trailing token that is not a value start.
Concatenated objects are also silently accepted by a second `Decode` unless
EOF is asserted.

**Fix / Prevention**: After the first `Decode`, call `dec.Token()` and require
`errors.Is(err, io.EOF)` — any non-whitespace trailing byte (including `]`,
`}`, or garbage) then fails the decode. See `internal/httpapi/responses.go`.

### Convention: Authentication stays outside the shared admission pipeline

**What**: `internal/httpapi` splits handler admission into `authenticate`
(bearer + key check, run before any body read) and `admit(..., principal)`
(model/policy → capability precheck → clamps → rate limit → quota reserve).
The package header documents the enforced order; `auth_order_test.go` pins
that an invalid key + malformed/oversized body gets 401 before any decode.

**Why**: A V1.2 refactor moved auth inside the post-decode pipeline; the
spec's "401 before any body read" rule regressed silently until review. The
order is structural now, not convention.

**Boundary**: When adding admission stages, never fold authentication into
`admit` — the principal is an input, and every new model-protocol handler
(`chat`, `responses`, future ones) must call `authenticate` first.

### Convention: New serving features are capability-matrix-gated and pre-provider rejected

**What**: Every new model-serving feature (embeddings, retrieval profiles,
future citations/vision-style additions) ships as a capability-matrix
attribute first: a `Capabilities` field in `internal/model`, declared in the
catalog (seeded in a migration), enforced in `model.CheckCapabilities` for its
protocol label, and rejected with the stable `capability_not_supported` 400
before any provider invocation. Endpoint-level rollback switches
(`GATEWAY_RESPONSES_ENABLED`, `GATEWAY_EMBEDDINGS_ENABLED`) un-register the
route independently; per-model enable/disable reuses the audited admin
toggle. Directory-style attributes the gateway declares and clients read
(e.g. `embedding_dim`, retrieval profiles) are additive JSONB/catalog data —
the server reads them from discovery, never the reverse.

**Why**: Capability rejection before the provider keeps un-deployed features
unreachable per model even when the code ships; the matrix doubles as the
rollout mechanism, and audit rows can be correlated with the declaration in
force via `config_version`.

**Boundary**: Never gate a feature only by configuration presence or route
registration — the matrix is the enforcement point. Adapters without native
support declare the capability false and return
`model.ErrCapabilityNotSupported` as defense in depth (see Anthropic
embeddings).

### Convention: Golden fixtures lock wire compatibility before refactors

**What**: `internal/httpapi/testdata/golden/` pins the V1 chat wire shapes
(non-streaming envelope, SSE chunk framing/field order). Regenerate only with
a deliberate contract change, and keep `docs/gateway-client-contract.md` in
the same commit.

**Why**: Wire refactors (e.g. unified protocol translation) can silently
change chunk fields; the fixtures turn that into a test failure. Note the
correct stream chunk `object` is `chat.completion.chunk`.

### Convention: Async job state is exactly-once, upstream effects at-least-once

**What**: `internal/async` owns a closed 6-state job machine where every
transition is a status/lease-keyed CAS (single conditional DB update decides
each race). Exactly one racer (cancel / commit / recover / claim /
heartbeat-loss) performs the one terminal audit + settlement; losers release
their reservation and write nothing — pinned by iteration tests on both
stores. Requeues: retry-expecting paths (transient re-admission, limiter
denial, retryable upstream failure) consume an attempt and delay via
`visible_at` backoff, terminal-failing with a stable class on exhaustion;
infrastructure outages requeue with backoff but **never** consume an attempt
or terminal-fail — an outage is never disguised as a job outcome.

**Why**: Earlier revisions recycled jobs past `MaxAttempts` on timeouts,
lost terminal commits in the drain window (commits rode the cancelled pool
context), and duplicated audit correlation IDs from stale claim copies; the
CAS discipline plus these pins close all three classes.

**Boundary**: Upstream provider effects are at-least-once (a crash between
the upstream call and the terminal commit re-executes the job — documented);
local state and audit are exactly-once. Since cost governance landed, the
one exception to "losers write nothing": when cancel wins after the provider
produced output, the losing worker settles both reservations to the observed
known usage (one settled ledger row; the cancel winner keeps the single
audit) — recovery-loss losers still release. Idempotency `KeyTTL`
enforcement is owned by the data-lifecycle child.

### Convention: Release rehearsals are validation-first and staged

**What**: A rollout-validation task drives a real HTTP + PostgreSQL + worker
end-to-end rehearsal (not mocks) structured as: flags-off = previous-release
behavior (golden byte-for-byte, readiness reflects disabled features) →
per-flag progression → rollback rehearsal of every documented rollback point
(disable + drain, enforcement-off retaining ledger, legacy auth path).
Validation is expected to find defects; fixes ship with regression tests that
demonstrably fail on the old code (temporary reversion), and a tenant/scope
sweep re-classifies every query added during the roadmap.

**Why**: The V1.4 rehearsal surfaced two production-only defects no unit test
could catch (a jobs sweep that could never converge against the ledger FK,
and a sweep SQL rejected by PostgreSQL entirely) precisely because it ran the
whole machine against real services.

**Boundary**: Behavior changes are limited to validation-found defects;
load thresholds map to existing scrape-time metrics with dashboard/alert
mapping in docs rather than ad-hoc load scripts.

### Convention: Provider adapters share one offline contract suite

**What**: `internal/provider/contract_test.go` runs the same scenario set
(text/usage, streaming assembly, cancellation, tools round-trip, structured
output, 400/401/429/500, timeout, malformed) against every adapter, offline.

**Why**: Capability or translation bugs in one adapter can't hide behind the
routing core; a new adapter inherits the suite for free. Capability-unsupported
must be rejected before provider invocation (call counters pin this).
Fixture text must include multi-byte UTF-8 content (e.g. Chinese) — an
ASCII-only suite let a byte-index sharding bug corrupt streamed CJK into
U+FFFD unnoticed (issue #3).

### Convention: Admin RBAC — one matrix, one tenant authority

**What**: Every admin route's scope requirement lives in exactly one place
(`adminRoutePolicy` in `internal/httpapi/admin_auth.go`); unlisted routes
default to platform-admin + global-only. Tenant authorization always derives
from `subjects.tenant_id` via a join — `api_keys.tenant_id` is a label, never
the authority — and tenant-bound principals get non-leaky 403/404 on
cross-tenant access. The legacy `GATEWAY_ADMIN_TOKEN` authenticates as an
explicit bootstrap platform-admin identity so scoped credentials and the
legacy path coexist until production-verified.

**Why**: Two real defects hid in the seams: a tenant-bound admin could mint
API keys labeled with a foreign tenant (booking budgets/async jobs against
it), and audit-details degradation would have silently rewritten
atomicity-fault payloads. Matrix-in-one-place + join-based tenancy made both
greppable.

**Boundary**: Key-creation labels follow the `/admin/admins` rule (empty →
caller's tenant, foreign → uniform 403 before store access); scope
implications are operator/billing ⇒ viewer, platform-admin ⇒ all; failure
throttling is per client host and counts invalid tokens too. `/admin/usage`
is tenant-predicated, `/admin/management-log` is platform-scope (treat as
platform-scope in lifecycle exports).

### Convention: Quota reservation/settlement behind quota.Gate

**What**: Daily/monthly token quotas (`internal/quota`) reserve a deterministic
bounded estimate before provider invocation (declared `max_tokens`, then policy
ceiling, then 4096; input ≈ chars/4) and finalize exactly once after the
response: settle to reported total, or keep the conservative reservation when
usage is unknown — never fabricate zero. Release on pre-output failure is
idempotent (Redis: `SET NX` finalize marker; both period counters adjusted in
one atomic Lua script).

**Why**: Prevents concurrent instances from oversubscribing a period budget
without needing provider tokenizers; exactly-once finalization prevents double
adjustment across settle/release races.

**Boundary**: Settlement runs on a detached context on purpose — a post-response
client disconnect must not lose accounting. Streaming requests now settle too:
encoders attach upstream-reported usage to the terminal success event and the
handler settles exactly once through the same finalize; unknown stream usage
keeps the conservative reservation. A crash between reserve and finalize leaves
the reservation charged until period rollover (accepted, documented in README).

### Convention: Anthropic structured output via the forced-tool pattern

**What**: `response_format: json_schema` for Anthropic models translates to a
synthesized `structured_output` tool (schema as `input_schema`, forced
`tool_choice`), whose tool input is unwrapped as the JSON result — non-streaming
text or streamed text deltas — so downstream `model.ValidateOutput` is
protocol-independent. JSON mode remains unsupported for Anthropic and rejects
before provider invocation.

**Why**: Keeps structured output protocol-independent in the domain layer and
lets the shared offline contract suite pin both dialects with identical
scenarios.

### Common Mistake: Env-gated tests sharing persistent service state

**Symptom**: `TestRedisAllow` fails on the second run against the same Redis
within the lease TTL, passes in isolation.

**Cause**: The test intentionally abandons one concurrency lease; in a
persistent Redis (not miniredis) the abandoned lease occupies capacity for
`DefaultLeaseTTL` (5 min), so cross-run state breaks the next run's
expectations.

**Fix / Prevention**: Env-gated integration tests that write state to a shared
persistent service must namespace keys per run (random prefix) or clean up in
defer; never rely on the service being empty.

### Convention: Management mutations commit atomically with their audit record

**What**: Admin mutations that must be audited (e.g. model enable/disable) run
the catalog update and the `admin_audit` insert in one transaction
(`SetModelEnabledWithAudit`); the handler applies runtime refresh only after
commit via an injected `ApplyModelChange` boundary.

**Why**: An unaudited committed change (or a phantom audit row for a rolled-back
change) breaks the operational evidence chain. Failure semantics are split:
`update_failed` = nothing changed; `refresh_failed` = change committed and
audited but the live process may be divergent — never collapse the two.

**Boundary**: Metrics contracts that cannot be measured yet are staged as
always-present fields that stay `null` (cost, first-token latency) — stable
names so dashboards detect availability, `null` means "not measured", never
fabricated zero.

### Convention: Decoder compatibility is additive, never weaker

**What**: Strict request decoding (`DisallowUnknownFields`, single JSON
document, trailing-byte rejection) stays at the outer boundary. When a real
SDK is verified against the API and sends standard shapes the decoder
rejected (e.g. openai-python 3.5.0's flat Responses tool object and
`text.format`), accept those shapes via dedicated wire types with their own
inner `DisallowUnknownFields`, pinned by tests that prove: both dialects
decode, unknown inner fields still 400, conflicting spellings 400, and zero
provider calls on decode failure.

**Why**: The compatibility pass (openai-python 3.5.0) showed SDKs strip unset
parameters — incompatibilities come from shape dialects, not injected fields;
relaxing the outer boundary would have traded real validation for nothing.

**Boundary**: Every accepted shape must be documented in
`docs/api-versioning.md` with the verified SDK version, and the pass matrix
re-run live before release.

### Convention: Failover routing stays behind interfaces

**What**: Provider failover (primary/backup route table + circuit breaker in `internal/router`) is selected before any provider call; streaming never switches providers after output starts; retries stay bounded under the total request deadline. Route breaker permits are taken lazily, one per attempt (`AdmitRoute` immediately before the provider call, `Record` immediately after); `Resolve` builds the plan permit-free via `EnabledRoutes`.

**Why**: Taking permits upfront orphaned half-open permits — a request that stopped at the primary permanently wedged the backup's recovery probe (failsafe-go has no permit release; faking a Record for an unattempted route would corrupt probe semantics). All-open breakers now reject at execution time with the same 503 `no_route_available` envelope and zero provider traffic.

**Why**: Lets fault tests (`internal/gateway/failover_test.go`, `halfopen_probe_test.go`) pin the semantics and keeps vendor protocols isolated in `internal/provider`.

