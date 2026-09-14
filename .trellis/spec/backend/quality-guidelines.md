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
client disconnect must not lose accounting. Streaming usage is not parsed yet,
so successful streams keep the conservative reservation until stream usage is
surfaced. A crash between reserve and finalize leaves the reservation charged
until period rollover (accepted, documented in README).

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

### Convention: Failover routing stays behind interfaces

**What**: Provider failover (primary/backup route table + circuit breaker in `internal/router`) is selected before any provider call; streaming never switches providers after output starts; retries stay bounded under the total request deadline.

**Why**: Lets fault tests (`internal/gateway/failover_test.go`) pin the semantics and keeps vendor protocols isolated in `internal/provider`.

