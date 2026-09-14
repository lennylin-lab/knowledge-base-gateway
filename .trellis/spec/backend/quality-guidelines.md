# Backend Quality Guidelines

This is an early Go gateway with no implementation or CI yet. New code must preserve `docs/agent-start.md` and keep the service startable and testable.

Run `gofmt` and `go test ./...`; add static analysis when the Go module is introduced. Test request validation, authentication, policy decisions, error mapping, retry/deadline behavior, SSE termination, cancellation, and redaction. Use interfaces at provider, store, clock and limiter boundaries.

Do not add heavyweight frameworks without a documented need, expose provider SDK types through HTTP, accept client-supplied upstream URLs, store plaintext keys, or use unbounded retries/goroutines. Do not modify `../knowledge-base-server` for gateway-only work.

Review auth-before-provider ordering, stable error envelopes, IDs, bounded resources, stream cancellation, migration coverage, and provider failure paths.

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

### Convention: Infra failure vs denial in boundary interfaces

**What**: Interfaces with pass/fail semantics (e.g. `limiter.Gate.Allow`) return a distinguishable error (sentinel `limiter.ErrUnavailable`) for infrastructure failure, separate from a plain `false` denial.

**Why**: A Redis outage that fails closed looked identical to a rate limit and was reported to clients as 429 — wrong status, wrong Retry-After semantics, wrong metrics.

**Example**: `ok, retryAfter, release, err := gate.Allow(...)`; check `err` first, map to 503 `limiter_unavailable`; only `ok=false, err=nil` is a 429.

### Convention: Failover routing stays behind interfaces

**What**: Provider failover (primary/backup route table + circuit breaker in `internal/router`) is selected before any provider call; streaming never switches providers after output starts; retries stay bounded under the total request deadline.

**Why**: Lets fault tests (`internal/gateway/failover_test.go`) pin the semantics and keeps vendor protocols isolated in `internal/provider`.

