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

**Boundary**: `/readyz` currently validates config only — it must gain store checks when real `store` implementations land.

