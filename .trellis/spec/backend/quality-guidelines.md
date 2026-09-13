# Backend Quality Guidelines

This is an early Go gateway with no implementation or CI yet. New code must preserve `docs/agent-start.md` and keep the service startable and testable.

Run `gofmt` and `go test ./...`; add static analysis when the Go module is introduced. Test request validation, authentication, policy decisions, error mapping, retry/deadline behavior, SSE termination, cancellation, and redaction. Use interfaces at provider, store, clock and limiter boundaries.

Do not add heavyweight frameworks without a documented need, expose provider SDK types through HTTP, accept client-supplied upstream URLs, store plaintext keys, or use unbounded retries/goroutines. Do not modify `../knowledge-base-server` for gateway-only work.

Review auth-before-provider ordering, stable error envelopes, IDs, bounded resources, stream cancellation, migration coverage, and provider failure paths.

