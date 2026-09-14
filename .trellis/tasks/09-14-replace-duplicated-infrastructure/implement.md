# Implementation Plan

1. Record the selected dependency versions and verify they are compatible with
   Go 1.27.1 and the repository's reproducible/offline build expectations.
2. Replace the hand-written metrics registry/exposition with official
   Prometheus collectors; add request duration instrumentation and update tests.
3. Introduce bounded exponential backoff with jitter in gateway retries while
   preserving retry classification, total deadlines, and streaming semantics.
4. Replace or wrap the breaker state machine, retaining ordered primary/backup
   route behavior and adding concurrent half-open tests.
5. Update the Redis Lua admission script to prune stale leases atomically and
   test release idempotency, cancellation, and infrastructure failure.
6. Add a standard migration command/configuration, document operational usage,
   and keep migration integration tests for forward/down behavior.
7. Update README and backend database/quality guidance with the adopted
   dependencies and explicit local-vs-production modes.
8. Run `gofmt`, `go test ./...`, `go vet ./...`, and `go build ./...`; inspect
   the diff for compatibility, secret redaction, and accidental scope expansion.

Rollback points: each dependency adapter, the Redis script change, and the
migration CLI wiring can be reverted independently without changing the public
HTTP contract.
