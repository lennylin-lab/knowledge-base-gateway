# Replace duplicated infrastructure implementations

## Goal

Reduce production risk from hand-written replacements for mature infrastructure
protocols while preserving the gateway's public API and domain-specific routing
semantics. The outcome is a smaller maintenance surface with standard metrics,
retry, circuit-breaking, and migration behavior.

## Background and Confirmed Findings

- The project is Go 1.27.1 and currently depends directly on `pgx/v5` and
  `go-redis/v9`; it has no ORM or migration framework.
- `internal/metrics/metrics.go:1-97` manually implements Prometheus exposition
  and label serialization. It does not provide the roadmap's duration metric.
- `internal/gateway/service.go:130-219` manually implements bounded retries with
  a fixed delay; `internal/router/breaker.go:10-71` manually implements a
  circuit breaker.
- `internal/limiter/redis.go:15-113` uses an appropriate Lua atomic operation,
  but its sorted-set lease handling does not remove expired members before
  `ZCARD`, so abandoned leases can occupy concurrency capacity.
- `internal/store/pg/migration_test.go:15-105` executes SQL files directly for
  tests; no production migration versioning/locking tool is configured.
- Provider adapters, HTTP schema/error handling, domain repositories, route
  ordering, and the streaming no-switch rule are project-specific boundaries
  and should remain owned by this repository.

## Requirements

1. Replace manual Prometheus text generation with the official Prometheus Go
   client, including counters and request duration histogram, while retaining
   existing metric names/labels where compatible.
2. Replace or wrap retry delay/backoff with a mature implementation supporting
   bounded exponential backoff, jitter, context/deadline cancellation, and the
   existing retry classification. Preserve the rule that streaming cannot
   fail over after output starts.
3. Replace or wrap the circuit breaker with a mature implementation that keeps
   primary/backup route ordering and admits at most one half-open probe.
4. Fix Redis concurrency lease expiry atomically, or adopt a mature lease
   primitive, so abandoned members cannot permanently consume capacity. Keep
   local mode explicitly development-only.
5. Adopt and document a versioned PostgreSQL migration tool with forward and
   rollback coverage; existing schema and data contracts remain compatible.
6. Keep provider secrets, prompts, and completions out of logs and audit data.
7. Add focused tests and update documentation for all changed operational
   behavior. No unrelated feature or API redesign is in scope.

## Out of Scope

- Introducing a full ORM or replacing domain repositories with generated models.
- Rewriting provider protocol adapters or the OpenAI-compatible HTTP contract.
- Changing route-selection policy, model authorization semantics, or streaming
  failover behavior.
- Adding external provider credentials or requiring live third-party services
  for unit tests.

## Acceptance Criteria

- **A1 Metrics:** `/metrics` is emitted by the official Prometheus client,
  handles label values safely, and exposes request count, duration,
  upstream-error, token, and rate-limit metrics.
- **A2 Retry:** eligible pre-output failures use bounded exponential backoff
  with jitter and stop at the total deadline; non-retryable failures do not
  retry; streaming never switches after the first successful send.
- **A3 Breaker:** repeated route failures open the circuit, exactly one
  half-open probe is admitted after cooldown, and successful probes close it;
  primary/backup ordering remains unchanged.
- **A4 Redis limits:** expired leases are removed during admission, release is
  idempotent, and fault-injection tests cover cancellation and Redis errors.
- **A5 Migrations:** a standard migration command can apply and roll back the
  schema with version tracking/locking, and PostgreSQL migration tests pass.
- **A6 Compatibility/security:** `go test ./...`, `go vet ./...`, and `go build
  ./...` pass; public endpoints and redaction guarantees remain compatible.

## Key Decisions and Deferred Risks

- Prefer small, focused dependencies (`prometheus/client_golang`, a mature
  backoff/breaker library, and a migration tool) over an ORM.
- Keep `pgx` as the database driver and existing SQL repositories. SQL remains
  explicit where repository behavior is domain-specific.
- Exact library selection is an implementation detail constrained by Go module
  compatibility and offline/reproducible builds; it must be recorded during
  implementation.
- A migration CLI may be an operational dependency rather than a runtime
  dependency; startup must remain free of implicit schema mutation.

## Open Questions

None blocking. Library choices and rollout sequencing are technical decisions
to be validated during implementation.

## Goal

Adopt mature dependencies for metrics, retry, circuit breaking, and migration management where they improve production correctness.

## Requirements

- TBD

## Acceptance Criteria

- [ ] TBD

## Notes

- Keep `prd.md` focused on requirements, constraints, and acceptance criteria.
- Lightweight tasks can remain PRD-only.
- For complex tasks, add `design.md` for technical design and `implement.md` for execution planning before `task.py start`.
