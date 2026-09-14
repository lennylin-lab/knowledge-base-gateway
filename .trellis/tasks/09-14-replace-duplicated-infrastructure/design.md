# Technical Design

## Boundaries

`internal/metrics` owns metric definitions and registration but delegates
exposition to `prometheus/client_golang`. `internal/gateway` retains retry
eligibility and total-deadline orchestration while delegating delay generation
to a backoff helper. `internal/router` retains route ordering and delegates
breaker state transitions to a tested breaker implementation. `internal/limiter`
keeps the Lua script and adds atomic stale-lease cleanup. Migration lifecycle is
provided by a versioned CLI/configuration; `internal/store/pg` remains the
repository boundary.

## Compatibility

Preserve existing metric names and HTTP behavior where possible. Preserve the
pre-output-only retry/failover rule and route priority ordering. No runtime
auto-migration is introduced. Migration files remain additive, with an explicit
down path for the v1.1 schema.

## Rollout and Rollback

Make each dependency replacement independently reviewable. Keep adapters behind
the existing local interfaces so a failed rollout can revert the adapter while
leaving domain code unchanged. Validate metrics output, breaker transitions,
Redis cancellation, and migration up/down in automated tests before enabling a
new operational command in documentation.

## Risks

Dependency API or Go-version incompatibility, changed retry timing, and metric
label cardinality are the primary risks. Pin versions, preserve existing error
classification tests, bound labels to existing dimensions, and run the full Go
test/vet/build gate.
