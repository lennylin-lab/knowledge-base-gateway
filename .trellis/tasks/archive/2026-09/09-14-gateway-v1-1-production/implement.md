# Implementation Plan: Gateway V1.1

## Ordered Work

1. Freeze V1 compatibility with contract tests; inspect current config, stores, provider seams, and migration baseline.
2. Add PostgreSQL migrations, seed data, repository interfaces/implementations, readiness wiring, and migration tests.
3. Implement API-key lifecycle and persisted Principal/tenant/policy resolution, including management CLI or internal API boundary.
4. Implement catalog/provider registry and policy authorization with capability and limit checks.
5. Add the second provider adapter plus router health, primary/backup failover, circuit breaker, retry/backoff, and deadline tests.
6. Implement Redis rate/concurrency/token leases, explicit local-mode behavior, and cancellation-safe release.
7. Add audit persistence, metrics, tracing, redaction, cost metadata, SSRF and payload/response protections.
8. Add gateway-client fixtures and end-to-end tests for non-streaming/streaming workflows and fault injection.
9. Run full quality, migration rollback, concurrency, security, container startup, and documentation checks.

## Validation

`go test ./...`; `go vet ./...`; configured static analyzer; migration up/down tests; deterministic provider contract/fault tests; Redis multi-instance tests; redaction/SSRF/size-limit tests; container health/readiness smoke test; gateway-client end-to-end tests.

## Review and Rollback Gates

- No public contract change without compatibility tests and README/docs update.
- Review secret handling and migration safety before enabling production persistence.
- Keep provider routing behind an interface; disable backup route if fault tests fail.
- Do not enable Redis multi-instance mode unless readiness verifies Redis and lease cleanup is tested.
- Preserve additive migrations and audit data on rollback.

## Pre-Start Gate

Planning artifacts and context manifests must validate; implementation starts only after this task is explicitly activated. This planning turn does not modify product code.
