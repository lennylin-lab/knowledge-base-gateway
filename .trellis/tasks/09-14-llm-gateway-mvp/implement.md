# Implementation Plan: LLM Gateway MVP

## Ordered Checklist

1. Confirm Go toolchain, dependency policy, local PostgreSQL/Redis/container conventions, then initialize the module and minimal command/config/server lifecycle.
2. Define request/response/error envelopes, provider interfaces, catalog/policy types, and contract tests before adapter details.
3. Add bounded decoding, request IDs, structured logging, graceful shutdown, `/healthz`, `/readyz`, and `/metrics`.
4. Implement API-key hashing/lookup, `Principal` middleware, static catalog, and policy checks with denial/audit tests.
5. Implement one provider adapter and non-streaming completion with timeout, cancellation, normalized errors, and eligible retries.
6. Implement SSE framing, flushing, usage/end-event handling, stream cancellation, and the no-retry-after-output rule.
7. Add PostgreSQL migrations/repositories, Redis limiter/usage state, readiness probes, and development-only local substitutes.
8. Add audit persistence, metrics/tracing correlation, secret/prompt redaction tests, and knowledge-base-server-compatible request examples.
9. Run full verification and integration/startup checks; document configuration, limits, failure behavior, and any deferred provider-specific gaps.

## Validation Commands

- `go test ./...`
- `go vet ./...` and the repository-selected formatter/static analyzer
- migration up/down or schema validation tests
- container build/startup and health/readiness smoke checks
- provider contract tests for non-streaming, SSE, timeout, cancellation, 429, 5xx, and malformed responses

## Risky Areas and Rollback Points

- Public JSON/SSE contract: lock with tests before implementation slices.
- Authentication and secret handling: review hashes, logs, and error paths before provider integration.
- Retry/stream state machine: use deterministic fake providers and rollback to non-streaming route if needed.
- Distributed limit semantics: keep Redis implementation behind an interface and disable multi-instance mode unless readiness confirms it.
- Migrations: additive changes first; rollback only through tested down migrations and preserve audit data.

## Pre-Start Review Gate

- `prd.md`, `design.md`, and this checklist agree on scope and acceptance criteria.
- No blocking product questions remain.
- Implementation and check context manifests contain the backend specs and `docs/agent-start.md`.
- Starting this task authorizes implementation in a later phase, but this planning turn performs no code changes.
