# Technical Design: LLM Gateway MVP

## Architecture

The service is a Go HTTP process with these boundaries:

- `internal/http`: routing, body/message limits, request/response schemas, auth middleware, error envelope, SSE writer.
- `internal/config`: environment/secret-manager references, defaults, validation, and total-deadline settings.
- `internal/auth`: API-key hashing/lookup and `Principal` construction.
- `internal/policy`: model existence, subject permissions, rate/concurrency/token checks.
- `internal/gateway`: catalog lookup, timeout/deadline orchestration, retry eligibility, cancellation, and normalized errors.
- `internal/provider`: provider interface plus the first adapter; no SDK types cross this boundary.
- `internal/store`: interfaces and PostgreSQL/Redis implementations for key metadata, catalog/policy, counters, and audit persistence.
- `internal/audit`: metadata-only event creation and usage/status normalization.
- `cmd/gateway`: process lifecycle, dependency wiring, graceful shutdown.

## Request Flow

HTTP receives a bounded JSON body, assigns or accepts a request ID, authenticates the bearer key, validates the request, checks catalog and policy, consumes limits, then calls the gateway. Non-streaming returns a normalized OpenAI response. Streaming flushes normalized SSE events, propagates context cancellation, and emits `[DONE]` only on normal completion. All terminal paths publish audit metadata and metrics.

## Contracts

Provider adapters implement `Complete(ctx, ChatRequest) (ChatResponse, error)` and `Stream(ctx, ChatRequest, send func([]byte) error) error`. The catalog maps public model to provider, upstream model, capabilities, default timeout, and enabled state. Errors are normalized to authentication, authorization, validation, rate-limit, timeout, unavailable, or internal classes with request ID.

## Persistence and Secrets

PostgreSQL stores salted API-key hashes and metadata, catalog, policies, and request audit metadata. Redis stores distributed rate/concurrency/short-term usage state. Provider credentials are resolved only at process configuration time from environment/Secret Manager references. Migrations and readiness checks are explicit; local in-memory substitutes are development-only.

## Compatibility, Rollout, and Rollback

Keep the public contract independent of provider SDKs and reserve fields for future Responses API support. Roll out in slices: bootstrap, contracts, auth/catalog, non-streaming, streaming/reliability, persistence/observability, then integration. Each slice must leave the service buildable and testable. Roll back by disabling the new route/provider/catalog entry or reverting the slice; do not perform data-destructive rollback of audit records.

## Security and Operational Trade-offs

Metadata-only audit preserves privacy at the cost of reduced prompt-level replay. Retry is deliberately conservative to avoid duplicate provider work. Redis is required for multi-instance consistency, while a local limiter is permitted only for development and must be visibly configured.
