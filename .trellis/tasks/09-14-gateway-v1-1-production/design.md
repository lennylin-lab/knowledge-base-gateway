# Technical Design: Gateway V1.1

## Components and Boundaries

Extend the V1 packages with `catalog`, `router`, `limits`, and `observability` boundaries while retaining HTTP schema ownership in `internal/httpapi`. `auth` owns key lifecycle and Principal lookup; `policy` evaluates persisted rules; `store` exposes repository interfaces and PostgreSQL/Redis implementations; `provider` normalizes vendor protocols; `gateway/router` owns health, failover, circuit state, retry, and deadline orchestration; `audit/observability` publish redacted metadata.

## Data Flow

Request ID/trace setup -> API-key lookup -> bounded schema validation -> catalog/capability lookup -> tenant/subject policy -> Redis rate/concurrency/quota lease -> router selects healthy primary -> provider call -> normalized response/SSE -> release lease -> audit/metrics/trace. Failover is allowed only before irreversible client output.

## Persistence

Use additive PostgreSQL migrations for tenants, subjects, key hashes/lifecycle timestamps, catalog/providers/routes, policies, and request metadata. Repositories must avoid leaking database models into HTTP or provider layers. Redis keys and leases are namespaced by policy dimension and request ID, with TTLs and recovery on cancellation.

## Routing and Failure Semantics

Routes contain provider, upstream model, priority, capabilities, timeout, and enabled/config version. Circuit breakers track provider/route failures and half-open probes. Retry classification admits network errors, 429, and recoverable 5xx/timeouts before output, bounded by the request deadline; authentication, validation, authorization, and business rejections fail immediately.

## Security and Observability

Provider URLs come only from trusted configuration; validate allowed schemes/hosts to prevent SSRF. Bound request and upstream response sizes. Attach request/trace IDs and subject/model/provider/status/latency fields to structured logs and metrics while redacting secrets and content. Audit stores usage when supplied and unknown otherwise.

## Compatibility and Rollback

Preserve V1 endpoint/error/SSE shapes. Introduce persistence behind interfaces and feature flags/configuration, allowing local mode during migration. Roll back by disabling new routes/providers or reverting additive migrations through tested down paths; retain audit history.
