# Productionize gateway v1.1

## Goal

Implement the v1.1 production enhancements defined in docs/v1.1-production-roadmap.md

## Requirements

- TBD

## Acceptance Criteria

- [ ] TBD

## Notes

- Keep `prd.md` focused on requirements, constraints, and acceptance criteria.
- Lightweight tasks can remain PRD-only.
- For complex tasks, add `design.md` for technical design and `implement.md` for execution planning before `task.py start`.
# PRD: Gateway V1.1 Productionization

## Goal

Raise the completed V1 gateway to a durable internal platform with persisted identity and policy, multi-provider routing, distributed limits, production observability, and a stable client integration contract.

## Baseline and Constraints

- Requirements are defined by `docs/v1.1-production-roadmap.md`; current behavior is the V1 code and README, not assumptions from the roadmap.
- Preserve the OpenAI-compatible chat and SSE contract unless a compatibility test and migration note justify a change.
- Provider secrets remain environment/Secret Manager only; prompt and completion bodies remain absent from default logs and audit storage.
- Do not modify `../knowledge-base-server` in this task; provide the gateway-side client contract and integration fixtures only.

## In Scope

1. PostgreSQL migrations, repositories, seed data, and durable tenants, subjects, API keys, model catalog, providers/routes, policies, and request audit metadata.
2. API-key lifecycle: create with one-time plaintext display, metadata listing, rotation, revocation, expiry, and Principal resolution.
3. Database-backed model/provider registry, public-model to upstream-model mapping, capability checks, and subject/tenant policy enforcement.
4. At least two provider adapters with primary/backup routing, health state, configurable failover, circuit breaking, bounded backoff retries, and total deadlines.
5. Redis-backed distributed rate/concurrency/token controls with explicit single-instance development behavior.
6. Prometheus metrics, OpenTelemetry trace correlation, structured redacted logs, audit usage/error/cost metadata, and request-size/SSRF/upstream-response protections.
7. Gateway client contract and end-to-end test fixtures covering the principal knowledge-base-server chat workflows without changing that repository.

## Out of Scope

Agent/RAG/MCP behavior, document authorization, WebSocket, management UI, billing settlement, payments, prompt marketplaces, custom workflows, and broad provider expansion beyond the first primary/backup pair.

## Acceptance Criteria

- **A1 Identity:** create, rotate, revoke, expire, and list API keys with one-time plaintext response; invalid keys never invoke a provider and return stable 401 errors.
- **A2 Policy:** model existence, capability, tenant/subject authorization, RPM, concurrency, and token ceilings follow the documented ordering and return non-leaky errors.
- **A3 Persistence:** migrations upgrade and rollback; restart preserves keys, policies, catalog, and audit metadata.
- **A4 Routing:** a recoverable primary timeout/429/5xx can fail over to backup before output; streaming never switches after output begins; non-retryable failures do not retry.
- **A5 Limits:** Redis multi-instance behavior is consistent and emits 429 with `Retry-After` when calculable; local mode is explicit and documented.
- **A6 Observability/security:** request and trace IDs correlate logs, metrics, traces, and audit; no full secrets, provider credentials, prompts, or completions are emitted; SSRF and size limits are enforced.
- **A7 Integration:** a client using one gateway URL, internal key, `Authorization`, `X-Request-ID`, and the stable error envelope can complete non-streaming and streaming workflows.
- **A8 Verification:** unit, contract, migration, fault-injection, concurrency/security, container startup, and end-to-end checks pass with `go test ./...` and static analysis.

## Key Decisions and Deferred Risks

- Keep provider SDK details behind `internal/provider`; route by public model catalog data.
- Use additive migrations and interface-backed stores so development substitutes remain testable.
- Cost estimation and exact OpenTelemetry exporter deployment may be staged, but correlation and metadata contracts are required in this task.
- Real provider credentials and external service availability are not prerequisites for offline tests; use deterministic fakes and fault injection.

## Open Questions

None blocking. Provider pair selection and concrete database/Redis libraries are implementation choices constrained by existing Go module compatibility and will be recorded during execution.
