# Implement LLM gateway MVP

## Goal

Plan and implement the first-phase Go LLM gateway MVP defined by docs/agent-start.md

## Requirements

- TBD

## Acceptance Criteria

- [ ] TBD

## Notes

- Keep `prd.md` focused on requirements, constraints, and acceptance criteria.
- Lightweight tasks can remain PRD-only.
- For complex tasks, add `design.md` for technical design and `implement.md` for execution planning before `task.py start`.
# PRD: LLM Gateway MVP

## Goal

Deliver a stable, auditable Go gateway between `knowledge-base-server` and LLM providers. The gateway owns model access, API-key policy, reliability controls, and request metadata; document permissions and RAG/Agent behavior remain in `knowledge-base-server`.

## Background and Confirmed Constraints

- The source requirements are defined in `docs/agent-start.md`.
- The repository currently has no Go module or implementation; this task starts the gateway from the documented boundaries.
- Provider secrets must come from environment variables or a Secret Manager and must never be persisted in plaintext.
- Requests require correlation via a trace/request ID, and default logs/audit records must not contain full prompts or completions.
- The initial external protocol is HTTP plus OpenAI-compatible Chat Completions and SSE; the design must leave room for a future Responses API.
- Changes to `../knowledge-base-server` are out of scope unless a later task explicitly requests integration work.

## In Scope

1. Go service bootstrap, validated configuration, HTTP server, graceful shutdown, structured logging, and health/readiness/metrics endpoints.
2. API-key authentication producing a `Principal`; invalid, expired, and revoked keys are rejected before any provider call.
3. Static/configured model catalog and provider whitelist, with subject/model policy checks, rate/concurrency controls, and token ceilings.
4. `POST /v1/chat/completions` request validation and OpenAI-compatible non-streaming responses.
5. SSE streaming with cancellation propagation and a terminal `data: [DONE]` event.
6. Provider abstraction and at least one adapter, with bounded timeout, eligible finite retries, upstream error classification, and no provider-secret leakage.
7. PostgreSQL/Redis-backed interfaces and initial implementations for API-key metadata, model/policy data, rate/usage state, and audit metadata; local development substitutes may be explicitly supported.
8. Audit events and Prometheus/OpenTelemetry correlation for subject, model, status, latency, usage when supplied, and trace/request ID.
9. Unit, contract, failure-mode, migration, and startup checks sufficient to validate the acceptance criteria below.

## Out of Scope

Knowledge-base document authorization, vector search, Agent orchestration, tool execution, complex workflows, management UI, complete billing settlement, WebSocket, and unrestricted public management APIs.

## Requirements and Acceptance Criteria

- **R1 Authentication:** invalid, expired, or revoked keys return `401` with the documented error envelope, do not call an upstream provider, and never expose the key value.
- **R2 Authorization:** unknown models and disallowed models produce non-leaky `403`/appropriate client errors; the documented order is authentication, request validation, model existence, subject policy, limits, then provider invocation.
- **R3 Contract:** valid non-streaming calls preserve `id`, `object`, `created`, `model`, `choices`, and `usage`; streaming emits valid SSE data and `[DONE]`, including upstream errors and client cancellation behavior.
- **R4 Reliability:** request and total-deadline limits are enforced; retries are finite, configurable, restricted to eligible pre-output network/429/5xx failures, and never switch providers after stream output begins.
- **R5 Limits:** rate, concurrency, and configured token ceilings are enforced consistently in single-instance and Redis modes, or documented where development mode differs.
- **R6 Audit/observability:** every request has a correlatable ID; audit/log/metric data records required metadata without default prompt/completion or secret leakage; `/healthz`, `/readyz`, and `/metrics` behave as specified.
- **R7 Integration readiness:** a Python client can call the gateway using one base URL, an internal API key, `Authorization`, `X-Request-ID`, and a stable error envelope.
- **R8 Verification:** `go test ./...`, static checks, migration tests, and container/startup checks are defined and pass before implementation completion.

## Key Decisions

- Use standard-library `net/http` unless repository discovery during implementation demonstrates a compelling need for a small router; avoid heavyweight frameworks.
- Keep HTTP, auth, policy, gateway, provider, audit, store, and config responsibilities separated as documented in `agent-start.md`.
- Treat public model names as the only client model identifiers; upstream URLs and provider model names remain server-side catalog data.
- Start with one provider adapter and configuration-driven catalog; add providers through the isolated interface rather than coupling HTTP handlers to SDK types.

## Risks and Deferred Items

- No existing Go module, deployment manifest, or live PostgreSQL/Redis setup is present; dependency versions and local integration harness must be selected during implementation and documented.
- Provider-specific streaming/error semantics may require adapter-level normalization tests before broad compatibility can be claimed.
- Distributed quota precision and OpenTelemetry deployment details are deferred to implementation design, but cannot weaken the security and audit requirements above.

## Open Questions

None blocking planning. Product scope and acceptance behavior are fixed by `docs/agent-start.md`; technical unknowns are explicit implementation risks.
