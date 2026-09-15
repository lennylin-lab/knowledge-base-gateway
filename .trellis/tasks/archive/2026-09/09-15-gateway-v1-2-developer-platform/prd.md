# Implement V1.2 unified protocol and developer platform

## Goal

Implement the V1.2 unified model protocol, tool calling, structured output, discovery, management queries, and developer platform defined in docs/v1.2-developer-platform-roadmap.md

## Requirements

- TBD

## Acceptance Criteria

- [ ] TBD

## Notes

- Keep `prd.md` focused on requirements, constraints, and acceptance criteria.
- Lightweight tasks can remain PRD-only.
- For complex tasks, add `design.md` for technical design and `implement.md` for execution planning before `task.py start`.
# PRD: V1.2 Unified Model Protocol and Developer Platform

## Goal

Extend the V1.1 gateway so OpenAI SDKs, internal agents, and other clients can use a stable unified model protocol for responses, streaming, tool calls, and structured output without breaking the existing Chat Completions integration.

## Baseline and Constraints

- The authoritative scope is `docs/v1.2-developer-platform-roadmap.md`, with V1/V1.1 behavior defined by `docs/agent-start.md`, `docs/v1.1-production-roadmap.md`, and `docs/gateway-client-contract.md`.
- The repository is currently on branch `v1.2`; V1.1 is implemented and archived, and the current workspace contains user-owned uncommitted startup/Compose/env changes that must be preserved.
- Existing `POST /v1/chat/completions` request fields, response fields, SSE termination, error envelope, request IDs, and client behavior remain compatible.
- Clients choose only public model names. Provider SDK types, provider URLs/secrets, database models, and internal route details must not cross the public HTTP boundary.
- The gateway transports tool definitions and tool results but never executes tools, runs an Agent loop, or accesses business data.

## In Scope

1. Compatibility regression coverage and golden fixtures for the existing Chat Completions JSON/SSE/error contract.
2. A provider-neutral request, response, event, and model-capability domain layer used by both Chat Completions and Responses adapters.
3. `POST /v1/responses` with bounded validation, non-streaming output, stable text/tool/refusal output, usage, errors, and the documented SSE event types.
4. Tool calling, JSON mode, and structured-output/schema forwarding and normalization, including tool/schema limits, incremental argument assembly, and capability prechecks.
5. Contract and consistency tests for both connected providers, plus deterministic mock transport, fixtures, protocol replay, and end-to-end test tooling.
6. `GET /v1/models` and `GET /v1/models/{model}` with caller-authorized public capabilities and limits only.
7. Independently authenticated management queries/CLI for models, providers, policies, keys, audit, usage, health, and management-operation audit records.
8. Curl, Python, Go, and OpenAI SDK examples, local mock-provider quickstart, API compatibility/deprecation documentation, and smoke tests sharing request fixtures.

## Out of Scope

Built-in Agent loops, automatic tool execution, knowledge-base/RAG/MCP behavior, user-facing chat UI, prompt marketplace, workflow editor, WebSocket, cross-region active-active, full billing, broad private-provider extension coverage, forced migration of Chat Completions clients, and asynchronous cancel/batch APIs unless separately approved after synchronous protocols stabilize.

## Acceptance Criteria

- **A1 Chat compatibility:** Existing non-streaming, SSE, error-envelope, request-ID, retry, and cancellation behavior passes golden/regression tests; existing `knowledge-base-server` clients need no change.
- **A2 Responses contract:** `/v1/responses` accepts only the documented MVP fields and returns stable `id`, `object`, `created`, `model`, `status`, `output`, and `usage` fields without provider-private fields.
- **A3 Responses streaming:** SSE emits the documented created/text/tool/completed/failed events, handles client cancellation and upstream disconnects, returns a unified pre-first-event failure, and never switches provider after the first event.
- **A4 Capability enforcement:** Unsupported tools, JSON mode, structured output, vision, reasoning, protocol, or other declared capabilities return `capability_not_supported` before provider invocation; model and input limits are enforced.
- **A5 Tool safety:** Tool names/schemas/count/size/depth are validated, argument deltas assemble and validate deterministically, tool results round-trip as input, and the gateway executes no arbitrary function or logs tool/prompt content.
- **A6 Structured output:** JSON mode/schema version and size are checked before invocation; final output validation records failures and never marks invalid output as successful silently.
- **A7 Provider consistency:** At least two providers pass the same offline contract suite for text, streaming, usage, tools, structured output, unsupported capabilities, cancellation, malformed responses, timeout, and 400/401/429/5xx mapping; routing core has no provider-specific branch.
- **A8 Discovery and management:** Model endpoints expose only caller-authorized public metadata; management access is separately authenticated, permission-checked, audited, and never returns plaintext keys, secrets, internal URLs, or prompt/completion bodies.
- **A9 Developer experience:** Curl, Python, Go, and OpenAI SDK examples run against the mock provider and document base URL, errors, retry/`Retry-After`, request/trace IDs, compatibility, and deprecation rules.
- **A10 Verification:** `go test ./...`, race/static checks, protocol/fixture/replay tests, security and limit tests, and mock-provider end-to-end smoke tests pass while the service remains startable at every implementation slice.

## Key Decisions

- Normalize both public protocols into one internal domain request/response/event model; keep separate encoders so Chat Completions field semantics and `[DONE]` behavior do not change.
- Extend provider boundaries with capability discovery and normalized complete/stream operations; never expose vendor SDK types.
- Treat capability declarations as authorization/routing constraints, not advisory metadata; reject unsupported features before provider calls.
- Keep tool execution and business permissions outside the gateway; callers submit tool results in later requests.
- Version capabilities/adapter configuration independently and introduce new behavior behind model or tenant feature flags; breaking semantics require `/v2`.

## Risks and Deferred Items

- Provider streaming and schema dialects differ; adapters need fixtures and consistency tests before enabling a capability for a model.
- Existing V1.2 branch changes are uncommitted and may touch startup/config files; implementation must rebase its decisions on the actual working tree and avoid reverting them.
- Async response cancellation/batch work is explicitly deferred until synchronous and streaming semantics are stable.
- Exact cost-estimation and exporter deployment details may remain staged, but request/trace correlation and metadata-only audit are mandatory.

## Open Questions

None blocking. Provider library choices, internal package names, and concrete management CLI versus read-only admin routes are implementation choices constrained by the existing V1.1 seams and security requirements.
