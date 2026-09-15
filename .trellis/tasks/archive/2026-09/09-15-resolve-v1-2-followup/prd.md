# PRD: Resolve Issue #1 V1.2 Follow-Up Gaps

## Goal

Close GitHub issue #1 by resolving the remaining V1.2 protocol, streaming, quota, management, security, replay, and artifact-cleanup gaps through independently testable stages while preserving the existing V1 Chat Completions contract.

## User Value

Make the V1.2 gateway safe to continue toward release: invalid protocol inputs fail before provider calls, streaming outcomes are not falsely marked successful, quota/input ceilings are enforced consistently, management mutations are auditable and operationally visible, and the remaining verification/tooling gaps are tracked to completion.

## Source Evidence

- GitHub issue #1: [v1.2] Follow up on remaining protocol, quota, and management gaps.
- Roadmap acceptance source: docs/v1.2-developer-platform-roadmap.md.
- Compatibility sources: docs/agent-start.md, docs/v1.1-production-roadmap.md, docs/gateway-client-contract.md.
- Current review anchors include internal/httpapi/common.go:117-138, internal/model/validate.go:94-115, internal/provider/openai.go:351-427, internal/provider/anthropic.go:257-262 and :425-428, internal/httpapi/chat.go:279-292, internal/httpapi/responses.go:257-262 and :337-354, internal/store/pg/pg.go:327-344, internal/store/pg/mgmt.go:46-56, internal/httpapi/admin.go:120-140, internal/router/routes.go:59-68, internal/provider/urlsafe.go:31-41, and internal/mgmt/mgmt.go:38-43 and :195-244.

## Task Map

1. 09-15-v1-2-protocol-streaming: protocol admission, Responses decode strictness, OpenAI stream termination, Anthropic usage presence, and final streamed-output validation.
2. 09-15-v1-2-quota-provider-safety: model/subject input ceilings, route circuit-breaker semantics, and provider base URL SSRF protections.
3. 09-15-v1-2-management-replay-finish: live/atomic management mutations, management metrics contract, standalone protocol replay, archived PRD placeholder cleanup, final issue closure verification.

## Requirements

- R1: Split the issue into independently verifiable stages that can be implemented, checked, and archived separately.
- R2: Each stage must include focused regression or contract tests for the exact issue items it owns.
- R3: All stages must preserve V1 Chat Completions JSON/SSE/error/request-ID compatibility and keep existing V1.2 Responses behavior compatible except where the issue requires stricter rejection.
- R4: All stages must keep credentials, DSNs, provider base URLs, prompt/completion content, tool arguments, and private runtime details out of logs, audit records, public responses, task artifacts, and issue updates.
- R5: The final integration pass must update issue #1 with the resolved scope and any explicit deferred work, without exposing secrets or private environment data.

## Acceptance Criteria

- AC1: Three child tasks exist and map every issue #1 checkbox to exactly one owning task.
- AC2: Each child task has a converged PRD plus design and implementation plan before it is started.
- AC3: Each child task has non-empty implement/check context manifests containing only docs/spec/research entries, not implementation source files.
- AC4: The first implementation stage can be activated after this planning review without additional product-scope decisions.
- AC5: Final closure requires go test ./..., go test -race ./..., go vet ./..., git diff --check, protocol/replay/smoke checks that are available in the repository, and an explicit note for environment-gated checks that could not run locally.

## Out of Scope

- Adding new provider families or changing public model naming.
- Building Agent loops, automatic tool execution, knowledge-base/RAG/MCP behavior, or user-facing chat UI.
- Full billing/pricing implementation beyond the issue's management-metrics and cost-field tracking requirements.
- Publishing secrets, prompt/completion content, local machine paths, private endpoints, or environment-specific values in GitHub comments.

## Risks and Deferred Items

- Some checks may require PostgreSQL, Redis, Docker, or external runtime availability; if unavailable, record the exact skipped gate and keep deterministic unit/contract coverage for the changed behavior.
- The management live-refresh design may require a small runtime boundary addition; the child task must keep it scoped and rollback-friendly.
- Provider URL hostname resolution rules can affect local development; any exception must be explicit, narrow, and documented.

## Open Questions

None blocking. The staged task split follows issue priority and current code boundaries.
