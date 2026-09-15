# PRD: Fix V1.2 Protocol and Streaming Correctness

## Goal

Resolve the issue #1 protocol and streaming P1/P2 defects so invalid request shapes, unsupported tools/schema inputs, truncated OpenAI streams, missing Anthropic usage, and invalid streamed outputs are handled deterministically before release.

## Requirements

- R1: Wire model.ValidateTools into both Chat and Responses admission so tool count, name format, duplicates, schema size, and schema depth are rejected before provider invocation. Evidence: internal/httpapi/common.go:117-123 and internal/model/validate.go:94-115.
- R2: Make /v1/responses bounded decoding require exactly one JSON document and reject trailing JSON or garbage. Evidence: internal/httpapi/responses.go:257-262.
- R3: Require OpenAI-compatible upstream streams to include data: [DONE] before emitting a successful completed event. Evidence: internal/provider/openai.go:351-427.
- R4: Preserve unknown Anthropic usage rather than fabricating known zero usage when upstream usage fields are absent. Evidence: internal/provider/anthropic.go:257-262 and :425-428.
- R5: Validate final streamed Chat and Responses outputs, including tool arguments and structured JSON/schema output, and record/emit failure instead of silently marking invalid streams successful. Evidence: internal/httpapi/chat.go:279-292, internal/httpapi/responses.go:337-354, internal/httpapi/responseswire.go:201-217.
- R6: Preserve existing V1 Chat Completions JSON/SSE/error/request-ID behavior except for stricter rejection of invalid V1.2 tool/schema inputs.

## Acceptance Criteria

- AC1: Invalid tool names, duplicates, excessive count, oversized schemas, and over-depth schemas are rejected for Chat and Responses before provider calls.
- AC2: /v1/responses rejects two concatenated JSON objects and non-whitespace trailing bytes with the stable invalid_request envelope.
- AC3: OpenAI stream EOF without [DONE] returns a provider/protocol failure and does not emit completed.
- AC4: Anthropic responses and streams without usage leave Usage.Known false, do not settle quota to zero, and record unknown audit tokens.
- AC5: Streamed invalid tool arguments or structured-output JSON/schema failures produce deterministic audit error_class behavior and a terminal stream failure where the protocol permits it.
- AC6: Existing golden Chat tests, Responses tests, provider contract tests, and quota unknown-usage tests pass.

## Out of Scope

- Input quota/context enforcement, router circuit-breaker changes, provider URL validation, management API changes, replay CLI, and archived PRD cleanup; those belong to sibling tasks.

## Open Questions

None blocking.
