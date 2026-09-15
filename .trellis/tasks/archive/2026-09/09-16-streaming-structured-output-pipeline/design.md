# Design: streaming usage, structured output, first-token latency

## Boundaries

- All vendor translation stays inside `internal/provider` adapters; the
  domain layer (`internal/model`) gains only what the pipeline needs (usage
  on stream completion events, structured-output streaming parity).
- Quota settlement remains the handler's job (`internal/httpapi`), via the
  existing exactly-once `Reservation.Settle`; encoders surface usage, they
  never touch quota.
- Audit columns change only via additive migration 0004 (up + down).

## Anthropic structured output

- Translate `response_format: json_schema` to a synthesized forced tool:
  tool name `structured_output`, `input_schema` = the request schema,
  `tool_choice` forced to that tool. The tool result (assistant `tool_use`
  block input) is unwrapped as the JSON result and streamed as
  text/args deltas consistent with the OpenAI/fake shape, so downstream
  validation (`model.ValidateOutput`) is protocol-independent.
- Streaming: `content_block_delta` inputs assemble as argument deltas; the
  assembled object becomes the output text event sequence. Failure to
  produce valid JSON follows the existing `invalid_tool_arguments` /
  `schema_validation_failed` path.
- Capability gating unchanged: `Capabilities.StructuredOutput` per model;
  rejection happens before provider invocation (contract suite pins it).

## Streaming usage settlement

- OpenAI: outbound stream requests add `stream_options: {"include_usage":
  true}`; the final usage chunk (empty `choices`) parses into `model.Usage`.
  Upstreams that ignore the option leave usage unknown — conservative
  reservation retained, never fabricated.
- Anthropic: `message_delta` usage (optional pointers from the v1.2
  follow-up) flows into the completion event.
- Encoders attach usage to their terminal success event; handlers settle
  exactly once via the existing idempotent finalize (memory `finalized` set,
  Redis `SET NX` marker). Settle(nil) on unknown stays the no-op.
- Non-streaming behavior unchanged.

## First-token latency

- Stream path: handlers timestamp the first output event (text/args delta)
  after the provider call starts; the value lands on the audit event as
  `FirstTokenMillis *int64`.
- Non-streaming: records null this task (the equivalent "time to full
  response" is already `LatencyMillis`); avoids a second semantic.
- Migration 0004 (additive): `ALTER TABLE llm_requests ADD COLUMN
  first_token_millis INTEGER;` down drops it. pg audit writer persists it;
  memory audit keeps the field. `/admin/usage` percentiles (PostgreSQL
  mode) may include it; dev mode documents its mean-based approximation or
  omits it — either is acceptable, documented either way.

## Compatibility and rollback

- Golden fixtures must not change (V1 chat wire untouched; usage settlement
  is transport-invisible).
- Rollback points: adapter translation (anthropic.go), stream usage plumbing
  (provider + encoders), latency column (migration + audit writer) revert
  independently.
- `GATEWAY_RESPONSES_ENABLED` and quota semantics untouched.
