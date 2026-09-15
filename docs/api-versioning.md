# API versioning, compatibility, and deprecation policy

## Stable V1 surface

Both public model protocols are part of stable **V1**:

| Endpoint | Method | Since | Status |
|---|---|---|---|
| `/v1/chat/completions` | POST | V1 | Stable. The request/response shapes, SSE termination (`data: [DONE]`), error envelope, and request-ID behavior are frozen; see `docs/gateway-client-contract.md`. |
| `/v1/responses` | POST | V1.2 | Stable for the documented MVP fields (see below). New fields are added additively. |
| `/v1/models`, `/v1/models/{model}` | GET | V1.2 | Stable, read-only. |
| `/admin/*` | any | V1 / V1.2 | Internal, token-gated, not a public contract; changes are announced but not versioned. |

The error envelope `{"error":{type, code, message, request_id}}` and the HTTP
status mapping are frozen across both protocols for all of V1.

## Compatibility rules

- **Additive evolution inside `/v1`.** New optional request fields (for
  example `tools`, `tool_choice`, and `response_format` on Chat
  Completions in V1.2) are added without breaking existing clients.
  Existing `knowledge-base-server` clients require no changes.
- **Unknown fields.** Chat Completions ignores unknown request fields (as
  before). Responses **rejects** unknown top-level fields with `400
  invalid_request`, so the accepted contract stays explicit.
- **Breaking changes require `/v2`.** A semantic change to an existing
  field, a removal, or a status/code change must ship as a new version
  prefix. V1 behavior is never altered by configuration alone.
- **Unknown fields in responses.** Clients must tolerate new fields in
  responses and SSE events. Event *types* are stable; new types may be
  added between existing ones and clients must ignore unrecognized ones.

## Responses API MVP contract

Accepted request fields: `model`, `input` (string or typed item array),
`instructions`, `temperature`, `max_output_tokens`, `stream`, `tools`,
`tool_choice`, `response_format`, `text`, `metadata`. Everything else is
rejected.

Two spellings are accepted for two fields, covering both the gateway MVP
dialect and the native Responses shapes the openai SDK sends:

- `tools` entries may use the nested chat-completions shape
  (`{"type":"function","function":{name, description, parameters}}`) or the
  flat Responses shape (`{"type":"function","name":...,
  "description":...,"parameters":...}`). Unknown fields are rejected inside
  either shape.
- Structured output may use `response_format`
  (`{"type":"json_schema","json_schema":{name, schema, strict}}`, the MVP
  dialect) or `text` (`{"format":{"type":"json_schema","name":...,
  "schema":...,"strict":...}}`, the native SDK parameter;
  `{"format":{"type":"text"}}` selects plain text and
  `{"format":{"type":"json_object"}}` JSON mode). `text` and
  `response_format` are mutually exclusive.

Verified against openai-python 3.5.0: chat completions (non-streaming and
streaming) and `/v1/responses` (non-streaming, streaming, native tools,
native structured output) pass through the SDK with no `extra_body`
workarounds once these shapes are accepted.

Stable event types (SSE): `response.created`,
`response.output_item.added` (V1.2.1: announces a function call item with
`call_id`/`name` before its argument fragments), `response.output_text.delta`, `response.output_text.done`,
`response.function_call_arguments.delta`,
`response.function_call_arguments.done`, `response.completed`,
`response.failed`. Terminal streams end with exactly one of
`response.completed` / `response.failed`.

## Capability flags and rollout

Model capabilities are declared in the catalog (`model_catalog.capabilities`
JSONB) and enforced **before** a provider is called: unsupported features
return `400 capability_not_supported`. This matrix doubles as the rollout
mechanism:

- A capability (for example `responses` or `structured_output`) can be
  enabled per model, then per tenant through grants; nothing is enabled by
  configuration accident.
- The Responses endpoint has an independent kill switch:
  `GATEWAY_RESPONSES_ENABLED=false` disables `/v1/responses` without
  touching Chat Completions.
- Capability/adapter configuration versions are recorded per catalog row
  (`config_version`) and surfaced in `/v1/models/{model}` and the admin
  model view, so audit rows can be correlated with the declaration that was
  in force.

## Deprecation

- Deprecations are announced in release notes and in this file at least one
  full release before removal.
- Deprecated fields/endpoints keep their current behavior until removal;
  the gateway does not reinterpret them.
- When something is deprecated, `/v1/models` responses gain a documented
  `deprecated` marker before the removal lands.
