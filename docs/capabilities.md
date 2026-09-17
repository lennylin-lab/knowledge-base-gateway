# Model capabilities reference

This is the authoritative reference for the gateway's per-model capability
matrix. The matrix is a routing and authorization constraint, not advisory
metadata: a request that declares a capability the model does not declare is
rejected **before any provider invocation** with a stable
`capability_not_supported` error.

Source of truth: `model.Capabilities` in `internal/model/model.go`; the
enforcement points are `CheckCapabilities` / `ValidateResponseSpec` in
`internal/model/validate.go`.

## Where capabilities come from

Capabilities live on the catalog row (`model_catalog.capabilities`, JSONB).
The column is free-form: adding a key requires **no schema migration**. When
a catalog row declares nothing (`capabilities` absent or `{}`), the gateway
derives the matrix from the provider adapter. Discovery
(`GET /v1/models`, `GET /v1/models/{model}`) serves the effective public
matrix so clients can check before calling.

## Boolean capability keys

Every key defaults to **false when undeclared**, which means the gated
surface rejects the request.

| Key | Semantics | Gated surfaces | Error capability name |
|---|---|---|---|
| `chat` | Model serves the Chat Completions protocol | `POST /v1/chat/completions` | `'chat'` |
| `responses` | Model serves the Responses protocol | `POST /v1/responses` | `'responses'` |
| `embeddings` | Model serves the Embeddings protocol | `POST /v1/embeddings` | `'embeddings'` |
| `stream` | Model supports SSE streaming | `stream: true` on chat/responses | `'stream'` |
| `tools` | Model accepts function tool definitions | `tools` on chat/responses (tool round trips included) | `'tools'` |
| `structured_output` | Model accepts JSON-Schema response formats | `response_format: json_schema` / `text.format` | `'structured_output'` |
| `json_mode` | Model accepts JSON-object response format | `response_format: json_object` | `'json_mode'` |
| `usage` | Model reports token usage (advisory; unset usage is never coerced to zero, quota keeps the conservative reservation) | all protocols | n/a (not request-gated) |
| `vision` | **Reserved.** Declared in the struct, no surface gates on it yet | none yet | n/a |
| `reasoning` | **Reserved.** Declared in the struct, no surface gates on it yet | none yet | n/a |

Reserved keys: any future surface that gates on `vision` or `reasoning` must
follow the extension protocol below — it cannot start rejecting requests
without a matrix gate and a named error.

## Numeric bounds

Numeric keys are ceilings; `0` (undeclared) means "no declared bound" for
gateway gating purposes and documented defaults apply. Requests exceeding a
declared bound are rejected or clamped before any provider work.

| Key | Semantics | When undeclared |
|---|---|---|
| `context_tokens` | Declared context window. Inputs whose deterministic estimate (chars/4) exceeds it are rejected 400 before rate limit, quota, or provider | no context gate |
| `max_output_tokens` | Output ceiling; request `max_tokens` above it is clamped down silently (never an error) | no output clamp |
| `max_tools` | Maximum tool definitions per request; over-limit is rejected 400 | defaults to 16 (`DefaultMaxTools`) |
| `embedding_dim` | Fixed pgvector column width the catalog declares for an embeddings model. A directory attribute served via discovery: clients read it, the gateway declares it; changing it is a migration event, never request-time negotiation | absent from discovery |

## The error clients receive

```json
{"error": {"type": "invalid_request_error", "code": "capability_not_supported",
           "message": "model does not declare capability 'tools' (protocol chat)",
           "request_id": "req_..."}}
```

- Status `400`, type `invalid_request_error`, code `capability_not_supported`
  — this envelope is frozen; only `message` identifies the failed key.
- The message is **content-free**: the capability matrix key and the protocol
  name only. It never echoes request content, model internals, or provider
  details.
- The rejection always happens before any provider invocation, rate-limit
  consumption, or quota reservation.

`retrieval_profile` is **not** a capability. It is a model-level catalog
attribute (opaque retrieval-threshold JSON) served via model discovery; it
describes data associated with the model and is intentionally not
request-gated — there is no request surface to gate.

## Extension protocol (adding a capability)

The contract for adding any new gated feature:

1. New feature ⇒ new key in `model.Capabilities` (`internal/model/model.go`),
   with `omitempty` JSON tagging consistent with existing keys.
2. Parse the key from the catalog JSONB — free-form, no migration needed.
3. Gate it in `CheckCapabilities` (or `ValidateResponseSpec` for
   structured-output modes) in `internal/model/validate.go`, **before** any
   provider routing.
4. Return `*CapabilityError{Capability: "<key>", Protocol: protocol}` so the
   client's `capability_not_supported` message names the key.
5. Tests: a case in `internal/model/validate_test.go` (rejection + message
   format) and a pre-provider rejection test in the gating surface's
   `_test.go` (`internal/httpapi/`).
6. Document the key in the tables above (this file).

Related docs: `docs/developer-quickstart.md` (getting started, error table),
`docs/gateway-client-contract.md` (server-side integration contract),
`docs/api-versioning.md` (spellings accepted per protocol).
