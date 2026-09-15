# Developer quickstart

This guide gets you from `git clone` to calling both gateway protocols
against the built-in mock provider. All examples share the request samples in
`docs/examples/requests/`, which are exercised by automated tests
(`internal/e2e/examples_test.go`), so documentation and behavior cannot
drift.

## 1. Start the gateway with the mock provider

```bash
# Development mode: in-memory stores, fake provider, no external services.
export GATEWAY_API_KEYS="key-1:subject-demo:kb_dev_key_123"
export GATEWAY_MODELS="gateway-echo:fake:echo-model"
export GATEWAY_ADDR=":8080"
export GATEWAY_ADMIN_TOKEN="dev-admin-token"   # enables the admin API on :8081

go run ./cmd/gateway
```

The mock provider (`fake`) is fully deterministic:

- Plain input echoes: `echo: <text>`.
- A request with tools returns one tool call `get_weather`-style against the
  first declared tool with `{"input": "<last text>"}`.
- A request carrying a tool result answers `tool ok: <content>`.
- `response_format` wraps output as `{"echo": "<text>"}`.
- Text starting with `/refuse` produces a refusal output item.

Verify readiness:

```bash
curl -s http://127.0.0.1:8080/healthz
curl -s http://127.0.0.1:8080/readyz
```

## 2. Discover models

```bash
KEY="kb_dev_key_123"
curl -s -H "Authorization: Bearer $KEY" http://127.0.0.1:8080/v1/models
curl -s -H "Authorization: Bearer $KEY" http://127.0.0.1:8080/v1/models/gateway-echo
```

`/v1/models` lists only models your subject is authorized to use; details
include the public capability matrix, context/output limits, supported
protocols, status, and configuration version. Provider names, upstream model
names, and URLs are never exposed.

## 3. Call the protocols

- Chat Completions (non-streaming and SSE): `docs/examples/scripts/chat.sh`
- Responses (non-streaming, SSE, tool calling, structured output):
  `docs/examples/scripts/responses.sh`
- Python (OpenAI SDK through `base_url`): `docs/examples/python_openai_sdk.py`
- Python (plain HTTP): `docs/examples/python_client.py`
- Go: `docs/examples/go_client.go`

Every request should carry `X-Request-ID` (client generated) so gateway
audit rows, metrics, and traces correlate with your logs.

## 4. Errors, retries, and IDs

Errors use one stable envelope on both protocols:

```json
{"error": {"type": "invalid_request_error", "code": "capability_not_supported",
           "message": "...", "request_id": "req_..."}}
```

| Status | Codes | Client action |
|---|---|---|
| 401 | `invalid_api_key`, `api_key_expired`, `api_key_revoked` | rotate key (admin API), fail fast |
| 403 | `model_not_allowed` | unknown and disallowed models are indistinguishable |
| 400 | `invalid_request`, `capability_not_supported`, `upstream_rejected_request` | fix payload; capability errors mean the model's matrix does not allow the feature |
| 429 | `rate_limit_exceeded`, `quota_exceeded` (with `Retry-After`) | back off; quota clears at the next UTC day/month boundary |
| 503 | `upstream_unavailable`, `upstream_rate_limited`, `no_route_available`, `limiter_unavailable` | retry with backoff; `limiter_unavailable` has no `Retry-After` |
| 504 | `upstream_timeout` | retry allowed |

Retry rules:

- The gateway already retries pre-output network/429/5xx/timeout failures
  and fails over primary to backup before any output, under one total
  deadline. Clients should only retry 429/503/504 and network errors,
  honoring `Retry-After`.
- Never retry a streaming call after events were received.
- Streaming: once the first SSE event arrived, failures surface as a
  terminal event (`data: [DONE]` for chat after success; `response.failed`
  for Responses), never as a provider switch.

## 5. Management API (operators)

Enabled by `GATEWAY_ADMIN_TOKEN`; listens on `GATEWAY_ADMIN_ADDR`
(default `:8081`) and must stay on an internal network.

```bash
ADMIN="Authorization: Bearer dev-admin-token"
curl -s -H "$ADMIN" http://127.0.0.1:8081/admin/models        # catalog + capabilities
curl -s -H "$ADMIN" http://127.0.0.1:8081/admin/providers     # registry status
curl -s -H "$ADMIN" http://127.0.0.1:8081/admin/policies      # subject grants
curl -s -H "$ADMIN" "http://127.0.0.1:8081/admin/audit?request_id=req_1"   # audit lookup
curl -s -H "$ADMIN" "http://127.0.0.1:8081/admin/usage"       # tokens, errors, latency
curl -s -X POST -H "$ADMIN" http://127.0.0.1:8081/admin/models/gateway-echo/disable  # audited toggle
curl -s -H "$ADMIN" http://127.0.0.1:8081/admin/management-log
```

Management responses are metadata only: no keys, secrets, URLs, or
prompt/completion content.

## 6. Production mode

PostgreSQL persistence, Redis limiting, and real providers are configured
through `GATEWAY_DATABASE_URL`, `GATEWAY_LIMITS_MODE=redis`, and the
provider registry; see the main `README.md` and the container stack section.
Capability declarations live in `model_catalog.capabilities` (see migration
`0003_v1_2_developer_platform.up.sql` for the full schema).
