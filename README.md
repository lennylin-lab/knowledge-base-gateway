# knowledge-base-gateway

A standalone Go LLM gateway that sits between `knowledge-base-server` (or any
OpenAI-compatible client) and LLM providers. It owns API-key authentication,
model whitelisting, rate/concurrency limits, timeout/retry policy, and
metadata-only request auditing. See `docs/agent-start.md` for the full
requirement document.

## Quick start (development, fake provider)

```bash
export GATEWAY_API_KEYS="key-1:tenant-a:sk-dev-internal-key"
export GATEWAY_MODELS="gpt-4o-mini:fake:gpt-4o-mini"
export GATEWAY_PROVIDER=fake
export GATEWAY_ADDR=:8080
go run ./cmd/gateway
```

Call it:

```bash
curl -s localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer sk-dev-internal-key" \
  -H "X-Request-ID: req-demo-1" \
  -d '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hello"}]}'
```

Streaming: add `"stream": true` to the body; the response is
`text/event-stream` terminated by `data: [DONE]`.

## Using the real OpenAI-compatible provider

```bash
export GATEWAY_PROVIDER=openai
export OPENAI_API_KEY=sk-...        # never persisted; process env only
export OPENAI_BASE_URL=https://api.openai.com/v1
export GATEWAY_MODELS="gpt-4o-mini:openai:gpt-4o-mini"
```

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `GATEWAY_ADDR` | `:8080` | Listen address |
| `GATEWAY_PROVIDER` | `fake` | `fake` (dev echo) or `openai` |
| `OPENAI_API_KEY` | – | Provider secret; required when provider is `openai` |
| `OPENAI_BASE_URL` | `https://api.openai.com/v1` | OpenAI-compatible base URL |
| `GATEWAY_API_KEYS` | – | Dev-only: `id:subject:plaintext-key` comma-separated. Production keys live in PostgreSQL (`api_keys` table, salted hashes). |
| `GATEWAY_MODELS` | – | `public-name:provider:upstream-model` comma-separated |
| `GATEWAY_MAX_RETRIES` | `2` | Finite retries for pre-output network/429/5xx/timeout failures |
| `GATEWAY_RATE_PER_MINUTE` | `120` | Per-subject request rate (fixed window) |

Request size limits: 1 MiB body, 64 messages, 32k characters per message
(constants in `internal/config`).

## Endpoints

- `POST /v1/chat/completions` — OpenAI-compatible chat completions (streaming and non-streaming)
- `GET /healthz` — liveness, no dependency checks
- `GET /readyz` — configuration and wiring validated
- `GET /metrics` — Prometheus text format (`gateway_requests_total{model,status}`)

## Error envelope

```json
{
  "error": {
    "type": "authentication_error",
    "code": "invalid_api_key",
    "message": "invalid API key",
    "request_id": "req_..."
  }
}
```

Mapping: invalid/expired/revoked key -> 401; unknown or disallowed model ->
403 (indistinguishable on purpose); invalid input -> 400; rate limit -> 429;
upstream rejected request -> 400; upstream timeout -> 504; upstream
unavailable/rate-limited -> 503; unknown internal -> 500. Provider status
codes are never passed through raw.

## Design notes and known limitations (MVP)

- **Rate limiting is in-memory** (`internal/limiter`) and per-process. It is
  correct only for a single instance. Multi-instance deployments need the
  Redis-backed implementation behind `store.LimiterState` (interface declared,
  implementation deferred).
- **Persistence is interface-only** (`internal/store`): the MVP uses in-memory
  auth/catalog/audit substitutes wired in `cmd/gateway`. The initial
  PostgreSQL schema is in `migrations/0001_init.sql`; repository
  implementations and a migration test harness are the next slice.
- **Audit is metadata-only** (subject, model, status, latency, token usage
  when reported, request id). Prompts/completions are never logged or stored;
  unknown token usage is recorded as unknown, never zero.
- **Retries** are finite (`GATEWAY_MAX_RETRIES`), deadline-bounded, and only
  for pre-output network/429/5xx/timeout failures. Streaming never retries
  after output has started and never switches providers mid-stream.
- The `Known` flag on usage is internal; token usage absent from an upstream
  response is not fabricated.

## Development

```bash
go test ./...   # all package tests
go vet ./...
go build ./...
```
