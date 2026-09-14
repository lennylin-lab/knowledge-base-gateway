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

## V1.1 production mode

Set `GATEWAY_DATABASE_URL` to enable PostgreSQL persistence (keys, catalog,
providers, primary/backup routes, policies, audit records). Migrations live in
`migrations/` and are applied out of band; there is no auto-migration at
startup. Set `GATEWAY_LIMITS_MODE=redis` (+ `GATEWAY_REDIS_ADDR`) for the
distributed rate/concurrency limiter — readiness fails until Redis answers.
Set `GATEWAY_ADMIN_TOKEN` to enable the admin API on `GATEWAY_ADMIN_ADDR`
(default `:8081`, internal network only).

| Variable | Default | Meaning |
|---|---|---|
| `GATEWAY_DATABASE_URL` | – | PostgreSQL DSN; enables persistent keys/catalog/routes/policies/audit |
| `GATEWAY_LIMITS_MODE` | `local` | `local` (dev-only, single instance) or `redis` |
| `GATEWAY_REDIS_ADDR` | `127.0.0.1:6379` | Redis address for distributed limits |
| `GATEWAY_ADMIN_TOKEN` | – | Bearer token for the admin API (admin API disabled when unset) |
| `GATEWAY_ADMIN_ADDR` | `:8081` | Admin API listen address |
| `GATEWAY_PROVIDER` | `fake` | `fake`, `openai`, or `anthropic` |
| `ANTHROPIC_API_KEY` / `ANTHROPIC_BASE_URL` | – | Anthropic credentials (env only) |
| `GATEWAY_ALLOW_INSECURE_BASE_URLS` | `false` | Allow `http://` provider base URLs (development only) |

### Admin API

Token-gated (`Authorization: Bearer $GATEWAY_ADMIN_TOKEN`):

- `POST /admin/keys` `{"subject":"svc","tenant_id":"...","expires_in_hours":24}` — returns the plaintext key exactly once
- `GET /admin/keys?subject=svc` — metadata only (prefix, status, timestamps)
- `POST /admin/keys/{id}/rotate` — new plaintext, old key revoked
- `POST /admin/keys/{id}/revoke`

### Routing and reliability

Public models route through `model_routes` (priority order) to a primary and
a backup provider. Circuit breakers track consecutive failures per route and
recover via half-open probes. Retries remain finite, deadline-bounded, and
only for pre-output network/429/5xx/timeout failures; streaming never switches
providers once output has reached the client. Provider base URLs are validated
at startup against SSRF rules (https only unless explicitly allowed).

### Metrics

`/metrics` exposes `gateway_requests_total{model,status}`,
`gateway_upstream_errors_total{model,provider,class}`,
`gateway_tokens_total{model,kind}`, and
`gateway_rate_limit_total{model}`. `X-Trace-ID` is honored (or derived from
the request ID) and echoed for log/trace/audit correlation.

See `docs/gateway-client-contract.md` for the knowledge-base-server client
contract.

## Design notes and known limitations

- **Audit is metadata-only** (subject, model, provider, status, latency, token
  usage when reported, request/trace id). Prompts/completions are never logged
  or stored; unknown token usage is recorded as unknown, never zero.
- **Token ceilings** cap `max_tokens` from policy; upstream-reported usage is
  required for accounting, and daily/monthly quota enforcement beyond policy
  storage is not yet implemented.
- **Local limiter** is development-only and per-process; multi-instance
  deployments must use `GATEWAY_LIMITS_MODE=redis` (readiness gates this).
- The `Known` flag on usage is internal; token usage absent from an upstream
  response is not fabricated.

## Development

```bash
go test ./...   # all package tests
go vet ./...
go build ./...
```
