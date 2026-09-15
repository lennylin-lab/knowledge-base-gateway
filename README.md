# knowledge-base-gateway

A standalone Go LLM gateway that sits between `knowledge-base-server` (or any
OpenAI-compatible client) and LLM providers. It owns API-key authentication,
model whitelisting, rate/concurrency limits, timeout/retry policy, and
metadata-only request auditing. See `docs/agent-start.md` for the full
requirement document.

## Quick start (development, fake provider)

```bash
cp .env.example .env
go run ./cmd/gateway
```

The gateway and migration commands load `.env` from the current directory when
it exists. Existing process environment variables take precedence, so
production deployments can continue to inject configuration directly without
using a dotenv file. Keep real credentials in an ignored `.env`, never in
`.env.example`.

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

Edit `.env`:

```bash
GATEWAY_PROVIDER=openai
OPENAI_API_KEY=sk-...        # keep only in ignored .env; never commit
OPENAI_BASE_URL=https://api.openai.com/v1
GATEWAY_MODELS="gpt-4o-mini:openai:gpt-4o-mini"
```

Then start the gateway normally:

```bash
go run ./cmd/gateway
```

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `GATEWAY_ADDR` | `:8080` | Listen address |
| `GATEWAY_PROVIDER` | `fake` | `fake`, `openai`, or `anthropic`; local dev mode only — ignored when `GATEWAY_DATABASE_URL` is set |
| `OPENAI_API_KEY` | – | Provider secret; required when provider is `openai` |
| `OPENAI_BASE_URL` | `https://api.openai.com/v1` | OpenAI-compatible base URL |
| `GATEWAY_API_KEYS` | – | Dev-only: `id:subject:plaintext-key` comma-separated. Production keys live in PostgreSQL (`api_keys` table, salted hashes). |
| `GATEWAY_MODELS` | – | Dev-only: `public-name:provider:upstream-model` comma-separated. Production catalog lives in PostgreSQL. |
| `GATEWAY_MAX_RETRIES` | `2` | Finite retries for pre-output network/429/5xx/timeout failures |
| `GATEWAY_RATE_PER_MINUTE` | `120` | Per-subject request rate (fixed window) |

Request size limits: 1 MiB body, 64 messages, 32k characters per message
(constants in `internal/config`).

## Endpoints

- `POST /v1/chat/completions` — OpenAI-compatible chat completions (streaming and non-streaming)
- `GET /healthz` — liveness, no dependency checks
- `GET /readyz` — configuration and wiring validated
- `GET /metrics` — Prometheus text format emitted by the official
  `prometheus/client_golang` (label values are escaped safely)

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
daily/monthly token quota exhausted -> 429 `quota_exceeded`; upstream
rejected request -> 400; upstream timeout -> 504; upstream
unavailable/rate-limited -> 503; unknown internal -> 500. Provider status
codes are never passed through raw. Limiter/quota infrastructure failures
are always 503 `limiter_unavailable` with no `Retry-After` and never
masquerade as a 429.

## Token quotas (`daily_tokens` / `monthly_tokens`)

Per-subject budgets configured in `access_policies` are enforced before any
provider invocation:

- **Periods**: UTC calendar day and UTC calendar month, tracked
  independently. `NULL` or `0` means that period is unlimited; subjects with
  no budget configured keep the pre-quota behavior. Counters are keyed by
  subject and UTC period and expire at the boundary (plus a grace window for
  settlements of requests that straddle it), so periods reset without manual
  cleanup.
- **Reserve**: a deterministic bounded estimate is charged atomically before
  the provider is called: declared `max_tokens` (already capped by the
  policy output ceiling), otherwise the policy `max_output_tokens`,
  otherwise a conservative default of 4096 output tokens, plus roughly
  `message content chars / 4` input tokens. Provider-specific tokenizers are
  not used.
- **Settle**: after a successful non-streaming response the reservation is
  adjusted exactly once to the upstream-reported total token usage. If the
  upstream reports no usage, the conservative reservation stays charged —
  unknown usage is never fabricated as zero in enforcement, audit, or
  metrics.
- **Release**: requests that fail before any provider output (including
  client cancellation) get the reservation back idempotently.
- **Atomicity**: multi-instance deployments (`GATEWAY_LIMITS_MODE=redis`)
  check and charge both periods in one Redis Lua script, so concurrent
  instances cannot oversubscribe a budget; Redis unavailability fails closed
  as 503 `limiter_unavailable`. Single-process development mode uses the
  same semantics in memory.

## V1.1 production mode

Set `GATEWAY_DATABASE_URL` to enable PostgreSQL persistence (keys, catalog,
providers, primary/backup routes, policies, audit records). Migrations live in
`migrations/` and are applied with `go run ./cmd/migrate` (see "Schema
migrations" below); there is no auto-migration at startup. Set
`GATEWAY_LIMITS_MODE=redis` (+ `GATEWAY_REDIS_ADDR`) for the
distributed rate/concurrency limiter — readiness fails until Redis answers.
Set `GATEWAY_ADMIN_TOKEN` to enable the admin API on `GATEWAY_ADMIN_ADDR`
(default `:8081`, internal network only).

`GATEWAY_DATABASE_URL` is the configuration-mode boundary. In database mode
the persisted configuration is authoritative: the development-only
`GATEWAY_API_KEYS` and `GATEWAY_MODELS` lists are optional (a supplied value
is still validated so a typo fails startup), and the legacy `GATEWAY_PROVIDER`
selector is ignored. Every enabled `providers` row is credential-checked at
startup — `fake` needs no secret, `openai` requires `OPENAI_API_KEY`, and
`anthropic` requires `ANTHROPIC_API_KEY` — and a missing credential aborts
startup before the server can report ready. Secrets are read from the process
environment only and are never persisted, logged, or echoed in errors.

| Variable | Default | Meaning |
|---|---|---|
| `GATEWAY_DATABASE_URL` | – | PostgreSQL DSN; enables persistent keys/catalog/routes/policies/audit |
| `GATEWAY_LIMITS_MODE` | `local` | `local` (dev-only, single instance) or `redis`; selects the backing store for rate/concurrency limits and token quotas |
| `GATEWAY_REDIS_ADDR` | `127.0.0.1:6379` | Redis address for distributed limits |
| `GATEWAY_ADMIN_TOKEN` | – | Bearer token for the admin API (admin API disabled when unset) |
| `GATEWAY_ADMIN_ADDR` | `:8081` | Admin API listen address |
| `GATEWAY_PROVIDER` | `fake` | `fake`, `openai`, or `anthropic`; local dev mode only — ignored when `GATEWAY_DATABASE_URL` is set |
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
a backup provider. Circuit breakers (failsafe-go) track consecutive failures
per route, admit exactly one half-open probe after a cool-down, and recover
via successful probes. Retries remain finite and deadline-bounded with
exponential backoff and jitter between attempts (cenkalti/backoff), only for
pre-output network/429/5xx/timeout failures; streaming never switches
providers once output has reached the client. Provider base URLs are validated
at startup against SSRF rules (https only unless explicitly allowed).

### Metrics

`/metrics` is served by the official Prometheus Go client and exposes:

- `gateway_requests_total{model,status}`
- `gateway_request_duration_seconds{model,status}` (histogram)
- `gateway_upstream_errors_total{model,provider,class}`
- `gateway_tokens_total{model,kind}`
- `gateway_rate_limit_total{model}`

Plus the standard Go runtime and process collectors. `X-Trace-ID` is honored
(or derived from the request ID) and echoed for log/trace/audit correlation.

### Schema migrations

Schema changes are versioned in `migrations/` (`<version>_<name>.up.sql` /
`.down.sql`) and applied with the operational CLI — the gateway process never
mutates the schema at startup:

```bash
go run ./cmd/migrate up      # apply all pending
go run ./cmd/migrate steps -1 # roll back one version
go run ./cmd/migrate down     # roll back everything (destructive)
go run ./cmd/migrate version  # current schema version
```

The migration command reads `GATEWAY_DATABASE_URL` from `.env` when no `-dsn`
flag is supplied. An explicit `-dsn` still takes precedence.

The tool (golang-migrate on the pgx/v5 driver) tracks the applied version in
`schema_migrations` and takes a PostgreSQL advisory lock, so concurrent
invocations are safe. New schema changes must ship as a new forward migration
plus a real down path; never edit an applied migration.

See `docs/gateway-client-contract.md` for the knowledge-base-server client
contract.

## Container stack and smoke test

`Dockerfile` is a multi-stage build pinned to the Go version declared by
`go.mod`; the runtime stage is a minimal Alpine image running as non-root
(uid/gid 10001). No provider secret, API key, or DSN is baked into the image —
all configuration is injected at run time by Compose or the deployment
environment.

`docker-compose.yml.example` is the safe template for the deployment-shaped
stack. Copy it to the local, ignored `docker-compose.yml` before starting:

```bash
cp docker-compose.yml.example docker-compose.yml
```

The stack runs PostgreSQL and Redis with health checks, a one-shot migration
job (`cmd/migrate up`, idempotent for a fully-applied schema), and the gateway
in database-backed Redis mode
(catalog/routes/policies/keys come from PostgreSQL; the dev-only
`GATEWAY_API_KEYS` / `GATEWAY_MODELS` variables are not used). The gateway
also runs the admin API with a deterministic throwaway token (loopback-only
host port; override `GATEWAY_ADMIN_TOKEN` for anything non-disposable). Docker
Compose reads the root `.env` for `${...}` interpolation; only variables
listed in the Compose `environment:` sections are passed to the Go process.
Destructive rollback is never part of startup.

```bash
docker compose build            # build kb-gateway:local
docker compose up -d            # postgres + redis + migrate + gateway
docker compose ps               # migrate shows Exited (0); gateway healthy
docker compose logs -f gateway  # follow gateway logs
docker compose stop             # stop everything (keep the data volume)
docker compose down             # remove containers + network
docker compose down -v          # also remove the PostgreSQL data volume
scripts/smoke.sh                # bounded end-to-end smoke (see below)
```

### Smoke test

`scripts/smoke.sh` builds and starts the stack, waits within bounded
timeouts, verifies the migration job completed successfully, asserts
`/healthz` is 200 and `/readyz` becomes 200, then stops and restarts Redis
and PostgreSQL to prove `/readyz` fails (503) while `/healthz` stays 200 and
readiness recovers afterwards. It then exercises the chat path end to end:
using the throwaway admin token configured in `docker-compose.yml` (dev stack
only), it mints a throwaway API key for the seeded subject through the admin
API, completes a non-streaming chat request against the seeded fake provider
(`gateway-echo`), and asserts an OpenAI-compatible success envelope before
revoking the key. On any failure it prints the relevant service logs and
exits with a distinct non-zero status:

| Exit | Meaning |
|---|---|
| `0` | success |
| `1` | preflight error (docker/curl missing, bad arguments) |
| `2` | stack failed to build/start or the migration job did not complete |
| `3` | `/healthz` never returned 200 |
| `4` | `/readyz` never returned 200 |
| `5` | Redis outage: `/readyz` did not fail while Redis was down |
| `6` | PostgreSQL outage: `/readyz` did not fail while PostgreSQL was down |
| `7` | Redis restart: `/readyz` did not return to 200 |
| `8` | PostgreSQL restart: `/readyz` did not return to 200 |
| `9` | liveness regression: `/healthz` stopped answering during an outage |
| `10` | admin API: never became ready or the API key could not be minted |
| `11` | chat completion failed or the envelope was not a successful OpenAI-compatible completion |

The admin token and minted API keys are sent in request headers only and are
never echoed — not in smoke output and not in gateway logs. Flags:
`--skip-outage`, `--down` (tear the stack down after success), `--timeout N`
(per-phase wait budget, default 90s; every phase above gets its own budget).

### Environment overrides

Host ports default to values chosen not to collide with a co-existing
knowledge-base-server stack (which uses 5432/6379):

| Variable | Default | Meaning |
|---|---|---|
| `GATEWAY_HOST_PORT` | `8091` | Host port for the gateway (`/healthz`, `/readyz`, `/v1/...`) |
| `GATEWAY_ADMIN_HOST_PORT` | `8092` | Host port for the admin API (`/admin/...`, loopback only) |
| `GATEWAY_ADMIN_TOKEN` | `smoke-admin-throwaway` | Admin API bearer token for the local stack; deterministic throwaway (same policy as the PostgreSQL password) — set your own for anything non-disposable |
| `POSTGRES_HOST_PORT` | `5433` | Host port for PostgreSQL |
| `REDIS_HOST_PORT` | `6381` | Host port for Redis |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | `gateway` / `gateway-local-throwaway` / `gateway` | Local PostgreSQL credentials; the default password is a deterministic throwaway (same policy as CI) — override it for anything non-disposable |
| `GATEWAY_IMAGE` | `kb-gateway:local` | Image tag built and used by the gateway and migrate services |
| `GATEWAY_SMOKE_URL` | `http://127.0.0.1:${GATEWAY_HOST_PORT:-8091}` | Base URL the smoke script targets |
| `GATEWAY_ADMIN_URL` | `http://127.0.0.1:${GATEWAY_ADMIN_HOST_PORT:-8092}` | Admin API base URL the smoke script targets |

To run operational migration commands against the Compose database, reuse the
one-shot service (the image entrypoint is the gateway binary, the migrate
service resets it):

```bash
docker compose run --rm --no-deps migrate            # idempotent `up`
```

`down`-style rollbacks stay operational-only (`cmd/migrate down` against a
database you own); the Compose stack never executes them.

## Design notes and known limitations

- **Audit is metadata-only** (subject, model, provider, status, latency, token
  usage when reported, request/trace id). Prompts/completions are never logged
  or stored; unknown token usage is recorded as unknown, never zero.
- **Token ceilings** cap `max_tokens` from policy; daily/monthly token
  quotas are enforced via reserve-before-call / settle-after-response (see
  "Token quotas" above). A reservation whose outcome was never finalized
  (e.g. a crashed process) stays charged until the UTC period rolls over.
  Streaming usage is not parsed today, so admitted streams keep their
  conservative reservation.
- **Local limiter** is development-only and per-process; multi-instance
  deployments must use `GATEWAY_LIMITS_MODE=redis` (readiness gates this).
  The token-quota gate follows the same mode.
- The `Known` flag on usage is internal; token usage absent from an upstream
  response is not fabricated.

## Development

```bash
go test ./...   # all package tests
go vet ./...
go build ./...
```
