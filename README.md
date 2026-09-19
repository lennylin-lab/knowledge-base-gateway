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
| `<KIND>_API_KEY__<PROVIDER_NAME>` | – | Optional per-provider credential override; see "Per-provider credentials" below |
| `GATEWAY_API_KEYS` | – | Dev-only: `id:subject:plaintext-key` comma-separated. Production keys live in PostgreSQL (`api_keys` table, salted hashes). |
| `GATEWAY_MODELS` | – | Dev-only: `public-name:provider:upstream-model` comma-separated. Production catalog lives in PostgreSQL. |
| `GATEWAY_MAX_RETRIES` | `2` | Finite retries for pre-output network/429/5xx/timeout failures (non-streaming requests) |
| `GATEWAY_STREAM_STALL_TIMEOUT` | `30s` | Max silent gap between stream frames (first frame included); a stall is a timeout-class failure retried like any other pre-output failure. Set `0` to disable (rollback switch) |
| `GATEWAY_STREAM_TOTAL_TIMEOUT` | `10m` | Coarse safety cap for one streaming request; healthy streams are never bound by the non-streaming request deadline. Set `0` to disable |
| `GATEWAY_STREAM_MAX_RETRIES` | `10` | Finite retries on the primary route for pre-output streaming failures (stall/timeout/network/429/5xx); after output begins a stream is never retried |
| `GATEWAY_RATE_PER_MINUTE` | `120` | Per-subject request rate (fixed window) |
| `GATEWAY_RESPONSES_ENABLED` | `true` | Set `false` to disable `/v1/responses` (rollback switch) |
| `GATEWAY_EMBEDDINGS_ENABLED` | `true` | Set `false` to disable `/v1/embeddings` (rollback switch) |
| `GATEWAY_ASYNC_ENABLED` | `false` | Set `true` to accept `background: true` Responses jobs (V1.4; requires database mode) |
| `GATEWAY_BUDGETS_ENABLED` | `false` | Set `true` to enforce monetary budgets (V1.4; database mode). Ledger capture is always on in database mode — enforcement can be rolled back without losing cost evidence |
| `GATEWAY_LIFECYCLE_ENABLED` | `true` | Data lifecycle (retention/archive/export, V1.4; database mode). Set `false` to disable the admin lifecycle endpoints and stop `cmd/maintain` — the documented rollback point. Inert until `retention_policies` rows exist; see `docs/data-lifecycle.md` |
| `GATEWAY_LIFECYCLE_ARCHIVE_DIR` | `archives` | Filesystem archive-sink root for retention archives; production configuration must point this at a durable location (see `docs/data-lifecycle.md`) |
| `GATEWAY_ASYNC_WORKERS` | `2` | Background-job worker goroutines (1..64) |
| `GATEWAY_ASYNC_POLL_INTERVAL` | `1s` | Queue poll / recovery sweep cadence |
| `GATEWAY_ASYNC_LEASE` | `60s` | Worker claim lease, heartbeat-extended |
| `GATEWAY_ASYNC_JOB_TIMEOUT` | `10m` | Per-job upstream execution deadline (async requests only; the synchronous deadline is unchanged) |
| `GATEWAY_ASYNC_MAX_ATTEMPTS` | `3` | Job executions before terminal failure |
| `GATEWAY_ASYNC_RESULT_TTL` | `24h` | Terminal result retention; expired results answer 410 `response_expired` |
| `GATEWAY_ASYNC_IDEMPOTENCY_TTL` | `24h` | `Idempotency-Key` mapping retention |
| `GATEWAY_ASYNC_MAX_RESULT_BYTES` | `1048576` | Stored result size cap (>= 1024); larger responses fail as `result_too_large` |
| `GATEWAY_ASYNC_DRAIN_TIMEOUT` | `10s` | Graceful-shutdown window for in-flight jobs to commit |
| `GATEWAY_OTLP_ENABLED` | `false` | Set `true` to install the OpenTelemetry SDK and export spans over OTLP/HTTP (V1.4; independent kill switch — serving, Prometheus, and workers are unaffected either way). See `docs/observability.md` |
| `GATEWAY_OTLP_ENDPOINT` | `localhost:4318` | OTLP/HTTP collector endpoint (`host:port`) |
| `GATEWAY_OTLP_INSECURE` | `false` | Set `true` for plain-HTTP export (dev/loopback collectors) |
| `GATEWAY_OTLP_SAMPLING_RATIO` | `1.0` | Root-span trace sampling ratio 0..1 (spans parented by a sampled caller follow that decision) |
| `GATEWAY_OTLP_TIMEOUT` | `10s` | Per-export timeout; export failures never block serving |
| `GATEWAY_SETTLEMENT_BACKLOG_MAX` | `100` | `/readyz` threshold: reserved ledger rows older than 10 minutes above this flip readiness unhealthy |
| `GATEWAY_DEFAULT_MODELS` | – | Dev-only default models: `subject:chat-model[:embedding-model]` comma-separated. Production slots live in `access_policies`. |

Request size limits: 1 MiB body, 64 messages, 32k characters per message
(constants in `internal/config`).

### Per-provider credentials

Multiple providers of the same kind can use different credentials, e.g. an
openai-kind chat upstream and an openai-kind embeddings upstream side by side,
each with its own key. For every enabled `providers` row the gateway resolves
the credential from `<KIND>_API_KEY__<PROVIDER_NAME>` first — provider name
uppercased, non-alphanumerics mapped to `_` — and falls back to the kind-level
`<KIND>_API_KEY` (`OPENAI_API_KEY` / `ANTHROPIC_API_KEY`) when the
per-provider variable is unset. Example: two openai-kind rows named `chat`
and `openai-embed`, each with its own key, plus a fallback for everything
else:

```bash
OPENAI_API_KEY=sk-kind-level...             # fallback for rows without their own variable
OPENAI_API_KEY__CHAT=sk-chat-upstream...    # provider row "chat"
OPENAI_API_KEY__OPENAI_EMBED=sk-embed-2...  # provider row "openai-embed"
```

`anthropic` rows work symmetrically (`ANTHROPIC_API_KEY__<PROVIDER_NAME>`),
`fake` rows need no credential, and deployments without any per-provider
variable behave exactly as before. An enabled row with neither variable
refuses startup with an error naming the checked variables — never any
value.

## Endpoints

- `POST /v1/chat/completions` — OpenAI-compatible chat completions (streaming and non-streaming)
- `POST /v1/responses` — Responses-compatible protocol (V1.2): text, tool
  calling, JSON mode / structured output, SSE events; disable with
  `GATEWAY_RESPONSES_ENABLED=false` (independent rollback switch). With
  `background: true` (V1.4) the request persists as a durable job and
  returns `202` with a status envelope (see "V1.4: background Responses
  jobs" below)
- `GET /v1/responses/{id}` — background-job status and stored result
  (owner-scoped; V1.4, requires `GATEWAY_ASYNC_ENABLED=true`)
- `POST /v1/responses/{id}/cancel` — idempotent cancellation of a queued or
  running background job (V1.4)
- `POST /v1/embeddings` — OpenAI-compatible embeddings proxy (V1.3): string
  or string-array input, `object: "list"` envelope, input-token-only usage;
  disable with `GATEWAY_EMBEDDINGS_ENABLED=false` (independent rollback
  switch)
- `GET /v1/models`, `GET /v1/models/{model}` — caller-filtered model
  discovery with public capability/limit metadata only (V1.2); the detail
  adds `retrieval_profile` when the catalog row declares one (V1.3)
- `GET /healthz` — liveness, no dependency checks
- `GET /readyz` — configuration and wiring validated
- `GET /metrics` — Prometheus text format emitted by the official
  `prometheus/client_golang` (label values are escaped safely)

Capability enforcement is a routing constraint: features a model's catalog
declaration does not allow (tools, structured output, JSON mode, streaming,
vision, reasoning, the Responses or Embeddings protocol itself) are rejected
with 400 `capability_not_supported` before any provider is contacted.

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

- **Input ceilings**: `max_input_tokens` (per subject) and the model's
  declared `context_tokens` cap the deterministic input estimate
  (`message content chars / 4`). Requests above either ceiling are rejected
  with 400 `invalid_request` before rate limiting, quota reservation, or any
  provider call. `NULL`/`0` means unset.
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
- **Settle**: after a successful response the reservation is adjusted
  exactly once to the upstream-reported total token usage — non-streaming
  from the response usage, and streams from the reported stream usage
  (OpenAI upstreams are asked with `stream_options.include_usage`; Anthropic
  reports usage on `message_delta`). If the upstream reports no usage, the
  conservative reservation stays charged — unknown usage is never fabricated
  as zero in enforcement, audit, or metrics.
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
startup — `fake` needs no secret, `openai` resolves
`OPENAI_API_KEY__<PROVIDER_NAME>` with fallback to `OPENAI_API_KEY`, and
`anthropic` resolves `ANTHROPIC_API_KEY__<PROVIDER_NAME>` with fallback to
`ANTHROPIC_API_KEY` (see "Per-provider credentials" above) — and a missing
credential aborts startup before the server can report ready. Secrets are read
from the process environment only and are never persisted, logged, or echoed
in errors.

| Variable | Default | Meaning |
|---|---|---|
| `GATEWAY_DATABASE_URL` | – | PostgreSQL DSN; enables persistent keys/catalog/routes/policies/audit |
| `GATEWAY_LIMITS_MODE` | `local` | `local` (dev-only, single instance) or `redis`; selects the backing store for rate/concurrency limits and token quotas |
| `GATEWAY_REDIS_ADDR` | `127.0.0.1:6379` | Redis address for distributed limits |
| `GATEWAY_ADMIN_TOKEN` | – | Legacy bootstrap bearer token for the admin API (platform-admin). In database mode the admin API also runs without it once scoped admin credentials exist; see `docs/admin-rbac.md` |
| `GATEWAY_ADMIN_ADDR` | `:8081` | Admin API listen address |
| `GATEWAY_PROVIDER` | `fake` | `fake`, `openai`, or `anthropic`; local dev mode only — ignored when `GATEWAY_DATABASE_URL` is set |
| `ANTHROPIC_API_KEY` / `ANTHROPIC_BASE_URL` | – | Anthropic credentials (env only); per-provider override `ANTHROPIC_API_KEY__<PROVIDER_NAME>` |
| `GATEWAY_ALLOW_INSECURE_BASE_URLS` | `false` | Development only: allow `http://` provider base URLs and loopback/private IP-literal hosts (e.g. `127.0.0.1`). Restricted IP destinations (link-local/cloud metadata, multicast, unspecified) stay rejected in every mode |

### Admin API

Authenticated per the V1.4 admin identity model (see
`docs/admin-rbac.md`): scoped admin credentials (`kba_...`, minted through
`/admin/admins`, checked against a per-route scope) plus the legacy
`GATEWAY_ADMIN_TOKEN` as the platform-admin bootstrap identity during the
migration — both paths stay live until scoped credentials are
production-verified (`Authorization: Bearer ...`):

- `POST /admin/admins` `{"admin_subject":"ops","scopes":["operator"],"tenant_id":"...","expires_in_hours":24}` — mints a scoped admin credential; plaintext returned exactly once
- `GET /admin/admins`, `POST /admin/admins/{id}/rotate`, `POST /admin/admins/{id}/revoke`
- `POST /admin/keys` `{"subject":"svc","tenant_id":"...","expires_in_hours":24}` — returns the plaintext key exactly once
- `GET /admin/keys?subject=svc` — metadata only (prefix, status, timestamps)
- `POST /admin/keys/{id}/rotate` — new plaintext, old key revoked
- `POST /admin/keys/{id}/revoke`
- `POST /admin/policies/{subject}/default-model` `{"model":"...","kind":"chat"|"embedding"}` — sets the
  subject's default model slot (V1.3); commits atomically with its
  management-audit record and refreshes the running policy without a restart

### Default models (V1.3)

`access_policies.default_model` and `access_policies.default_embedding_model`
(nullable FKs into `model_catalog`) carry per-subject defaults. Requests that
omit `model` are backfilled before model resolution: chat/responses use the
chat slot, embeddings use the embedding slot. An explicit `model` always wins
(A/B testing and escape hatches); a request with neither a configured default
nor a `model` field gets the stable 400 `invalid_request`. Defaults never
bypass grants: a default pointing at a model the subject cannot use stays the
non-leaky 403. Local development mode assigns slots with
`GATEWAY_DEFAULT_MODELS="subject:chat-model[:embedding-model],..."`.

### Multi-row policy folding

A subject may hold one `access_policies` row per granted model; the gateway
folds the rows into one effective policy per subject:

- **Default-model slots** take the first non-empty value in row `id` order
  (chat and embedding judged independently); a NULL slot on one row never
  erases a default declared on another.
- **Ceilings** (`rate_per_minute`, `max_concurrent`, `daily_tokens`,
  `monthly_tokens`, `max_input_tokens`, `max_output_tokens`) take the
  minimum declared value across the subject's rows — adding a row can never
  raise a quota. A row without a cap does not constrain that field; if no
  row declares a cap, the subject stays uncapped for it.

The admin policies view (`GET /admin/policies`) attaches an
`effective_limits` block to every row — the exact folded ceilings
enforcement uses — so a tightening row is visible to operators rather than
silent.

Note for existing multi-row deployments: where a subject's rows declared
different ceiling values, folding previously took the last row's values (by
`id` order); after upgrading, the minimum declared value applies, which can
lower the effective quotas of multi-row subjects. Single-row subjects are
unaffected.

### Routing and reliability

Public models route through `model_routes` (priority order) to a primary and
a backup provider. Circuit breakers (failsafe-go) track consecutive failures
per route, admit exactly one half-open probe after a cool-down, and recover
via successful probes; while every route is open, no traffic is admitted —
breakers are never bypassed. Route permits are taken per attempt, so a
request that stops at the primary can never consume the backup's recovery
probe. Retries remain finite and deadline-bounded with
exponential backoff and jitter between attempts (cenkalti/backoff), only for
pre-output network/429/5xx/timeout failures; streaming never switches
providers once output has reached the client. Streaming requests are bounded
by a frame-gap stall window (`GATEWAY_STREAM_STALL_TIMEOUT`) instead of the
non-streaming request deadline: a silent upstream is noticed per frame, a
healthy long-lived stream is only capped by the coarse streaming total
(`GATEWAY_STREAM_TOTAL_TIMEOUT`), and pre-output stalls retry on the primary
route up to `GATEWAY_STREAM_MAX_RETRIES`. Provider base URLs are validated
at startup against SSRF rules (https only unless explicitly allowed; unsafe
IP destinations such as loopback, private, link-local/metadata, multicast,
and unspecified addresses are rejected outside explicit local development).

### Metrics

`/metrics` is served by the official Prometheus Go client and exposes:

- `gateway_requests_total{model,status}`
- `gateway_request_duration_seconds{model,status}` (histogram)
- `gateway_upstream_errors_total{model,provider,class}`
- `gateway_tokens_total{model,kind}`
- `gateway_rate_limit_total{model}`
- `gateway_async_jobs_total{status}` (terminal background-job statuses)
- `gateway_async_queue_depth` (currently queued background jobs)

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

## V1.2: unified model protocol and developer platform

V1.2 keeps the V1/V1.1 contracts frozen and adds a provider-neutral domain
layer (`internal/model`) that both protocols translate through:

- **Unified protocol** — Chat Completions and `/v1/responses` share one
  routing, retry, failover, quota, and audit pipeline with per-protocol
  encoders, so the two surfaces can never drift.
- **Tool calling** — the gateway validates tool names/schemas/count/size,
  translates definitions per provider, assembles streamed argument
  fragments deterministically, and validates the final JSON. It never
  executes tools and never logs tool arguments; callers return tool results
  as input in the next request.
- **Structured output** — JSON mode and `json_schema` specs are validated
  (dialect, size, depth) before invocation; final output validation failures
  are recorded in audit as `schema_validation_failed` and never silently
  treated as clean successes. The Anthropic adapter translates
  `json_schema` to the standard forced-tool pattern (synthesized
  `structured_output` tool, forced tool choice) and unwraps the tool input
  as the JSON result, non-streaming and streaming; JSON mode stays
  OpenAI-only and is rejected per-model before invocation.
- **Provider contract tests** — both built-in adapters (OpenAI-compatible,
  Anthropic) pass the same offline suite over a mock transport: text/usage,
  streaming deltas and assembly, tool calls and result round-trip, structured
  output, unsupported-capability rejection, cancellation, malformed
  responses, 400/401/429/5xx/timeout mapping. The routing core has no
  provider-specific branches.
- **Management API** — `/admin/models`, `/admin/providers`,
  `/admin/policies`, `/admin/audit`, `/admin/usage`,
  `/admin/management-log`, and the audited
  `POST /admin/models/{name}/enable|disable` switch, all behind the admin
  token and metadata-only by construction. The toggle commits the mutation
  and its audit record in one transaction and refreshes the running
  catalog/routes immediately — no restart; a post-commit refresh failure is
  reported as `refresh_failed` with the audit evidence intact. Provider
  views carry live breaker/health state and a recent error summary; usage
  views carry error rate, true percentiles, and first-token latency
  percentiles over recorded streams (cost stays the staged `null` contract
  field until a pricing decision lands).

Docs: `docs/developer-quickstart.md` (mock provider setup, examples),
`docs/api-versioning.md` (compatibility, deprecation, flags),
`docs/examples/` (curl, Python, OpenAI SDK, Go — the same request samples
the automated smoke tests run).

## V1.3: model control plane

V1.3 makes the gateway the single control plane for model-related
configuration, additively and per-model gated like every protocol before it:

- **Embeddings proxy** (`POST /v1/embeddings`) — OpenAI-compatible
  passthrough (`model`, `input` string or string array; `object: "list"`
  response with `data[].embedding` and input-token-only `usage`) served by
  the same auth/admission/rate-limit/quota/audit pipeline. Vectors come from
  the provider adapter; the fake provider returns a deterministic
  input-hash-derived vector of exactly the declared `embedding_dim` width.
  A vector whose width differs from the catalog declaration is a gateway
  configuration error (500 `embedding_dim_mismatch`): it fails loud and
  never returns a wrong-width vector. Vector and input content never reach
  logs or audit. The capability matrix gains `embeddings` and
  `embedding_dim` (the directory attribute: pgvector width is fixed per
  model, the gateway declares it and clients read it), and the endpoint is
  independently togglable per model and via
  `GATEWAY_EMBEDDINGS_ENABLED=false`.
- **Subject default models** — see "Default models" above. Both slots ship
  in migration 0005, and the admin mutation goes through the atomic
  mutation + management-audit path with live runtime refresh.
- **Retrieval profiles** — `model_catalog.retrieval_profile` JSONB (nullable)
  holds an opaque JSON object of retrieval thresholds (the server's
  `SEARCH_*` family) attached to the catalog model row. It rides model
  discovery: `GET /v1/models/{model}` includes `retrieval_profile` when the
  row declares one; the list stays unchanged. Profiles are catalog data:
  changes bump `config_version` and are owned by SQL/catalog tooling (no
  admin mutation surface). Server env values remain the fallback so the
  server still starts standalone when the gateway is down.
- **Shared token pool** — embeddings usage settles into the subject's
  existing daily/monthly token pool (one budget, no new policy columns),
  settling exactly once to the reported input-token total; unknown usage
  keeps the conservative reservation.

## V1.4: background Responses jobs

Long-running Responses requests can execute durably. A request with the new
optional field `"background": true` returns `202 Accepted` immediately with a
status envelope (`id`, `object: "response"`, `status: "queued"`, `model`,
`created`, `request_id`); the synchronous contract is unchanged for every
request that omits the field. Background jobs are opt-in via
`GATEWAY_ASYNC_ENABLED=true` and require database mode (PostgreSQL owns the
job state machine); with the flag off, background requests answer the stable
`503 job_queue_unavailable`, which is also the rollback posture.

- **Lifecycle** — `queued`, `running`, `completed`, `failed`, `cancelled`,
  `expired`. Workers claim jobs with `FOR UPDATE SKIP LOCKED` under a
  heartbeat-extended lease; expired leases return jobs to the queue (bounded
  by `GATEWAY_ASYNC_MAX_ATTEMPTS`), and recovery after a restart needs only
  the database. Background requests pass the same authentication, admission,
  rate limiting, and token-quota gates as synchronous ones — the queue is
  never a bypass.
- **Retries and backoff** — a requeue that expects another try (transient
  re-admission failure such as a dropped route, rate-limit denial, retryable
  upstream failure) consumes one attempt, delays the job's next claimability
  by the poll interval (`async_jobs.visible_at`), and terminal-fails the job
  with the stable class (`no_route_available`, `rate_limit_exceeded`, or the
  upstream class) once `GATEWAY_ASYNC_MAX_ATTEMPTS` is reached — a failing
  job can neither loop in a tight claim cycle nor head-of-line-block younger
  queued work. Infrastructure outages (limiter, quota, or policy store
  unreachable) requeue with the same backoff but never consume an attempt and
  never terminal-fail: an outage is never disguised as a job outcome, and
  jobs proceed when the infrastructure recovers. Shutdown aborts and lease
  recovery re-queue immediately (no backoff, shutdown aborts cost no
  attempt).
- **Query** — `GET /v1/responses/{id}` is owner-scoped (any other caller,
  including for existing jobs, gets the same `404 response_not_found`).
  `queued`/`running` answers carry a `Retry-After` hint; `completed` and
  `failed` return the stored public envelope (normalized gateway shape only,
  never provider-private fields); expired results answer `410
  response_expired` and never re-trigger execution. Queries never start work.
- **Cancel** — `POST /v1/responses/{id}/cancel` is idempotent, returns the
  final observable state, and propagates to the in-flight upstream call.
  Cancel-versus-completion races are decided by one conditional database
  update: exactly one terminal outcome and one audit record exist per job.
- **Idempotency** — an `Idempotency-Key` header scopes to the calling
  subject: the same key with the same request returns the original job; the
  same key with a different request is `409 idempotency_conflict`. Only
  hashes and request digests are stored. Background streaming is a stable
  `400`.
- **New error codes** — `idempotency_conflict` (409),
  `response_not_found` (404), `response_expired` (410), `job_queue_unavailable`
  (503). All existing synchronous error codes are unchanged.

## Design notes and known limitations

- **Audit is metadata-only** (subject, model, provider, status, latency, token
  usage when reported, request/trace id). Prompts/completions are never logged
  or stored; unknown token usage is recorded as unknown, never zero.
- **Token ceilings** cap `max_tokens` from policy; daily/monthly token
  quotas are enforced via reserve-before-call / settle-after-response (see
  "Token quotas" above). A reservation whose outcome was never finalized
  (e.g. a crashed process) stays charged until the UTC period rolls over.
  Streams settle to the upstream-reported usage when one is reported;
  streams without reported usage keep their conservative reservation.
- **First-token latency** is recorded for streams as the time to the first
  output event (`llm_requests.first_token_millis`, nullable); non-streaming
  requests record NULL (their full-latency equivalent is `latency_ms`).
  `/admin/usage` reports true first-token percentiles in PostgreSQL mode;
  development mode omits the fields. `cost_micros` remains the staged
  always-null contract field until real pricing data exists.
- **Local limiter** is development-only and per-process; multi-instance
  deployments must use `GATEWAY_LIMITS_MODE=redis` (readiness gates this).
  The token-quota gate follows the same mode.
- **Background jobs** store the normalized post-admission request and the
  public response envelope only — never raw client bytes, credentials, or
  provider-private fields. A crash between the upstream call and the terminal
  commit re-executes the job (upstream effects are at-least-once); the local
  terminal state and its accounting remain exactly-once.
- The `Known` flag on usage is internal; token usage absent from an upstream
  response is not fabricated.

## Development

```bash
go test ./...   # all package tests
go vet ./...
go build ./...
go run ./cmd/replay   # offline protocol replay over the checked-in fixtures
```

`cmd/replay` runs the deterministic provider fixtures through the real
adapters — mock-provider fixtures directly, and OpenAI/Anthropic fixtures
through a canned stub transport — asserting the normalized responses and
stream events. It performs no network I/O and needs no API keys. Custom
fixture directories: `go run ./cmd/replay -dir <path>`.

## V1.4: cost governance (pricing, usage ledger, monetary budgets)

Every request — synchronous or background — settles into a durable usage
ledger (`usage_ledger`): a `reserved` row is written before any provider
work, then finalized exactly once as `settled` (with the reported token
classes, the selected price version, and the computed cost) or `released`
(no billable outcome). The same request can never produce two settled
records, and repeated finalization is a no-op.

- **Pricing** — prices live in the versioned `pricing_catalog`: micros per
  token (integer micro units, never floats) for input, output, and — when
  the upstream reports them — reasoning and cached-input tokens, plus a
  currency and an `effective_from` instant. Settlement uses the version in
  force at request time and records it, so every known cost is exactly
  recomputable from tokens + price version. A class is charged only when
  the upstream reported it; a reported class without a price — or unknown
  usage — keeps the cost NULL. Unknown cost is never fabricated as zero.
  Manage the catalog with `GET/POST /admin/prices`.
- **Budgets** — `budget_policies` caps spend per subject and per tenant,
  daily and monthly on UTC boundaries, in one currency per applicable
  policy. Checks run before the provider is invoked: the subject's and the
  tenant's counters are checked and charged in one atomic operation (Redis
  in `GATEWAY_LIMITS_MODE=redis`, in-process otherwise), so concurrent
  instances cannot oversell either dimension. True exhaustion answers `429
  budget_exceeded` with the UTC boundary as `Retry-After`; a Redis or
  database outage answers `503 limiter_unavailable` — an outage is never a
  denial. A configured budget with no effective price (or a price in
  another currency) refuses the request before provider work as `500
  pricing_unavailable` — a priceless request can never bypass a budget.
  Manage policies with `GET/POST /admin/budgets`.
- **Rollback posture** — `GATEWAY_BUDGETS_ENABLED` (default `false`) gates
  enforcement only. Ledger capture is unconditional in database mode, so
  enforcement can be disabled without losing cost evidence.
- **Observability** — `/admin/usage` merges the ledger into the per-model
  usage rows (`cost_micros` sum of known cost, `unknown_cost_requests`,
  distinct `price_versions`) and reports current-period budget utilization
  (`budgets`: limit, known spend, and unknown-cost settlements counted
  separately — never as zero spend). Prometheus counters:
  `gateway_budget_denials_total{scope}` and
  `gateway_ledger_settlement_failures_total` (a failed settlement leaves
  its `reserved` ledger row as retryable evidence — it is never silently
  marked settled).
- **Background jobs** — a cancelled job that already produced upstream
  output settles by the usage the provider reported (the losing worker
  records the one settlement); cancellation before any output releases the
  reservation. A budget denial at execution time fails the job terminally
  with `budget_exceeded` (or `pricing_unavailable`); infrastructure
  outages requeue the job free instead.
