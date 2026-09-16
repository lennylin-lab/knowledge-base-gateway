# Model control plane: embeddings proxy, default model, retrieval profiles

## Goal

Make the Gateway the single control plane for model-related configuration the
server still owns: embedding calls (proxied with key/quota/audit), which model
a subject uses (subject default model), and RAG retrieval thresholds
(retrieval profiles delivered with model discovery). Implements issue #4
(features 1-3); feature 4 (capability-matrix-only feature isolation) is a
continuing principle, already the repo pattern, captured in spec.

## Background and Confirmed Facts

- `access_policies` (migrations/0001_init.up.sql:37-47) has per-subject model
  limits; no default-model column. `model_catalog.capabilities` JSONB carries
  the capability matrix; `model.Capabilities` (internal/model/model.go:213)
  has bool capability fields plus `ContextTokens/MaxOutputTokens/MaxTools`.
- Server embedding config (sibling repo config.py:36-41, env-prefixed `KB_`):
  `EMBEDDING_BASE_URL` (defaults to api.openai.com), `EMBEDDING_API_KEY`,
  `EMBEDDING_MODEL`, `EMBEDDING_DIM: 1536` — currently bypassing the gateway.
- Server retrieval thresholds (config.py:175-221): `SEARCH_BM25_MIN_SCORE`,
  `SEARCH_BM25_MIN_COVERAGE`, `SEARCH_VECTOR_MAX_DISTANCE`,
  `SEARCH_VECTOR_RESCUE_MARGIN`, `SEARCH_VECTOR_RESCUE_MAX_DISTANCE`,
  `SEARCH_VECTOR_RESCUE_TRIGGER_MAX_DISTANCE`, `SEARCH_RRF_MIN_RELATIVE`,
  `SEARCH_MAX_QUERY_LENGTH`.
- Quota/limiter/audit/admission pipeline is shared and protocol-agnostic
  (`internal/httpapi/common.go` admit()); `quota.Estimate` is input-chars/4;
  usage-known settlement exists for chat/responses (incl. streams).
- `/v1/models/{model}` detail already exposes the public capability matrix,
  limits, protocols, and `config_version`; provider/upstream names never
  exposed. Capability rejection happens pre-provider (`capability_not_supported`).
- openai-python 3.5.0 verified for chat+responses; `client.embeddings.create`
  is the SDK surface the server's `llm/embeddings.py` would switch onto via
  `base_url`.
- Gateway never stores provider secrets; embeddings proxy must not change
  that (server sends its gateway key; gateway injects the upstream credential).

## Requirements

### F1: Embeddings proxy (`POST /v1/embeddings`)

- OpenAI-compatible request/response passthrough (`model`, `input` string or
  array; response `object:"list"`, `data[].embedding`, `usage`).
- Capability matrix gains `embeddings: bool` and `embedding_dim: int`
  (directory attribute: pgvector column width is fixed per model; changing
  model/dim is a migration event, so the gateway declares it and the server
  reads it — never the reverse).
- Embeddings usage settles into the subject's existing daily/monthly token
  pool (decision 2026-09-16: same pool as chat — one budget, no new policy
  columns); settle to reported usage (embeddings usage is input-token only);
  unknown usage keeps the conservative reservation.
- `embeddings` independently togglable per model via admin disable (same
  mechanism as other capabilities).
- Rollback switch `GATEWAY_EMBEDDINGS_ENABLED=false` (mirrors the responses
  flag).
- SSRF/URL rules, redaction, and the no-provider-secret boundary unchanged.
- Server migration: `KB_EMBEDDING_BASE_URL/API_KEY` point at the gateway;
  `KB_EMBEDDING_DIM` reads from `/v1/models/{model}` (env stays as an
  offline/test escape hatch).

### F2: Subject default model

- `access_policies.default_model` and `access_policies.default_embedding_model`
  — nullable FKs → `model_catalog(public_name)`; both slots ship day one
  (decision 2026-09-16) so one migration covers chat and embeddings defaults.
- Chat/responses requests missing `model` backfill the subject's
  `default_model` before model resolution; embeddings requests missing
  `model` backfill `default_embedding_model`. Explicit `model` keeps current
  behavior (A/B and escape hatch). Requests with no configured default and
  no `model` field → stable 400 `invalid_request`.
- Server: `KB_CHAT_MODEL` becomes optional (set = explicit, current behavior;
  unset = omit `model` in requests).
- Admin: `/admin/policies` surfaces default; changing it goes through the
  atomic mutation + management-audit path (same pattern as model toggles).
- Local dev mode: `GATEWAY_MODELS` entry may name a default via an optional
  segment or a new env (`GATEWAY_DEFAULT_MODELS=subject:model,...`) — decide
  in design.md, documented either way.

### F3: Retrieval profile hosting

- A retrieval profile is a JSON object of retrieval thresholds (the server's
  SEARCH_* family: vector max distance, rescue margin/max/trigger, RRF
  relative floor, BM25 coverage, max query length) attached to the catalog
  model row (`model_catalog.retrieval_profile` JSONB, nullable; additive
  migration 0005).
- Delivery rides model discovery: `GET /v1/models/{model}` includes
  `retrieval_profile` in the detail (absent when unset). `/v1/models` list
  stays unchanged in shape except the additive field where present.
- Precedence: server env values remain the fallback; profile overrides when
  present — the server must start standalone when the gateway is down.
- Profiles are catalog data: changes bump `config_version`, are visible to
  clients, and are auditable via the existing catalog lifecycle (no new
  admin mutation surface this task; SQL/catalog tooling owns it, consistent
  with migrations-own-schema).
- Granularity: model-level only (decision 2026-09-16); per-subject override
  deferred until a real tenant need appears.

### F4 (principle, spec-only)

- New features stay capability-matrix-gated and pre-provider rejected;
  captured in backend spec (already the pattern; make it explicit for
  embeddings/retrieval-profile/citations-style features).

## Compatibility Promise (from the issue)

- Fully additive: current `model`-required semantics unchanged when clients
  send it; `/v1/models` shape changes only additively; migrations additive
  with downs; env escape hatches preserved on the server; gateway without
  the new features behaves exactly as today.

## Acceptance Criteria

- [ ] `POST /v1/embeddings` with a capability-enabled model returns an
  OpenAI-compatible list envelope through the fake provider (deterministic
  vector) and through the OpenAI adapter contract suite; `embedding_dim`
  mismatch between catalog and upstream is a startup/config error.
- [ ] Requests missing `model` resolve to the subject default and audit rows
  record the resolved model; explicit `model` behavior unchanged; no default
  + no model → 400 `invalid_request`; admin policy view shows default and
  changes are management-audited.
- [ ] `/v1/models/{model}` returns `retrieval_profile` when the catalog row
  declares one; absent otherwise; `config_version` bumps are visible.
- [ ] Embeddings usage settles quota exactly once; unknown usage keeps the
  conservative reservation; rate limit and concurrency apply per subject.
- [ ] Golden V1 fixtures byte-identical; trailing-JSON/strict-decoding rules
  intact; redaction unchanged (no embeddings/vector content in audit).
- [ ] `gofmt`, `go vet`, `go test -race ./...` pass with env-gated tests run
  for real (`TEST_DATABASE_URL`/`TEST_REDIS_ADDR`, zero skips); migration
  0005 up/down verified against real PostgreSQL; replay 12/12; actionlint
  clean if CI touched.

## Out of Scope

- Per-subject retrieval-profile overrides; billing/cost pricing; embedding
  batching/caching; server-side code changes (server migrates in its own
  repo/tasks).

## Key Decisions (user, 2026-09-16)

- Q1: `default_model` + `default_embedding_model` both in migration 0005.
- Q2: embeddings quota shares the subject's existing daily/monthly token
  pool (no independent pool, no new policy columns).
- Q3: retrieval profiles are model-level only (`model_catalog`
  `retrieval_profile` JSONB); per-subject overrides deferred.

## Open Questions (blocking)

- None. Q1-Q3 resolved.

## Planning Status

- Task size: complex; needs design.md + implement.md + curated jsonl.
- Blocking: Q1-Q3 (user decisions).
