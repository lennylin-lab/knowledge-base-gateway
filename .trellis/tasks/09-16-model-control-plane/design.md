# Design: model control plane (embeddings, default model, retrieval profiles)

## Migration 0005 (additive, with down)

- `ALTER TABLE access_policies ADD COLUMN default_model TEXT REFERENCES
  model_catalog(public_name);` and `... ADD COLUMN default_embedding_model
  TEXT REFERENCES model_catalog(public_name);`
- `ALTER TABLE model_catalog ADD COLUMN retrieval_profile JSONB;`
- FKs follow the existing text-FK convention; down drops columns in reverse.
- No seeds required; seeded mock model may gain `embeddings: true` +
  `embedding_dim` + a demo `retrieval_profile` for local parity with issue
  examples (capabilities JSONB keys are additive).

## Domain and config

- `model.Capabilities` gains `Embeddings bool` (`embeddings`) and
  `EmbeddingDim int` (`embedding_dim`, omitempty; 0 = undeclared). Fake
  provider declares both; deterministic embeddings output is a fixed
  pseudo-random vector of exactly `embedding_dim` floats derived from the
  input hash (stable, offline-testable).
- `config.EmbeddingsEnabled` from `GATEWAY_EMBEDDINGS_ENABLED` (default
  true), same pattern as `ResponsesEnabled`; false unregisters the route.
- Default models: policy resolution already loads per-subject limits; extend
  the policy loader (pg + memory) to carry `DefaultModel` /
  `DefaultEmbeddingModel`; admin policies view surfaces both.

## Admissions and protocol

- `POST /v1/embeddings` reuses the shared admission pipeline with a new
  protocol label `embeddings` (audit `llm_requests.protocol` value; existing
  `chat`/`responses` values untouched):
  auth → model/policy (backfill `default_embedding_model` when `model`
  absent; chat/responses backfill `default_model`) → capability precheck
  (`embeddings`) → rate limit → quota reserve → provider call → settle to
  reported input-token usage. `embedding_dim` mismatch between catalog
  declaration and provider response is a 500-class config error (fail loud,
  never return a wrong-width vector silently).
- Embeddings requests reject `stream`, tools, and response fields with 400;
  unknown-field policy follows chat (ignore) — the endpoint is OpenAI
  chat-family shaped.
- New provider surface: `Embeddings(ctx, req model.EmbeddingsRequest)
  (model.EmbeddingsResponse, error)` on the `Provider` interface; adapters
  translate (openai real, fake deterministic). Anthropic returns
  capability-unsupported (declared false by default).
- Retrieval profile: `model_catalog.retrieval_profile` JSONB loaded with the
  catalog, surfaced opaquely (validated JSON, no schema enforcement in
  gateway) as `retrieval_profile` in `/v1/models/{model}` detail only.
  `config_version` bump semantics unchanged.

## Admin

- `/admin/policies` rows gain `default_model` / `default_embedding_model`;
- `POST /admin/policies/{subject}/default-model` sets either slot
  (body `{model, kind: "chat"|"embedding"}`), through the atomic
  mutation + `admin_audit` transaction (pattern of `SetModelEnabledWithAudit`)
  with runtime refresh via the injected apply boundary.
- No retrieval-profile mutation endpoint (catalog data owned by SQL tooling).

## Rollback

- Migration 0005 down; `GATEWAY_EMBEDDINGS_ENABLED=false`; default-model
  backfill is inert unless policies declare defaults; retrieval_profile is
  additive JSONB. Each feature reverts independently.
