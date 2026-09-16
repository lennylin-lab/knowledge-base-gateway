# Per-provider credential injection for openai-kind providers

## Goal

Allow multiple providers of the same kind to use different credentials via an
env naming convention, so a deployment can host e.g. an openai-kind chat
upstream (key₁) and an openai-kind embeddings upstream (key₂) side by side.
Implements issue #5 (mechanism option 1, the filer's preferred choice).

## Background and Confirmed Facts

- `providers` table rows carry per-row `kind` and `base_url`, but credentials
  are kind-level: `cmd/gateway/main.go::newProviderFromRegistry` reads
  `cfg.OpenAIKey` / `cfg.AnthropicKey` (from `OPENAI_API_KEY` /
  `ANTHROPIC_API_KEY`) for every provider of that kind — two openai upstreams
  necessarily share one key.
- Startup validation names the missing env variable and never the value;
  per-provider lookup must keep that property (error names the specific
  variable that was absent, checked first-then-fallback).
- `internal/config/config.go` loads credentials into `Config` before
  validation (mode-boundary rule); the per-provider convention is resolved
  at use time by provider name, so no config struct change is required —
  but the resolution helper belongs where `cfg` and provider name meet.
- Dev mode (`GATEWAY_PROVIDER=openai`) passes the literal provider name
  "openai" — its convention key would be `OPENAI_API_KEY__OPENAI`; the
  fallback to `OPENAI_API_KEY` keeps dev mode zero-config.

## Requirements

- Credential resolution per provider: env `<KIND>_API_KEY__<PROVIDER_NAME>`
  (provider name uppercased, non-alphanumerics → `_`) wins; falls back to
  `<KIND>_API_KEY`; applied to both `openai` and `anthropic` kinds
  (symmetry, zero cost). `fake` unaffected.
- Startup validation semantics unchanged: an enabled provider missing both
  the per-provider and kind-level variable refuses startup with an error
  naming the variable(s) checked — never any value.
- No migration, no HTTP contract change, no admin surface, no CI change.
- README configuration table documents the convention and the fallback
  order with an example (chat upstream + embeddings upstream, two keys).
- Backward compatibility: deployments without per-provider variables behave
  exactly as today (golden/e2e untouched).

## Acceptance Criteria

- [ ] Two openai-kind providers (different `base_url`, different
  `OPENAI_API_KEY__<NAME>` values) start healthy and each request carries
  its own key upstream (stub upstream asserts the Authorization header).
- [ ] Per-provider variable absent → kind-level fallback used (current
  behavior, test-pinned).
- [ ] Both absent → startup fails naming the checked variables, no secret
  or value in the message; same for anthropic kind.
- [ ] Provider names with hyphens/dots uppercased with non-alphanumerics
  mapped to `_` (e.g. `openai-embed` → `OPENAI_API_KEY__OPENAI_EMBED`).
- [ ] `gofmt`, `go vet`, `go test -race ./...` pass with env-gated tests run
  for real (`TEST_DATABASE_URL`/`TEST_REDIS_ADDR`, zero skips).

## Out of Scope

- `providers.credential_env` column (option 2) and any secret storage in
  the database; per-request or per-subject credentials; key rotation
  tooling.

## Key Decisions

- Mechanism option 1 (env convention), per the issue filer's preference and
  zero-migration/backward-compatible properties.

## Open Questions (blocking)

- None.
