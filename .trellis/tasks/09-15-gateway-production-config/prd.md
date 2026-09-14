# Fix production-mode configuration

## Goal

Allow the Gateway's PostgreSQL-backed production mode to start from database
configuration without requiring development-only API keys or model lists, and
validate Provider credentials deterministically without leaking secrets.

## Confirmed Facts

- `internal/config/config.go` currently rejects an empty
  `GATEWAY_API_KEYS`/`GATEWAY_MODELS` before the database mode is known.
- The Anthropic environment Secret is assigned after the provider switch, so a
  valid `GATEWAY_PROVIDER=anthropic` configuration is rejected before the key
  is visible to validation.
- In database mode `cmd/gateway/main.go` loads keys, catalog, routes, and
  policies from PostgreSQL; environment key/model lists are not used.
- Database Provider rows contain the Provider kind and base URL, while Provider
  credentials remain process environment Secrets.

## Requirements

- Load all Provider Secret environment variables before any provider validation.
- Treat `GATEWAY_DATABASE_URL` as the configuration-mode boundary:
  - local mode keeps requiring valid `GATEWAY_API_KEYS` and `GATEWAY_MODELS`;
  - database mode permits both variables to be absent and does not use them to
    populate production stores.
- Preserve validation for malformed values when a development variable is
  explicitly supplied, rather than silently accepting a typo.
- Validate every enabled database Provider before serving traffic: `fake` needs
  no Secret, OpenAI requires `OPENAI_API_KEY`, and Anthropic requires
  `ANTHROPIC_API_KEY`. Errors must identify the missing configuration kind but
  never include the Secret value or full DSN.
- Keep `GATEWAY_PROVIDER` backward compatible for local mode. In database mode
  the catalog/provider registry is authoritative; the legacy selector must not
  override database rows.
- Add focused tests for local valid/invalid configurations, database mode with
  no dev key/model variables, valid OpenAI and Anthropic credentials, missing
  credentials, and malformed optional development variables.

## Acceptance Criteria

- [ ] `FromEnv` succeeds in database mode with only database-mode settings,
  Redis settings as applicable, and required Provider Secrets; no development
  API key or model list is required.
- [ ] `FromEnv` still rejects local mode without keys/models and still catches
  malformed supplied key/model entries.
- [ ] A valid Anthropic configuration is accepted, and missing OpenAI or
  Anthropic credentials are rejected at the correct startup boundary.
- [ ] A database-backed startup test proves Provider registry validation occurs
  before the HTTP server can report ready, without making a real Provider call.
- [ ] `go test ./internal/config ./cmd/gateway/...`, `go vet ./...`, and
  `go build ./...` pass for the changed behavior.

## Out of Scope

- Database schema or migration changes.
- Runtime configuration reload or an administration API for Provider Secrets.
- CI workflow and Docker/Compose files; those are separate child tasks that
  depend on this one.

## Dependencies

- Parent: `09-15-gateway-production-readiness`.
- This child must complete before the CI and container smoke children are
  activated.

## Planning Status

- Lightweight implementation slice; this PRD is sufficient for activation.
- Blocking product questions: none.
