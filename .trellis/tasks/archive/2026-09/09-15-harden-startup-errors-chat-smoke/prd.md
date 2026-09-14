# Harden startup error paths and chat smoke coverage

## Goal

Close the three deferred observations from the gateway-production-readiness
parent: DSN echo in connect errors, `os.Exit` in listener goroutines, and the
container smoke not exercising the chat request path.

## Background and Confirmed Facts

- `cmd/gateway/main.go:68` wraps the pgx connect error with
  `database connect: %w`. Verified against the pinned pgx v5: URL-format parse
  errors redact the password but keep host/db/params; keyword/value errors
  keep `user=`/`database=`. The non-password DSN therefore reaches logs.
- `cmd/gateway/main.go:261-264` — the listener goroutine logs and calls
  `os.Exit(1)` on `ListenAndServe` failure; the admin listener
  (`cmd/gateway/main.go:275`) follows the same pattern. All wiring failures
  now return through `run()`, but runtime listener failures bypass it.
- `scripts/smoke.sh` exercises only `/healthz` and `/readyz` plus outage
  recovery. `docker-compose.yml` sets no `GATEWAY_ADMIN_TOKEN`, so the smoke
  cannot mint an API key; the chat path is never exercised in containers.
- The gateway's chat endpoint requires an authenticated subject and a seeded
  catalog model; the admin API (create/rotate/revoke) exists behind
  `GATEWAY_ADMIN_TOKEN`.
- Spec conventions already pin: secrets/DSN never in logs (quality
  guidelines), stable error envelopes (error handling), smoke exit codes
  documented with per-phase timeouts (quality guidelines container section).

## Requirements

- Sanitize the database connect error before wrapping: the logged error must
  not contain the DSN, its password, or its full connection string; it should
  name the operation (e.g. host reachability/auth failure classification)
  without echoing credentials. Add a test asserting no DSN substring appears.
- Replace `os.Exit(1)` in the listener goroutines with a structured shutdown
  path: a serve failure cancels the run context / returns through `run()` so
  main exits non-zero cleanly (no double-shutdown races, graceful for the
  sibling listener). Covered by a test or documented hermetic verification.
- Extend `scripts/smoke.sh` with a chat-path phase: set a throwaway
  `GATEWAY_ADMIN_TOKEN` on the compose gateway (dev stack only), mint an API
  key via the admin API, call the chat completions endpoint against the
  seeded fake provider (non-streaming at minimum), and assert an
  OpenAI-compatible success envelope. The phase respects the documented
  per-phase timeout budget and exit-code map, and never echoes the admin
  token or key.
- Keep all existing behavior: local mode, dev env lists, CI workflow, and
  quota semantics unchanged.

## Acceptance Criteria

- [ ] A failing (bad-host / bad-credentials) `GATEWAY_DATABASE_URL` logs an
  error with no DSN, password, or connection-string substring; test-pinned.
- [ ] A runtime listener failure (e.g. port already in use) exits non-zero
  through the structured path without `os.Exit` in goroutines; both HTTP and
  admin listeners shut down cleanly.
- [ ] `scripts/smoke.sh` includes a chat phase that mints a key via the admin
  API and receives a successful non-streaming chat completion from the seeded
  fake provider inside the compose stack; full smoke exits 0.
- [ ] `gofmt`, `go vet ./...`, `go build ./...`, `go test ./...` pass; env-gated
  tests run for real against `TEST_DATABASE_URL`/`TEST_REDIS_ADDR`.
- [ ] README (smoke section, exit codes) updated; no secret/token in any log
  output.

## Out of Scope

- Provider secret-redaction changes beyond the connect error (already clean).
- Chat streaming coverage in the smoke; quota/admin surface changes;
  CI workflow restructuring.

## Key Decisions

- Treat all three items as one lightweight hardening task: same files
  (`cmd/gateway/main.go`, `scripts/smoke.sh`, `docker-compose.yml`, README),
  each independently revertable.
- Throwaway admin token lives only in the compose dev stack (same policy as
  the throwaway database password); production deployments set their own.

## Planning Status

- Task size: lightweight; this PRD is the only required planning artifact.
- Blocking open questions: none.
