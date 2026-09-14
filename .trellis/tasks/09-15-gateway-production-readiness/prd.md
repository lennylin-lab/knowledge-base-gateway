# Gateway production readiness

## Goal

Make the PostgreSQL/Redis-backed Gateway deployable and continuously
verifiable after token quota enforcement. A production deployment must start
from its database-backed catalog and credentials, pass automated real-service
checks, and expose a reproducible container smoke path.

## Background and Confirmed Facts

- PostgreSQL persistence, Redis distributed limits, versioned migrations, and
  dependency readiness checks already exist and have been exercised manually.
- The completed token quota work is archived separately; this parent task must
  not change quota semantics.
- `internal/config.FromEnv` currently requires development-only
  `GATEWAY_API_KEYS` and `GATEWAY_MODELS` even when `GATEWAY_DATABASE_URL` is
  set. It also validates the Anthropic provider before loading its environment
  secret.
- The repository has no Gateway Dockerfile, Gateway Compose stack, or CI
  workflow. PostgreSQL/Redis tests are still primarily environment-gated.
- `knowledge-base-server` owns its own Compose stack; this task must keep the
  Gateway deployment self-contained and must not modify the sibling repository.

## Deliverables

1. Production-mode configuration and startup validation.
2. CI workflow with PostgreSQL/Redis services and migration lifecycle checks.
3. Non-root Gateway container, one-shot migration job, Compose wiring, and
   health/readiness smoke verification.

The first deliverable is the execution prerequisite for the other two because
CI and Compose must prove that database-backed startup works without
development key/model environment variables. CI and container smoke remain
independently verifiable after that prerequisite.

## Acceptance Criteria

- [ ] A database-backed Gateway starts with no `GATEWAY_API_KEYS` or
  `GATEWAY_MODELS`, loads catalog/routes/policies/keys from PostgreSQL, and
  fails startup clearly when an enabled OpenAI/Anthropic Provider lacks its
  required environment Secret.
- [ ] Development-mode configuration remains backward compatible, and the
  Anthropic Secret is loaded before provider validation.
- [ ] CI runs `go test -race ./...`, `go vet ./...`, and `go build ./...` with
  PostgreSQL and Redis services, then runs isolated migration up/version/
  down/up and the real-service integration tests.
- [ ] A reproducible Compose smoke path starts PostgreSQL, Redis, a one-shot
  migration job, and the Gateway in dependency order; the Gateway runs as a
  non-root user and passes `/healthz` and `/readyz` checks.
- [ ] CI and smoke failures expose actionable logs, never print Provider
  secrets, and destructive migration rollback is limited to an isolated test
  database.
- [ ] Existing unit, integration, migration, API-contract, and quota tests
  retain their current behavior.

## Out of Scope

- Changes to `knowledge-base-server` or its Compose stack.
- New policy/catalog administration APIs, billing, dashboards, or OpenTelemetry
  instrumentation.
- Changes to token quota algorithms, limits schema, Provider protocols, or
  public chat response semantics.

## Risks and Deferred Items

- CI service startup and Docker health checks need bounded retries so a slow
  PostgreSQL/Redis boot is reported as a useful failure rather than a race.
- The migration `down` path is destructive and must never target a shared
  developer or production database.
- The existing sandbox may prohibit loopback listeners; verification must
  distinguish environment restrictions from code failures.

## Planning Status

- Parent task: complex; `prd.md`, `design.md`, and `implement.md` are required.
- Child task `gateway-production-config` owns the first implementation slice.
- Blocking product questions: none.
