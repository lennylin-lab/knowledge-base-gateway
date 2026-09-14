# Add CI real-service verification

## Goal

Turn the Gateway's manual PostgreSQL/Redis verification into a repeatable CI
quality gate that validates code, migrations, and real service behavior in an
isolated environment.

## Requirements

- Add a repository CI workflow that provisions supported PostgreSQL and Redis
  service versions with health checks and deterministic credentials.
- Run `gofmt` verification, `go test -race ./...`, `go vet ./...`, and
  `go build ./...` on every relevant change.
- Create an isolated Gateway test database, run migration `up`, assert the
  migration `version`, run a destructive `down`, and run `up` again. Never
  point rollback commands at a shared service database.
- Export `TEST_DATABASE_URL` and `TEST_REDIS_ADDR` only for the integration
  test step, run the PostgreSQL store/migration and real-Redis tests, and fail
  the job on unexpected skips or service health failures.
- Preserve useful logs and test artifacts on failure without exposing database
  passwords, Provider Secrets, API keys, or request bodies.
- Keep the workflow compatible with the repository's Go version and avoid
  depending on the sibling `knowledge-base-server` Compose project.

## Acceptance Criteria

- [ ] A clean CI runner executes all requested Go checks and the real
  PostgreSQL/Redis integration tests successfully.
- [ ] Migration lifecycle verification proves up/version/down/up against a
  disposable database and leaves no dependency on a developer database.
- [ ] A PostgreSQL or Redis startup failure produces a bounded, diagnostic
  failure rather than a flaky connection race.
- [ ] The workflow does not silently pass because real-service tests were
  skipped when the services are expected to be available.
- [ ] Existing local commands remain valid, and the workflow files pass YAML
  syntax/format validation.

## Out of Scope

- Building or publishing the Gateway container image; owned by the container
  smoke child.
- Changes to Provider behavior, token quotas, or the sibling server.
- External deployment environments beyond the CI service containers.

## Dependencies

- Parent: `09-15-gateway-production-readiness`.
- Depends on `09-15-gateway-production-config` so CI exercises the corrected
  database-backed startup contract.

## Planning Status

- Complex verification slice; add technical design and execution checklist
  before activation.
- Blocking product questions: none.
