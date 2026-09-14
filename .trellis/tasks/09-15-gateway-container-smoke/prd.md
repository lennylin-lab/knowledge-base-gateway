# Containerize gateway and add smoke test

## Goal

Provide a reproducible, secure local/deployment-shaped container stack that
starts PostgreSQL and Redis, applies Gateway migrations once, starts the
Gateway in database-backed Redis mode, and proves health/readiness behavior.

## Requirements

- Add a multi-stage Gateway Dockerfile that builds the pinned Go module and
  runs a minimal non-root runtime image. No Provider Secret, API key, or DSN
  may be baked into image layers.
- Add a Gateway-owned Compose file with PostgreSQL and Redis health checks, a
  one-shot migration service, and a Gateway service that depends on migration
  completion plus Redis/PostgreSQL readiness.
- Use the corrected production-mode configuration: the Gateway service must
  load catalog/routes/policies from PostgreSQL, use Redis for distributed
  limits/quotas, and not require development key/model variables.
- Make the migration service idempotent for `up`; keep destructive rollback
  commands out of the normal startup path.
- Add a bounded smoke script or test that waits for dependencies, verifies
  `/healthz` returns 200, verifies `/readyz` returns 200 only after dependencies
  are ready, and reports container logs on failure.
- Run the Gateway and migration processes as non-root where practical, use
  explicit service names/ports, and document required environment overrides.

## Acceptance Criteria

- [ ] A clean host with Docker Compose can build and start the stack without
  manual database setup.
- [ ] The migration job completes once, the Gateway becomes ready, and health
  and readiness probes pass within a bounded timeout.
- [ ] Stopping PostgreSQL or Redis makes `/readyz` fail while `/healthz` remains
  a liveness response, and recovery returns readiness.
- [ ] Image inspection confirms the runtime user is non-root and no Secret is
  present in the image/config defaults.
- [ ] Smoke failure output includes the relevant service logs and uses a
  distinct non-zero exit status.

## Out of Scope

- Publishing images to a registry or deploying to Kubernetes.
- Modifying `../knowledge-base-server/docker-compose.yml`.
- New application features, schema changes, or Provider protocol changes.

## Dependencies

- Parent: `09-15-gateway-production-readiness`.
- Depends on `09-15-gateway-production-config` for database-backed startup.
- May reuse the CI service/migration commands from
  `09-15-gateway-ci-real-services`, but does not require that child to be
  complete before local smoke development.

## Planning Status

- Complex deployment slice; add technical design and execution checklist
  before activation.
- Blocking product questions: none.
