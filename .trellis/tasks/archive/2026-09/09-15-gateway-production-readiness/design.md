# Production Readiness Design

## Boundaries

The parent coordinates three independently verifiable slices:

- `internal/config` and Gateway startup validate local versus database-backed
  configuration and Provider Secrets.
- A repository CI workflow owns disposable PostgreSQL/Redis services, Go
  quality checks, integration environment variables, and migration lifecycle
  checks.
- A Gateway-owned Dockerfile, Compose file, and smoke command own the
  deployment-shaped startup path.

No public chat contract, quota algorithm, database schema, or sibling
`knowledge-base-server` source changes are part of this work.

## Data Flow

```text
environment
  -> config.FromEnv (mode-aware, secrets loaded before validation)
  -> database provider/catalog/route loading
  -> provider factory validates required secret kinds
  -> readiness checks PostgreSQL + Redis + catalog
```

CI and Compose use the same migration CLI and database-backed startup path. CI
uses disposable services and a dedicated test database; Compose uses a
one-shot migration service before the Gateway service.

## Compatibility

- Local development continues to use `GATEWAY_API_KEYS` and
  `GATEWAY_MODELS`.
- Database mode is authoritative for keys, models, routes, policies, and
  Provider kinds. Provider Secrets remain environment-only.
- Existing migrations are not edited. Compose and CI run the operational
  migration CLI instead.
- `/healthz` remains liveness-only; `/readyz` remains dependency-aware.

## Trade-offs

- CI service containers are preferred over coupling to the sibling project's
  Compose stack so failures are isolated and reproducible.
- The Compose stack uses the seeded fake Provider/catalog for smoke testing;
  real Provider credentials are injected only for deployments that need them.
- Migration rollback is tested only against disposable databases because it is
  intentionally destructive.

## Rollback

Each child is independently revertible. A configuration change can be rolled
back without changing schema. CI and Compose files can be disabled or reverted
without affecting the running binary, while the existing migration CLI remains
the sole schema owner.
