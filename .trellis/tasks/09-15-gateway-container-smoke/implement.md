# Gateway Container Smoke Implementation Plan

1. Add the multi-stage Dockerfile and non-root runtime configuration.
2. Add Gateway-owned Compose services for PostgreSQL, Redis, migration, and
   Gateway with health checks and dependency conditions.
3. Add the bounded smoke command and failure log collection.
4. Document build/start/stop commands and required environment overrides.
5. Verify image user, absence of baked Secrets, migration completion, health,
   readiness, dependency outage, and recovery.

Rollback: remove or disable the Compose/Docker artifacts; the Go binary and
operational migration CLI remain usable directly.
