# CI Real-Service Verification Design

## Workflow Shape

Use one CI job with PostgreSQL and Redis service containers. Health checks must
complete before test commands run. The job creates a dedicated Gateway test
database and exports `TEST_DATABASE_URL` and `TEST_REDIS_ADDR` only to the
integration steps.

## Migration Lifecycle

The lifecycle step runs the existing `cmd/migrate` binary against the
disposable database in this order:

```text
up -> version -> down -> up
```

No shared or developer DSN may be used by the rollback step. The subsequent Go
test step runs the PostgreSQL migration/store tests and real-Redis tests with
the service addresses supplied by CI.

## Quality Checks

Run formatting validation, race-enabled tests, vet, and build as separate
steps so failures identify the broken gate. Service logs are collected on
failure, with credentials masked by the CI platform.

## Compatibility

The workflow uses the Go version declared by `go.mod`, keeps existing local
commands valid, and does not assume the sibling server or its Compose network.
