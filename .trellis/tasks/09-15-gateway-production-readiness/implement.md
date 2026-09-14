# Production Readiness Implementation Plan

## Ordered Checklist

1. Activate and implement `09-15-gateway-production-config`; add mode-aware
   config/provider-secret tests and verify local behavior is unchanged.
2. Review the config child and activate `09-15-gateway-ci-real-services`; add
   disposable PostgreSQL/Redis services, quality checks, and migration
   up/version/down/up verification.
3. Activate `09-15-gateway-container-smoke`; add the non-root image, migration
   service, Compose dependency graph, and bounded health/readiness smoke path.
4. Run the parent integration review: inspect secret redaction, service
   startup ordering, migration isolation, and the no-sibling-repository rule.

## Validation Gates

- `gofmt` check, `go test -race ./...`, `go vet ./...`, and `go build ./...`.
- Environment-backed PostgreSQL/Redis store and limiter tests.
- Migration CLI `up`, `version`, destructive `down`, and `up` on a disposable
  database.
- Docker Compose build/start, `/healthz`, `/readyz`, dependency outage, and
  recovery smoke checks.

## Rollback Points

- Before config activation: no production behavior changed.
- Before CI activation: workflow-only changes can be reverted independently.
- Before Compose activation: image and Compose changes remain isolated from
  application source and schema.
