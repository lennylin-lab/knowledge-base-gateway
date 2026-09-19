# Implementation plan: V1.4 parent

## Ordered Checklist

1. Complete and approve compatibility/migrations.
2. Complete async lifecycle and prove recovery/cancellation invariants.
3. Add shared accounting and integrate sync and async finalization.
4. Add scoped admin identity before exposing new management operations.
5. Add retention, partitioning, archive, and export over stable schemas.
6. Complete telemetry, readiness, and graceful worker lifecycle.
7. Run rollout validation and final cross-child review.

## Integration Gates

- Do not start a dependent child until predecessor contracts and migrations are
  merged and rollback is demonstrated.
- After children 2 and 3, review cancellation-versus-settlement races.
- After children 3-5, review tenant and scope enforcement for every query.
- Before release, compare V1.3 golden fixtures byte-for-byte and rehearse flags
  off, model rollout, tenant rollout, and rollback.

## Validation

- `GOCACHE=/tmp/kb-gateway-go-cache go test ./...`
- `GOCACHE=/tmp/kb-gateway-go-cache go test -race -count=1 ./...`
- `go vet ./...`
- Real PostgreSQL migrations and integration suite with zero skips.
- Real Redis multi-instance budget/fault suite with zero skips.
- `go run ./cmd/replay` and `scripts/smoke.sh`.

## Rollback Points

- Disable async creation, drain workers, then roll back async writers.
- Disable monetary enforcement while retaining ledger writes for diagnosis.
- Retain legacy admin token until scoped credentials are production-verified.
- Disable OTLP independently; Prometheus and serving remain live.
