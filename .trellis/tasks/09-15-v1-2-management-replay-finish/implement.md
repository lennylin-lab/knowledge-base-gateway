# Implementation Plan: Management and Replay Finish

## Ordered Checklist

1. Add tests for model enable/disable affecting live resolution and for audit failure semantics.
2. Implement atomic management mutation/audit behavior and runtime refresh/update boundary.
3. Add or stage management provider health/error/breaker and usage metric fields with tests/docs.
4. Add offline protocol replay command and deterministic fixture coverage.
5. Remove stale TBD header block from the archived v1.2 PRD without altering the completed requirements.
6. Run full validation gates and produce a redacted issue #1 update.

## Validation Commands

- GOCACHE=/tmp/kb-gateway-go-cache go test ./internal/httpapi ./internal/mgmt ./internal/store/pg ./internal/provider ./internal/e2e
- GOCACHE=/tmp/kb-gateway-go-cache go test ./...
- GOCACHE=/tmp/kb-gateway-go-cache go test -race ./...
- go vet ./...
- git diff --check
- Replay command added by this stage, run against checked-in fixtures.

## Rollback Points

- Runtime refresh/update boundary should be revertible independently from management response field changes.
- Replay command should be additive and removable without changing serving code.
