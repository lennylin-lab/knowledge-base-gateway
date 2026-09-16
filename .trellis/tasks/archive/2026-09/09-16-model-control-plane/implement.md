# Implementation Plan: model control plane

## Ordered Checklist

1. Contract tests first: fake embeddings determinism (fixed dim from
   catalog, input-derived vector), openai embeddings adapter translation
   against a stub upstream, capability rejection pre-provider, dim-mismatch
   config error.
2. HTTP failing tests: `/v1/embeddings` admission order, quota settle
   exactly once (same pool), 429/503 semantics, default-model backfill for
   chat/responses/embeddings, no-default error, admin policy mutation +
   management audit, `/v1/models/{model}` retrieval_profile passthrough,
   `GATEWAY_EMBEDDINGS_ENABLED=false` rollback.
3. Migration 0005 (up/down) + policy loader fields + catalog retrieval
   profile + pg mgmt surfacing; env-gated real-PostgreSQL test.
4. Implement: domain types, fake/openai embeddings, protocol handler +
   wire, backfill in admission, admin endpoints, models-detail field.
5. Docs: README (config table, embeddings endpoint, flags), quickstart,
   api-versioning (additive fields), client contract untouched except
   additive notes.
6. Validation: gofmt/vet, go test -race ./... with env gates for real
   (zero skips), migration up/down via cmd/migrate, replay 12/12, golden
   fixtures unchanged, actionlint if CI touched (embeddings replay fixture
   optional).

## Validation Commands

- GOCACHE=/tmp/kb-gateway-go-cache go test ./internal/... ./cmd/...
- GOCACHE=/tmp/kb-gateway-go-cache go test -race ./...
- go vet ./...
- go run ./cmd/replay
- TEST_DATABASE_URL=... go test ./internal/store/pg/ && TEST_REDIS_ADDR=127.0.0.1:6379 go test ./internal/limiter/ ./internal/quota/

## Rollback Points

- Embeddings (route + provider surface + flag) reverts independently.
- Default-model backfill reverts independently (inert without policy data).
- Migration 0005 + retrieval profile reverts independently.

## Review Gates

- Golden V1 fixtures byte-identical; strict decoding intact on responses;
  embeddings follows chat unknown-field policy.
- No provider secrets in requests/audit; vector content never audited.
- Capability rejection before provider; settle exactly once; redaction.
