# Implementation Plan: streaming usage, structured output, latency

## Ordered Checklist

1. Contract-suite tests first: Anthropic structured output (non-streaming
   and streaming) against the expected forced-tool translation and unwrapped
   result; usage-present and usage-absent stream terminations for both
   adapters.
2. HTTP-level failing tests: stream settles quota to reported usage exactly
   once (both protocols), conservative reservation retained when stream usage
   is unknown, audit rows carry known stream tokens; streamed structured
   output validates through the existing path.
3. Migration 0004 up/down for `llm_requests.first_token_millis` + audit
   writer/memory field + failing persistence test (env-gated).
4. Implement Anthropic structured output translation (adapter-local), stream
   usage plumbing (OpenAI stream_options, Anthropic message_delta), encoder
   usage attachment, handler exactly-once settle, first-token timestamping.
5. Update `/admin/usage` surfaces and README/quickstart docs (staged cost
   note stays).
6. Validation: gofmt, go vet, go test -race ./... with env gates for real
   (zero skips), migration up/down against real PostgreSQL, replay 12/12,
   golden fixtures unchanged.

## Validation Commands

- GOCACHE=/tmp/kb-gateway-go-cache go test ./internal/provider ./internal/httpapi ./internal/model ./internal/quota ./internal/store/pg ./internal/mgmt
- GOCACHE=/tmp/kb-gateway-go-cache go test -race ./...
- go vet ./...
- go run ./cmd/replay
- TEST_DATABASE_URL=... go test ./internal/store/pg/ && TEST_REDIS_ADDR=127.0.0.1:6379 go test ./internal/limiter/ ./internal/quota/

## Rollback Points

- anthropic.go structured-output translation reverts independently.
- Stream usage plumbing (openai.go stream_options, encoder attachment,
  handler settle) reverts independently.
- Migration 0004 + audit writer field reverts independently.

## Review Gates

- No V1 wire change (golden fixtures byte-identical).
- Unknown usage never fabricated; settle exactly once.
- Capability rejection before provider invocation preserved.
- Redaction: no prompt/completion content in audit/metrics.
