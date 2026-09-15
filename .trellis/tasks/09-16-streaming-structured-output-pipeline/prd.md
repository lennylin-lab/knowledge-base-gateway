# Complete streaming usage and structured output pipeline

## Goal

Close the three staged functional gaps recorded after the v1.2 follow-up:
Anthropic structured output translation, streaming usage settled into quotas,
and first-token latency recording (cost stays a staged contract field pending
a pricing decision).

## Background and Confirmed Facts

- `internal/provider/anthropic.go` declares structured output unsupported and
  rejects it before provider invocation; the catalog gates the capability per
  model. `internal/provider/fake.go` implements a deterministic JSON wrap as
  the reference behavior, and `internal/model/validate.go` validates outputs
  against the bounded JSON-Schema subset.
- Streaming usage: the OpenAI adapter does not request `stream_options.
  include_usage` and stream chunks carry no usage; the Anthropic adapter
  parses stream `usage` as optional pointers (v1.2 follow-up). Non-streaming
  settles quota to the reported total (`chat.go` `adm.qres.Settle
  (usageTotal(resp.Usage))`); successful streams keep the conservative
  reservation (`internal/httpapi` stream paths never Settle with usage).
- `llm_requests` has `cost_micros` (nullable, staged null) and
  `LatencyMillis` (total request), but no first-token column; `audit.Event`
  carries `CostMicros *int64`. `model_catalog` has no pricing columns.
- Spec conventions in force: unknown usage/cost is never fabricated as zero;
  permits/quota admission ordering fixed; streaming never fails over after
  output; golden fixtures lock V1 wire shapes; adapters share the offline
  contract suite.

## Requirements

- Anthropic structured output: translate `response_format` json_schema to the
  standard forced-tool pattern (synthesized `structured_output` tool with the
  schema as input_schema, forced tool choice), unwrap the tool input as the
  JSON result, and stream/unstream it through the existing validation path;
  capability stays per-model catalog-gated; the adapter contract suite must
  cover it.
- Streaming usage settlement: request usage on upstream streams where the
  dialect supports it, parse it in both adapters, surface it through the
  stream event/response objects, and settle the quota reservation exactly
  once to the reported total; unknown stream usage keeps the conservative
  reservation; audit and metrics report known stream tokens.
- First-token latency: record time-to-first-output-event for streams (and
  full-latency equivalent semantics for non-streaming) into audit; additive
  migration for the new nullable column; metrics/usage surfaces may expose it
  per the staged contract (true percentiles in PostgreSQL mode).
- Cost: remains the staged always-null contract field (decision 2026-09-16:
  pricing data does not exist yet; a later task will add catalog pricing
  columns when real prices are available); never fabricated.

## Key Decisions

- Cost stays staged null (option a): first-token latency is recorded now;
  cost pricing columns and computation are deferred to a future task with
  real business data.

## Open Questions (blocking)

- None. Q1 resolved: keep cost staged null.
- Preserve V1 wire compatibility (golden fixtures), quota semantics, error
  envelopes, redaction, and all existing tests.

## Acceptance Criteria

- [ ] A catalog-gated Anthropic model with `structured_output` returns a
  schema-valid JSON result for a json_schema response_format, non-streaming
  and streaming, pinned by the shared contract suite; models without the
  capability still reject before provider invocation.
- [ ] Streaming requests whose upstream reports usage settle the quota
  reservation to the reported total exactly once (both protocols); streams
  without usage keep the conservative reservation; audit rows reflect known
  stream tokens and never fabricate zeros.
- [ ] First-token latency is recorded for streams in audit (new nullable
  column via additive migration 0004 with down); `/admin/usage` reports true
  percentiles in PostgreSQL mode; dev mode documents its approximation.
- [ ] `cost_micros` remains null (or is populated only if the pricing
  decision requires it), never fabricated.
- [ ] Golden fixtures unchanged; `gofmt`, `go vet`, `go test -race ./...`
  pass with env-gated tests run for real against
  `TEST_DATABASE_URL=postgres://kb:kb@127.0.0.1:5432/kb_gateway_test?
  sslmode=disable` and `TEST_REDIS_ADDR=127.0.0.1:6379` (zero skips);
  migration up/down verified against real PostgreSQL.

## Out of Scope

- Pricing data modeling/population (pending the decision below); billing or
  reconciliation surfaces.
- Async/batch endpoints; non-Anthropic adapter capability changes; admin API
  surface changes beyond usage percentiles.

## Open Questions (blocking)

- None. Q1 resolved 2026-09-16: keep cost staged null.

## Planning Status

- Task size: complex; needs design.md + implement.md + curated jsonl after Q1
  resolves.
- Blocking open questions: Q1 only.
