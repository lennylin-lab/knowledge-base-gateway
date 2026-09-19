# Async and cost observability

## Goal

Make async execution, accounting, retention, and admin failures diagnosable while
preserving low-cardinality metrics and the project's metadata-only logging rule.

## Requirements

- Add queue depth/age, queue latency, terminal outcomes, lease expiry, settlement
  retries, unknown cost, budget rejection, result expiry, archive, and worker
  health metrics without subject/tenant/job labels.
- Add OTLP/OpenTelemetry configuration and spans across HTTP, admission, job
  enqueue/claim, worker, routing/provider attempts, database, and settlement.
- Propagate trace context through persisted jobs while generating a distinct
  worker span; never persist arbitrary baggage.
- Structured logs include IDs, public model, state/action, latency, and error
  class, never prompt/completion/tool args, credentials, URLs, or raw SQL.
- `/readyz` reflects configured PostgreSQL, Redis, durable queue, worker, and
  settlement health. Shutdown stops intake, then claims, then drains/releases.

## Acceptance Criteria

- [ ] Metrics expose all roadmap signals with stable bounded labels and tests.
- [ ] A test exporter proves one trace links HTTP enqueue through worker/provider
  and settlement; exporter failure never breaks serving.
- [ ] Readiness fails for each required dependency and distinguishes degradation
  from client denial; liveness stays process-only.
- [ ] Shutdown tests show no new claims, bounded drain, lease recovery, and no
  duplicate finalization; redaction tests reject prohibited content.

## Out of Scope

- APM vendor dashboards, alert-manager deployment, or distributed log storage.
