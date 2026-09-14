# Daily and monthly token quota enforcement

## Goal

Enforce the persisted daily and monthly token quotas for each gateway subject,
so a subject cannot continue making quota-consuming calls after its configured
period budget is exhausted.

## Background and Confirmed Facts

- `access_policies` already stores nullable `daily_tokens` and `monthly_tokens`
  limits, and `internal/policy.Limits` loads them for each subject.
- The chat handler currently applies only the per-request `max_tokens` ceiling;
  it does not check or consume daily/monthly usage.
- Successful provider responses expose usage only when the upstream reports it;
  unknown usage is intentionally represented as unknown and is never fabricated
  as zero.
- PostgreSQL is the source of persisted policy/audit data. Redis is already the
  production boundary for distributed, short-lived rate/concurrency state.
- Existing error conventions map a client quota denial to HTTP 429 with a
  stable normalized error envelope; provider secrets and prompt/completion
  content must remain out of logs and audit records.

## Requirements

- Enforce both configured periods independently per subject: UTC calendar day
  and UTC calendar month. A zero or NULL limit means that period is unlimited.
- Account the upstream-reported total token usage for completed requests, with
  one atomic distributed decision/update so concurrent gateway instances cannot
  admit requests past a period limit.
- Apply quota checks before provider invocation by reserving a deterministic,
  bounded estimate derived from the request's declared `max_tokens` and input
  size. If `max_tokens` is omitted, use the subject policy's output ceiling or
  a documented conservative default; provider-specific tokenizers are out of
  scope.
- Settle a reservation exactly once after the provider returns: adjust it to
  the reported total usage when usage is known, and retain the conservative
  reservation when usage is unknown. Unknown usage remains NULL/unknown in
  audit and metrics; it is never fabricated as zero.
- Release any temporary reservation when a request fails before provider output
  or is canceled before a billable response, using an idempotent operation.
- Return a normalized quota error without leaking policy internals, and include
  the existing request/trace correlation identifiers. Quota denials must not
  invoke a provider and must be distinguishable from Redis infrastructure
  failure (which remains a 503-class error).
- Keep quota state scoped by subject and period, expire or roll over counters at
  UTC period boundaries, and avoid storing prompt/completion content.
- Cover local/unit behavior plus Redis-backed atomicity and period rollover;
  retain an environment-gated real-Redis integration path where appropriate.
- Update the relevant README/API/spec documentation so operators know how
  `daily_tokens` and `monthly_tokens` are enforced and how unknown usage is
  handled.

## Acceptance Criteria

- [ ] A subject with a configured daily limit is admitted while its period
  budget remains available and receives HTTP 429 `quota_exceeded` once the
  budget is exhausted; rejected calls do not reach a provider.
- [ ] Monthly limits behave the same way independently of daily limits, and
  both counters reset at the next UTC calendar boundary without manual data
  cleanup.
- [ ] Concurrent requests across multiple gateway instances cannot oversubscribe
  a daily or monthly budget; Redis failures map to the existing 503
  `limiter_unavailable` contract instead of 429.
- [ ] Each admitted request creates at most one quota reservation; settlement or
  release is idempotent, and a known upstream total is reflected exactly once
  in both applicable period counters.
- [ ] Unknown upstream usage never becomes a fabricated numeric count: the
  conservative reservation remains charged for enforcement while audit and
  metrics retain unknown usage and request/trace correlation.
- [ ] Existing requests with no configured quota retain current behavior, and
  all existing authentication, authorization, rate/concurrency, streaming,
  retry, and error-envelope tests remain green.
- [ ] Focused quota tests and `go test ./...`, `go vet ./...`, and `go build ./...`
  pass.

## Key Decisions

- Use pre-invocation reservation with post-response settlement (option 1). This
  preserves quota enforcement under concurrency without requiring provider
  tokenizers, accepting conservative admission and unknown-usage charging as
  the trade-off.
- Quota counters are keyed by subject and UTC period and are updated atomically
  for daily and monthly limits together. A quota denial is HTTP 429
  `quota_exceeded`; storage/Redis unavailability remains a 503-class error.
- The task remains lightweight and does not add an administration surface;
  existing policy fields are the configuration source.

## Out of Scope

- Daily/monthly quota administration UI or a new management API.
- Historical billing, cost estimation, retroactive reconciliation, or quota
  analytics dashboards.
- Changes to `knowledge-base-server` or provider-specific tokenizers.

## Planning Status

- Task size: lightweight; this PRD is the only required planning artifact.
- Blocking open questions: none.
