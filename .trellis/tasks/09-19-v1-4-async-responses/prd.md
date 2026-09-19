# Async Responses job lifecycle

## Goal

Make long Responses requests durable, resumable, cancellable, queryable, and
tenant-safe while keeping the default synchronous request path unchanged.

## Requirements

- `POST /v1/responses` accepts optional `background:true`; reject background
  streaming, authenticate before parsing, run normal admission, and return 202.
- Implement `queued`, `running`, `completed`, `failed`, `cancelled`, `expired`
  with legal conditional transitions, one active lease, retries only before first
  output, recovery after restart, and bounded worker concurrency.
- Apply `Idempotency-Key` per subject: same digest returns the job; different
  digest returns 409. Store hashes/digests, never plaintext keys.
- Add owner/scoped-admin GET and idempotent cancel. Non-owner and absent jobs use
  the same `response_not_found`; expired results return 410 without re-execution.
- Persist only normalized gateway requests/results, usage, error class, request
  ID, ownership, policy/config snapshot, size, lease, retry, and expiry metadata.
- Propagate cancellation to provider context; database transition decides races.

## Acceptance Criteria

- [ ] Wire shapes, statuses, errors, `Retry-After`, SDK use, and sync regression
  behavior match the roadmap.
- [ ] Concurrent duplicate creation yields one job; digest conflict is 409.
- [ ] Multi-worker claim, lease expiry, restart, queue outage, provider failure,
  cancellation/completion races, and result expiry pass deterministic tests.
- [ ] Exactly one terminal audit/accounting handoff occurs and results never leak
  provider fields, content through logs, or cross-owner existence.

## Out of Scope

- Batch jobs, webhooks, WebSockets, agent/tool execution, and monetary pricing.
