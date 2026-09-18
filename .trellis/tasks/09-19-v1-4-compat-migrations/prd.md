# V1.4 compatibility and migration foundation

## Goal

Freeze V1.3 behavior and add the minimum additive schema foundation required by
later V1.4 children, with a proven upgrade and rollback path from version 5.

## Requirements

- Add golden coverage for all existing sync endpoints, SSE termination, stable
  errors, request IDs, defaults, capabilities, quotas, and provider redaction.
- Add migrations in dependency order for async jobs/results/idempotency, pricing
  and ledger/budgets, admin credentials/scopes, and lifecycle metadata.
- Encode tenant/subject ownership, closed state sets, uniqueness, integer money,
  timestamps, and indexes in schema constraints; do not implement features here.
- Update migration parser, real-PostgreSQL lifecycle test, CI expected version,
  and cleanup lists. Never edit migrations 0001-0005.

## Acceptance Criteria

- [ ] Golden tests prove V1.3 wire behavior before feature work.
- [ ] Fresh up, version-5 upgrade, each down boundary, and re-up pass on PostgreSQL.
- [ ] Invalid states, duplicate idempotency/final ledger keys, cross-owner foreign
  keys, negative amounts, and malformed scopes fail at the database boundary.
- [ ] Gateway version-5 behavior remains startable while new feature flags are off.

## Out of Scope

- HTTP endpoints, workers, accounting behavior, RBAC enforcement, and data jobs.
