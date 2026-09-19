# Retention partitioning archive and export

## Goal

Bound operational data growth while preserving required evidence and providing
tenant-safe, redacted exports without blocking online requests.

## Requirements

- Configurable TTLs for requests, async jobs/results, idempotency records, and
  usage ledger, with archival before deletion and legal state/hold protection.
- Time-partition high-volume request/job/ledger data with online creation,
  attachment, pruning, and migration of existing rows.
- Query/export by request, trace, job, tenant, subject, model, and time range;
  exports stream bounded pages and apply RBAC plus tenant predicates.
- Archive manifests include range, row counts, checksum, schema version, and
  completion marker. Partial archives are never eligible for source deletion.
- Result/content fields remain excluded or explicitly redacted according to the
  roadmap; archive/export operations are audited and observable.

## Acceptance Criteria

- [ ] TTL archives then removes only eligible rows; retrying every phase is
  idempotent and a partial/failed archive deletes nothing.
- [ ] Partition maintenance and detach/drop stay within lock/latency budgets
  under concurrent writes and queries.
- [ ] Export filters and RBAC prevent cross-tenant records and prohibited fields;
  checksums and counts verify the artifact.
- [ ] Existing unpartitioned production-shaped data migrates online and rollback
  is rehearsed without loss.

## Out of Scope

- A data warehouse, arbitrary analytics, restore UI, or provider-content export.
