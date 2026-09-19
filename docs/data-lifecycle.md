# Data lifecycle: retention, archive, and export (V1.4)

Operational runbook for bounding data growth while preserving audit and
billing evidence. The lifecycle is a maintenance surface **separate from
request serving**: nothing sweeps, archives, or deletes on the request path.

## Design stance

- **Archive-based lifecycle, not physical repartitioning.** Converting
  `llm_requests` (or the other governed tables) to a partitioned table would
  rewrite the table and break rolling upgrades — the migration-0009 decision,
  kept. Expired data leaves the online tables through bounded primary-key
  range batches (`SELECT ... ORDER BY created_at LIMIT n` then
  `DELETE ... WHERE pk = ANY(...)`) whose lock footprint stays far below any
  partition operation; the declared partition intent lives in
  `retention_policies`. Physical partitioning returns to the roadmap only
  with a shadow-table conversion that can be rehearsed without breaking
  rolling upgrades.
- **Archive before delete, delete only after verification.** Every batch is
  written to the archive sink, read back, and verified (row count +
  SHA-256) before its manifest is published. Only a completed manifest
  unlocks the delete phase, so a partial or failed archive deletes nothing
  and every phase retries idempotently.
- **Live state is never a retention candidate.** `llm_requests`,
  `async_jobs` (terminal jobs only), `async_job_results`, and
  `usage_ledger` (settled/released rows only) are eligible. Reserved ledger
  rows are the settlement path's exactly-once input and are never eligible;
  deletes re-check eligibility in their `WHERE` clause.
- **Billing evidence is preserved, not destroyed.** The ledger archive is
  the full billing projection (tokens, price version, currency, cost).
  Content (prompts, completions, stored response bodies, request snapshots)
  is never archived or exported; projections omit those columns by
  construction.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `GATEWAY_LIFECYCLE_ENABLED` | `true` | Rollback point. `false` disables the admin lifecycle endpoints (404) and `cmd/maintain` refuses to run. |
| `GATEWAY_LIFECYCLE_ARCHIVE_DIR` | `archives` | Filesystem archive-sink root. Production configuration must point this at a durable location. |

Retention is inert until `retention_policies` rows exist: an absent row
means keep forever; a row with `enabled = false` is a legal hold and the
sweep skips the table entirely.

## Retention policies

Manage through the admin API (platform-admin, global-only; reads are viewer,
global-only — platform operational metadata, like the management log):

```sh
curl -X POST http://127.0.0.1:8081/admin/lifecycle/policies \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{"table":"llm_requests","ttl_seconds":2592000,"archive_before_delete":true,"enabled":true}'
curl http://127.0.0.1:8081/admin/lifecycle/policies ...
```

Governed tables: `llm_requests`, `async_jobs`, `async_job_results`,
`usage_ledger`. Keep `async_jobs` TTL at or above `async_job_results` TTL —
the jobs sweep archives remaining result records (metadata only) before the
delete cascade removes them.

Note on `archive_before_delete`: archiving before deletion is unconditional
— the sweeper never deletes a batch whose archive was not verified, so the
flag is accepted, stored, and audited but currently **reserved** (it cannot
turn archiving off). Deleting without archiving is not a supported posture
under this design.

Mutation and audit: policy changes commit with their management-audit record
atomically, including the previous value.

## Sweeps

- Scheduled operation: `cmd/maintain` (separate process).

  ```sh
  go run ./cmd/maintain                     # one cycle, all tables
  go run ./cmd/maintain -dry                # report eligible rows only
  go run ./cmd/maintain -table usage_ledger -batch 500 -max-batches 20
  go run ./cmd/maintain -interval 1h        # loop cadence
  ```

  It also reclaims expired idempotency keys (KeyTTL) and refuses to run when
  `GATEWAY_LIFECYCLE_ENABLED=false`.
- Operator-triggered: `POST /admin/lifecycle/runs` with `{"dry_run":true}`
  and/or `{"table":"..."}` runs one bounded cycle in-process (platform-admin,
  global-only, management-audited). `GET /admin/lifecycle/runs` shows recent
  runs. Crashed runs left `running` are failed by the next cycle; nothing
  needs recovery because deletes only follow a verified archive.

Bounds: 200 rows per batch, 8 batches per table per cycle by default, so a
triggered run stays inside request-sized lock/latency budgets.

## Archives

Each batch becomes `<table>_<ts>_<rand>.ndjson` plus
`<table>_<ts>_<rand>.manifest.json` under the archive root. The manifest
carries the table, row count, SHA-256 over the stored bytes, the producing
schema version (`schema_version`), the batch time range, and `complete:
true` — the completion marker that only exists after read-back verification.
Record lines are typed NDJSON (`{"type":"llm_request","record":{...}}`),
shared with the export contract.

Verified archives are never destroyed. Restoring rows from an archive is a
deliberate DBA operation (replays of the NDJSON records); a restore UI is
out of scope.

## Exports

`POST /admin/exports` (viewer scope; billing's usage export rides the
viewer implication) streams `application/x-ndjson`:

```jsonc
{"export":{"id":"exp_...","requested_by":"...","filters":{...},"generated_at":"..."}}
{"type":"llm_request","record":{...}}          // redacted metadata projection
{"type":"usage_ledger","record":{...}}
{"summary":{"rows":123,"sha256":"...","complete":true}}
```

- Filters: `request_id`, `trace_id`, `job_id`, `tenant`, `subject`,
  `model`, `from`/`to` (RFC3339), `max_rows` (default 5000, hard cap 20000).
  Paging is server-side keyset `(created_at, pk)`.
- Verification: `sha256` covers exactly the record-line bytes; recount and
  re-hash to verify the artifact. `complete: false` marks a failed stream —
  such an artifact must fail verification. The `data_exports` row keeps the
  filter metadata, row count, and checksum.
- Tenant boundary: tenant-bound admins are forced to their own tenant as a
  mandatory predicate (every source query predicates through
  `subjects.tenant_id` or the row's tenant column). Cross-tenant records
  never appear.
- Management log: platform scope, never tenant-exportable (child-4 ruling).
  A tenant-bound caller requesting `include_management_log` gets a uniform
  403; only platform-global callers may include it.

## KeyTTL (idempotency keys)

The idempotency mapping created with a background job carries `expires_at`
(`GATEWAY_ASYNC_IDEMPOTENCY_TTL`). Enforcement is at the lookup boundary: an
expired key no longer replays and a resend with the same key creates a fresh
job (reclaiming the expired mapping in the same transaction). The reclamation
sweep (`SweepExpiredIdempotencyKeys`) runs on the async worker's sweep
cadence and in `cmd/maintain`.

## Metrics

- `gateway_lifecycle_rows_archived_total{table}` / `_deleted_total{table}`
- `gateway_lifecycle_exports_total`

## Rollback

1. Set `GATEWAY_LIFECYCLE_ENABLED=false` (or stop `cmd/maintain`) — the
   documented rollback point. Serving is unaffected: nothing on the request
   path ever swept.
2. Policies with `enabled=false` are per-table legal holds; delete a policy
   row to return a table to "keep forever".
3. Verified archives are never removed by any lifecycle operation. Deletes
   re-check eligibility, so a rollback between phases leaves the source rows
   in place (worst case: rows archived twice under two manifests).
