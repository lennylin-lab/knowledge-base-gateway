package pg

// PostgreSQL implementation of the V1.4 data-lifecycle persistence boundary
// (internal/lifecycle). Every projection here is redacted by construction:
// the SELECT column lists are the contract, and the async_job_results
// response column — completion content — is never selected for archive or
// export. Eligibility rules keep live state out of every sweep: terminal
// jobs only, and settled/released ledger rows only (reserved rows are the
// exactly-once settlement input and are never eligible). Deletes re-check
// eligibility in their WHERE clause so a row that changed between select and
// delete is kept.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/knowledge-base/knowledge-base-gateway/internal/lifecycle"
	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
)

// LifecycleStore implements lifecycle.Store, lifecycle.PolicyWriter,
// lifecycle.RunReader, and lifecycle.ExportStore on the shared pool.
type LifecycleStore struct {
	DB *DB
}

var _ lifecycle.Store = (*LifecycleStore)(nil)
var _ lifecycle.PolicyWriter = (*LifecycleStore)(nil)
var _ lifecycle.RunReader = (*LifecycleStore)(nil)
var _ lifecycle.ExportStore = (*LifecycleStore)(nil)

// --- Retention policies ----------------------------------------------------

// RetentionPolicies implements lifecycle.Store.
func (s *LifecycleStore) RetentionPolicies(ctx context.Context) ([]lifecycle.Policy, error) {
	rows, err := s.DB.Pool.Query(ctx, `
		SELECT table_name, ttl_seconds, archive_before_delete, enabled, updated_at
		FROM retention_policies ORDER BY table_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []lifecycle.Policy
	for rows.Next() {
		var p lifecycle.Policy
		if err := rows.Scan(&p.Table, &p.TTL, &p.ArchiveBeforeDelete, &p.Enabled, &p.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// UpsertRetentionPolicy implements lifecycle.PolicyWriter: the policy change
// and its management-audit record commit as one transaction, with the
// previous state read inside it for the redacted old/new summary. The table
// name is validated against the closed set before this runs (PolicyInput.
// Validate); the schema CHECK is the second boundary.
func (s *LifecycleStore) UpsertRetentionPolicy(ctx context.Context, in lifecycle.PolicyInput, op mgmt.AdminOp) error {
	tx, err := s.DB.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var previous any
	err = tx.QueryRow(ctx, `
		SELECT json_build_object('ttl_seconds', ttl_seconds,
		       'archive_before_delete', archive_before_delete, 'enabled', enabled)
		FROM retention_policies WHERE table_name = $1`, string(in.Table)).Scan(&previous)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO retention_policies (table_name, ttl_seconds, archive_before_delete, enabled, updated_at)
		VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (table_name) DO UPDATE SET
			ttl_seconds = EXCLUDED.ttl_seconds,
			archive_before_delete = EXCLUDED.archive_before_delete,
			enabled = EXCLUDED.enabled,
			updated_at = now()`,
		string(in.Table), in.TTL, in.ArchiveBeforeDelete, in.Enabled); err != nil {
		return err
	}
	op.Detail = mgmt.MergeDetail(op.Detail, map[string]any{
		"table": string(in.Table), "ttl_seconds": in.TTL,
		"archive_before_delete": in.ArchiveBeforeDelete, "enabled": in.Enabled,
		"previous": previous,
	})
	if err := writeOp(ctx, tx, op); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// --- Archive runs ------------------------------------------------------------

// MarkStaleRunsFailed implements lifecycle.Store: runs left 'running' by a
// crashed process are closed as failed. No source data needs recovery —
// deletes only ever follow a verified archive inside the same run.
func (s *LifecycleStore) MarkStaleRunsFailed(ctx context.Context, staleAfter time.Duration, now time.Time) (int, error) {
	tag, err := s.DB.Pool.Exec(ctx, `
		UPDATE archive_runs SET status = 'failed', finished_at = $2,
		       detail = detail || '{"recovered": true}'::jsonb
		WHERE status = 'running' AND started_at < $1`, now.Add(-staleAfter), now)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

// StartRun implements lifecycle.Store.
func (s *LifecycleStore) StartRun(ctx context.Context, table lifecycle.TableName, dryRun bool, now time.Time) (int64, error) {
	detail, _ := json.Marshal(map[string]any{"dry_run": dryRun})
	var id int64
	err := s.DB.Pool.QueryRow(ctx, `
		INSERT INTO archive_runs (table_name, status, detail, started_at)
		VALUES ($1, 'running', $2, $3) RETURNING id`, string(table), detail, now).Scan(&id)
	return id, err
}

// FinishRun implements lifecycle.Store.
func (s *LifecycleStore) FinishRun(ctx context.Context, runID int64, status lifecycle.RunStatus, archived, deleted int64, detail json.RawMessage) error {
	tag, err := s.DB.Pool.Exec(ctx, `
		UPDATE archive_runs SET status = $2, rows_archived = $3, rows_deleted = $4,
		       finished_at = $5, detail = detail || $6::jsonb
		WHERE id = $1 AND status = 'running'`,
		runID, string(status), archived, deleted, time.Now(), detail)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("pg: archive run %d not open", runID)
	}
	return nil
}

// RecentRuns implements lifecycle.RunReader.
func (s *LifecycleStore) RecentRuns(ctx context.Context, limit int) ([]lifecycle.RunView, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	rows, err := s.DB.Pool.Query(ctx, `
		SELECT id, table_name, status, rows_archived, rows_deleted, started_at, finished_at, detail
		FROM archive_runs ORDER BY id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []lifecycle.RunView
	for rows.Next() {
		var v lifecycle.RunView
		if err := rows.Scan(&v.ID, &v.Table, &v.Status, &v.RowsArchived, &v.RowsDeleted,
			&v.StartedAt, &v.FinishedAt, &v.Detail); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// WriteOp implements lifecycle.Store (management-audit boundary).
func (s *LifecycleStore) WriteOp(ctx context.Context, op mgmt.AdminOp) error {
	return writeOp(ctx, s.DB.Pool, op)
}

// SchemaVersion implements lifecycle.Store: the migration head stamps every
// manifest so archives remain interpretable across schema evolution.
func (s *LifecycleStore) SchemaVersion(ctx context.Context) (int, error) {
	var v int
	err := s.DB.Pool.QueryRow(ctx,
		`SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1`).Scan(&v)
	return v, err
}

// --- Sweep selection and deletion --------------------------------------------

// terminalJobStatuses is the closed eligibility set for async_jobs sweeps:
// queued/running jobs are live work, never retention candidates.
const terminalJobStatuses = `('completed','failed','cancelled','expired')`

// jobsUnanchoredByLedger is the jobs-eligibility guard for the usage_ledger
// foreign key: a job that still anchors a ledger row cannot be deleted (the
// ledger row is the durable billing evidence and outlives the job whenever
// the ledger TTL is longer), and the non-cascading FK would fail the whole
// batch. Aligning count, select, and delete on this predicate makes the
// sweep converge instead: ledger-anchored jobs are simply not eligible yet,
// and the jobs sweep reclaims them in a later cycle once the ledger sweep
// has aged their rows out.
const jobsUnanchoredByLedger = `NOT EXISTS (SELECT 1 FROM usage_ledger l WHERE l.job_id = async_jobs.job_id)`

// terminalLedgerStatuses is the ledger eligibility rule: reserved rows are
// the settlement path's exactly-once input; only settled/released rows —
// whose billing evidence the archive preserves — age out.
const terminalLedgerStatuses = `('settled','released')`

// CountEligible implements lifecycle.Store.
func (s *LifecycleStore) CountEligible(ctx context.Context, table lifecycle.TableName, cutoff time.Time) (int64, error) {
	var query string
	switch table {
	case lifecycle.TableRequests:
		query = `SELECT count(*) FROM llm_requests WHERE created_at < $1`
	case lifecycle.TableJobs:
		query = `SELECT count(*) FROM async_jobs WHERE created_at < $1 AND status IN ` + terminalJobStatuses + ` AND ` + jobsUnanchoredByLedger
	case lifecycle.TableResults:
		query = `SELECT count(*) FROM async_job_results WHERE created_at < $1`
	case lifecycle.TableLedger:
		query = `SELECT count(*) FROM usage_ledger WHERE created_at < $1 AND settle_status IN ` + terminalLedgerStatuses
	default:
		return 0, fmt.Errorf("%w: %q", lifecycle.ErrUnknownTable, string(table))
	}
	var n int64
	err := s.DB.Pool.QueryRow(ctx, query, cutoff).Scan(&n)
	return n, err
}

// SelectBatch implements lifecycle.Store: one bounded, ordered batch of
// eligible rows as redacted NDJSON record lines plus the delete keys.
func (s *LifecycleStore) SelectBatch(ctx context.Context, table lifecycle.TableName, cutoff time.Time, limit int) (lifecycle.Batch, error) {
	b := lifecycle.Batch{Table: table}
	var err error
	switch table {
	case lifecycle.TableRequests:
		err = s.selectRequests(ctx, cutoff, limit, &b)
	case lifecycle.TableResults:
		err = s.selectResults(ctx, cutoff, limit, &b)
	case lifecycle.TableJobs:
		err = s.selectJobs(ctx, cutoff, limit, &b)
	case lifecycle.TableLedger:
		err = s.selectLedger(ctx, cutoff, limit, &b)
	default:
		return b, fmt.Errorf("%w: %q", lifecycle.ErrUnknownTable, string(table))
	}
	if err != nil {
		return lifecycle.Batch{}, err
	}
	return b, nil
}

// selectRequests archives the audit metadata rows. llm_requests stores no
// content, so the full audit projection is metadata by construction.
func (s *LifecycleStore) selectRequests(ctx context.Context, cutoff time.Time, limit int, b *lifecycle.Batch) error {
	rows, err := s.DB.Pool.Query(ctx, `
		SELECT request_id, subject_id, key_id, model, provider, status,
		       COALESCE(error_class,''), latency_ms, prompt_tokens, completion_tokens,
		       first_token_millis, streaming, created_at, trace_id, route_attempts,
		       cost_micros, COALESCE(protocol,'chat')
		FROM llm_requests
		WHERE created_at < $1
		ORDER BY created_at, request_id
		LIMIT $2`, cutoff, limit)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var status int64
		var prompt, completion *int64
		var firstToken *int64
		var cost *int64
		var streaming bool
		var createdAt time.Time
		var requestID, subjectID, keyID, model, provider, errorClass, traceID, protocol string
		var latency int64
		var routeAttempts int
		if err := rows.Scan(&requestID, &subjectID, &keyID, &model, &provider, &status,
			&errorClass, &latency, &prompt, &completion, &firstToken, &streaming,
			&createdAt, &traceID, &routeAttempts, &cost, &protocol); err != nil {
			return err
		}
		line, err := lifecycle.EncodeRecord(lifecycle.RecordRequest, map[string]any{
			"request_id": requestID, "subject_id": subjectID, "key_id": keyID,
			"model": model, "provider": provider, "status": status,
			"error_class": nilIfEmpty(errorClass), "latency_ms": latency,
			"prompt_tokens": nullOrValue(prompt), "completion_tokens": nullOrValue(completion),
			"first_token_ms": nullOrValue(firstToken), "streaming": streaming,
			"created_at": createdAt, "trace_id": traceID,
			"route_attempts": routeAttempts, "cost_micros": nullOrValue(cost),
			"protocol": protocol,
		})
		if err != nil {
			return err
		}
		b.Append(line, requestID, createdAt)
	}
	return rows.Err()
}

// selectResults archives result metadata only: error class, usage, size, and
// timestamps. The response column (completion content) is deliberately not
// selected — the archive is content-free by construction.
func (s *LifecycleStore) selectResults(ctx context.Context, cutoff time.Time, limit int, b *lifecycle.Batch) error {
	rows, err := s.DB.Pool.Query(ctx, `
		SELECT job_id, error_class, usage_prompt_tokens, usage_completion_tokens,
		       usage_total_tokens, result_bytes, retention_expires_at, created_at
		FROM async_job_results
		WHERE created_at < $1
		ORDER BY created_at, job_id
		LIMIT $2`, cutoff, limit)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var jobID string
		var errorClass *string
		var prompt, completion, total *int64
		var resultBytes int
		var retentionExpires, createdAt time.Time
		if err := rows.Scan(&jobID, &errorClass, &prompt, &completion, &total,
			&resultBytes, &retentionExpires, &createdAt); err != nil {
			return err
		}
		var ec any
		if errorClass != nil {
			ec = *errorClass
		}
		line, err := lifecycle.EncodeRecord(lifecycle.RecordResult, map[string]any{
			"job_id": jobID, "error_class": ec,
			"usage_prompt_tokens": nullOrValue(prompt), "usage_completion_tokens": nullOrValue(completion),
			"usage_total_tokens": nullOrValue(total), "result_bytes": resultBytes,
			"retention_expires_at": retentionExpires, "created_at": createdAt,
			"response_redacted": true,
		})
		if err != nil {
			return err
		}
		b.Append(line, jobID, createdAt)
	}
	return rows.Err()
}

// selectJobs archives terminal job metadata. The job's remaining result rows
// are archived in the same batch (one content-free record each) so the
// delete cascade below never removes an unarchived result; the request
// snapshot (async_job_requests) holds the normalized request payload —
// content that archives deliberately do not retain — and is removed by the
// same cascade. Jobs that still anchor a usage_ledger row are not eligible
// (see jobsUnanchoredByLedger): they stay in place until their billing
// evidence ages out, so batches never fail on the ledger foreign key and
// never re-archive rows they cannot delete.
func (s *LifecycleStore) selectJobs(ctx context.Context, cutoff time.Time, limit int, b *lifecycle.Batch) error {
	rows, err := s.DB.Pool.Query(ctx, `
		SELECT job_id, subject_id, tenant_id, protocol, public_model, request_digest,
		       status, attempt_count, COALESCE(final_request_id,''), created_at, updated_at
		FROM async_jobs
		WHERE created_at < $1 AND status IN `+terminalJobStatuses+` AND `+jobsUnanchoredByLedger+`
		ORDER BY created_at, job_id
		LIMIT $2`, cutoff, limit)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var jobID, subjectID, tenantID, protocol, publicModel, digest, status, finalRequestID string
		var attemptCount int
		var createdAt, updatedAt time.Time
		if err := rows.Scan(&jobID, &subjectID, &tenantID, &protocol, &publicModel, &digest,
			&status, &attemptCount, &finalRequestID, &createdAt, &updatedAt); err != nil {
			return err
		}
		line, err := lifecycle.EncodeRecord(lifecycle.RecordJob, map[string]any{
			"job_id": jobID, "subject_id": subjectID, "tenant_id": tenantID,
			"protocol": protocol, "public_model": publicModel, "request_digest": digest,
			"status": status, "attempt_count": attemptCount,
			"final_request_id": nilIfEmpty(finalRequestID),
			"created_at":       createdAt, "updated_at": updatedAt,
		})
		if err != nil {
			return err
		}
		b.Append(line, jobID, createdAt)
		// The job's stored result (if any) rides the same artifact before the
		// cascade removes it.
		if err := s.appendResultForJob(ctx, jobID, b); err != nil {
			return err
		}
	}
	return rows.Err()
}

// appendResultForJob adds the content-free result record for one job.
func (s *LifecycleStore) appendResultForJob(ctx context.Context, jobID string, b *lifecycle.Batch) error {
	var (
		errorClass                  *string
		prompt, completion, total   *int64
		resultBytes                 int
		retentionExpires, createdAt time.Time
	)
	err := s.DB.Pool.QueryRow(ctx, `
		SELECT error_class, usage_prompt_tokens, usage_completion_tokens,
		       usage_total_tokens, result_bytes, retention_expires_at, created_at
		FROM async_job_results WHERE job_id = $1`, jobID).
		Scan(&errorClass, &prompt, &completion, &total, &resultBytes, &retentionExpires, &createdAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var ec any
	if errorClass != nil {
		ec = *errorClass
	}
	line, err := lifecycle.EncodeRecord(lifecycle.RecordResult, map[string]any{
		"job_id": jobID, "error_class": ec,
		"usage_prompt_tokens": nullOrValue(prompt), "usage_completion_tokens": nullOrValue(completion),
		"usage_total_tokens": nullOrValue(total), "result_bytes": resultBytes,
		"retention_expires_at": retentionExpires, "created_at": createdAt,
		"response_redacted": true,
	})
	if err != nil {
		return err
	}
	// The delete key stays the job id: the result row is removed by cascade.
	b.Append(line, jobID, createdAt)
	return nil
}

// selectLedger archives settled/released billing rows (full projection —
// this is the durable billing evidence the archive exists to preserve).
func (s *LifecycleStore) selectLedger(ctx context.Context, cutoff time.Time, limit int, b *lifecycle.Batch) error {
	rows, err := s.DB.Pool.Query(ctx, `
		SELECT id, COALESCE(request_id,''), COALESCE(job_id,''), subject_id, tenant_id,
		       protocol, public_model, price_version, currency,
		       prompt_tokens, completion_tokens, reasoning_tokens, cached_input_tokens,
		       cost_micros, settle_status, settled_at, created_at
		FROM usage_ledger
		WHERE created_at < $1 AND settle_status IN `+terminalLedgerStatuses+`
		ORDER BY created_at, id
		LIMIT $2`, cutoff, limit)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var requestID, jobID, subjectID, tenantID, protocol, publicModel string
		var priceVersion *int64
		var currency *string
		var prompt, completion, reasoning, cached, cost *int64
		var settleStatus string
		var settledAt *time.Time
		var createdAt time.Time
		if err := rows.Scan(&id, &requestID, &jobID, &subjectID, &tenantID,
			&protocol, &publicModel, &priceVersion, &currency,
			&prompt, &completion, &reasoning, &cached,
			&cost, &settleStatus, &settledAt, &createdAt); err != nil {
			return err
		}
		line, err := lifecycle.EncodeRecord(lifecycle.RecordLedger, map[string]any{
			"id": id, "request_id": nilIfEmpty(requestID), "job_id": nilIfEmpty(jobID),
			"subject_id": subjectID, "tenant_id": tenantID,
			"protocol": protocol, "public_model": publicModel,
			"price_version": nullOrValue(priceVersion), "currency": currency,
			"prompt_tokens": nullOrValue(prompt), "completion_tokens": nullOrValue(completion),
			"reasoning_tokens": nullOrValue(reasoning), "cached_input_tokens": nullOrValue(cached),
			"cost_micros": nullOrValue(cost), "settle_status": settleStatus,
			"settled_at": settledAt, "created_at": createdAt,
		})
		if err != nil {
			return err
		}
		b.Append(line, strconv.FormatInt(id, 10), createdAt)
	}
	return rows.Err()
}

// DeleteBatch implements lifecycle.Store: the WHERE re-checks eligibility so
// only rows that are still terminal/delete-eligible are removed (a protected
// row keeps its archived copy — the archive is never a destruction record).
func (s *LifecycleStore) DeleteBatch(ctx context.Context, table lifecycle.TableName, b lifecycle.Batch) (int64, error) {
	if len(b.Keys) == 0 {
		return 0, nil
	}
	var tag pgconn.CommandTag
	var err error
	switch table {
	case lifecycle.TableRequests:
		tag, err = s.DB.Pool.Exec(ctx, `DELETE FROM llm_requests WHERE request_id = ANY($1)`, b.Keys)
	case lifecycle.TableResults:
		tag, err = s.DB.Pool.Exec(ctx, `DELETE FROM async_job_results WHERE job_id = ANY($1)`, b.Keys)
	case lifecycle.TableJobs:
		tag, err = s.DB.Pool.Exec(ctx,
			`DELETE FROM async_jobs WHERE job_id = ANY($1) AND status IN `+terminalJobStatuses+` AND `+jobsUnanchoredByLedger, b.Keys)
	case lifecycle.TableLedger:
		ids, convErr := toInt64s(b.Keys)
		if convErr != nil {
			return 0, convErr
		}
		tag, err = s.DB.Pool.Exec(ctx,
			`DELETE FROM usage_ledger WHERE id = ANY($1) AND settle_status IN `+terminalLedgerStatuses, ids)
	default:
		return 0, fmt.Errorf("%w: %q", lifecycle.ErrUnknownTable, string(table))
	}
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// --- KeyTTL sweep -------------------------------------------------------

// SweepExpiredIdempotencyKeys implements lifecycle.Store: removes idempotency
// mappings past expires_at. Enforcement also happens at lookup time (the
// replay path ignores expired keys), so this is bounded reclamation, not a
// correctness dependency.
func (s *LifecycleStore) SweepExpiredIdempotencyKeys(ctx context.Context, now time.Time) (int, error) {
	tag, err := s.DB.Pool.Exec(ctx, `DELETE FROM idempotency_keys WHERE expires_at < $1`, now)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

// --- Exports ---------------------------------------------------------------

// CreateExport implements lifecycle.ExportStore. The tenant column resolves
// through the tenants table: an empty tenant inserts a platform (NULL)
// export, an existing tenant its id, and an unknown tenant inserts zero rows
// and returns lifecycle.ErrUnknownTenant (never a foreign-key error).
func (s *LifecycleStore) CreateExport(ctx context.Context, id, requestedBy, tenant string, f lifecycle.ExportFilter) error {
	raw, err := json.Marshal(map[string]any{"filter": f.Metadata()})
	if err != nil {
		return err
	}
	tag, err := s.DB.Pool.Exec(ctx, `
		INSERT INTO data_exports (id, requested_by, tenant_id, filters, status, created_at)
		SELECT $1, $2, t.id, $3, 'queued', $4
		FROM (SELECT NULLIF($5, '') AS id) probe
		LEFT JOIN tenants t ON t.id = probe.id
		WHERE probe.id IS NULL OR t.id IS NOT NULL`,
		id, requestedBy, raw, time.Now(), tenant)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return lifecycle.ErrUnknownTenant
	}
	return nil
}

// CompleteExport implements lifecycle.ExportStore: terminal status with the
// verification metadata merged into the filters block.
func (s *LifecycleStore) CompleteExport(ctx context.Context, id, status string, rows int64, sha256 string) error {
	detail, err := json.Marshal(map[string]any{"rows": rows, "sha256": sha256})
	if err != nil {
		return err
	}
	tag, err := s.DB.Pool.Exec(ctx, `
		UPDATE data_exports SET status = $2, completed_at = $3, filters = filters || $4::jsonb
		WHERE id = $1`, id, status, time.Now(), detail)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("pg: export %s not found", id)
	}
	return nil
}

// ListExports implements lifecycle.ExportStore: a non-empty tenant is a
// mandatory predicate (tenant-bound callers see only their tenant's export
// records; platform callers see all).
func (s *LifecycleStore) ListExports(ctx context.Context, tenant string, limit int) ([]lifecycle.ExportView, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	rows, err := s.DB.Pool.Query(ctx, `
		SELECT id, requested_by, COALESCE(tenant_id,''), filters, status, created_at, completed_at
		FROM data_exports
		WHERE ($1 = '' OR tenant_id = $1)
		ORDER BY created_at DESC LIMIT $2`, tenant, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []lifecycle.ExportView
	for rows.Next() {
		var v lifecycle.ExportView
		if err := rows.Scan(&v.ID, &v.RequestedBy, &v.TenantID, &v.Filters, &v.Status,
			&v.CreatedAt, &v.CompletedAt); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// WriteExportAudit implements lifecycle.ExportStore.
func (s *LifecycleStore) WriteExportAudit(ctx context.Context, op mgmt.AdminOp) error {
	return writeOp(ctx, s.DB.Pool, op)
}

// Page implements lifecycle.ExportStore: one keyset page of redacted records
// for one source. The filter's tenant is a mandatory predicate resolved
// through the authoritative subject/tenant bindings (llm_requests and
// async_job_results via subjects, async_jobs and usage_ledger via their
// tenant columns).
func (s *LifecycleStore) Page(ctx context.Context, source lifecycle.ExportSource, f lifecycle.ExportFilter, cur lifecycle.ExportCursor, limit int) ([]lifecycle.ExportRecord, lifecycle.ExportCursor, error) {
	switch source {
	case lifecycle.SourceRequests:
		return s.pageRequests(ctx, f, cur, limit)
	case lifecycle.SourceResults:
		return s.pageResults(ctx, f, cur, limit)
	case lifecycle.SourceJobs:
		return s.pageJobs(ctx, f, cur, limit)
	case lifecycle.SourceLedger:
		return s.pageLedger(ctx, f, cur, limit)
	case lifecycle.SourceManagementLog:
		return s.pageManagementLog(ctx, f, cur, limit)
	default:
		return nil, lifecycle.ExportCursor{}, fmt.Errorf("lifecycle: unknown export source %q", string(source))
	}
}

// exportWhere is the shared filter predicate for the subject-based sources.
// Every clause is opt-in via the empty-string idiom used across the mgmt
// queries; the tenant clause is a mandatory predicate when non-empty.
const exportWhere = `
	  ($1 = '' OR subject_id = $1)
	  AND ($2 = '' OR model = $2)
	  AND ($3::timestamptz IS NULL OR created_at >= $3)
	  AND ($4::timestamptz IS NULL OR created_at <= $4)
	  AND ($5 = '' OR subject_id IN (SELECT id FROM subjects WHERE tenant_id = $5))`

func (s *LifecycleStore) pageRequests(ctx context.Context, f lifecycle.ExportFilter, cur lifecycle.ExportCursor, limit int) ([]lifecycle.ExportRecord, lifecycle.ExportCursor, error) {
	rows, err := s.DB.Pool.Query(ctx, `
		SELECT request_id, subject_id, key_id, model, provider, status,
		       COALESCE(error_class,''), latency_ms, prompt_tokens, completion_tokens,
		       first_token_millis, streaming, created_at, trace_id, route_attempts,
		       cost_micros, COALESCE(protocol,'chat')
		FROM llm_requests
		WHERE `+exportWhere+`
		  AND ($6 = '' OR request_id = $6)
		  AND ($7 = '' OR trace_id = $7)
		  AND ($8::timestamptz IS NULL OR (created_at, request_id) > ($8::timestamptz, $9))
		ORDER BY created_at, request_id
		LIMIT $10`,
		f.Subject, f.Model, nullTime(f.From), nullTime(f.To), f.Tenant,
		f.RequestID, f.TraceID, nullTime(cur.At), cur.Key, limit)
	if err != nil {
		return nil, lifecycle.ExportCursor{}, err
	}
	return scanRequestPage(rows, limit)
}

func scanRequestPage(rows pgx.Rows, limit int) ([]lifecycle.ExportRecord, lifecycle.ExportCursor, error) {
	defer rows.Close()
	var out []lifecycle.ExportRecord
	next := lifecycle.ExportCursor{}
	for rows.Next() {
		var status int64
		var prompt, completion, firstToken, cost *int64
		var streaming bool
		var createdAt time.Time
		var requestID, subjectID, keyID, model, provider, errorClass, traceID, protocol string
		var latency int64
		var routeAttempts int
		if err := rows.Scan(&requestID, &subjectID, &keyID, &model, &provider, &status,
			&errorClass, &latency, &prompt, &completion, &firstToken, &streaming,
			&createdAt, &traceID, &routeAttempts, &cost, &protocol); err != nil {
			return nil, next, err
		}
		line, err := lifecycle.EncodeRecord(lifecycle.RecordRequest, map[string]any{
			"request_id": requestID, "subject_id": subjectID, "key_id": keyID,
			"model": model, "provider": provider, "status": status,
			"error_class": nilIfEmpty(errorClass), "latency_ms": latency,
			"prompt_tokens": nullOrValue(prompt), "completion_tokens": nullOrValue(completion),
			"first_token_ms": nullOrValue(firstToken), "streaming": streaming,
			"created_at": createdAt, "trace_id": traceID,
			"route_attempts": routeAttempts, "cost_micros": nullOrValue(cost),
			"protocol": protocol,
		})
		if err != nil {
			return nil, next, err
		}
		out = append(out, lifecycle.ExportRecord{Line: line})
		next = lifecycle.ExportCursor{At: createdAt, Key: requestID}
	}
	if err := rows.Err(); err != nil {
		return nil, lifecycle.ExportCursor{}, err
	}
	if len(out) < limit {
		next = lifecycle.ExportCursor{} // source exhausted
	}
	return out, next, nil
}

// jobsWhere is the filter predicate for tenant-column sources (async_jobs).
const jobsWhere = `
	  ($1 = '' OR j.subject_id = $1)
	  AND ($2 = '' OR j.public_model = $2)
	  AND ($3::timestamptz IS NULL OR j.created_at >= $3)
	  AND ($4::timestamptz IS NULL OR j.created_at <= $4)
	  AND ($5 = '' OR j.tenant_id = $5)`

func (s *LifecycleStore) pageJobs(ctx context.Context, f lifecycle.ExportFilter, cur lifecycle.ExportCursor, limit int) ([]lifecycle.ExportRecord, lifecycle.ExportCursor, error) {
	rows, err := s.DB.Pool.Query(ctx, `
		SELECT j.job_id, j.subject_id, j.tenant_id, j.protocol, j.public_model,
		       j.request_digest, j.status, j.attempt_count,
		       COALESCE(j.final_request_id,''), j.created_at, j.updated_at
		FROM async_jobs j
		WHERE `+jobsWhere+`
		  AND ($6 = '' OR j.job_id = $6)
		  AND ($7::timestamptz IS NULL OR (j.created_at, j.job_id) > ($7::timestamptz, $8))
		ORDER BY j.created_at, j.job_id
		LIMIT $9`,
		f.Subject, f.Model, nullTime(f.From), nullTime(f.To), f.Tenant,
		f.JobID, nullTime(cur.At), cur.Key, limit)
	if err != nil {
		return nil, lifecycle.ExportCursor{}, err
	}
	defer rows.Close()
	var out []lifecycle.ExportRecord
	next := lifecycle.ExportCursor{}
	for rows.Next() {
		var jobID, subjectID, tenantID, protocol, publicModel, digest, status, finalRequestID string
		var attemptCount int
		var createdAt, updatedAt time.Time
		if err := rows.Scan(&jobID, &subjectID, &tenantID, &protocol, &publicModel, &digest,
			&status, &attemptCount, &finalRequestID, &createdAt, &updatedAt); err != nil {
			return nil, next, err
		}
		line, err := lifecycle.EncodeRecord(lifecycle.RecordJob, map[string]any{
			"job_id": jobID, "subject_id": subjectID, "tenant_id": tenantID,
			"protocol": protocol, "public_model": publicModel, "request_digest": digest,
			"status": status, "attempt_count": attemptCount,
			"final_request_id": nilIfEmpty(finalRequestID),
			"created_at":       createdAt, "updated_at": updatedAt,
		})
		if err != nil {
			return nil, next, err
		}
		out = append(out, lifecycle.ExportRecord{Line: line})
		next = lifecycle.ExportCursor{At: createdAt, Key: jobID}
	}
	if err := rows.Err(); err != nil {
		return nil, lifecycle.ExportCursor{}, err
	}
	if len(out) < limit {
		next = lifecycle.ExportCursor{}
	}
	return out, next, nil
}

// resultsWhere joins async_jobs for the tenant/subject/model predicates; the
// projection never selects the response column.
const resultsWhere = `
	  ($1 = '' OR j.subject_id = $1)
	  AND ($2 = '' OR j.public_model = $2)
	  AND ($3::timestamptz IS NULL OR r.created_at >= $3)
	  AND ($4::timestamptz IS NULL OR r.created_at <= $4)
	  AND ($5 = '' OR j.tenant_id = $5)`

func (s *LifecycleStore) pageResults(ctx context.Context, f lifecycle.ExportFilter, cur lifecycle.ExportCursor, limit int) ([]lifecycle.ExportRecord, lifecycle.ExportCursor, error) {
	rows, err := s.DB.Pool.Query(ctx, `
		SELECT r.job_id, r.error_class, r.usage_prompt_tokens, r.usage_completion_tokens,
		       r.usage_total_tokens, r.result_bytes, r.retention_expires_at, r.created_at
		FROM async_job_results r
		JOIN async_jobs j ON j.job_id = r.job_id
		WHERE `+resultsWhere+`
		  AND ($6 = '' OR r.job_id = $6)
		  AND ($7::timestamptz IS NULL OR (r.created_at, r.job_id) > ($7::timestamptz, $8))
		ORDER BY r.created_at, r.job_id
		LIMIT $9`,
		f.Subject, f.Model, nullTime(f.From), nullTime(f.To), f.Tenant,
		f.JobID, nullTime(cur.At), cur.Key, limit)
	if err != nil {
		return nil, lifecycle.ExportCursor{}, err
	}
	defer rows.Close()
	var out []lifecycle.ExportRecord
	next := lifecycle.ExportCursor{}
	for rows.Next() {
		var jobID string
		var errorClass *string
		var prompt, completion, total *int64
		var resultBytes int
		var retentionExpires, createdAt time.Time
		if err := rows.Scan(&jobID, &errorClass, &prompt, &completion, &total,
			&resultBytes, &retentionExpires, &createdAt); err != nil {
			return nil, next, err
		}
		var ec any
		if errorClass != nil {
			ec = *errorClass
		}
		line, err := lifecycle.EncodeRecord(lifecycle.RecordResult, map[string]any{
			"job_id": jobID, "error_class": ec,
			"usage_prompt_tokens": nullOrValue(prompt), "usage_completion_tokens": nullOrValue(completion),
			"usage_total_tokens": nullOrValue(total), "result_bytes": resultBytes,
			"retention_expires_at": retentionExpires, "created_at": createdAt,
			"response_redacted": true,
		})
		if err != nil {
			return nil, next, err
		}
		out = append(out, lifecycle.ExportRecord{Line: line})
		next = lifecycle.ExportCursor{At: createdAt, Key: jobID}
	}
	if err := rows.Err(); err != nil {
		return nil, lifecycle.ExportCursor{}, err
	}
	if len(out) < limit {
		next = lifecycle.ExportCursor{}
	}
	return out, next, nil
}

// ledgerWhere filters by the ledger's own tenant column.
const ledgerWhere = `
	  ($1 = '' OR subject_id = $1)
	  AND ($2 = '' OR public_model = $2)
	  AND ($3::timestamptz IS NULL OR created_at >= $3)
	  AND ($4::timestamptz IS NULL OR created_at <= $4)
	  AND ($5 = '' OR tenant_id = $5)`

func (s *LifecycleStore) pageLedger(ctx context.Context, f lifecycle.ExportFilter, cur lifecycle.ExportCursor, limit int) ([]lifecycle.ExportRecord, lifecycle.ExportCursor, error) {
	rows, err := s.DB.Pool.Query(ctx, `
		SELECT id, COALESCE(request_id,''), COALESCE(job_id,''), subject_id, tenant_id,
		       protocol, public_model, price_version, currency,
		       prompt_tokens, completion_tokens, reasoning_tokens, cached_input_tokens,
		       cost_micros, settle_status, settled_at, created_at
		FROM usage_ledger
		WHERE `+ledgerWhere+`
		  AND ($6 = '' OR job_id = $6)
		  AND ($7::timestamptz IS NULL OR (created_at, id) > ($7::timestamptz, NULLIF($8,'')::bigint))
		ORDER BY created_at, id
		LIMIT $9`,
		f.Subject, f.Model, nullTime(f.From), nullTime(f.To), f.Tenant,
		f.JobID, nullTime(cur.At), cur.Key, limit)
	if err != nil {
		return nil, lifecycle.ExportCursor{}, err
	}
	defer rows.Close()
	var out []lifecycle.ExportRecord
	next := lifecycle.ExportCursor{}
	for rows.Next() {
		var id int64
		var requestID, jobID, subjectID, tenantID, protocol, publicModel string
		var priceVersion *int64
		var currency *string
		var prompt, completion, reasoning, cached, cost *int64
		var settleStatus string
		var settledAt *time.Time
		var createdAt time.Time
		if err := rows.Scan(&id, &requestID, &jobID, &subjectID, &tenantID,
			&protocol, &publicModel, &priceVersion, &currency,
			&prompt, &completion, &reasoning, &cached,
			&cost, &settleStatus, &settledAt, &createdAt); err != nil {
			return nil, next, err
		}
		line, err := lifecycle.EncodeRecord(lifecycle.RecordLedger, map[string]any{
			"id": id, "request_id": nilIfEmpty(requestID), "job_id": nilIfEmpty(jobID),
			"subject_id": subjectID, "tenant_id": tenantID,
			"protocol": protocol, "public_model": publicModel,
			"price_version": nullOrValue(priceVersion), "currency": currency,
			"prompt_tokens": nullOrValue(prompt), "completion_tokens": nullOrValue(completion),
			"reasoning_tokens": nullOrValue(reasoning), "cached_input_tokens": nullOrValue(cached),
			"cost_micros": nullOrValue(cost), "settle_status": settleStatus,
			"settled_at": settledAt, "created_at": createdAt,
		})
		if err != nil {
			return nil, next, err
		}
		out = append(out, lifecycle.ExportRecord{Line: line})
		next = lifecycle.ExportCursor{At: createdAt, Key: strconv.FormatInt(id, 10)}
	}
	if err := rows.Err(); err != nil {
		return nil, lifecycle.ExportCursor{}, err
	}
	if len(out) < limit {
		next = lifecycle.ExportCursor{}
	}
	return out, next, nil
}

// pageManagementLog exports the platform-scope management audit trail.
// lifecycle.Admin only requests this source for platform-global callers, so
// there is no tenant predicate by design — this is the child-4 ruling:
// management-log rows are platform scope, never tenant-exportable.
func (s *LifecycleStore) pageManagementLog(ctx context.Context, f lifecycle.ExportFilter, cur lifecycle.ExportCursor, limit int) ([]lifecycle.ExportRecord, lifecycle.ExportCursor, error) {
	rows, err := s.DB.Pool.Query(ctx, `
		SELECT id, created_at, action, target, admin_subject, detail
		FROM admin_audit
		WHERE ($1::timestamptz IS NULL OR created_at >= $1)
		  AND ($2::timestamptz IS NULL OR created_at <= $2)
		  AND ($3::bigint IS NULL OR id > $3::bigint)
		ORDER BY id
		LIMIT $4`,
		nullTime(f.From), nullTime(f.To), nullableKey(cur.Key), limit)
	if err != nil {
		return nil, lifecycle.ExportCursor{}, err
	}
	defer rows.Close()
	var out []lifecycle.ExportRecord
	next := lifecycle.ExportCursor{}
	for rows.Next() {
		var id int64
		var action, target, adminSubject string
		var createdAt time.Time
		var detail []byte
		if err := rows.Scan(&id, &createdAt, &action, &target, &adminSubject, &detail); err != nil {
			return nil, next, err
		}
		line, err := lifecycle.EncodeRecord(lifecycle.RecordManagementLog, map[string]any{
			"id": id, "created_at": createdAt, "action": action,
			"target": target, "admin_subject": adminSubject, "detail": json.RawMessage(detail),
		})
		if err != nil {
			return nil, next, err
		}
		out = append(out, lifecycle.ExportRecord{Line: line})
		next = lifecycle.ExportCursor{At: createdAt, Key: strconv.FormatInt(id, 10)}
	}
	if err := rows.Err(); err != nil {
		return nil, lifecycle.ExportCursor{}, err
	}
	if len(out) < limit {
		next = lifecycle.ExportCursor{}
	}
	return out, next, nil
}

// --- helpers -----------------------------------------------------------------

// toInt64s converts string keys to bigint ids.
func toInt64s(keys []string) ([]int64, error) {
	out := make([]int64, 0, len(keys))
	for _, k := range keys {
		n, err := strconv.ParseInt(k, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("pg: ledger key %q: %w", k, err)
		}
		out = append(out, n)
	}
	return out, nil
}

// nullableKey maps the empty cursor key to NULL.
func nullableKey(key string) any {
	if key == "" {
		return nil
	}
	return key
}

// nilIfEmpty maps empty strings to JSON null.
func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// nullOrValue passes pointers through as JSON value or null.
func nullOrValue[T any](v *T) any {
	if v == nil {
		return nil
	}
	return *v
}
