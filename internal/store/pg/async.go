package pg

// PostgreSQL implementation of the async job store (internal/async). Every
// state transition is a conditional UPDATE keyed on status and lease owner,
// so cancel/commit/recovery races are decided exactly once by the database.
// Multi-worker claims use FOR UPDATE SKIP LOCKED; the single live lease per
// job is the (lease_owner, lease_expires_at) pair cleared on every terminal
// transition. Store outages surface as async.ErrUnavailable so callers can
// never disguise infrastructure failure as a job outcome.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/knowledge-base/knowledge-base-gateway/internal/async"
	"github.com/knowledge-base/knowledge-base-gateway/internal/tracing"
)

const jobColumns = `job_id, subject_id, tenant_id, protocol, public_model,
	request_digest, status, attempt_count, lease_owner, lease_expires_at,
	final_request_id, result_expires_at, created_at, updated_at,
	trace_id, parent_span_id, trace_sampled`

// jobColumnsQualified is jobColumns with the async_jobs alias applied, for
// the idempotency JOIN.
const jobColumnsQualified = `j.job_id, j.subject_id, j.tenant_id, j.protocol, j.public_model,
	j.request_digest, j.status, j.attempt_count, j.lease_owner, j.lease_expires_at,
	j.final_request_id, j.result_expires_at, j.created_at, j.updated_at,
	j.trace_id, j.parent_span_id, j.trace_sampled`

// AsyncStore implements async.Store on top of the connection pool. It is a
// distinct wrapper type because DB itself already carries the auth-lifecycle
// Create/Get aliases with different signatures.
type AsyncStore struct {
	DB *DB
}

// scanJob projects one async_jobs row.
func scanJob(scan func(dest ...any) error) (async.Job, error) {
	var j async.Job
	var leaseOwner, finalRequestID *string
	var leaseExpires, resultExpires *time.Time
	if err := scan(&j.ID, &j.SubjectID, &j.TenantID, &j.Protocol, &j.PublicModel,
		&j.RequestDigest, &j.Status, &j.AttemptCount, &leaseOwner, &leaseExpires,
		&finalRequestID, &resultExpires, &j.CreatedAt, &j.UpdatedAt,
		&j.TraceID, &j.ParentSpanID, &j.TraceSampled); err != nil {
		return async.Job{}, err
	}
	if leaseOwner != nil {
		j.LeaseOwner = *leaseOwner
	}
	if leaseExpires != nil {
		j.LeaseExpiresAt = *leaseExpires
	}
	if finalRequestID != nil {
		j.FinalRequestID = *finalRequestID
	}
	if resultExpires != nil {
		j.ResultExpiresAt = *resultExpires
	}
	return j, nil
}

// unavailable wraps an unexpected store failure in the shared sentinel.
func unavailable(err error) error {
	return fmt.Errorf("%w: %v", async.ErrUnavailable, err)
}

// isUniqueViolation reports a PostgreSQL unique-constraint failure.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// Create implements async.Store. The idempotency mapping, the job row, and
// its request snapshot commit in one transaction; a concurrent creator that
// won the (subject, key_hash) mapping first causes exactly one rollback and a
// re-read of the winner's mapping, so duplicate creation collapses to one job.
func (s AsyncStore) Create(ctx context.Context, in async.CreateInput) (async.CreateOutcome, error) {
	for attempt := 0; attempt < 2; attempt++ {
		out, retryable, err := s.createOnce(ctx, in)
		if err == nil && !retryable {
			return out, nil
		}
		if !retryable {
			return async.CreateOutcome{}, err
		}
	}
	return async.CreateOutcome{}, unavailable(errors.New("idempotency insert raced repeatedly"))
}

func (s AsyncStore) createOnce(ctx context.Context, in async.CreateInput) (out async.CreateOutcome, retryable bool, err error) {
	tx, err := s.DB.Pool.Begin(ctx)
	if err != nil {
		return async.CreateOutcome{}, false, unavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after Commit

	if in.KeyHash != "" {
		job, found, err := lookupIdempotentJob(ctx, tx, in.SubjectID, in.KeyHash, in.Now)
		if err != nil {
			return async.CreateOutcome{}, false, unavailable(err)
		}
		if found {
			if job.RequestDigest != in.RequestDigest {
				return async.CreateOutcome{}, false, async.ErrConflict
			}
			return async.CreateOutcome{Job: job, Replay: true}, false, nil
		}
		// An expired mapping is treated as absent: the KeyTTL replay window
		// closed, so this request creates a fresh job.
		if in.KeyHash != "" {
			// Reclaim the expired mapping inside this transaction so the
			// (subject, key_hash) unique index admits the fresh one. Racing
			// creators serialize on the delete: the loser's insert hits the
			// winner's new mapping and the retry replays it.
			if _, err := tx.Exec(ctx,
				`DELETE FROM idempotency_keys WHERE subject_id = $1 AND key_hash = $2 AND expires_at <= $3`,
				in.SubjectID, in.KeyHash, in.Now); err != nil {
				return async.CreateOutcome{}, false, unavailable(err)
			}
		}
	}

	j := async.Job{
		ID: in.JobID, SubjectID: in.SubjectID, TenantID: in.TenantID,
		Protocol: in.Protocol, PublicModel: in.PublicModel,
		RequestDigest: in.RequestDigest, Status: async.StatusQueued,
		CreatedAt: in.Now, UpdatedAt: in.Now,
		// Persist only normalized trace identity: anything that is not a
		// well-formed lowercase-hex W3C pair collapses to empty before it can
		// reach the row (internal/tracing owns the shape contract).
		TraceID: in.TraceID, ParentSpanID: in.SpanID, TraceSampled: in.TraceSampled,
	}
	if tc, ok := tracing.Normalize(j.TraceID, j.ParentSpanID); ok {
		j.TraceID, j.ParentSpanID = tc.TraceID, tc.SpanID
	} else {
		j.TraceID, j.ParentSpanID, j.TraceSampled = "", "", false
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO async_jobs (job_id, subject_id, tenant_id, protocol, public_model,
			request_digest, status, created_at, updated_at, visible_at,
			trace_id, parent_span_id, trace_sampled)
		VALUES ($1,$2,$3,$4,$5,$6,'queued',$7,$7,$7,$8,$9,$10)`,
		j.ID, j.SubjectID, j.TenantID, j.Protocol, j.PublicModel,
		j.RequestDigest, in.Now, j.TraceID, j.ParentSpanID, j.TraceSampled); err != nil {
		return async.CreateOutcome{}, false, unavailable(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO async_job_requests (job_id, request, created_at)
		VALUES ($1,$2,$3)`,
		j.ID, []byte(in.Request), in.Now); err != nil {
		return async.CreateOutcome{}, false, unavailable(err)
	}
	if in.KeyHash != "" {
		_, err := tx.Exec(ctx, `
			INSERT INTO idempotency_keys (id, subject_id, key_hash, request_digest, job_id, expires_at, created_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7)`,
			in.JobID, in.SubjectID, in.KeyHash, in.RequestDigest, in.JobID,
			in.Now.Add(in.KeyTTL), in.Now)
		if err != nil {
			if isUniqueViolation(err) {
				// A concurrent identical-key creation won the mapping; roll
				// back and replay the winner's job (or report the conflict).
				return async.CreateOutcome{}, true, nil
			}
			return async.CreateOutcome{}, false, unavailable(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return async.CreateOutcome{}, false, unavailable(err)
	}
	return async.CreateOutcome{Job: j}, false, nil
}

// lookupIdempotentJob resolves a (subject, key hash) mapping to its job. The
// expires_at predicate enforces the KeyTTL contract at the lookup boundary:
// an expired key no longer replays, regardless of whether the reclamation
// sweep has run yet.
func lookupIdempotentJob(ctx context.Context, tx pgx.Tx, subject, keyHash string, now time.Time) (async.Job, bool, error) {
	row := tx.QueryRow(ctx, `
		SELECT `+jobColumnsQualified+` FROM async_jobs j
		JOIN idempotency_keys k ON k.job_id = j.job_id
		WHERE k.subject_id = $1 AND k.key_hash = $2 AND k.expires_at > $3`, subject, keyHash, now)
	job, err := scanJob(row.Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return async.Job{}, false, nil
		}
		return async.Job{}, false, err
	}
	return job, true, nil
}

// Get implements async.Store, including lazy result expiry: a terminal job
// whose result TTL passed transitions to expired (result dropped) inside the
// same call. Expiry never re-triggers execution.
func (s AsyncStore) Get(ctx context.Context, jobID string, now time.Time) (async.Job, error) {
	row := s.DB.Pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM async_jobs WHERE job_id = $1`, jobID)
	job, err := scanJob(row.Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return async.Job{}, async.ErrNotFound
		}
		return async.Job{}, unavailable(err)
	}
	if job.Status != async.StatusCompleted && job.Status != async.StatusFailed {
		return job, nil
	}
	if job.ResultExpiresAt.IsZero() || !job.ResultExpiresAt.Before(now) {
		return job, nil
	}
	tx, err := s.DB.Pool.Begin(ctx)
	if err != nil {
		return async.Job{}, unavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	ct, err := tx.Exec(ctx, `
		UPDATE async_jobs SET status = 'expired', updated_at = $2
		WHERE job_id = $1 AND status IN ('completed','failed') AND result_expires_at < $2`,
		jobID, now)
	if err != nil {
		return async.Job{}, unavailable(err)
	}
	if ct.RowsAffected() == 0 {
		// A racing expiry (the sweep or another Get) committed first: return
		// the pre-read state. One stale completed answer is benign — the next
		// read observes expired, and the result read of that later call
		// finds the row already deleted and falls back to the status
		// envelope. Never re-triggers execution either way.
		return job, nil
	}
	if _, err := tx.Exec(ctx, `DELETE FROM async_job_results WHERE job_id = $1`, jobID); err != nil {
		return async.Job{}, unavailable(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return async.Job{}, unavailable(err)
	}
	job.Status = async.StatusExpired
	job.UpdatedAt = now
	return job, nil
}

// Result implements async.Store.
func (s AsyncStore) Result(ctx context.Context, jobID string) (async.Result, error) {
	var r async.Result
	var errClass *string
	var prompt, completion, total *int64
	var resp []byte
	err := s.DB.Pool.QueryRow(ctx, `
		SELECT response, error_class, usage_prompt_tokens, usage_completion_tokens,
		       usage_total_tokens, result_bytes, retention_expires_at, created_at
		FROM async_job_results WHERE job_id = $1`, jobID).
		Scan(&resp, &errClass, &prompt, &completion, &total, &r.ResultBytes, &r.RetentionExpiresAt, &r.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return async.Result{}, async.ErrNotFound
		}
		return async.Result{}, unavailable(err)
	}
	if errClass != nil {
		r.ErrorClass = *errClass
	}
	if prompt != nil || completion != nil || total != nil {
		r.Usage = &async.Usage{PromptTokens: prompt, CompletionTokens: completion, TotalTokens: total}
	}
	r.Response = json.RawMessage(resp)
	return r, nil
}

// Cancel implements async.Store: queued first, then running, both as
// conditional updates; terminal jobs keep their state (idempotent no-op).
func (s AsyncStore) Cancel(ctx context.Context, jobID string, now time.Time) (async.Job, async.CancelOutcome, error) {
	tx, err := s.DB.Pool.Begin(ctx)
	if err != nil {
		return async.Job{}, 0, unavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	ct, err := tx.Exec(ctx, `
		UPDATE async_jobs SET status = 'cancelled', lease_owner = NULL,
		       lease_expires_at = NULL, updated_at = $2
		WHERE job_id = $1 AND status = 'queued'`, jobID, now)
	if err != nil {
		return async.Job{}, 0, unavailable(err)
	}
	if ct.RowsAffected() == 1 {
		job, err := readJobTx(ctx, tx, jobID)
		if err != nil {
			return async.Job{}, 0, err
		}
		if err := tx.Commit(ctx); err != nil {
			return async.Job{}, 0, unavailable(err)
		}
		return job, async.CancelledQueued, nil
	}
	ct, err = tx.Exec(ctx, `
		UPDATE async_jobs SET status = 'cancelled', lease_owner = NULL,
		       lease_expires_at = NULL, updated_at = $2
		WHERE job_id = $1 AND status = 'running'`, jobID, now)
	if err != nil {
		return async.Job{}, 0, unavailable(err)
	}
	if ct.RowsAffected() == 1 {
		job, err := readJobTx(ctx, tx, jobID)
		if err != nil {
			return async.Job{}, 0, err
		}
		if err := tx.Commit(ctx); err != nil {
			return async.Job{}, 0, unavailable(err)
		}
		return job, async.CancelledRunning, nil
	}
	job, err := readJobTx(ctx, tx, jobID)
	if err != nil {
		return async.Job{}, 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return async.Job{}, 0, unavailable(err)
	}
	return job, async.CancelNoop, nil
}

func readJobTx(ctx context.Context, tx pgx.Tx, jobID string) (async.Job, error) {
	row := tx.QueryRow(ctx, `SELECT `+jobColumns+` FROM async_jobs WHERE job_id = $1`, jobID)
	job, err := scanJob(row.Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return async.Job{}, async.ErrNotFound
		}
		return async.Job{}, unavailable(err)
	}
	return job, nil
}

// Claim implements async.Store: oldest queued job first, claimed under a
// fresh lease with SKIP LOCKED so concurrent claimers never share a job.
func (s AsyncStore) Claim(ctx context.Context, in async.ClaimInput) (async.Claimed, bool, error) {
	tx, err := s.DB.Pool.Begin(ctx)
	if err != nil {
		return async.Claimed{}, false, unavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	row := tx.QueryRow(ctx, `
		UPDATE async_jobs SET status = 'running', lease_owner = $1,
		       lease_expires_at = $2, updated_at = $3
		WHERE job_id = (
			SELECT job_id FROM async_jobs
			WHERE status = 'queued' AND visible_at <= $3
			ORDER BY created_at, job_id
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING `+jobColumns, in.Owner, in.Now.Add(in.Lease), in.Now)
	job, err := scanJob(row.Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return async.Claimed{}, false, nil // queue empty
		}
		return async.Claimed{}, false, unavailable(err)
	}
	var payload []byte
	if err := tx.QueryRow(ctx,
		`SELECT request FROM async_job_requests WHERE job_id = $1`, job.ID).Scan(&payload); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Cannot happen (creation writes both rows in one transaction);
			// treat as infra failure so the lease simply expires.
			return async.Claimed{}, false, unavailable(fmt.Errorf("job %s has no request payload", job.ID))
		}
		return async.Claimed{}, false, unavailable(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return async.Claimed{}, false, unavailable(err)
	}
	return async.Claimed{Job: job, Request: payload}, true, nil
}

// Heartbeat implements async.Store.
func (s AsyncStore) Heartbeat(ctx context.Context, jobID, owner string, lease time.Duration, now time.Time) error {
	ct, err := s.DB.Pool.Exec(ctx, `
		UPDATE async_jobs SET lease_expires_at = $3, updated_at = $3
		WHERE job_id = $1 AND lease_owner = $2 AND status = 'running'`,
		jobID, owner, now.Add(lease))
	if err != nil {
		return unavailable(err)
	}
	if ct.RowsAffected() == 0 {
		return async.ErrLeaseLost
	}
	return nil
}

// CommitSuccess implements async.Store: the conditional running→completed
// transition and the result insert are one transaction decided by the lease.
func (s AsyncStore) CommitSuccess(ctx context.Context, in async.SuccessInput) (bool, error) {
	tx, err := s.DB.Pool.Begin(ctx)
	if err != nil {
		return false, unavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	ct, err := tx.Exec(ctx, `
		UPDATE async_jobs SET status = 'completed', lease_owner = NULL,
		       lease_expires_at = NULL, final_request_id = $3,
		       result_expires_at = $4, attempt_count = attempt_count + $5,
		       updated_at = $6
		WHERE job_id = $1 AND lease_owner = $2 AND status = 'running'`,
		in.JobID, in.Owner, in.FinalRequestID, in.ResultExpiresAt,
		bumpArg(in.BumpAttempt), in.Now)
	if err != nil {
		return false, unavailable(err)
	}
	if ct.RowsAffected() == 0 {
		return false, nil // a racing transition won the job
	}
	if err := insertResultTx(ctx, tx, async.Result{
		Response: in.Response, Usage: in.Usage,
		ResultBytes: in.ResultBytes, RetentionExpiresAt: in.ResultExpiresAt,
		CreatedAt: in.Now,
	}, in.JobID); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, unavailable(err)
	}
	return true, nil
}

// CommitFailure implements async.Store under the same lease discipline.
func (s AsyncStore) CommitFailure(ctx context.Context, in async.FailureInput) (bool, error) {
	tx, err := s.DB.Pool.Begin(ctx)
	if err != nil {
		return false, unavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	ct, err := tx.Exec(ctx, `
		UPDATE async_jobs SET status = 'failed', lease_owner = NULL,
		       lease_expires_at = NULL, final_request_id = $3,
		       result_expires_at = $4, attempt_count = attempt_count + $5,
		       updated_at = $6
		WHERE job_id = $1 AND lease_owner = $2 AND status = 'running'`,
		in.JobID, in.Owner, in.FinalRequestID, in.ResultExpiresAt,
		bumpArg(in.BumpAttempt), in.Now)
	if err != nil {
		return false, unavailable(err)
	}
	if ct.RowsAffected() == 0 {
		return false, nil
	}
	if err := insertResultTx(ctx, tx, async.Result{
		Response: in.Response, ErrorClass: in.ErrorClass,
		ResultBytes: in.ResultBytes, RetentionExpiresAt: in.ResultExpiresAt,
		CreatedAt: in.Now,
	}, in.JobID); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, unavailable(err)
	}
	return true, nil
}

func bumpArg(bump bool) int {
	if bump {
		return 1
	}
	return 0
}

func insertResultTx(ctx context.Context, tx pgx.Tx, r async.Result, jobID string) error {
	if len(r.Response) == 0 {
		// A failed job may legitimately carry no stored result (GET falls
		// back to the status envelope); response is NOT NULL, so skip.
		return nil
	}
	var usage *async.Usage
	if r.Usage != nil {
		usage = r.Usage
	}
	var prompt, completion, total any
	if usage != nil {
		prompt, completion, total = usage.PromptTokens, usage.CompletionTokens, usage.TotalTokens
	}
	var errClass any
	if r.ErrorClass != "" {
		errClass = r.ErrorClass
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO async_job_results (job_id, response, error_class,
			usage_prompt_tokens, usage_completion_tokens, usage_total_tokens,
			result_bytes, retention_expires_at, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		jobID, []byte(r.Response), errClass, prompt, completion, total,
		r.ResultBytes, r.RetentionExpiresAt, r.CreatedAt); err != nil {
		return unavailable(err)
	}
	return nil
}

// Requeue implements async.Store: running→queued under the caller's lease,
// with attempt accounting and the option's backoff (visible_at). A zero
// NoClaimBefore keeps the column's now() default behavior — immediately
// claimable.
func (s AsyncStore) Requeue(ctx context.Context, jobID, owner string, opts async.RequeueOptions, now time.Time) (bool, error) {
	visible := now
	if !opts.NoClaimBefore.IsZero() {
		visible = opts.NoClaimBefore
	}
	ct, err := s.DB.Pool.Exec(ctx, `
		UPDATE async_jobs SET status = 'queued', lease_owner = NULL,
		       lease_expires_at = NULL, attempt_count = attempt_count + $3,
		       visible_at = $4, updated_at = $5
		WHERE job_id = $1 AND lease_owner = $2 AND status = 'running'`,
		jobID, owner, bumpArg(opts.BumpAttempt), visible, now)
	if err != nil {
		return false, unavailable(err)
	}
	return ct.RowsAffected() == 1, nil
}

// RecoverExpiredLeases implements async.Store in one transaction: jobs whose
// lease lapsed either terminal-fail (attempts exhausted, failure result
// stored) or return to the queue with the lost attempt counted.
func (s AsyncStore) RecoverExpiredLeases(ctx context.Context, in async.RecoverInput) (async.RecoverOutcome, error) {
	tx, err := s.DB.Pool.Begin(ctx)
	if err != nil {
		return async.RecoverOutcome{}, unavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	out := async.RecoverOutcome{}

	// Exhausted leases terminal-fail with their stored failure result.
	rows, err := tx.Query(ctx, `
		UPDATE async_jobs SET status = 'failed', lease_owner = NULL,
		       lease_expires_at = NULL, attempt_count = attempt_count + 1,
		       result_expires_at = $2, updated_at = $2
		WHERE job_id IN (
			SELECT job_id FROM async_jobs
			WHERE status = 'running' AND lease_expires_at < $1
			  AND attempt_count + 1 >= $3
			FOR UPDATE SKIP LOCKED
		)
		RETURNING `+jobColumns, in.Now, in.Now, in.MaxAttempts)
	if err != nil {
		return async.RecoverOutcome{}, unavailable(err)
	}
	var failed []async.Job
	for rows.Next() {
		j, err := scanJob(rows.Scan)
		if err != nil {
			rows.Close()
			return async.RecoverOutcome{}, unavailable(err)
		}
		failed = append(failed, j)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return async.RecoverOutcome{}, unavailable(err)
	}
	for _, j := range failed {
		if in.FailureResult == nil {
			continue
		}
		res := in.FailureResult(j.ID)
		res.RetentionExpiresAt = in.Now
		res.CreatedAt = in.Now
		if err := insertResultTx(ctx, tx, res, j.ID); err != nil {
			return async.RecoverOutcome{}, err
		}
		out.Failed = append(out.Failed, j)
	}

	// The remaining lapsed leases return to the queue, immediately claimable.
	requeueRows, err := tx.Query(ctx, `
		UPDATE async_jobs SET status = 'queued', lease_owner = NULL,
		       lease_expires_at = NULL, attempt_count = attempt_count + 1,
		       visible_at = $2, updated_at = $2
		WHERE job_id IN (
			SELECT job_id FROM async_jobs
			WHERE status = 'running' AND lease_expires_at < $1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING job_id`, in.Now, in.Now)
	if err != nil {
		return async.RecoverOutcome{}, unavailable(err)
	}
	for requeueRows.Next() {
		var id string
		if err := requeueRows.Scan(&id); err != nil {
			requeueRows.Close()
			return async.RecoverOutcome{}, unavailable(err)
		}
		out.Requeued = append(out.Requeued, id)
	}
	requeueRows.Close()
	if err := requeueRows.Err(); err != nil {
		return async.RecoverOutcome{}, unavailable(err)
	}

	if err := tx.Commit(ctx); err != nil {
		return async.RecoverOutcome{}, unavailable(err)
	}
	return out, nil
}

// ExpireDueResults implements async.Store: terminal jobs past their result
// TTL become expired and their stored results are dropped.
func (s AsyncStore) ExpireDueResults(ctx context.Context, now time.Time) (int, error) {
	tx, err := s.DB.Pool.Begin(ctx)
	if err != nil {
		return 0, unavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		UPDATE async_jobs SET status = 'expired', updated_at = $1
		WHERE job_id IN (
			SELECT job_id FROM async_jobs
			WHERE status IN ('completed','failed') AND result_expires_at < $1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING job_id`, now)
	if err != nil {
		return 0, unavailable(err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, unavailable(err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, unavailable(err)
	}
	if len(ids) > 0 {
		if _, err := tx.Exec(ctx,
			`DELETE FROM async_job_results WHERE job_id = ANY($1)`, ids); err != nil {
			return 0, unavailable(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, unavailable(err)
	}
	return len(ids), nil
}

// QueueDepth implements async.Store.
func (s AsyncStore) QueueDepth(ctx context.Context) (int, error) {
	var n int
	if err := s.DB.Pool.QueryRow(ctx,
		`SELECT count(*) FROM async_jobs WHERE status = 'queued'`).Scan(&n); err != nil {
		return 0, unavailable(err)
	}
	return n, nil
}

// QueueOldestAge implements async.Store: the age of the oldest queued job in
// seconds (0 when the queue is empty), measured against the database clock so
// the gauge stays correct across process clock skew.
func (s AsyncStore) QueueOldestAge(ctx context.Context) (time.Duration, error) {
	var secs float64
	if err := s.DB.Pool.QueryRow(ctx, `
		SELECT COALESCE(EXTRACT(EPOCH FROM (now() - min(created_at))), 0)
		FROM async_jobs WHERE status = 'queued'`).Scan(&secs); err != nil {
		return 0, unavailable(err)
	}
	return time.Duration(secs * float64(time.Second)), nil
}

// SweepExpiredIdempotencyKeys implements async.Store: removes idempotency
// mappings past expires_at. Enforcement already happens on the lookup path
// (the replay query filters on expires_at), so this is bounded reclamation.
func (s AsyncStore) SweepExpiredIdempotencyKeys(ctx context.Context, now time.Time) (int, error) {
	tag, err := s.DB.Pool.Exec(ctx, `DELETE FROM idempotency_keys WHERE expires_at < $1`, now)
	if err != nil {
		return 0, unavailable(err)
	}
	return int(tag.RowsAffected()), nil
}

// Ready implements async.Store (delegates to the pool ping).
func (s AsyncStore) Ready(ctx context.Context) error { return s.DB.Ready(ctx) }
