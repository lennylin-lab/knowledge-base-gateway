package async

// In-memory Store with the same conditional-update semantics as the
// PostgreSQL implementation: status/owner-keyed transitions decided under one
// lock, so tests for cancel/settle and claim races are deterministic and run
// under -race. It is a single-process development/test harness, never a
// multi-instance queue.

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
)

// MemoryStore is the mutex-guarded Store implementation.
type MemoryStore struct {
	mu   sync.Mutex
	now  func() time.Time
	jobs map[string]*memJob
	// idem maps subject + "\x00" + key hash to the key's mapping: the job it
	// created and its expiry (the persisted KeyTTL contract).
	idem map[string]memIdem
	seq  int
}

// memIdem is one idempotency mapping with its expiry.
type memIdem struct {
	jobID     string
	expiresAt time.Time
}

// memJob is one job plus its payload, terminal result, and retry backoff.
type memJob struct {
	job     Job
	request json.RawMessage
	result  *Result
	// visibleAt hides the queued job from claiming until the backoff
	// expires; zero means immediately visible (fresh and abort-requeued
	// jobs, lease-recovery requeues).
	visibleAt time.Time
}

// NewMemoryStore builds an empty store. now injects the clock (tests fake
// lease expiry); nil means time.Now.
func NewMemoryStore(now func() time.Time) *MemoryStore {
	if now == nil {
		now = time.Now
	}
	return &MemoryStore{now: now, jobs: map[string]*memJob{}, idem: map[string]memIdem{}}
}

// Create implements Store. The whole decision runs under the mutex, so the
// idempotency mapping is race-free by construction. A mapping past its
// expires_at no longer replays (the KeyTTL contract): it is dropped and the
// request creates a fresh job, exactly as the PostgreSQL store does.
func (m *MemoryStore) Create(_ context.Context, in CreateInput) (CreateOutcome, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if in.KeyHash != "" {
		mapKey := in.SubjectID + "\x00" + in.KeyHash
		if entry, ok := m.idem[mapKey]; ok {
			if in.Now.After(entry.expiresAt) {
				// Expired key: the replay window closed; remove the mapping
				// and fall through to fresh creation.
				delete(m.idem, mapKey)
			} else {
				existing, ok := m.jobs[entry.jobID]
				if !ok {
					return CreateOutcome{}, fmt.Errorf("%w: idempotency mapping references missing job", ErrUnavailable)
				}
				if existing.job.RequestDigest != in.RequestDigest {
					return CreateOutcome{}, ErrConflict
				}
				return CreateOutcome{Job: existing.job, Replay: true}, nil
			}
		}
	}
	if _, dup := m.jobs[in.JobID]; dup {
		// Job IDs are generated fresh per creation attempt; a duplicate is a
		// caller bug, not a retryable state.
		return CreateOutcome{}, fmt.Errorf("async: duplicate job id %s", in.JobID)
	}
	j := Job{
		ID: in.JobID, SubjectID: in.SubjectID, TenantID: in.TenantID,
		Protocol: in.Protocol, PublicModel: in.PublicModel,
		RequestDigest: in.RequestDigest, Status: StatusQueued,
		CreatedAt: in.Now, UpdatedAt: in.Now,
	}
	rec := &memJob{job: j, request: in.Request} // fresh jobs are immediately claimable
	m.jobs[in.JobID] = rec
	if in.KeyHash != "" {
		m.idem[in.SubjectID+"\x00"+in.KeyHash] = memIdem{jobID: in.JobID, expiresAt: in.Now.Add(in.KeyTTL)}
	}
	return CreateOutcome{Job: j}, nil
}

// Get implements Store, including lazy result expiry.
func (m *MemoryStore) Get(_ context.Context, jobID string, now time.Time) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.jobs[jobID]
	if !ok {
		return Job{}, ErrNotFound
	}
	m.expireLocked(rec, now)
	return rec.job, nil
}

// Result implements Store.
func (m *MemoryStore) Result(_ context.Context, jobID string) (Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.jobs[jobID]
	if !ok || rec.result == nil {
		return Result{}, ErrNotFound
	}
	return *rec.result, nil
}

// Cancel implements Store.
func (m *MemoryStore) Cancel(_ context.Context, jobID string, now time.Time) (Job, CancelOutcome, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.jobs[jobID]
	if !ok {
		return Job{}, 0, ErrNotFound
	}
	switch rec.job.Status {
	case StatusQueued:
		rec.job.Status = StatusCancelled
		rec.job.LeaseOwner, rec.job.LeaseExpiresAt = "", time.Time{}
		rec.job.UpdatedAt = now
		return rec.job, CancelledQueued, nil
	case StatusRunning:
		rec.job.Status = StatusCancelled
		rec.job.LeaseOwner, rec.job.LeaseExpiresAt = "", time.Time{}
		rec.job.UpdatedAt = now
		return rec.job, CancelledRunning, nil
	default:
		return rec.job, CancelNoop, nil
	}
}

// Claim implements Store: oldest visible queued job wins, concurrent
// claimers are serialized by the mutex so a job is never claimed twice.
// Backed-off jobs (visibleAt in the future) are invisible to claiming, so
// retrying work cannot starve the rest of the queue.
func (m *MemoryStore) Claim(_ context.Context, in ClaimInput) (Claimed, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]string, 0, len(m.jobs))
	for id, rec := range m.jobs {
		if rec.job.Status != StatusQueued {
			continue
		}
		if !rec.visibleAt.IsZero() && rec.visibleAt.After(in.Now) {
			continue
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return Claimed{}, false, nil
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := m.jobs[ids[i]].job, m.jobs[ids[j]].job
		if !a.CreatedAt.Equal(b.CreatedAt) {
			return a.CreatedAt.Before(b.CreatedAt)
		}
		return a.ID < b.ID
	})
	rec := m.jobs[ids[0]]
	rec.job.Status = StatusRunning
	rec.job.LeaseOwner = in.Owner
	rec.job.LeaseExpiresAt = in.Now.Add(in.Lease)
	rec.job.UpdatedAt = in.Now
	rec.visibleAt = time.Time{}
	return Claimed{Job: rec.job, Request: rec.request}, true, nil
}

// Heartbeat implements Store.
func (m *MemoryStore) Heartbeat(_ context.Context, jobID, owner string, lease time.Duration, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.jobs[jobID]
	if !ok || rec.job.Status != StatusRunning || rec.job.LeaseOwner != owner {
		return ErrLeaseLost
	}
	rec.job.LeaseExpiresAt = now.Add(lease)
	return nil
}

// CommitSuccess implements Store: one lock-held decision replaces the
// database's conditional update.
func (m *MemoryStore) CommitSuccess(_ context.Context, in SuccessInput) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.jobs[in.JobID]
	if !ok || rec.job.Status != StatusRunning || rec.job.LeaseOwner != in.Owner {
		return false, nil
	}
	rec.job.Status = StatusCompleted
	rec.job.LeaseOwner, rec.job.LeaseExpiresAt = "", time.Time{}
	rec.job.FinalRequestID = in.FinalRequestID
	rec.job.ResultExpiresAt = in.ResultExpiresAt
	rec.job.UpdatedAt = in.Now
	if in.BumpAttempt {
		rec.job.AttemptCount++
	}
	rec.result = &Result{
		Response: in.Response, Usage: in.Usage,
		ResultBytes: in.ResultBytes, RetentionExpiresAt: in.ResultExpiresAt,
		CreatedAt: in.Now,
	}
	return true, nil
}

// CommitFailure implements Store.
func (m *MemoryStore) CommitFailure(_ context.Context, in FailureInput) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.jobs[in.JobID]
	if !ok || rec.job.Status != StatusRunning || rec.job.LeaseOwner != in.Owner {
		return false, nil
	}
	rec.job.Status = StatusFailed
	rec.job.LeaseOwner, rec.job.LeaseExpiresAt = "", time.Time{}
	rec.job.FinalRequestID = in.FinalRequestID
	rec.job.ResultExpiresAt = in.ResultExpiresAt
	rec.job.UpdatedAt = in.Now
	if in.BumpAttempt {
		rec.job.AttemptCount++
	}
	rec.result = &Result{
		Response: in.Response, ErrorClass: in.ErrorClass,
		ResultBytes: in.ResultBytes, RetentionExpiresAt: in.ResultExpiresAt,
		CreatedAt: in.Now,
	}
	return true, nil
}

// Requeue implements Store: running→queued under the caller's lease, with
// attempt accounting and retry backoff per the options.
func (m *MemoryStore) Requeue(_ context.Context, jobID, owner string, opts RequeueOptions, now time.Time) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.jobs[jobID]
	if !ok || rec.job.Status != StatusRunning || rec.job.LeaseOwner != owner {
		return false, nil
	}
	rec.job.Status = StatusQueued
	rec.job.LeaseOwner, rec.job.LeaseExpiresAt = "", time.Time{}
	rec.job.UpdatedAt = now
	if opts.BumpAttempt {
		rec.job.AttemptCount++
	}
	rec.visibleAt = opts.NoClaimBefore
	return true, nil
}

// RecoverExpiredLeases implements Store.
func (m *MemoryStore) RecoverExpiredLeases(_ context.Context, in RecoverInput) (RecoverOutcome, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := RecoverOutcome{}
	// Fail exhausted leases first, then requeue the remainder (same order as
	// the SQL sweep, so the two stores behave identically).
	var requeueable []*memJob
	for _, id := range m.orderedIDs() {
		rec := m.jobs[id]
		if rec.job.Status != StatusRunning || rec.job.LeaseExpiresAt.IsZero() || !rec.job.LeaseExpiresAt.Before(in.Now) {
			continue
		}
		if rec.job.AttemptCount+1 >= in.MaxAttempts {
			rec.job.Status = StatusFailed
			rec.job.LeaseOwner, rec.job.LeaseExpiresAt = "", time.Time{}
			rec.job.ResultExpiresAt = in.Now // failed jobs still expire their stored reason
			rec.job.UpdatedAt = in.Now
			rec.job.AttemptCount++
			if in.FailureResult != nil {
				res := in.FailureResult(rec.job.ID)
				res.RetentionExpiresAt = in.Now
				res.CreatedAt = in.Now
				rec.result = &res
			}
			out.Failed = append(out.Failed, rec.job)
			continue
		}
		requeueable = append(requeueable, rec)
	}
	for _, rec := range requeueable {
		rec.job.Status = StatusQueued
		rec.job.LeaseOwner, rec.job.LeaseExpiresAt = "", time.Time{}
		rec.job.UpdatedAt = in.Now
		rec.job.AttemptCount++
		rec.visibleAt = time.Time{} // recovered work is immediately claimable
		out.Requeued = append(out.Requeued, rec.job.ID)
	}
	return out, nil
}

// ExpireDueResults implements Store.
func (m *MemoryStore) ExpireDueResults(_ context.Context, now time.Time) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, rec := range m.jobs {
		if rec.job.Status != StatusCompleted && rec.job.Status != StatusFailed {
			continue
		}
		if rec.job.ResultExpiresAt.IsZero() || !rec.job.ResultExpiresAt.Before(now) {
			continue
		}
		rec.job.Status = StatusExpired
		rec.job.UpdatedAt = now
		rec.result = nil
		n++
	}
	return n, nil
}

// QueueDepth implements Store.
func (m *MemoryStore) QueueDepth(_ context.Context) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, rec := range m.jobs {
		if rec.job.Status == StatusQueued {
			n++
		}
	}
	return n, nil
}

// SweepExpiredIdempotencyKeys implements Store: removes mappings whose
// expires_at passed.
func (m *MemoryStore) SweepExpiredIdempotencyKeys(_ context.Context, now time.Time) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for k, entry := range m.idem {
		if now.After(entry.expiresAt) {
			delete(m.idem, k)
			n++
		}
	}
	return n, nil
}

// Ready implements Store.
func (m *MemoryStore) Ready(context.Context) error { return nil }

// expireLocked lazily expires a terminal job whose result TTL passed. Caller
// holds the mutex.
func (m *MemoryStore) expireLocked(rec *memJob, now time.Time) {
	if rec.job.Status != StatusCompleted && rec.job.Status != StatusFailed {
		return
	}
	if rec.job.ResultExpiresAt.IsZero() || !rec.job.ResultExpiresAt.Before(now) {
		return
	}
	rec.job.Status = StatusExpired
	rec.job.UpdatedAt = m.now()
	rec.result = nil
}

// orderedIDs returns job IDs oldest-first with a stable tiebreak, mirroring
// the SQL `ORDER BY created_at, job_id` claim scan.
func (m *MemoryStore) orderedIDs() []string {
	ids := make([]string, 0, len(m.jobs))
	for id := range m.jobs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := m.jobs[ids[i]].job, m.jobs[ids[j]].job
		if !a.CreatedAt.Equal(b.CreatedAt) {
			return a.CreatedAt.Before(b.CreatedAt)
		}
		return a.ID < b.ID
	})
	return ids
}
