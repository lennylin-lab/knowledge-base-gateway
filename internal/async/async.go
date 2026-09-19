// Package async implements the durable background-job lifecycle for Responses
// requests. PostgreSQL owns the state machine: every transition is a
// conditional update, so cancel/complete/fail races are decided exactly once
// by the database and at most one worker lease is ever live per job. The
// in-memory store in this package mirrors those semantics for tests and
// single-process development; it is not a multi-instance job queue.
//
// Security invariants: jobs belong to the subject that created them, stored
// payloads are normalized gateway requests only (never raw client bytes or
// credentials), and terminal results carry only the public response envelope.
package async

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Status is the closed job state set. Terminal: completed, failed, cancelled,
// expired. Non-terminal: queued, running.
type Status string

// Job states (DB CHECK constraints mirror this set).
const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
	StatusExpired   Status = "expired"
)

// Terminal reports whether the state admits no further execution work.
func (s Status) Terminal() bool {
	switch s {
	case StatusCompleted, StatusFailed, StatusCancelled, StatusExpired:
		return true
	}
	return false
}

// Errors surfaced across the store boundary. ErrUnavailable wraps store
// infrastructure failures (a down database) so callers can distinguish them
// from job-level outcomes and never disguise an outage as a job failure.
var (
	// ErrNotFound: no such job (ownership checks stay with callers).
	ErrNotFound = errors.New("async: job not found")
	// ErrConflict: the idempotency key exists with a different request digest.
	ErrConflict = errors.New("async: idempotency key conflict")
	// ErrUnavailable: the job store (queue) is temporarily unavailable.
	ErrUnavailable = errors.New("async: job store unavailable")
	// ErrLeaseLost: the caller no longer owns the job's worker lease.
	ErrLeaseLost = errors.New("async: worker lease lost")
)

// Job is the durable state-machine row projection.
type Job struct {
	ID              string
	SubjectID       string
	TenantID        string
	Protocol        string
	PublicModel     string
	RequestDigest   string
	Status          Status
	AttemptCount    int
	LeaseOwner      string
	LeaseExpiresAt  time.Time // zero when no lease is held
	FinalRequestID  string
	ResultExpiresAt time.Time // zero until a terminal result exists
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// Usage carries optional token counts for a stored result. A nil count stays
// unknown and is never fabricated as zero.
type Usage struct {
	PromptTokens     *int64
	CompletionTokens *int64
	TotalTokens      *int64
}

// Result is the stored terminal outcome. Response holds the public envelope
// bytes (never provider-private fields); ErrorClass names the failure class
// for failed jobs and is empty for completed ones.
type Result struct {
	Response           json.RawMessage
	ErrorClass         string
	Usage              *Usage
	ResultBytes        int
	RetentionExpiresAt time.Time
	CreatedAt          time.Time
}

// CreateInput carries everything creation persists atomically: the job row,
// its request snapshot, and the optional idempotency mapping. KeyHash is the
// subject-scoped hash of the caller's Idempotency-Key ("" when none was sent).
type CreateInput struct {
	JobID         string
	SubjectID     string
	TenantID      string
	Protocol      string
	PublicModel   string
	RequestDigest string
	Request       json.RawMessage
	KeyHash       string
	ResultTTL     time.Duration
	KeyTTL        time.Duration
	Now           time.Time
}

// CreateOutcome reports whether the input created a fresh job or replayed the
// job an earlier identical request created under the same idempotency key.
type CreateOutcome struct {
	Job    Job
	Replay bool
}

// CancelOutcome distinguishes the two ways a cancel can win so the caller can
// signal a local worker (CancelledRunning) or know that no execution ever
// started (CancelledQueued). CancelNoop means the job was already terminal.
type CancelOutcome int

// Cancel outcomes.
const (
	CancelledQueued CancelOutcome = iota
	CancelledRunning
	CancelNoop
)

// CreateInput-based store surface. Implementations must make every
// transition a conditional update keyed on status and lease owner, so two
// racing decisions (cancel vs settle, recovery vs slow worker) resolve to
// exactly one winner at the database.
type Store interface {
	// Create inserts a queued job with its request snapshot and optional
	// idempotency mapping in one transaction. With a key: same subject+key
	// and same digest returns the original job (Replay), a different digest
	// returns ErrConflict.
	Create(ctx context.Context, in CreateInput) (CreateOutcome, error)
	// Get loads one job. A terminal job whose result TTL has passed is
	// lazily transitioned to StatusExpired (result dropped) inside this call;
	// expiry never re-triggers execution.
	Get(ctx context.Context, jobID string, now time.Time) (Job, error)
	// Result loads the stored terminal result.
	Result(ctx context.Context, jobID string) (Result, error)
	// Cancel conditionally transitions queued→cancelled or running→cancelled.
	// It never touches terminal jobs; repeat cancels are idempotent no-ops.
	Cancel(ctx context.Context, jobID string, now time.Time) (Job, CancelOutcome, error)
	// Claim claims one queued job (oldest first) under a fresh lease. claimed
	// is false when the queue is empty. Concurrent claimers never receive the
	// same job.
	Claim(ctx context.Context, in ClaimInput) (claimed Claimed, ok bool, err error)
	// Heartbeat extends the caller's live lease. ErrLeaseLost when the job
	// left running or the lease changed hands.
	Heartbeat(ctx context.Context, jobID, owner string, lease time.Duration, now time.Time) error
	// CommitSuccess transitions running→completed with the stored result in
	// one transaction, conditional on the caller still owning the lease. won
	// is false when a racing transition (cancel, recovery) won instead.
	CommitSuccess(ctx context.Context, in SuccessInput) (won bool, err error)
	// CommitFailure transitions running→failed with the stored failure result
	// under the same lease-conditional discipline as CommitSuccess.
	CommitFailure(ctx context.Context, in FailureInput) (won bool, err error)
	// Requeue returns a running job to queued when the caller still owns the
	// lease (infra abort, retry, or shutdown). opts.BumpAttempt records a
	// consumed attempt; opts.NoClaimBefore delays the job's next
	// claimability (retry backoff — zero makes it immediately visible), so a
	// repeatedly failing job cannot head-of-line-block the queue.
	Requeue(ctx context.Context, jobID, owner string, opts RequeueOptions, now time.Time) (won bool, err error)
	// RecoverExpiredLeases returns running jobs whose lease lapsed to the
	// queue (counting the lost attempt) or fails those that exhausted their
	// attempts, storing FailureResult for the failed ones.
	RecoverExpiredLeases(ctx context.Context, in RecoverInput) (RecoverOutcome, error)
	// ExpireDueResults transitions terminal jobs whose result TTL passed to
	// expired and drops their results. Returns the number of jobs expired.
	ExpireDueResults(ctx context.Context, now time.Time) (int, error)
	// QueueDepth reports the number of queued jobs (operational metric).
	QueueDepth(ctx context.Context) (int, error)
	// Ready reports store health for /readyz.
	Ready(ctx context.Context) error
}

// ClaimInput is one claim attempt.
type ClaimInput struct {
	Owner string
	Lease time.Duration
	Now   time.Time
}

// RequeueOptions carries the requeue accounting: whether the aborted
// execution consumes an attempt, and when the job becomes claimable again
// (retry backoff). A zero NoClaimBefore re-queues the job immediately.
type RequeueOptions struct {
	BumpAttempt   bool
	NoClaimBefore time.Time
}

// Claimed is a claimed job plus its persisted request snapshot.
type Claimed struct {
	Job     Job
	Request json.RawMessage
}

// SuccessInput commits a completed execution.
type SuccessInput struct {
	JobID           string
	Owner           string
	FinalRequestID  string
	Response        json.RawMessage
	Usage           *Usage
	ResultBytes     int
	ResultExpiresAt time.Time
	BumpAttempt     bool
	Now             time.Time
}

// FailureInput commits a failed execution. Response carries the public
// failure envelope; ErrorClass is the content-free classification.
type FailureInput struct {
	JobID           string
	Owner           string
	FinalRequestID  string
	ErrorClass      string
	Response        json.RawMessage
	ResultBytes     int
	ResultExpiresAt time.Time
	BumpAttempt     bool
	Now             time.Time
}

// RecoverInput drives the lease-recovery sweep. FailureResult is stored for
// jobs the sweep terminal-fails (attempts exhausted).
type RecoverInput struct {
	MaxAttempts   int
	FailureResult func(jobID string) Result
	Now           time.Time
}

// RecoverOutcome reports which jobs the sweep moved. Failed jobs are returned
// so the caller can write their single terminal audit record.
type RecoverOutcome struct {
	Requeued []string
	Failed   []Job
}
