package pg

// Env-gated tests for the PostgreSQL async job store (internal/async.Store
// contract on the real schema): idempotent creation, SKIP LOCKED multi-worker
// claims, lease heartbeat/recovery, the cancel-versus-commit single-winner
// race, payload round trips, result expiry, and outage classification. Run
// with TEST_DATABASE_URL set; the tests skip otherwise and are serialized
// against the other schema-driving suites with the same advisory lock.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	pgx5 "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"

	"github.com/knowledge-base/knowledge-base-gateway/internal/async"
	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
)

// ensureAsyncSchema brings the shared TEST_DATABASE_URL database to the
// latest migration version without dropping anything (other suites may hold
// live rows). Serialized with the same advisory lock as the other tests.
func ensureAsyncSchema(t *testing.T, dsn string) {
	t.Helper()
	lockTestDatabase(t, dsn)
	mdb, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("migrate connect: %v", err)
	}
	defer mdb.Close()
	driver, err := pgx5.WithInstance(mdb, &pgx5.Config{})
	if err != nil {
		t.Fatalf("migrate driver: %v", err)
	}
	m, err := migrate.NewWithDatabaseInstance(migrationsDirURL(t), "pgx5", driver)
	if err != nil {
		t.Fatalf("migrate instance: %v", err)
	}
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("migrate up: %v", err)
	}
}

// cleanAsyncTables removes all V1.4 async rows so tests start from an empty
// queue regardless of shared-database state.
func cleanAsyncTables(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, stmt := range []string{
		`DELETE FROM idempotency_keys`,
		`DELETE FROM async_job_results`,
		`DELETE FROM async_job_requests`,
		`DELETE FROM async_jobs`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("clean async tables: %v", err)
		}
	}
}

func newAsyncTestStore(t *testing.T) (*AsyncStore, *sql.DB) {
	t.Helper()
	dsn := testDatabaseURL(t)
	ensureAsyncSchema(t, dsn)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// Defensive seeds: the fixtures other tests rely on may have been
	// removed by their cleanups.
	for _, stmt := range []string{
		`INSERT INTO tenants (id, name) VALUES ('tenant_default', 'Default Tenant') ON CONFLICT (id) DO NOTHING`,
		`INSERT INTO subjects (id, tenant_id) VALUES ('subject_default', 'tenant_default') ON CONFLICT (id) DO NOTHING`,
		`UPDATE model_catalog SET enabled = true WHERE public_name = 'gateway-echo'`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("seed: %v: %v", stmt, err)
		}
	}
	cleanAsyncTables(t, db)
	t.Cleanup(func() { cleanAsyncTables(t, db) })

	ctx := context.Background()
	pool, err := Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pool connect: %v", err)
	}
	t.Cleanup(pool.Close)
	return &AsyncStore{DB: pool}, db
}

func testDatabaseURL(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping PostgreSQL async store test")
	}
	return dsn
}

// asyncTestRequest builds a representative admitted request.
func asyncTestRequest(text string) model.Request {
	maxTokens := 64
	return model.Request{
		PublicModel: "gateway-echo",
		Input:       []model.InputItem{{Role: model.RoleUser, Text: text}},
		MaxTokens:   &maxTokens,
	}
}

func asyncCreateInput(jobID string, now time.Time) async.CreateInput {
	snap, err := async.EncodeSnapshot(asyncTestRequest("hello"))
	if err != nil {
		panic(err)
	}
	return async.CreateInput{
		JobID: jobID, SubjectID: "subject_default", TenantID: "tenant_default",
		Protocol: "responses", PublicModel: "gateway-echo",
		RequestDigest: async.DigestRequest(snap),
		Request:       snap,
		ResultTTL:     time.Hour, KeyTTL: time.Hour, Now: now,
	}
}

func asyncCreateInputWithKey(jobID, keyHash string, now time.Time) async.CreateInput {
	in := asyncCreateInput(jobID, now)
	in.KeyHash = keyHash
	return in
}

func TestAsyncStoreCreateIdempotency(t *testing.T) {
	s, _ := newAsyncTestStore(t)
	ctx := context.Background()
	now := time.Now()

	out, err := s.Create(ctx, asyncCreateInputWithKey("job-1", "hash-1", now))
	if err != nil || out.Replay || out.Job.Status != async.StatusQueued {
		t.Fatalf("first create = %+v err=%v", out, err)
	}
	replay, err := s.Create(ctx, asyncCreateInputWithKey("job-2", "hash-1", now))
	if err != nil || !replay.Replay || replay.Job.ID != "job-1" {
		t.Fatalf("replay = %+v err=%v", replay, err)
	}
	depth, err := s.QueueDepth(ctx)
	if err != nil || depth != 1 {
		t.Fatalf("depth = %d err=%v (duplicate creation must collapse)", depth, err)
	}
	// Different digest under the same key: conflict.
	conflict := asyncCreateInputWithKey("job-3", "hash-1", now)
	otherSnap, err := async.EncodeSnapshot(asyncTestRequest("different"))
	if err != nil {
		t.Fatal(err)
	}
	conflict.RequestDigest = async.DigestRequest(otherSnap)
	if _, err := s.Create(ctx, conflict); !errors.Is(err, async.ErrConflict) {
		t.Fatalf("conflict err = %v", err)
	}
	// The job's payload round trips through JSONB semantically identically
	// (JSONB normalizes formatting, so bytes may differ; the decoded request
	// may not).
	cl, ok, err := s.Claim(ctx, async.ClaimInput{Owner: "w", Lease: time.Minute, Now: now})
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	want, err := async.DecodeSnapshot(asyncCreateInput("job-1", now).Request)
	if err != nil {
		t.Fatal(err)
	}
	got, err := async.DecodeSnapshot(cl.Request)
	if err != nil {
		t.Fatal(err)
	}
	reencodedGot, err := async.EncodeSnapshot(got)
	if err != nil {
		t.Fatal(err)
	}
	reencodedWant, err := async.EncodeSnapshot(want)
	if err != nil {
		t.Fatal(err)
	}
	if string(reencodedGot) != string(reencodedWant) {
		t.Fatal("stored payload must round trip semantically identically")
	}
	if cl.Job.LeaseOwner != "w" || cl.Job.Status != async.StatusRunning {
		t.Fatalf("claimed job = %+v", cl.Job)
	}
	if err := s.Heartbeat(ctx, cl.Job.ID, "w", time.Minute, now); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
}

// TestAsyncStoreCreateSameKeyRace pins the concurrent-creation discipline on
// the (subject, key_hash) mapping: N barrier-started creators collide on one
// fresh key, and every outcome must be a real result — a fresh job, a replay
// of the one winner's job, or (unreachable with identical digests, kept for
// the contract) a conflict. A zero outcome with nil error was a silent fake
// success: the retry loop's err==nil shortcut returned the retryable result
// of a lost mapping insert instead of re-reading the winner. Exactly one
// fresh job row may land.
func TestAsyncStoreCreateSameKeyRace(t *testing.T) {
	s, db := newAsyncTestStore(t)
	ctx := context.Background()
	now := time.Now()

	const racers = 16
	type raceResult struct {
		racer int
		jobID string
		out   async.CreateOutcome
		err   error
	}
	results := make(chan raceResult, racers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			jobID := fmt.Sprintf("job-race-%d", i)
			out, err := s.Create(ctx, asyncCreateInputWithKey(jobID, "hash-race", now))
			results <- raceResult{racer: i, jobID: jobID, out: out, err: err}
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)

	fresh, replay, conflict := 0, 0, 0
	for r := range results {
		switch {
		case errors.Is(r.err, async.ErrConflict):
			conflict++ // same digest, so not expected — but a legal outcome
		case r.err != nil:
			t.Fatalf("racer %d: unexpected error %v", r.racer, r.err)
		case r.out.Job.ID == "" && !r.out.Replay:
			t.Fatalf("racer %d: zero outcome with nil error (silent fake success)", r.racer)
		case r.out.Replay:
			replay++
			if r.out.Job.ID == "" {
				t.Fatalf("racer %d: replay without a job id", r.racer)
			}
		default:
			fresh++
			if r.out.Job.ID != r.jobID {
				t.Fatalf("racer %d: fresh job id = %q, want own %q", r.racer, r.out.Job.ID, r.jobID)
			}
		}
	}
	if fresh != 1 {
		t.Fatalf("fresh jobs = %d (replays=%d conflicts=%d), want exactly one winner", fresh, replay, conflict)
	}

	// Exactly one job row and one mapping landed, and every replay points at
	// the winner.
	var jobs int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM async_jobs WHERE job_id LIKE 'job-race-%'`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 {
		t.Fatalf("job rows = %d, want 1 (duplicate creation must collapse)", jobs)
	}
	var mappedJob string
	if err := db.QueryRowContext(ctx,
		`SELECT job_id FROM idempotency_keys WHERE key_hash = 'hash-race'`).Scan(&mappedJob); err != nil {
		t.Fatal(err)
	}
	var storedJobs []string
	rows, err := db.QueryContext(ctx, `SELECT job_id FROM async_jobs WHERE job_id LIKE 'job-race-%'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		storedJobs = append(storedJobs, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(storedJobs) != 1 || storedJobs[0] != mappedJob {
		t.Fatalf("mapping job %q does not match the single stored job %v", mappedJob, storedJobs)
	}
	// The mapping is the live one (KeyTTL 1h): a replay must still find it.
	replayOut, err := s.Create(ctx, asyncCreateInputWithKey("job-race-after", "hash-race", now))
	if err != nil || !replayOut.Replay || replayOut.Job.ID != mappedJob {
		t.Fatalf("post-race replay = %+v err=%v (want %q)", replayOut, err, mappedJob)
	}
}

func TestAsyncStoreLifecycleAndExpiry(t *testing.T) {
	s, _ := newAsyncTestStore(t)
	ctx := context.Background()
	now := time.Now()

	if _, err := s.Create(ctx, asyncCreateInput("job-lc", now)); err != nil {
		t.Fatal(err)
	}
	cl, ok, err := s.Claim(ctx, async.ClaimInput{Owner: "w", Lease: time.Minute, Now: now})
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	usage := &async.Usage{PromptTokens: int64Ptr(10), CompletionTokens: int64Ptr(11), TotalTokens: int64Ptr(21)}
	won, err := s.CommitSuccess(ctx, async.SuccessInput{
		JobID: cl.Job.ID, Owner: "w", FinalRequestID: "req-lc",
		Response: json.RawMessage(`{"id":"resp_lc","object":"response","status":"completed"}`),
		Usage:    usage, ResultBytes: 64,
		ResultExpiresAt: now.Add(time.Hour), BumpAttempt: true, Now: now,
	})
	if err != nil || !won {
		t.Fatalf("commit: won=%v err=%v", won, err)
	}
	job, err := s.Get(ctx, cl.Job.ID, now)
	if err != nil || job.Status != async.StatusCompleted || job.FinalRequestID != "req-lc" {
		t.Fatalf("job = %+v err=%v", job, err)
	}
	res, err := s.Result(ctx, cl.Job.ID)
	if err != nil || res.ErrorClass != "" || res.Usage == nil || *res.Usage.TotalTokens != 21 {
		t.Fatalf("result = %+v err=%v", res, err)
	}
	// Attempt accounting and single live lease: the lease is cleared on the
	// terminal commit, so heartbeats lose.
	if err := s.Heartbeat(ctx, cl.Job.ID, "w", time.Minute, now); !errors.Is(err, async.ErrLeaseLost) {
		t.Fatalf("post-commit heartbeat = %v, want ErrLeaseLost", err)
	}
	// Lazy expiry: past the TTL, Get transitions to expired and drops the
	// result; there is no execution path behind a query.
	job, err = s.Get(ctx, cl.Job.ID, now.Add(2*time.Hour))
	if err != nil || job.Status != async.StatusExpired {
		t.Fatalf("expired job = %+v err=%v", job, err)
	}
	if _, err := s.Result(ctx, cl.Job.ID); !errors.Is(err, async.ErrNotFound) {
		t.Fatalf("expired result err = %v", err)
	}
}

func TestAsyncStoreCancelRacesCommit(t *testing.T) {
	s, _ := newAsyncTestStore(t)
	ctx := context.Background()
	base := time.Now()

	for i := 0; i < 40; i++ {
		now := base.Add(time.Duration(i) * time.Minute)
		if _, err := s.Create(ctx, asyncCreateInput("job-race-"+string(rune('a'+i%26))+string(rune('a'+i/26)), now)); err != nil {
			t.Fatal(err)
		}
		cl, ok, err := s.Claim(ctx, async.ClaimInput{Owner: "w", Lease: time.Minute, Now: now})
		if err != nil || !ok {
			t.Fatalf("claim: ok=%v err=%v", ok, err)
		}
		var wg sync.WaitGroup
		var commitWon, cancelWon bool
		wg.Add(2)
		go func() {
			defer wg.Done()
			won, err := s.CommitSuccess(ctx, async.SuccessInput{
				JobID: cl.Job.ID, Owner: cl.Job.LeaseOwner, FinalRequestID: "req-race",
				Response: json.RawMessage(`{}`), ResultBytes: 2,
				ResultExpiresAt: now.Add(time.Hour), BumpAttempt: true, Now: now,
			})
			if err != nil {
				t.Errorf("commit: %v", err)
				return
			}
			commitWon = won
		}()
		go func() {
			defer wg.Done()
			_, outcome, err := s.Cancel(ctx, cl.Job.ID, now)
			if err != nil {
				t.Errorf("cancel: %v", err)
				return
			}
			cancelWon = outcome == async.CancelledRunning
		}()
		wg.Wait()
		if commitWon == cancelWon {
			t.Fatalf("iteration %d: exactly one side must win (commit=%v cancel=%v)", i, commitWon, cancelWon)
		}
		job, err := s.Get(ctx, cl.Job.ID, now)
		if err != nil {
			t.Fatal(err)
		}
		if commitWon {
			if job.Status != async.StatusCompleted {
				t.Fatalf("iteration %d: status = %s after winning commit", i, job.Status)
			}
		} else if job.Status != async.StatusCancelled {
			t.Fatalf("iteration %d: status = %s after winning cancel", i, job.Status)
		}
	}
}

func TestAsyncStoreMultiWorkerClaims(t *testing.T) {
	s, _ := newAsyncTestStore(t)
	ctx := context.Background()
	base := time.Now()
	const jobs, workers = 8, 12
	for i := 0; i < jobs; i++ {
		if _, err := s.Create(ctx, asyncCreateInput("job-mw-"+string(rune('a'+i)),
			base.Add(time.Duration(i)*time.Millisecond))); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	claimed := map[string]bool{}
	var wg sync.WaitGroup
	// Claim after the staggered creation timestamps (which double as the
	// visible_at values): claims run in the caller's clock domain, so the
	// claim time must be past every job's creation.
	claimAt := base.Add(time.Minute)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			owner := "worker-" + string(rune('a'+n))
			for {
				cl, ok, err := s.Claim(ctx, async.ClaimInput{Owner: owner, Lease: time.Minute, Now: claimAt})
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				if !ok {
					return
				}
				mu.Lock()
				if claimed[cl.Job.ID] {
					t.Errorf("job %s claimed twice", cl.Job.ID)
				}
				claimed[cl.Job.ID] = true
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	if len(claimed) != jobs {
		t.Fatalf("claimed %d distinct jobs, want %d", len(claimed), jobs)
	}
}

func TestAsyncStoreRecoveryAndRequeue(t *testing.T) {
	s, _ := newAsyncTestStore(t)
	ctx := context.Background()
	now := time.Now()

	if _, err := s.Create(ctx, asyncCreateInput("job-rec", now)); err != nil {
		t.Fatal(err)
	}
	// Worker claims and dies; the lease lapses.
	if _, ok, err := s.Claim(ctx, async.ClaimInput{Owner: "dead", Lease: time.Second, Now: now}); err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	// With attempts remaining, recovery requeues and counts the lost attempt.
	out, err := s.RecoverExpiredLeases(ctx, async.RecoverInput{
		MaxAttempts: 3, FailureResult: func(string) async.Result {
			return async.Result{ErrorClass: "lease_lost", Response: json.RawMessage(`{}`)}
		},
		Now: now.Add(2 * time.Second),
	})
	if err != nil || len(out.Requeued) != 1 || len(out.Failed) != 0 {
		t.Fatalf("recovery = %+v err=%v", out, err)
	}
	job, err := s.Get(ctx, "job-rec", now)
	if err != nil || job.Status != async.StatusQueued || job.AttemptCount != 1 {
		t.Fatalf("recovered job = %+v err=%v", job, err)
	}
	// Exhaust the attempts across recovery rounds: terminal failure with the
	// stored failure result. Each round's claim and recovery run at advancing
	// times (recovery makes the job visible from its own timestamp onward).
	for round := 0; round < 2; round++ {
		at := now.Add(time.Duration(10+round*10) * time.Minute)
		if _, ok, err := s.Claim(ctx, async.ClaimInput{Owner: "w", Lease: time.Second, Now: at}); err != nil || !ok {
			t.Fatalf("round %d claim: ok=%v err=%v", round, ok, err)
		}
		out, err = s.RecoverExpiredLeases(ctx, async.RecoverInput{
			MaxAttempts: 3, FailureResult: func(string) async.Result {
				return async.Result{ErrorClass: "lease_lost", Response: json.RawMessage(`{}`)}
			},
			Now: at.Add(2 * time.Second), // strictly past the 1s lease
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(out.Requeued) != 0 || len(out.Failed) != 1 {
		t.Fatalf("final recovery = %+v", out)
	}
	job, _ = s.Get(ctx, "job-rec", now)
	if job.Status != async.StatusFailed || job.AttemptCount != 3 {
		t.Fatalf("exhausted job = %+v", job)
	}
	res, err := s.Result(ctx, "job-rec")
	if err != nil || res.ErrorClass != "lease_lost" {
		t.Fatalf("lease-lost result = %+v err=%v", res, err)
	}
}

// TestAsyncStoreResultExpirySweep pins the worker-sweep reclamation path for
// expired results on the real schema: terminal jobs past their result TTL
// transition to expired with their result rows dropped, jobs still inside
// their TTL keep both, and — the regression this test pins — the sweep
// statement itself executes (an earlier revision passed an unused $1
// parameter, which PostgreSQL rejects with SQLSTATE 42P18 on every sweep
// pass, so results could never be reclaimed by the sweep in production).
func TestAsyncStoreResultExpirySweep(t *testing.T) {
	s, _ := newAsyncTestStore(t)
	ctx := context.Background()
	now := time.Now()

	// A completed job whose result TTL already passed, and one still inside.
	due := asyncCreateInput("job-exp-due", now.Add(-2*time.Hour))
	if _, err := s.Create(ctx, due); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.Claim(ctx, async.ClaimInput{Owner: "w", Lease: time.Minute, Now: due.Now}); err != nil || !ok {
		t.Fatalf("claim due: ok=%v err=%v", ok, err)
	}
	won, err := s.CommitSuccess(ctx, async.SuccessInput{
		JobID: "job-exp-due", Owner: "w", FinalRequestID: "req-exp-due",
		Response: json.RawMessage(`{"id":"job-exp-due"}`), ResultBytes: 18,
		ResultExpiresAt: now.Add(-time.Minute), // already past
		BumpAttempt:     true, Now: now,
	})
	if err != nil || !won {
		t.Fatalf("commit due: won=%v err=%v", won, err)
	}

	live := asyncCreateInput("job-exp-live", now.Add(-2*time.Hour))
	if _, err := s.Create(ctx, live); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.Claim(ctx, async.ClaimInput{Owner: "w2", Lease: time.Minute, Now: live.Now}); err != nil || !ok {
		t.Fatalf("claim live: ok=%v err=%v", ok, err)
	}
	won, err = s.CommitSuccess(ctx, async.SuccessInput{
		JobID: "job-exp-live", Owner: "w2", FinalRequestID: "req-exp-live",
		Response: json.RawMessage(`{"id":"job-exp-live"}`), ResultBytes: 19,
		ResultExpiresAt: now.Add(time.Hour), // still inside
		BumpAttempt:     true, Now: now,
	})
	if err != nil || !won {
		t.Fatalf("commit live: won=%v err=%v", won, err)
	}

	// A queued job is never a result-expiry candidate even with a past TTL
	// column value (it has no terminal status).
	if _, err := s.Create(ctx, asyncCreateInput("job-exp-queued", now)); err != nil {
		t.Fatal(err)
	}

	n, err := s.ExpireDueResults(ctx, now)
	if err != nil {
		t.Fatalf("expire sweep: %v", err) // must not fail (the 42P18 regression)
	}
	if n != 1 {
		t.Fatalf("expired = %d, want 1", n)
	}
	dueJob, err := s.Get(ctx, "job-exp-due", now)
	if err != nil || dueJob.Status != async.StatusExpired {
		t.Fatalf("due job = %+v err=%v, want expired", dueJob, err)
	}
	if _, err := s.Result(ctx, "job-exp-due"); !errors.Is(err, async.ErrNotFound) {
		t.Fatalf("due result = %v, want ErrNotFound (dropped)", err)
	}
	liveJob, err := s.Get(ctx, "job-exp-live", now)
	if err != nil || liveJob.Status != async.StatusCompleted {
		t.Fatalf("live job = %+v err=%v, want still completed", liveJob, err)
	}
	if _, err := s.Result(ctx, "job-exp-live"); err != nil {
		t.Fatalf("live result must survive: %v", err)
	}
	// The sweep is idempotent: the second pass expires nothing.
	if n, err := s.ExpireDueResults(ctx, now); err != nil || n != 0 {
		t.Fatalf("second sweep = %d err=%v, want 0", n, err)
	}
}

func TestAsyncStoreRequeueAndCancelQueued(t *testing.T) {
	s, _ := newAsyncTestStore(t)
	ctx := context.Background()
	now := time.Now()

	if _, err := s.Create(ctx, asyncCreateInput("job-rq", now)); err != nil {
		t.Fatal(err)
	}
	cl, ok, err := s.Claim(ctx, async.ClaimInput{Owner: "w", Lease: time.Minute, Now: now})
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	// Shutdown abort: requeue under the live lease without spending an
	// attempt, then a fresh claimer takes the job.
	if won, err := s.Requeue(ctx, cl.Job.ID, "w", async.RequeueOptions{}, now); err != nil || !won {
		t.Fatalf("requeue: won=%v err=%v", won, err)
	}
	job, _ := s.Get(ctx, "job-rq", now)
	if job.Status != async.StatusQueued || job.AttemptCount != 0 {
		t.Fatalf("requeued job = %+v", job)
	}
	// Retry backoff: a requeue with a future visible_at hides the job from
	// claiming on the real schema until the time passes.
	if _, ok, err := s.Claim(ctx, async.ClaimInput{Owner: "w2", Lease: time.Minute, Now: now}); err != nil || !ok {
		t.Fatalf("reclaim: ok=%v err=%v", ok, err)
	}
	visibleAt := now.Add(time.Hour)
	if won, err := s.Requeue(ctx, cl.Job.ID, "w2", async.RequeueOptions{BumpAttempt: true, NoClaimBefore: visibleAt}, now); err != nil || !won {
		t.Fatalf("backoff requeue: won=%v err=%v", won, err)
	}
	if _, ok, err := s.Claim(ctx, async.ClaimInput{Owner: "w3", Lease: time.Minute, Now: now}); err != nil || ok {
		t.Fatalf("backed-off job must not be claimable: ok=%v err=%v", ok, err)
	}
	if cl3, ok, err := s.Claim(ctx, async.ClaimInput{Owner: "w3", Lease: time.Minute, Now: visibleAt}); err != nil || !ok || cl3.Job.AttemptCount != 1 {
		t.Fatalf("post-backoff claim: ok=%v job=%+v err=%v", ok, cl3.Job, err)
	}
	// Hand the lease back without an attempt (abort semantics) so the job is
	// queued again for the cancel checks.
	if won, err := s.Requeue(ctx, cl.Job.ID, "w3", async.RequeueOptions{}, visibleAt); err != nil || !won {
		t.Fatalf("abort requeue: won=%v err=%v", won, err)
	}
	// Cancel from queued, then idempotent repeat.
	if _, outcome, err := s.Cancel(ctx, "job-rq", now); err != nil || outcome != async.CancelledQueued {
		t.Fatalf("queued cancel outcome=%v err=%v", outcome, err)
	}
	job, outcome, err := s.Cancel(ctx, "job-rq", now)
	if err != nil || outcome != async.CancelNoop || job.Status != async.StatusCancelled {
		t.Fatalf("repeat cancel = %+v outcome=%v err=%v", job, outcome, err)
	}
	// Terminal: no further commits.
	if _, ok, err := s.Claim(ctx, async.ClaimInput{Owner: "w2", Lease: time.Minute, Now: now}); err != nil || ok {
		t.Fatalf("cancelled job must not be claimable: ok=%v err=%v", ok, err)
	}
}

func TestAsyncStoreUnavailable(t *testing.T) {
	dsn := testDatabaseURL(t)
	ensureAsyncSchema(t, dsn)
	ctx := context.Background()
	pool, err := Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	s := &AsyncStore{DB: pool}
	pool.Close() // simulate an outage: the pool can no longer serve queries

	in := asyncCreateInput("job-out", time.Now())
	if _, err := s.Create(ctx, in); !errors.Is(err, async.ErrUnavailable) {
		t.Fatalf("create outage err = %v, want ErrUnavailable", err)
	}
	if _, err := s.Get(ctx, "job-out", time.Now()); !errors.Is(err, async.ErrUnavailable) {
		t.Fatalf("get outage err = %v, want ErrUnavailable", err)
	}
	if _, _, err := s.Cancel(ctx, "job-out", time.Now()); !errors.Is(err, async.ErrUnavailable) {
		t.Fatalf("cancel outage err = %v, want ErrUnavailable", err)
	}
	if _, _, err := s.Claim(ctx, async.ClaimInput{Owner: "w", Lease: time.Minute, Now: time.Now()}); !errors.Is(err, async.ErrUnavailable) {
		t.Fatalf("claim outage err = %v, want ErrUnavailable", err)
	}
}

// TestAsyncTraceContextRoundTrip proves the observability contract on the
// real schema: the normalized W3C trace context (trace ID, parent span ID,
// sampled bit) survives the create/claim round trip so the worker joins the
// caller's trace, and a malformed pair persists as empty (normalization
// happens at the store boundary).
func TestAsyncTraceContextRoundTrip(t *testing.T) {
	s, db := newAsyncTestStore(t)
	ctx := context.Background()
	now := time.Now()

	in := asyncCreateInput("job-trace-ok", now)
	in.TraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	in.SpanID = "00f067aa0ba902b7"
	in.TraceSampled = true
	out, err := s.Create(ctx, in)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	job, err := s.Get(ctx, out.Job.ID, now)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if job.TraceID != in.TraceID || job.ParentSpanID != in.SpanID || !job.TraceSampled {
		t.Fatalf("trace context not persisted: %+v", job)
	}

	// Claim returns the same identity: the worker's linkage input.
	claimed, ok, err := s.Claim(ctx, async.ClaimInput{Owner: "w-trace", Lease: time.Minute, Now: now})
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if claimed.Job.TraceID != in.TraceID || claimed.Job.ParentSpanID != in.SpanID || !claimed.Job.TraceSampled {
		t.Fatalf("claimed job lost its trace context: %+v", claimed.Job)
	}

	// A malformed pair is normalized to empty at the store boundary.
	bad := asyncCreateInput("job-trace-bad", now)
	bad.TraceID = "4BF92F3577B34DA6A3CE929D0E0E4736" // uppercase: rejected
	bad.SpanID = "'; DROP TABLE async_jobs; --"      // not hex: rejected
	if _, err := db.Exec(`DELETE FROM async_jobs WHERE job_id = 'job-trace-bad'`); err != nil {
		t.Fatalf("pre-clean: %v", err)
	}
	out, err = s.Create(ctx, bad)
	if err != nil {
		t.Fatalf("create bad: %v", err)
	}
	job, err = s.Get(ctx, out.Job.ID, now)
	if err != nil {
		t.Fatalf("get bad: %v", err)
	}
	if job.TraceID != "" || job.ParentSpanID != "" || job.TraceSampled {
		t.Fatalf("malformed trace context must persist as empty: %+v", job)
	}
}

// TestAsyncQueueOldestAge proves the scrape-time queue-age signal on the
// real schema: 0 on an empty queue, positive for a queued job, and
// unchanged by a terminal job's presence.
func TestAsyncQueueOldestAge(t *testing.T) {
	s, db := newAsyncTestStore(t)
	ctx := context.Background()
	now := time.Now()

	if age, err := s.QueueOldestAge(ctx); err != nil || age != 0 {
		t.Fatalf("empty queue age = %v err=%v, want 0", age, err)
	}

	out, err := s.Create(ctx, asyncCreateInput("job-age", now.Add(-5*time.Minute)))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(`UPDATE async_jobs SET created_at = $1 WHERE job_id = $2`,
		now.Add(-5*time.Minute), out.Job.ID); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	age, err := s.QueueOldestAge(ctx)
	if err != nil {
		t.Fatalf("queue age: %v", err)
	}
	if age < 4*time.Minute || age > 6*time.Minute {
		t.Fatalf("queue age = %v, want about 5m", age)
	}

	// Depth and age come from the same queued set: claim it and both drop.
	if _, ok, err := s.Claim(ctx, async.ClaimInput{Owner: "w-age", Lease: time.Minute, Now: now}); err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if depth, err := s.QueueDepth(ctx); err != nil || depth != 0 {
		t.Fatalf("depth after claim = %d err=%v, want 0", depth, err)
	}
	if age, err := s.QueueOldestAge(ctx); err != nil || age != 0 {
		t.Fatalf("queue age after claim = %v err=%v, want 0", age, err)
	}
}
