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
