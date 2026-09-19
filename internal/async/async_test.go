package async

// Store-semantics tests over the in-memory implementation, whose
// conditional-update discipline mirrors the PostgreSQL store: state-machine
// closure, claim exclusivity, idempotency replay/conflict, lease lifecycle,
// cancel-vs-commit single-winner arbitration, recovery, and result expiry.
// The wire/worker layers are exercised in internal/httpapi; the real SQL
// store in internal/store/pg (env-gated).

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
)

// testDomainRequest builds a representative admitted request: text input,
// an output ceiling, a structured-output spec, and tool declarations.
func testDomainRequest(text string) model.Request {
	maxTokens := 64
	temp := 0.5
	return model.Request{
		PublicModel: "model-x", Instructions: "be brief",
		Input:       []model.InputItem{{Role: model.RoleUser, Text: text}},
		Temperature: &temp, MaxTokens: &maxTokens,
		Tools:      []model.ToolDefinition{{Name: "lookup", Parameters: json.RawMessage(`{"type":"object"}`)}},
		ToolChoice: "auto",
		ResponseSpec: &model.ResponseSpec{
			Mode: model.ModeJSONSchema, Name: "answer", Schema: json.RawMessage(`{"type":"object"}`), Strict: true,
		},
		Metadata: json.RawMessage(`{"k":"v"}`),
	}
}

func createInput(jobID, keyHash string) CreateInput {
	snap, err := EncodeSnapshot(testDomainRequest("hello"))
	if err != nil {
		panic(err) // unreachable for the fixed fixture request
	}
	return CreateInput{
		JobID: jobID, SubjectID: "subject-a", TenantID: "tenant-a",
		Protocol: "responses", PublicModel: "model-x",
		RequestDigest: DigestRequest(snap),
		Request:       snap,
		KeyHash:       keyHash, ResultTTL: time.Hour, KeyTTL: time.Hour,
		Now: time.Now(),
	}
}

func TestSnapshotRoundTripAndDigest(t *testing.T) {
	req := testDomainRequest("hello")
	snap, err := EncodeSnapshot(req)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	again, err := EncodeSnapshot(got)
	if err != nil {
		t.Fatal(err)
	}
	if string(snap) != string(again) {
		t.Fatalf("snapshot round trip not stable:\n%s\n%s", snap, again)
	}
	if got.PublicModel != req.PublicModel || got.Instructions != req.Instructions ||
		got.ToolChoice != req.ToolChoice || len(got.Input) != len(req.Input) {
		t.Fatalf("round trip lost fields: %+v vs %+v", got, req)
	}
	if got.Input[0].Role != "user" || got.Input[0].Text != "hello" {
		t.Fatalf("input item wrong: %+v", got.Input[0])
	}
	if got.MaxTokens == nil || *got.MaxTokens != 64 {
		t.Fatalf("max tokens wrong: %+v", got.MaxTokens)
	}
	if got.ResponseSpec == nil || got.ResponseSpec.Mode != model.ModeJSONSchema || got.ResponseSpec.Name != "answer" || !got.ResponseSpec.Strict {
		t.Fatalf("response spec wrong: %+v", got.ResponseSpec)
	}
	if len(got.Tools) != 1 || got.Tools[0].Name != "lookup" {
		t.Fatalf("tools wrong: %+v", got.Tools)
	}
	if string(got.Metadata) != `{"k":"v"}` {
		t.Fatalf("metadata wrong: %s", got.Metadata)
	}
	// The digest is stable for identical requests and changes with content.
	if DigestRequest(snap) != DigestRequest(again) {
		t.Fatal("digest must be stable for identical snapshots")
	}
	other, err := EncodeSnapshot(testDomainRequest("different"))
	if err != nil {
		t.Fatal(err)
	}
	if DigestRequest(snap) == DigestRequest(other) {
		t.Fatal("different requests must produce different digests")
	}
}

func TestIdempotencyReplayAndConflict(t *testing.T) {
	s := NewMemoryStore(nil)
	ctx := context.Background()
	in := createInput("job-1", "key-hash-1")
	out, err := s.Create(ctx, in)
	if err != nil || out.Replay {
		t.Fatalf("first create: out=%+v err=%v", out, err)
	}
	replay, err := s.Create(ctx, createInput("job-2", "key-hash-1"))
	if err != nil || !replay.Replay {
		t.Fatalf("same key+digest must replay, got out=%+v err=%v", replay, err)
	}
	if replay.Job.ID != "job-1" {
		t.Fatalf("replay must return the original job, got %s", replay.Job.ID)
	}
	// The same key with a different request digest is a conflict.
	conflict := createInput("job-3", "key-hash-1")
	otherSnap, err := EncodeSnapshot(testDomainRequest("other"))
	if err != nil {
		t.Fatal(err)
	}
	conflict.RequestDigest = DigestRequest(otherSnap)
	if _, err := s.Create(ctx, conflict); !errors.Is(err, ErrConflict) {
		t.Fatalf("different digest must conflict, got %v", err)
	}
	// The same key string from another subject maps independently.
	other := createInput("job-4", "key-hash-1")
	other.SubjectID = "subject-b"
	if _, err := s.Create(ctx, other); err != nil {
		t.Fatalf("cross-subject key must be independent: %v", err)
	}
	// No key: every create is fresh.
	if _, err := s.Create(ctx, createInput("job-5", "")); err != nil {
		t.Fatalf("key-less create: %v", err)
	}
}

func TestClaimExclusivity(t *testing.T) {
	s := NewMemoryStore(nil)
	ctx := context.Background()
	const jobs, workers = 5, 20
	for i := 0; i < jobs; i++ {
		if _, err := s.Create(ctx, createInput(jobID("job", i), "")); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	claimed := map[string]bool{}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				cl, ok, err := s.Claim(ctx, ClaimInput{Owner: "w", Lease: time.Minute, Now: time.Now()})
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
		}()
	}
	wg.Wait()
	if len(claimed) != jobs {
		t.Fatalf("claimed %d distinct jobs, want %d", len(claimed), jobs)
	}
}

func jobID(prefix string, i int) string {
	return prefix + "-" + string(rune('a'+i))
}

func TestHeartbeatAndLeaseLoss(t *testing.T) {
	now := time.Now()
	s := NewMemoryStore(func() time.Time { return now })
	ctx := context.Background()
	if _, err := s.Create(ctx, createInput("job-hb", "")); err != nil {
		t.Fatal(err)
	}
	_, ok, err := s.Claim(ctx, ClaimInput{Owner: "w1", Lease: 50 * time.Millisecond, Now: now})
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if err := s.Heartbeat(ctx, "job-hb", "w1", time.Minute, now); err != nil {
		t.Fatalf("live heartbeat: %v", err)
	}
	// A different owner cannot extend someone else's lease.
	if err := s.Heartbeat(ctx, "job-hb", "w2", time.Minute, now); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("foreign heartbeat = %v, want ErrLeaseLost", err)
	}
	// A slow-but-live owner may re-extend before the sweep reclaims the job;
	// lease loss is observable once recovery has moved the job out of
	// running/this-owner (here: straight past the lease, sweep, then beat).
	if err := s.Heartbeat(ctx, "job-hb", "w1", time.Minute, now.Add(2*time.Minute)); err != nil && !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("late heartbeat must either re-extend or lose the lease, got %v", err)
	}
	out, err := s.RecoverExpiredLeases(ctx, RecoverInput{
		MaxAttempts: 3, FailureResult: func(string) Result { return Result{} },
		Now: now.Add(4 * time.Minute),
	})
	if err != nil || len(out.Requeued) != 1 {
		t.Fatalf("recovery = %+v err=%v", out, err)
	}
	// After recovery the job left this owner: any heartbeat loses.
	if err := s.Heartbeat(ctx, "job-hb", "w1", time.Minute, now.Add(4*time.Minute)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("post-recovery heartbeat = %v, want ErrLeaseLost", err)
	}
}

// TestCancelVersusCommitSingleWinner is the store-level arbitration contract
// behind the roadmap rule that cancel/completion races are decided by one
// conditional update and produce exactly one final outcome.
func TestCancelVersusCommitSingleWinner(t *testing.T) {
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		s := NewMemoryStore(nil)
		if _, err := s.Create(ctx, createInput("job-race", "")); err != nil {
			t.Fatal(err)
		}
		cl, ok, err := s.Claim(ctx, ClaimInput{Owner: "w", Lease: time.Minute, Now: time.Now()})
		if err != nil || !ok {
			t.Fatalf("claim: ok=%v err=%v", ok, err)
		}
		var commitWon, cancelWon bool
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			won, err := s.CommitSuccess(ctx, SuccessInput{
				JobID: cl.Job.ID, Owner: cl.Job.LeaseOwner,
				FinalRequestID: "req-x", Response: json.RawMessage(`{}`),
				ResultExpiresAt: time.Now().Add(time.Hour), BumpAttempt: true,
				Now: time.Now(),
			})
			if err != nil {
				t.Errorf("commit: %v", err)
				return
			}
			if won {
				commitWon = true
			}
		}()
		go func() {
			defer wg.Done()
			_, outcome, err := s.Cancel(ctx, cl.Job.ID, time.Now())
			if err != nil {
				t.Errorf("cancel: %v", err)
				return
			}
			if outcome == CancelledRunning {
				cancelWon = true
			}
		}()
		wg.Wait()
		if commitWon == cancelWon {
			t.Fatalf("iteration %d: exactly one side must win (commit=%v cancel=%v)", i, commitWon, cancelWon)
		}
		job, err := s.Get(ctx, "job-race", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		switch job.Status {
		case StatusCompleted:
			if !commitWon {
				t.Fatalf("completed without a winning commit")
			}
			if _, err := s.Result(ctx, "job-race"); err != nil {
				t.Fatalf("completed job must carry its result: %v", err)
			}
		case StatusCancelled:
			if !cancelWon {
				t.Fatalf("cancelled without a winning cancel")
			}
			if _, err := s.Result(ctx, "job-race"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("cancelled job must not carry a result, got %v", err)
			}
		default:
			t.Fatalf("final status must be terminal, got %s", job.Status)
		}
		// Terminal jobs accept no further transitions.
		if won, err := s.CommitSuccess(ctx, SuccessInput{
			JobID: "job-race", Owner: "w", FinalRequestID: "req-y",
			Response: json.RawMessage(`{}`), ResultExpiresAt: time.Now().Add(time.Hour),
			Now: time.Now(),
		}); err != nil || won {
			t.Fatalf("terminal job must not commit again: won=%v err=%v", won, err)
		}
	}
}

func TestRequeueAndRecovery(t *testing.T) {
	now := time.Now()
	s := NewMemoryStore(func() time.Time { return now })
	ctx := context.Background()
	if _, err := s.Create(ctx, createInput("job-q", "")); err != nil {
		t.Fatal(err)
	}
	cl, ok, err := s.Claim(ctx, ClaimInput{Owner: "w", Lease: time.Minute, Now: now})
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	// Requeue under the recorded owner returns the job to the queue.
	if won, err := s.Requeue(ctx, "job-q", cl.Job.LeaseOwner, RequeueOptions{BumpAttempt: true}, now); err != nil || !won {
		t.Fatalf("requeue: won=%v err=%v", won, err)
	}
	job, _ := s.Get(ctx, "job-q", now)
	if job.Status != StatusQueued || job.AttemptCount != 1 {
		t.Fatalf("requeued job = %+v", job)
	}
	// The next claim consumes the queued attempt.
	if _, ok, err := s.Claim(ctx, ClaimInput{Owner: "w2", Lease: time.Minute, Now: now}); err != nil || !ok {
		t.Fatalf("reclaim: ok=%v err=%v", ok, err)
	}
	// Recovery: an expired lease with attempts remaining goes back to queued.
	out, err := s.RecoverExpiredLeases(ctx, RecoverInput{
		MaxAttempts: 3, FailureResult: func(string) Result { return Result{ErrorClass: "lease_lost"} },
		Now: now.Add(2 * time.Minute),
	})
	if err != nil || len(out.Requeued) != 1 || len(out.Failed) != 0 {
		t.Fatalf("recovery = %+v err=%v", out, err)
	}
	job, _ = s.Get(ctx, "job-q", now)
	if job.Status != StatusQueued || job.AttemptCount != 2 {
		t.Fatalf("recovered job = %+v", job)
	}
	// Claim again and let the lease expire with attempts exhausted: the sweep
	// terminal-fails the job with its stored failure result.
	if _, ok, err := s.Claim(ctx, ClaimInput{Owner: "w3", Lease: time.Minute, Now: now}); err != nil || !ok {
		t.Fatalf("final claim: ok=%v err=%v", ok, err)
	}
	out, err = s.RecoverExpiredLeases(ctx, RecoverInput{
		MaxAttempts: 3, FailureResult: func(string) Result { return Result{ErrorClass: "lease_lost"} },
		Now: now.Add(5 * time.Minute),
	})
	if err != nil || len(out.Failed) != 1 || len(out.Requeued) != 0 {
		t.Fatalf("exhausted recovery = %+v err=%v", out, err)
	}
	job, _ = s.Get(ctx, "job-q", now)
	if job.Status != StatusFailed {
		t.Fatalf("exhausted job status = %s", job.Status)
	}
	res, err := s.Result(ctx, "job-q")
	if err != nil || res.ErrorClass != "lease_lost" {
		t.Fatalf("lease-lost result = %+v err=%v", res, err)
	}
}

// TestClaimBackoff pins the retry-backoff contract: a requeue with a future
// NoClaimBefore hides the job from claiming (and from the head-of-the-queue
// order), while an abort requeue (zero NoClaimBefore) is immediately
// claimable — a backed-off job cannot starve younger queued work.
func TestClaimBackoff(t *testing.T) {
	now := time.Now()
	s := NewMemoryStore(func() time.Time { return now })
	ctx := context.Background()
	if _, err := s.Create(ctx, createInput("job-backoff", "")); err != nil {
		t.Fatal(err)
	}
	cl, ok, err := s.Claim(ctx, ClaimInput{Owner: "w", Lease: time.Minute, Now: now})
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	// Younger job created after the backed-off one.
	younger := createInput("job-younger", "")
	younger.Now = now.Add(time.Second)
	if _, err := s.Create(ctx, younger); err != nil {
		t.Fatal(err)
	}
	// Backoff requeue: invisible until NoClaimBefore passes.
	if won, err := s.Requeue(ctx, cl.Job.ID, "w", RequeueOptions{BumpAttempt: true, NoClaimBefore: now.Add(time.Hour)}, now); err != nil || !won {
		t.Fatalf("backoff requeue: won=%v err=%v", won, err)
	}
	// The younger job is claimed first: the backed-off head does not block.
	got, ok, err := s.Claim(ctx, ClaimInput{Owner: "w2", Lease: time.Minute, Now: now.Add(time.Minute)})
	if err != nil || !ok {
		t.Fatalf("younger claim: ok=%v err=%v", ok, err)
	}
	if got.Job.ID != "job-younger" {
		t.Fatalf("backed-off job head-of-line-blocked the queue: claimed %s", got.Job.ID)
	}
	// Before the backoff passes, the job stays invisible; afterwards it is
	// claimable again.
	if _, ok, err := s.Claim(ctx, ClaimInput{Owner: "w3", Lease: time.Minute, Now: now.Add(30 * time.Minute)}); err != nil || ok {
		t.Fatalf("backed-off job must not be claimable early: ok=%v err=%v", ok, err)
	}
	cl2, ok, err := s.Claim(ctx, ClaimInput{Owner: "w3", Lease: time.Minute, Now: now.Add(2 * time.Hour)})
	if err != nil || !ok || cl2.Job.ID != "job-backoff" {
		t.Fatalf("post-backoff claim: ok=%v job=%+v err=%v", ok, cl2.Job, err)
	}
	// Abort requeue (zero NoClaimBefore): immediately claimable.
	if won, err := s.Requeue(ctx, cl2.Job.ID, "w3", RequeueOptions{}, now.Add(2*time.Hour)); err != nil || !won {
		t.Fatalf("abort requeue: won=%v err=%v", won, err)
	}
	if _, ok, err := s.Claim(ctx, ClaimInput{Owner: "w4", Lease: time.Minute, Now: now.Add(2 * time.Hour)}); err != nil || !ok {
		t.Fatalf("abort-requeued job must be immediately claimable: ok=%v err=%v", ok, err)
	}
}

func TestResultExpiry(t *testing.T) {
	now := time.Now()
	s := NewMemoryStore(func() time.Time { return now })
	ctx := context.Background()
	if _, err := s.Create(ctx, createInput("job-exp", "")); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.Claim(ctx, ClaimInput{Owner: "w", Lease: time.Minute, Now: now}); err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	won, err := s.CommitSuccess(ctx, SuccessInput{
		JobID: "job-exp", Owner: "w", FinalRequestID: "req-e",
		Response:        json.RawMessage(`{"id":"x"}`),
		ResultExpiresAt: now.Add(time.Hour), BumpAttempt: true, Now: now,
	})
	if err != nil || !won {
		t.Fatalf("commit: won=%v err=%v", won, err)
	}
	// Before the TTL: the result is readable and the job completed.
	if _, err := s.Get(ctx, "job-exp", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Result(ctx, "job-exp"); err != nil {
		t.Fatalf("live result: %v", err)
	}
	// After the TTL the lazy Get transitions to expired and drops the result;
	// this must never re-trigger execution (there is no execution path here).
	job, err := s.Get(ctx, "job-exp", now.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != StatusExpired {
		t.Fatalf("lazy expiry status = %s", job.Status)
	}
	if _, err := s.Result(ctx, "job-exp"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired result must be gone, got %v", err)
	}
	// The sweep reports the same expiry for any remaining due results.
	if _, err := s.Create(ctx, createInput("job-exp-2", "")); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.Claim(ctx, ClaimInput{Owner: "w", Lease: time.Minute, Now: now}); err != nil || !ok {
		t.Fatalf("claim 2: ok=%v err=%v", ok, err)
	}
	if _, err := s.CommitFailure(ctx, FailureInput{
		JobID: "job-exp-2", Owner: "w", FinalRequestID: "req-e2",
		ErrorClass: "timeout", Response: json.RawMessage(`{}`),
		ResultExpiresAt: now.Add(time.Hour), BumpAttempt: true, Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	n, err := s.ExpireDueResults(ctx, now.Add(3*time.Hour))
	if err != nil || n != 1 {
		t.Fatalf("sweep expiry = %d err=%v", n, err)
	}
	job2, _ := s.Get(ctx, "job-exp-2", now)
	if job2.Status != StatusExpired {
		t.Fatalf("swept job status = %s", job2.Status)
	}
}

func TestCancelQueuedAndIdempotentRepeat(t *testing.T) {
	now := time.Now()
	s := NewMemoryStore(func() time.Time { return now })
	ctx := context.Background()
	if _, err := s.Create(ctx, createInput("job-c", "")); err != nil {
		t.Fatal(err)
	}
	job, outcome, err := s.Cancel(ctx, "job-c", now)
	if err != nil || outcome != CancelledQueued || job.Status != StatusCancelled {
		t.Fatalf("queued cancel = %+v outcome=%v err=%v", job, outcome, err)
	}
	// Repeat cancels are no-ops returning the final observable state.
	job, outcome, err = s.Cancel(ctx, "job-c", now)
	if err != nil || outcome != CancelNoop || job.Status != StatusCancelled {
		t.Fatalf("repeat cancel = %+v outcome=%v err=%v", job, outcome, err)
	}
	if _, _, err := s.Cancel(ctx, "job-missing", now); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing cancel err = %v", err)
	}
}

func TestQueueDepthAndReady(t *testing.T) {
	now := time.Now()
	s := NewMemoryStore(func() time.Time { return now })
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := s.Create(ctx, createInput(jobID("qd", i), "")); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := s.QueueDepth(ctx); err != nil || n != 3 {
		t.Fatalf("depth = %d err=%v", n, err)
	}
	if err := s.Ready(ctx); err != nil {
		t.Fatalf("ready: %v", err)
	}
}
