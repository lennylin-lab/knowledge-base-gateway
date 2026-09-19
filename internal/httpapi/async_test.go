package httpapi

// V1.4 background Responses tests: the 202 creation contract, stable error
// codes, idempotency, ownership isolation, and the worker lifecycle over the
// in-memory async store (deterministic, race-checked). The real PostgreSQL
// store runs its own env-gated suite in internal/store/pg.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/async"
	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/gateway"
	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
	"github.com/knowledge-base/knowledge-base-gateway/internal/metrics"
	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
	"github.com/knowledge-base/knowledge-base-gateway/internal/quota"
)

// blockingCompleteProvider blocks in Complete until its context is cancelled
// (a stand-in for a long upstream generation), reporting what it observed.
type blockingCompleteProvider struct {
	inner   provider.Provider
	started chan struct{}
	sawIt   atomic.Bool
	once    sync.Once
}

func (p *blockingCompleteProvider) Name() string { return "blocking" }

func (p *blockingCompleteProvider) Capabilities(m string) model.Capabilities {
	return p.inner.Capabilities(m)
}

func (p *blockingCompleteProvider) Embeddings(context.Context, model.EmbeddingsRequest) (model.EmbeddingsResponse, error) {
	return model.EmbeddingsResponse{}, errors.New("embeddings not implemented by this test stub")
}

func (p *blockingCompleteProvider) Complete(ctx context.Context, req model.Request) (model.Response, error) {
	p.once.Do(func() { close(p.started) })
	<-ctx.Done()
	p.sawIt.Store(true)
	return model.Response{}, ctx.Err()
}

func (p *blockingCompleteProvider) Stream(ctx context.Context, req model.Request, emit func(model.Event) error) error {
	return p.inner.Stream(ctx, req, emit)
}

// alwaysFailProvider fails every Complete with a retryable upstream error.
type alwaysFailProvider struct{ inner provider.Provider }

func (p *alwaysFailProvider) Name() string { return "always-fail" }

func (p *alwaysFailProvider) Capabilities(m string) model.Capabilities {
	return p.inner.Capabilities(m)
}

func (p *alwaysFailProvider) Embeddings(context.Context, model.EmbeddingsRequest) (model.EmbeddingsResponse, error) {
	return model.EmbeddingsResponse{}, errors.New("embeddings not implemented by this test stub")
}

func (p *alwaysFailProvider) Complete(context.Context, model.Request) (model.Response, error) {
	return model.Response{}, &provider.Error{Class: provider.ClassServer, Msg: "upstream down"}
}

func (p *alwaysFailProvider) Stream(ctx context.Context, req model.Request, emit func(model.Event) error) error {
	return p.inner.Stream(ctx, req, emit)
}

// timeoutOnceProvider hangs its first Complete until its context is done (a
// job-timeout deadline), then serves the inner provider for later calls.
type timeoutOnceProvider struct {
	inner provider.Provider
	calls atomic.Int32
}

func (p *timeoutOnceProvider) Name() string { return "timeout-once" }

func (p *timeoutOnceProvider) Capabilities(m string) model.Capabilities {
	return p.inner.Capabilities(m)
}

func (p *timeoutOnceProvider) Embeddings(ctx context.Context, req model.EmbeddingsRequest) (model.EmbeddingsResponse, error) {
	return p.inner.Embeddings(ctx, req)
}

func (p *timeoutOnceProvider) Complete(ctx context.Context, req model.Request) (model.Response, error) {
	if p.calls.Add(1) == 1 {
		<-ctx.Done() // the detached execution deadline fires
		return model.Response{}, ctx.Err()
	}
	return p.inner.Complete(ctx, req)
}

func (p *timeoutOnceProvider) Stream(ctx context.Context, req model.Request, emit func(model.Event) error) error {
	return p.inner.Stream(ctx, req, emit)
}

// gatedCompleteProvider blocks its first Complete until released, then serves
// the inner provider — a long upstream generation whose finish time the test
// controls (used to land an execution inside the shutdown drain window).
type gatedCompleteProvider struct {
	inner   provider.Provider
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *gatedCompleteProvider) Name() string { return "gated" }

func (p *gatedCompleteProvider) Capabilities(m string) model.Capabilities {
	return p.inner.Capabilities(m)
}

func (p *gatedCompleteProvider) Embeddings(ctx context.Context, req model.EmbeddingsRequest) (model.EmbeddingsResponse, error) {
	return p.inner.Embeddings(ctx, req)
}

func (p *gatedCompleteProvider) Complete(ctx context.Context, req model.Request) (model.Response, error) {
	p.once.Do(func() { close(p.started) })
	select {
	case <-p.release:
	case <-ctx.Done():
		return model.Response{}, ctx.Err()
	}
	return p.inner.Complete(ctx, req)
}

func (p *gatedCompleteProvider) Stream(ctx context.Context, req model.Request, emit func(model.Event) error) error {
	return p.inner.Stream(ctx, req, emit)
}

// asyncFixture wires the Responses create path, the job query/cancel routes,
// and (optionally) a worker pool over the in-memory async store.
type asyncFixture struct {
	handler  *ResponsesHandler
	jobsAPI  *AsyncJobsHandler
	jobsMux  http.Handler // mux with the GET/cancel route patterns registered
	jobs     async.Store
	sink     *audit.MemorySink
	calls    *int32
	pol      *policy.Policy
	provider provider.Provider
	pool     *async.Pool
	// jobTimeoutOverride replaces the pool's default job deadline when set
	// before startPool (used by the timeout-accounting test).
	jobTimeoutOverride time.Duration
	// poolLimiter overrides the worker pool's rate limiter when set.
	poolLimiter limiter.Gate
}

// ctxFaithfulStore makes the in-memory store honor context cancellation like
// the PostgreSQL implementation does: an operation on a canceled context fails
// with ctx.Err() instead of silently succeeding. The shutdown-drain test
// depends on this to catch a commit issued on a dead pool context.
type ctxFaithfulStore struct{ inner async.Store }

func (s ctxFaithfulStore) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (s ctxFaithfulStore) Create(ctx context.Context, in async.CreateInput) (async.CreateOutcome, error) {
	if err := s.check(ctx); err != nil {
		return async.CreateOutcome{}, err
	}
	return s.inner.Create(ctx, in)
}

func (s ctxFaithfulStore) Get(ctx context.Context, jobID string, now time.Time) (async.Job, error) {
	if err := s.check(ctx); err != nil {
		return async.Job{}, err
	}
	return s.inner.Get(ctx, jobID, now)
}

func (s ctxFaithfulStore) Result(ctx context.Context, jobID string) (async.Result, error) {
	if err := s.check(ctx); err != nil {
		return async.Result{}, err
	}
	return s.inner.Result(ctx, jobID)
}

func (s ctxFaithfulStore) Cancel(ctx context.Context, jobID string, now time.Time) (async.Job, async.CancelOutcome, error) {
	if err := s.check(ctx); err != nil {
		return async.Job{}, 0, err
	}
	return s.inner.Cancel(ctx, jobID, now)
}

func (s ctxFaithfulStore) Claim(ctx context.Context, in async.ClaimInput) (async.Claimed, bool, error) {
	if err := s.check(ctx); err != nil {
		return async.Claimed{}, false, err
	}
	return s.inner.Claim(ctx, in)
}

func (s ctxFaithfulStore) Heartbeat(ctx context.Context, jobID, owner string, lease time.Duration, now time.Time) error {
	if err := s.check(ctx); err != nil {
		return err
	}
	return s.inner.Heartbeat(ctx, jobID, owner, lease, now)
}

func (s ctxFaithfulStore) CommitSuccess(ctx context.Context, in async.SuccessInput) (bool, error) {
	if err := s.check(ctx); err != nil {
		return false, err
	}
	return s.inner.CommitSuccess(ctx, in)
}

func (s ctxFaithfulStore) CommitFailure(ctx context.Context, in async.FailureInput) (bool, error) {
	if err := s.check(ctx); err != nil {
		return false, err
	}
	return s.inner.CommitFailure(ctx, in)
}

func (s ctxFaithfulStore) Requeue(ctx context.Context, jobID, owner string, opts async.RequeueOptions, now time.Time) (bool, error) {
	if err := s.check(ctx); err != nil {
		return false, err
	}
	return s.inner.Requeue(ctx, jobID, owner, opts, now)
}

func (s ctxFaithfulStore) RecoverExpiredLeases(ctx context.Context, in async.RecoverInput) (async.RecoverOutcome, error) {
	if err := s.check(ctx); err != nil {
		return async.RecoverOutcome{}, err
	}
	return s.inner.RecoverExpiredLeases(ctx, in)
}

func (s ctxFaithfulStore) ExpireDueResults(ctx context.Context, now time.Time) (int, error) {
	if err := s.check(ctx); err != nil {
		return 0, err
	}
	return s.inner.ExpireDueResults(ctx, now)
}

func (s ctxFaithfulStore) QueueDepth(ctx context.Context) (int, error) {
	if err := s.check(ctx); err != nil {
		return 0, err
	}
	return s.inner.QueueDepth(ctx)
}

func (s ctxFaithfulStore) Ready(ctx context.Context) error {
	if err := s.check(ctx); err != nil {
		return err
	}
	return s.inner.Ready(ctx)
}

type asyncOptions struct {
	provider    provider.Provider
	maxAttempts int
	resultTTL   time.Duration
	withPool    bool
	quotaGate   quota.Gate
	jobTimeout  time.Duration
	// poolLimiter replaces the worker pool's rate limiter when set (the
	// creation-path limiter stays permissive); used by the denial tests.
	poolLimiter limiter.Gate
}

func newAsyncFixture(t *testing.T, opts asyncOptions) *asyncFixture {
	t.Helper()
	if opts.provider == nil {
		opts.provider = provider.Fake{}
	}
	if opts.maxAttempts == 0 {
		opts.maxAttempts = 3
	}
	if opts.resultTTL == 0 {
		opts.resultTTL = 24 * time.Hour
	}
	if opts.quotaGate == nil {
		opts.quotaGate = quota.NewMemory()
	}
	store := auth.NewStore()
	for _, rec := range []struct{ id, subject, plaintext string }{
		{"key-a", "subject-r", testKey},
		{"key-b", "subject-other", otherKey},
	} {
		salt, err := auth.NewSalt()
		if err != nil {
			t.Fatal(err)
		}
		store.Put(auth.KeyRecord{ID: rec.id, Subject: rec.subject, Salt: salt,
			Hash: auth.HashAPIKey(salt, rec.plaintext), Status: auth.StatusActive})
	}
	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: "full-model", Provider: opts.provider.Name(), UpstreamModel: "upstream-full", Enabled: true, Capabilities: fullCaps},
	})
	pol := policy.New()
	pol.Allow("subject-r", "full-model")
	pol.Allow("subject-other", "full-model")
	calls := int32(0)
	counting := &countingCalls{inner: opts.provider, calls: &calls}
	svc := gateway.New(catalog, map[string]provider.Provider{opts.provider.Name(): counting}, 5*time.Second, 0)
	svc.RetryWait = time.Millisecond
	sink := audit.NewMemorySink(nil)
	cancels := async.NewCancelRegistry()
	var jobs async.Store = ctxFaithfulStore{inner: async.NewMemoryStore(nil)}
	bundle := &Async{
		Jobs: jobs, Cancels: cancels,
		Wake:      func() {}, // replaced when the pool starts
		ResultTTL: opts.resultTTL, KeyTTL: time.Hour,
		MaxResultBytes: 1 << 20, MaxKeyBytes: 256,
	}
	handler := &ResponsesHandler{
		Auth: store, Service: svc, Policy: pol, Limiter: limiter.New(1000, 100),
		Quota: opts.quotaGate, Audit: sink, Metrics: metrics.New(),
		MaxBody: 1 << 20, MaxItems: 64, MaxChars: 32_000,
		Async: bundle,
	}
	jobsAPI := &AsyncJobsHandler{
		Auth: store, Jobs: jobs, Cancels: cancels, Audit: sink, Metrics: metrics.New(),
		PollHint: 2 * time.Second,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/responses/{id}", jobsAPI.ServeHTTP)
	mux.HandleFunc("POST /v1/responses/{id}/cancel", jobsAPI.ServeHTTP)
	f := &asyncFixture{
		handler: handler, jobsAPI: jobsAPI, jobsMux: mux, jobs: jobs,
		sink: sink, calls: &calls, pol: pol, provider: opts.provider,
		jobTimeoutOverride: opts.jobTimeout, poolLimiter: opts.poolLimiter,
	}
	if opts.withPool {
		f.startPool(t, opts.maxAttempts)
	}
	return f
}

// startPool launches a worker pool wired to the fixture collaborators.
func (f *asyncFixture) startPool(t *testing.T, maxAttempts int) {
	t.Helper()
	jobTimeout := 5 * time.Second
	if f.jobTimeoutOverride > 0 {
		jobTimeout = f.jobTimeoutOverride
	}
	poolLimiter := limiter.Gate(limiter.New(1000, 100))
	if f.poolLimiter != nil {
		poolLimiter = f.poolLimiter
	}
	svc := f.handler.Service
	pool := async.NewPool(async.PoolDeps{
		Store: f.jobs, Service: svc, Policy: f.pol,
		Limiter: poolLimiter, Quota: quota.NewMemory(),
		Audit: f.sink, Metrics: metrics.New(), Cancels: f.handler.Async.Cancels,
		Encoder: NewAsyncEncoder(),
	}, async.PoolConfig{
		WorkerID: "worker-test", Count: 1, PollInterval: 5 * time.Millisecond,
		Lease: 30 * time.Second, JobTimeout: jobTimeout,
		MaxAttempts: maxAttempts, ResultTTL: f.handler.Async.ResultTTL,
		MaxResultBytes: 1 << 20, Drain: 2 * time.Second,
	})
	f.handler.Async.Wake = pool.Wake
	f.pool = pool
	pool.Start(context.Background())
	t.Cleanup(func() { pool.Stop(2 * time.Second) })
}

// countingCalls is a local call counter (responses_test.go's callCounter
// counts through an int; this one is goroutine-safe for pool tests).
type countingCalls struct {
	inner provider.Provider
	calls *int32
}

func (c *countingCalls) Name() string { return c.inner.Name() }

func (c *countingCalls) Capabilities(m string) model.Capabilities { return c.inner.Capabilities(m) }

func (c *countingCalls) Embeddings(ctx context.Context, req model.EmbeddingsRequest) (model.EmbeddingsResponse, error) {
	return c.inner.Embeddings(ctx, req)
}

func (c *countingCalls) Complete(ctx context.Context, req model.Request) (model.Response, error) {
	atomic.AddInt32(c.calls, 1)
	return c.inner.Complete(ctx, req)
}

func (c *countingCalls) Stream(ctx context.Context, req model.Request, emit func(model.Event) error) error {
	atomic.AddInt32(c.calls, 1)
	return c.inner.Stream(ctx, req, emit)
}

// postCreate issues a POST /v1/responses creation request.
func (f *asyncFixture) postCreate(t *testing.T, body, key, idemKey string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+key)
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)
	return rec
}

func (f *asyncFixture) getJob(t *testing.T, id, key string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/responses/"+id, nil)
	req.Header.Set("Authorization", "Bearer "+key)
	rec := httptest.NewRecorder()
	f.jobsMux.ServeHTTP(rec, req)
	return rec
}

func (f *asyncFixture) cancelJob(t *testing.T, id, key string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/responses/"+id+"/cancel", nil)
	req.Header.Set("Authorization", "Bearer "+key)
	rec := httptest.NewRecorder()
	f.jobsMux.ServeHTTP(rec, req)
	return rec
}

// awaitStatus polls the store until the job reaches a wanted status.
func (f *asyncFixture) awaitStatus(t *testing.T, id string, want ...async.Status) async.Job {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		job, err := f.jobs.Get(context.Background(), id, time.Now())
		if err == nil {
			for _, s := range want {
				if job.Status == s {
					return job
				}
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	job, _ := f.jobs.Get(context.Background(), id, time.Now())
	t.Fatalf("job %s never reached %v (now %s)", id, want, job.Status)
	return async.Job{}
}

// auditsOf returns audit events matching an error class ("" = no class).
func auditsOf(f *asyncFixture, class string) []audit.Event {
	var out []audit.Event
	for _, ev := range f.sink.Snapshot() {
		if ev.ErrorClass == class {
			out = append(out, ev)
		}
	}
	return out
}

// auditsWithStatus returns audit events carrying an HTTP status (the 202
// creation record, the 200 terminal success record).
func auditsWithStatus(f *asyncFixture, status int) []audit.Event {
	var out []audit.Event
	for _, ev := range f.sink.Snapshot() {
		if ev.Status == status {
			out = append(out, ev)
		}
	}
	return out
}

const asyncEchoBody = `{"model":"full-model","input":"hello","background":true}`

func TestAsyncCreate202Shape(t *testing.T) {
	f := newAsyncFixture(t, asyncOptions{})
	rec := f.postCreate(t, asyncEchoBody, testKey, "")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var out asyncStatusOut
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out.ID, "resp_") || out.Object != "response" ||
		out.Status != "queued" || out.Model != "full-model" ||
		out.Created <= 0 || out.RequestID == "" {
		t.Fatalf("202 envelope wrong: %+v", out)
	}
	if rec.Header().Get("X-Request-ID") == "" {
		t.Error("202 must echo X-Request-ID")
	}
	if rec.Header().Get("Retry-After") != "" {
		t.Error("Retry-After belongs to the query endpoints, not creation")
	}
	// Creation alone never invokes the provider.
	if got := atomic.LoadInt32(f.calls); got != 0 {
		t.Errorf("creation must not reach the provider, got %d calls", got)
	}
}

func TestAsyncBackgroundStreamingRejected(t *testing.T) {
	f := newAsyncFixture(t, asyncOptions{})
	rec := f.postCreate(t, `{"model":"full-model","input":"x","background":true,"stream":true}`, testKey, "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var env APIError
	_ = json.Unmarshal(rec.Body.Bytes(), &env)
	if env.Error.Code != "invalid_request" {
		t.Fatalf("code = %q", env.Error.Code)
	}
	if n, _ := f.jobs.QueueDepth(context.Background()); n != 0 {
		t.Fatalf("rejected stream background job must not be queued, depth=%d", n)
	}
}

func TestAsyncDisabledRollbackPosture(t *testing.T) {
	// No Async bundle: background acceptance answers the stable 503 while
	// the synchronous path is untouched.
	f := newAsyncFixture(t, asyncOptions{})
	f.handler.Async = nil
	rec := f.postCreate(t, asyncEchoBody, testKey, "")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var env APIError
	_ = json.Unmarshal(rec.Body.Bytes(), &env)
	if env.Error.Code != "job_queue_unavailable" || env.Error.Type != "service_unavailable" {
		t.Fatalf("envelope = %+v", env.Error)
	}
}

func TestAsyncAuthBeforeParse(t *testing.T) {
	f := newAsyncFixture(t, asyncOptions{})
	// Invalid body AND no key: the 401 must win, with zero parse work.
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{not-json`))
	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 before any body read", rec.Code)
	}
}

func TestAsyncIdempotencyReplayAndConflict(t *testing.T) {
	f := newAsyncFixture(t, asyncOptions{})
	first := f.postCreate(t, asyncEchoBody, testKey, "idem-1")
	if first.Code != http.StatusAccepted {
		t.Fatalf("first: %d %s", first.Code, first.Body.String())
	}
	var a asyncStatusOut
	_ = json.Unmarshal(first.Body.Bytes(), &a)

	// Same key + same request: the original job returns (no second job).
	second := f.postCreate(t, asyncEchoBody, testKey, "idem-1")
	if second.Code != http.StatusAccepted {
		t.Fatalf("second: %d %s", second.Code, second.Body.String())
	}
	var b asyncStatusOut
	_ = json.Unmarshal(second.Body.Bytes(), &b)
	if a.ID != b.ID {
		t.Fatalf("replay must return the original job: %s vs %s", a.ID, b.ID)
	}
	depth, _ := f.jobs.QueueDepth(context.Background())
	if depth != 1 {
		t.Fatalf("duplicate creation must collapse to one job, depth=%d", depth)
	}

	// Same key + different request: 409 idempotency_conflict.
	conflict := f.postCreate(t, `{"model":"full-model","input":"different","background":true}`, testKey, "idem-1")
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict: %d %s", conflict.Code, conflict.Body.String())
	}
	var env APIError
	_ = json.Unmarshal(conflict.Body.Bytes(), &env)
	if env.Error.Code != "idempotency_conflict" {
		t.Fatalf("code = %q", env.Error.Code)
	}
}

func TestAsyncQueryLifecycleWithWorker(t *testing.T) {
	f := newAsyncFixture(t, asyncOptions{}) // pool starts after the queued check
	created := f.postCreate(t, asyncEchoBody, testKey, "")
	if created.Code != http.StatusAccepted {
		t.Fatalf("create: %d %s", created.Code, created.Body.String())
	}
	var st asyncStatusOut
	_ = json.Unmarshal(created.Body.Bytes(), &st)

	// Queued query: observable state with a Retry-After hint.
	queued := f.getJob(t, st.ID, testKey)
	if queued.Code != http.StatusOK || !strings.Contains(queued.Body.String(), `"status":"queued"`) {
		t.Fatalf("queued query: %d %s", queued.Code, queued.Body.String())
	}
	if queued.Header().Get("Retry-After") == "" {
		t.Error("queued/running queries must carry a Retry-After hint")
	}

	f.startPool(t, 3)
	job := f.awaitStatus(t, st.ID, async.StatusCompleted)
	if job.AttemptCount != 1 {
		t.Errorf("attempt count = %d, want 1", job.AttemptCount)
	}
	done := f.getJob(t, st.ID, testKey)
	if done.Code != http.StatusOK {
		t.Fatalf("completed query: %d %s", done.Code, done.Body.String())
	}
	var resp responseObject
	if err := json.Unmarshal(done.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != model.StatusCompleted || resp.ID != st.ID ||
		len(resp.Output) != 1 || resp.Output[0].Content[0].Text != "echo: hello" {
		t.Fatalf("stored envelope wrong: %+v", resp)
	}
	if resp.Usage == nil || resp.Usage.TotalTokens != 21 {
		t.Fatalf("stored usage wrong: %+v", resp.Usage)
	}
	for _, leaked := range []string{"chatcmpl", "system_fingerprint", "upstream-full"} {
		if strings.Contains(done.Body.String(), leaked) {
			t.Errorf("result leaks provider field %q", leaked)
		}
	}
	// Terminal queries carry no polling hint.
	if done.Header().Get("Retry-After") != "" {
		t.Error("terminal queries must not suggest polling")
	}
	// Exactly one terminal audit handoff (plus the 202 creation record), and
	// its correlation IDs are the final request ID persisted on the job —
	// never empty (the claim-time job copy predates the generated ID).
	if n := len(auditsWithStatus(f, 200)); n != 1 {
		t.Errorf("terminal audit records = %d, want exactly 1", n)
	}
	if ev := auditsWithStatus(f, 200)[0]; ev.Protocol != "responses" || ev.Provider != "fake" {
		t.Errorf("terminal audit wrong: %+v", ev)
	} else if ev.RequestID == "" || ev.TraceID == "" {
		t.Errorf("terminal audit must carry request/trace IDs, got %+v", ev)
	} else if ev.RequestID != job.FinalRequestID {
		t.Errorf("terminal audit request ID %q must match the stored final ID %q", ev.RequestID, job.FinalRequestID)
	}
	if got := atomic.LoadInt32(f.calls); got != 1 {
		t.Errorf("provider calls = %d, want 1", got)
	}
}

func TestAsyncOwnershipIsolation(t *testing.T) {
	f := newAsyncFixture(t, asyncOptions{})
	var st asyncStatusOut
	_ = json.Unmarshal(f.postCreate(t, asyncEchoBody, testKey, "").Body.Bytes(), &st)

	// Another subject gets the same not-found as an absent job, for both
	// query and cancel; the owner gets real answers.
	if rec := f.getJob(t, st.ID, otherKey); rec.Code != http.StatusNotFound ||
		!strings.Contains(rec.Body.String(), "response_not_found") {
		t.Fatalf("foreign GET: %d %s", rec.Code, rec.Body.String())
	}
	if rec := f.cancelJob(t, st.ID, otherKey); rec.Code != http.StatusNotFound {
		t.Fatalf("foreign cancel: %d %s", rec.Code, rec.Body.String())
	}
	if rec := f.getJob(t, "resp_does-not-exist", testKey); rec.Code != http.StatusNotFound {
		t.Fatalf("absent GET: %d", rec.Code)
	}
	if rec := f.getJob(t, st.ID, testKey); rec.Code != http.StatusOK {
		t.Fatalf("owner GET: %d", rec.Code)
	}
}

func TestAsyncCancelQueuedIdempotent(t *testing.T) {
	f := newAsyncFixture(t, asyncOptions{}) // no pool: the job stays queued
	var st asyncStatusOut
	_ = json.Unmarshal(f.postCreate(t, asyncEchoBody, testKey, "").Body.Bytes(), &st)

	first := f.cancelJob(t, st.ID, testKey)
	if first.Code != http.StatusOK || !strings.Contains(first.Body.String(), `"status":"cancelled"`) {
		t.Fatalf("cancel: %d %s", first.Code, first.Body.String())
	}
	// Repeat cancel is idempotent and returns the final observable state.
	second := f.cancelJob(t, st.ID, testKey)
	if second.Code != http.StatusOK || !strings.Contains(second.Body.String(), `"status":"cancelled"`) {
		t.Fatalf("repeat cancel: %d %s", second.Code, second.Body.String())
	}
	// Exactly one cancellation handoff was audited for the job.
	if n := len(auditsOf(f, "cancelled")); n != 1 {
		t.Fatalf("cancelled audit records = %d, want 1", n)
	}
	// The queued job never reached a provider.
	if got := atomic.LoadInt32(f.calls); got != 0 {
		t.Errorf("cancelled queued job reached the provider (%d calls)", got)
	}
}

func TestAsyncCancelRunningPropagatesToProvider(t *testing.T) {
	blocked := &blockingCompleteProvider{inner: provider.Fake{}, started: make(chan struct{})}
	f := newAsyncFixture(t, asyncOptions{provider: blocked, withPool: true})
	var st asyncStatusOut
	_ = json.Unmarshal(f.postCreate(t, asyncEchoBody, testKey, "").Body.Bytes(), &st)

	select {
	case <-blocked.started:
	case <-time.After(3 * time.Second):
		t.Fatal("worker never started the provider call")
	}
	if rec := f.cancelJob(t, st.ID, testKey); rec.Code != http.StatusOK ||
		!strings.Contains(rec.Body.String(), `"status":"cancelled"`) {
		t.Fatalf("running cancel: %s", rec.Body.String())
	}
	// The cancel must propagate to the in-flight upstream call.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !blocked.sawIt.Load() {
		time.Sleep(2 * time.Millisecond)
	}
	if !blocked.sawIt.Load() {
		t.Fatal("client cancel did not propagate to the provider context")
	}
	// The cancel decided the job; no completion handoff may appear afterwards.
	time.Sleep(50 * time.Millisecond)
	if n := len(auditsOf(f, "cancelled")); n != 1 {
		t.Fatalf("cancelled audit records = %d, want exactly 1", n)
	}
	if n := len(auditsWithStatus(f, 200)); n != 0 {
		t.Fatalf("completed audit records = %d, want 0 (cancel owns the handoff)", n)
	}
	job, _ := f.jobs.Get(context.Background(), st.ID, time.Now())
	if job.Status != async.StatusCancelled {
		t.Fatalf("status after cancel = %s", job.Status)
	}
}

func TestAsyncWorkerRetryThenTerminalFailure(t *testing.T) {
	f := newAsyncFixture(t, asyncOptions{provider: &alwaysFailProvider{inner: provider.Fake{}}, maxAttempts: 2, withPool: true})
	var st asyncStatusOut
	_ = json.Unmarshal(f.postCreate(t, asyncEchoBody, testKey, "").Body.Bytes(), &st)

	job := f.awaitStatus(t, st.ID, async.StatusFailed)
	if job.AttemptCount != 2 {
		t.Fatalf("attempt count = %d, want 2 (one requeue, one terminal)", job.AttemptCount)
	}
	rec := f.getJob(t, st.ID, testKey)
	var resp responseObject
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "failed" {
		t.Fatalf("status = %q", resp.Status)
	}
	errBody, _ := resp.Error.(map[string]any)
	if errBody == nil || errBody["code"] != "upstream_unavailable" {
		t.Fatalf("failure error = %v", resp.Error)
	}
	// Exactly one terminal audit record for the exhausted job, with the same
	// non-empty correlation IDs as the success path.
	failedAudits := auditsOf(f, "server")
	if n := len(failedAudits); n != 1 {
		t.Fatalf("failed audit records = %d, want 1", n)
	}
	if failedAudits[0].RequestID == "" || failedAudits[0].TraceID == "" {
		t.Errorf("terminal failure audit must carry request/trace IDs, got %+v", failedAudits[0])
	}
}

func TestAsyncQuotaDeniedAtCreation(t *testing.T) {
	f := newAsyncFixture(t, asyncOptions{})
	f.pol.SetLimits("subject-r", policy.Limits{DailyTokens: 1}) // estimate 2+2048 exceeds this
	rec := f.postCreate(t, asyncEchoBody, testKey, "")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var env APIError
	_ = json.Unmarshal(rec.Body.Bytes(), &env)
	if env.Error.Code != "quota_exceeded" {
		t.Fatalf("code = %q", env.Error.Code)
	}
	if n, _ := f.jobs.QueueDepth(context.Background()); n != 0 {
		t.Fatalf("denied creation must not enqueue, depth=%d", n)
	}
}

func TestAsyncQuotaDeniedAtExecution(t *testing.T) {
	f := newAsyncFixture(t, asyncOptions{}) // pool starts after the budget change
	var st asyncStatusOut
	_ = json.Unmarshal(f.postCreate(t, asyncEchoBody, testKey, "").Body.Bytes(), &st)
	// Budget exhausts between creation and execution: the worker fails the
	// job with the stable class instead of bypassing the quota.
	f.pol.SetLimits("subject-r", policy.Limits{DailyTokens: 1})
	f.startPool(t, 3)
	f.awaitStatus(t, st.ID, async.StatusFailed)
	rec := f.getJob(t, st.ID, testKey)
	var resp responseObject
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	errBody, _ := resp.Error.(map[string]any)
	if errBody == nil || errBody["code"] != "quota_exceeded" {
		t.Fatalf("failure error = %v", resp.Error)
	}
}

func TestAsyncResultExpiryWithoutReexecution(t *testing.T) {
	f := newAsyncFixture(t, asyncOptions{resultTTL: 40 * time.Millisecond, withPool: true})
	var st asyncStatusOut
	_ = json.Unmarshal(f.postCreate(t, asyncEchoBody, testKey, "").Body.Bytes(), &st)
	f.awaitStatus(t, st.ID, async.StatusCompleted)
	if rec := f.getJob(t, st.ID, testKey); rec.Code != http.StatusOK {
		t.Fatalf("fresh result query: %d", rec.Code)
	}
	time.Sleep(60 * time.Millisecond) // let the result TTL pass
	rec := f.getJob(t, st.ID, testKey)
	if rec.Code != http.StatusGone {
		t.Fatalf("expired query status = %d body = %s", rec.Code, rec.Body.String())
	}
	var env APIError
	_ = json.Unmarshal(rec.Body.Bytes(), &env)
	if env.Error.Code != "response_expired" {
		t.Fatalf("code = %q", env.Error.Code)
	}
	// Expiry never re-executes: the provider call count is unchanged.
	if got := atomic.LoadInt32(f.calls); got != 1 {
		t.Fatalf("provider calls = %d, want 1 (no re-execution)", got)
	}
}

func TestAsyncRestartRecovery(t *testing.T) {
	f := newAsyncFixture(t, asyncOptions{}) // no pool yet: simulate a dead process
	var st asyncStatusOut
	_ = json.Unmarshal(f.postCreate(t, asyncEchoBody, testKey, "").Body.Bytes(), &st)

	// The "dead" process claimed the job and vanished: claim directly, then
	// let the lease lapse without any heartbeat.
	_, ok, err := f.jobs.Claim(context.Background(), async.ClaimInput{
		Owner: "dead-worker", Lease: 30 * time.Millisecond, Now: time.Now(),
	})
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	time.Sleep(50 * time.Millisecond)

	// A restarted process runs recovery before serving: the abandoned lease
	// returns the job to the queue (one lost attempt counted).
	out, err := f.jobs.RecoverExpiredLeases(context.Background(), async.RecoverInput{
		MaxAttempts: 3, FailureResult: func(string) async.Result { return async.Result{} },
		Now: time.Now(),
	})
	if err != nil || len(out.Requeued) != 1 {
		t.Fatalf("recovery = %+v err=%v", out, err)
	}

	// The fresh pool picks the job up and completes it.
	f.startPool(t, 3)
	job := f.awaitStatus(t, st.ID, async.StatusCompleted)
	if job.AttemptCount < 1 {
		t.Fatalf("attempt count = %d", job.AttemptCount)
	}
	if n := len(auditsWithStatus(f, 200)); n != 1 {
		t.Fatalf("terminal audit records = %d, want 1", n)
	}
}

func TestAsyncSyncPathUnaffectedByAsyncField(t *testing.T) {
	f := newAsyncFixture(t, asyncOptions{withPool: true})
	// A synchronous request (no background field) completes inline exactly as
	// before: 200 + full envelope, no job row.
	rec := f.postCreate(t, `{"model":"full-model","input":"hello"}`, testKey, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp responseObject
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != model.StatusCompleted || len(resp.Output) != 1 {
		t.Fatalf("sync envelope wrong: %+v", resp)
	}
	if depth, _ := f.jobs.QueueDepth(context.Background()); depth != 0 {
		t.Fatalf("sync request must not enqueue, depth=%d", depth)
	}
}

// TestAsyncJobTimeoutConsumesAnAttempt pins the attempt-accounting contract
// for job deadlines: a provider call that hangs until the job timeout is a
// real consumed attempt — the job requeues with the attempt counted and
// completes on the next execution. The free requeue path is reserved for
// genuine aborts (shutdown drain, client cancel, lost lease), so a hung
// provider can never recycle a job past MaxAttempts.
func TestAsyncJobTimeoutConsumesAnAttempt(t *testing.T) {
	hanging := &timeoutOnceProvider{inner: provider.Fake{}}
	f := newAsyncFixture(t, asyncOptions{
		provider: hanging, maxAttempts: 2, withPool: true, jobTimeout: 80 * time.Millisecond,
	})
	var st asyncStatusOut
	_ = json.Unmarshal(f.postCreate(t, asyncEchoBody, testKey, "").Body.Bytes(), &st)

	job := f.awaitStatus(t, st.ID, async.StatusCompleted)
	if job.AttemptCount != 2 {
		t.Fatalf("attempt count = %d, want 2 (one timeout, one success)", job.AttemptCount)
	}
	if got := hanging.calls.Load(); got != 2 {
		t.Fatalf("provider calls = %d, want 2", got)
	}
	// The completion handoff is the single terminal record.
	if n := len(auditsWithStatus(f, 200)); n != 1 {
		t.Errorf("terminal audit records = %d, want 1", n)
	}
}

// TestAsyncShutdownDrainCommitsInFlightJob pins Stop's contract: workers stop
// claiming first, then an execution that finishes inside the drain window
// commits normally on its detached execution context — no failed handoff, no
// lease loss, no re-execution, exactly one terminal record.
func TestAsyncShutdownDrainCommitsInFlightJob(t *testing.T) {
	gated := &gatedCompleteProvider{inner: provider.Fake{}, started: make(chan struct{}), release: make(chan struct{})}
	f := newAsyncFixture(t, asyncOptions{provider: gated, withPool: true})
	var st asyncStatusOut
	_ = json.Unmarshal(f.postCreate(t, asyncEchoBody, testKey, "").Body.Bytes(), &st)

	select {
	case <-gated.started:
	case <-time.After(3 * time.Second):
		t.Fatal("worker never started the provider call")
	}

	drained := make(chan struct{})
	go func() {
		f.pool.Stop(2 * time.Second) // intake stops; the pool context dies now
		close(drained)
	}()
	close(gated.release) // the upstream finishes inside the drain window

	select {
	case <-drained:
	case <-time.After(4 * time.Second):
		t.Fatal("pool stop did not return within the drain window")
	}
	job := f.awaitStatus(t, st.ID, async.StatusCompleted)
	if job.AttemptCount != 1 {
		t.Fatalf("attempt count = %d, want 1 (commit landed, no retry)", job.AttemptCount)
	}
	if got := atomic.LoadInt32(f.calls); got != 1 {
		t.Fatalf("provider calls = %d, want 1 (no re-execution after drain)", got)
	}
	if n := len(auditsWithStatus(f, 200)); n != 1 {
		t.Errorf("terminal audit records = %d, want 1", n)
	}
}

// storedErrorCode fetches a terminal job's stored envelope and returns its
// error code ("" for completed jobs).
func storedErrorCode(t *testing.T, f *asyncFixture, id string) string {
	t.Helper()
	rec := f.getJob(t, id, testKey)
	if rec.Code != http.StatusOK {
		t.Fatalf("result query = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp responseObject
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	errBody, _ := resp.Error.(map[string]any)
	if errBody == nil {
		return ""
	}
	code, _ := errBody["code"].(string)
	return code
}

// TestAsyncWorkerCorruptedSnapshotFailsTerminal pins the poison-payload
// contract: a job whose stored snapshot cannot be decoded (corrupt JSON) or
// whose snapshot version is unsupported fails terminally with the stable
// "internal" class — no provider attempt, no retry, one audited handoff each
// with non-empty correlation IDs.
func TestAsyncWorkerCorruptedSnapshotFailsTerminal(t *testing.T) {
	f := newAsyncFixture(t, asyncOptions{}) // pool started after the payloads are seeded
	now := time.Now()
	jobs := []struct {
		id      string
		payload json.RawMessage
	}{
		{newJobID(), json.RawMessage(`{not json`)},
		{newJobID(), json.RawMessage(`{"v":0,"public_model":"full-model","input":[]}`)},
	}
	for _, j := range jobs {
		if _, err := f.jobs.Create(context.Background(), async.CreateInput{
			JobID: j.id, SubjectID: "subject-r", TenantID: "tenant-a",
			Protocol: "responses", PublicModel: "full-model",
			RequestDigest: "digest-" + j.id, Request: j.payload,
			Now: now,
		}); err != nil {
			t.Fatalf("seed %s: %v", j.id, err)
		}
	}
	f.startPool(t, 3)
	for _, j := range jobs {
		job := f.awaitStatus(t, j.id, async.StatusFailed)
		if job.AttemptCount != 1 {
			t.Errorf("job %s attempt count = %d, want 1 (no retry on a poison payload)", j.id, job.AttemptCount)
		}
		if code := storedErrorCode(t, f, j.id); code != "internal" {
			t.Errorf("job %s failure code = %q, want internal", j.id, code)
		}
	}
	if got := atomic.LoadInt32(f.calls); got != 0 {
		t.Errorf("provider calls = %d, want 0 (poison payloads never reach a provider)", got)
	}
	ins := auditsOf(f, "internal")
	if len(ins) != len(jobs) {
		t.Fatalf("internal audit records = %d, want %d", len(ins), len(jobs))
	}
	for _, ev := range ins {
		if ev.RequestID == "" || ev.TraceID == "" {
			t.Errorf("terminal audit must carry request/trace IDs, got %+v", ev)
		}
	}
}

// TestAsyncTransientReadmitExhausts pins retry accounting on the transient
// re-admission path: a job whose model route vanished retries with backoff,
// consumes an attempt per try, and terminal-fails with the stable class once
// attempts are exhausted — it can neither loop forever nor starve the queue.
func TestAsyncTransientReadmitExhausts(t *testing.T) {
	f := newAsyncFixture(t, asyncOptions{maxAttempts: 2}) // pool starts after the route drop
	var st asyncStatusOut
	_ = json.Unmarshal(f.postCreate(t, asyncEchoBody, testKey, "").Body.Bytes(), &st)
	f.handler.Service.Routes.SetRoutes("full-model", nil) // route gone: transient re-admission failure
	f.startPool(t, 2)

	job := f.awaitStatus(t, st.ID, async.StatusFailed)
	if job.AttemptCount != 2 {
		t.Fatalf("attempt count = %d, want 2 (one backoff retry, one exhaustion)", job.AttemptCount)
	}
	if code := storedErrorCode(t, f, st.ID); code != "no_route_available" {
		t.Fatalf("failure code = %q, want no_route_available", code)
	}
	if got := atomic.LoadInt32(f.calls); got != 0 {
		t.Fatalf("provider calls = %d, want 0 (no route, no execution)", got)
	}
	if n := len(auditsOf(f, "no_route_available")); n != 1 {
		t.Fatalf("no_route audit records = %d, want 1", n)
	}
}

// TestAsyncLimiterDenialExhausts pins the limiter-denial path: a genuine
// rate-limit denial consumes an attempt and retries with backoff; a subject
// denied until the attempts run out terminal-fails with the stable
// rate_limit_exceeded class instead of cycling forever.
func TestAsyncLimiterDenialExhausts(t *testing.T) {
	f := newAsyncFixture(t, asyncOptions{maxAttempts: 2, poolLimiter: limiter.New(0, 100)}) // rate 0: always denied
	var st asyncStatusOut
	_ = json.Unmarshal(f.postCreate(t, asyncEchoBody, testKey, "").Body.Bytes(), &st)
	f.startPool(t, 2)

	job := f.awaitStatus(t, st.ID, async.StatusFailed)
	if job.AttemptCount != 2 {
		t.Fatalf("attempt count = %d, want 2 (one backoff retry, one exhaustion)", job.AttemptCount)
	}
	if code := storedErrorCode(t, f, st.ID); code != "rate_limit_exceeded" {
		t.Fatalf("failure code = %q, want rate_limit_exceeded", code)
	}
	if got := atomic.LoadInt32(f.calls); got != 0 {
		t.Fatalf("provider calls = %d, want 0 (denied before execution)", got)
	}
}

// TestAsyncLeaseLostAuditCarriesJobID pins the correlation contract for the
// sweep's terminal-failure handoff: a job that exhausts its attempts with a
// dead lease never committed a final request ID, so its audit record falls
// back to the client-visible job ID rather than an empty correlation ID.
func TestAsyncLeaseLostAuditCarriesJobID(t *testing.T) {
	f := newAsyncFixture(t, asyncOptions{}) // pool starts after the dead claims
	var st asyncStatusOut
	_ = json.Unmarshal(f.postCreate(t, asyncEchoBody, testKey, "").Body.Bytes(), &st)

	// The "dead" process claims the job and vanishes; each manual recovery
	// round counts the lost attempt. Rounds 1-2 stay under MaxAttempts=3.
	for i := 0; i < 2; i++ {
		if _, ok, err := f.jobs.Claim(context.Background(), async.ClaimInput{
			Owner: fmt.Sprintf("dead-%d", i), Lease: 20 * time.Millisecond, Now: time.Now(),
		}); err != nil || !ok {
			t.Fatalf("dead claim %d: ok=%v err=%v", i, ok, err)
		}
		time.Sleep(40 * time.Millisecond) // let the lease lapse
		out, err := f.jobs.RecoverExpiredLeases(context.Background(), async.RecoverInput{
			MaxAttempts: 3, FailureResult: func(string) async.Result { return async.Result{} },
			Now: time.Now(),
		})
		if err != nil || len(out.Requeued) != 1 || len(out.Failed) != 0 {
			t.Fatalf("recovery round %d = %+v err=%v", i, out, err)
		}
	}
	// Round 3: claim, let the lease lapse, then start the pool — its sweep
	// sees the exhausted job and terminal-fails it with the lease_lost result.
	if _, ok, err := f.jobs.Claim(context.Background(), async.ClaimInput{
		Owner: "dead-final", Lease: 20 * time.Millisecond, Now: time.Now(),
	}); err != nil || !ok {
		t.Fatalf("dead final claim: ok=%v err=%v", ok, err)
	}
	time.Sleep(40 * time.Millisecond)
	f.startPool(t, 3)

	// The sweep-failed job's stored reason carries zero retention, so the
	// observable state is failed (or expired right after, via lazy expiry).
	f.awaitStatus(t, st.ID, async.StatusFailed, async.StatusExpired)
	deadline := time.Now().Add(3 * time.Second)
	var lost []audit.Event
	for time.Now().Before(deadline) {
		if lost = auditsOf(f, "lease_lost"); len(lost) == 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if len(lost) != 1 {
		t.Fatalf("lease_lost audit records = %d, want 1", len(lost))
	}
	if lost[0].RequestID != st.ID || lost[0].TraceID == "" {
		t.Errorf("lease-lost audit must correlate via the job ID, got %+v", lost[0])
	}
}

// otherKey returns the second subject's plaintext bearer key; the fixture
// registers it for subject-other so ownership isolation is exercisable.
const otherKey = "sk-other-plaintext"
