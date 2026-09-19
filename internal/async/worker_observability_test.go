package async

// Worker lifecycle observability tests: the shutdown ordering is observable
// (worker health gauge, inflight gauge, stage logs), Stop admits no new
// claims, the drain window is bounded, aborted jobs hand their leases back
// without a terminal commit (lease recovery), and a job that commits inside
// the drain window finalizes exactly once.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/gateway"
	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
	"github.com/knowledge-base/knowledge-base-gateway/internal/metrics"
	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
	"github.com/knowledge-base/knowledge-base-gateway/internal/quota"
)

// blockingProvider holds the execution inside Complete until released, so
// tests decide when the drain window runs.
type blockingProvider struct {
	inner   provider.Provider
	entered chan struct{}
	release chan struct{}
}

func (b *blockingProvider) Name() string                             { return b.inner.Name() }
func (b *blockingProvider) Capabilities(m string) model.Capabilities { return b.inner.Capabilities(m) }
func (b *blockingProvider) Embeddings(ctx context.Context, req model.EmbeddingsRequest) (model.EmbeddingsResponse, error) {
	return b.inner.Embeddings(ctx, req)
}

func (b *blockingProvider) Complete(ctx context.Context, req model.Request) (model.Response, error) {
	b.entered <- struct{}{}
	select {
	case <-b.release:
	case <-ctx.Done():
		// aborted (drain timeout or cancel): a cancellation-honoring
		// upstream stops and reports the abort instead of returning output.
		return model.Response{}, ctx.Err()
	}
	return model.Response{
		Output:       []model.OutputItem{{Kind: model.OutputText, Text: "done"}},
		FinishReason: model.FinishStop,
		Status:       model.StatusCompleted,
		Usage:        &model.Usage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2, Known: true},
	}, nil
}

func (b *blockingProvider) Stream(ctx context.Context, req model.Request, emit func(model.Event) error) error {
	return b.inner.Stream(ctx, req, emit)
}

// testEncoder is the minimal ResultEncoder for worker tests (the real one
// lives in the HTTP layer, which cannot be imported here).
type testEncoder struct{}

func (testEncoder) EncodeResult(publicModel, id string, resp model.Response, _ []byte) ([]byte, error) {
	return json.Marshal(map[string]any{"id": id, "status": "completed"})
}

func (testEncoder) EncodeFailure(publicModel, id, class string) ([]byte, error) {
	return json.Marshal(map[string]any{"id": id, "status": "failed", "error": class})
}

// gaugeValue scrapes the exposition for one gauge's current value.
func gaugeValue(t *testing.T, reg *metrics.Registry, name string) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if strings.HasPrefix(line, name+" ") {
			v, err := strconv.ParseFloat(strings.TrimSpace(line[len(name)+1:]), 64)
			if err != nil {
				t.Fatalf("parse %s: %v", line, err)
			}
			return v
		}
	}
	t.Fatalf("gauge %s missing from exposition", name)
	return 0
}

// observabilityFixture wires a worker pool over the memory store with a
// blocking provider behind the gateway service.
type observabilityFixture struct {
	store    *MemoryStore
	pool     *Pool
	provider *blockingProvider
	sink     *audit.MemorySink
	reg      *metrics.Registry
}

func newObservabilityFixture(t *testing.T) *observabilityFixture {
	t.Helper()
	blocked := &blockingProvider{
		inner: provider.Fake{}, entered: make(chan struct{}, 1), release: make(chan struct{}, 1),
	}
	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: "m", Provider: blocked.Name(), UpstreamModel: "up", Enabled: true},
	})
	svc := gateway.New(catalog, map[string]provider.Provider{blocked.Name(): blocked}, 5*time.Second, 0)
	pol := policy.New()
	pol.Allow("s", "m")
	sink := audit.NewMemorySink(nil)
	reg := metrics.New()
	store := NewMemoryStore(nil)
	pool := NewPool(PoolDeps{
		Store: store, Service: svc, Policy: pol,
		Limiter: limiter.New(1000, 100), Quota: quota.NewMemory(),
		Audit: sink, Metrics: reg, Cancels: NewCancelRegistry(),
		Encoder: testEncoder{},
	}, PoolConfig{
		WorkerID: "worker-obs", Count: 1, PollInterval: 2 * time.Millisecond,
		Lease: 30 * time.Second, JobTimeout: 5 * time.Second,
		MaxAttempts: 3, ResultTTL: time.Hour, MaxResultBytes: 1 << 20,
	})
	pool.Start(context.Background())
	t.Cleanup(func() { pool.Stop(2 * time.Second) })
	return &observabilityFixture{store: store, pool: pool, provider: blocked, sink: sink, reg: reg}
}

func (f *observabilityFixture) enqueue(t *testing.T, id string) {
	t.Helper()
	snap, err := EncodeSnapshot(model.Request{
		PublicModel: "m",
		Input:       []model.InputItem{{Role: "user", Text: "probe"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Create(context.Background(), CreateInput{
		JobID: id, SubjectID: "s", TenantID: "tn", Protocol: "responses",
		PublicModel: "m", RequestDigest: "d-" + id, Request: snap, Now: time.Now(),
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	f.pool.Wake()
}

// TestWorkerHealthGaugeLifecycle pins the worker health/inflight gauges: 1
// while live, 0 once stopped; inflight tracks the executing job.
func TestWorkerHealthGaugeLifecycle(t *testing.T) {
	f := newObservabilityFixture(t)
	if got := gaugeValue(t, f.reg, "gateway_async_worker_healthy"); got != 1 {
		t.Fatalf("worker healthy after start = %v, want 1", got)
	}

	f.enqueue(t, "job-inflight")
	<-f.provider.entered
	if got := gaugeValue(t, f.reg, "gateway_async_worker_inflight"); got != 1 {
		t.Fatalf("worker inflight during execution = %v, want 1", got)
	}
	f.provider.release <- struct{}{}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := f.store.Get(context.Background(), "job-inflight", time.Now()); err == nil {
			if j, err := f.store.Get(context.Background(), "job-inflight", time.Now()); err == nil && j.Status == StatusCompleted {
				break
			}
		}
		time.Sleep(time.Millisecond)
	}
	if got := gaugeValue(t, f.reg, "gateway_async_worker_inflight"); got != 0 {
		t.Fatalf("worker inflight after completion = %v, want 0", got)
	}

	f.pool.Stop(time.Second)
	if got := gaugeValue(t, f.reg, "gateway_async_worker_healthy"); got != 0 {
		t.Fatalf("worker healthy after stop = %v, want 0", got)
	}
}

// TestStopOrderedShutdown is the acceptance-criteria shutdown test: after
// Stop, no new job is ever claimed; the drain is bounded; the aborted job's
// lease is returned to the queue without a terminal commit or finalization;
// a job completing inside the drain window finalizes exactly once.
func TestStopOrderedShutdown(t *testing.T) {
	f := newObservabilityFixture(t)
	ctx := context.Background()

	// Job A completes inside the drain window: exactly one finalization.
	f.enqueue(t, "job-drain-commit")
	<-f.provider.entered
	f.provider.release <- struct{}{}

	// Job B is still executing when Stop's drain elapses: abort + hand-back.
	f.enqueue(t, "job-abort")
	<-f.provider.entered

	drain := 20 * time.Millisecond
	start := time.Now()
	f.pool.Stop(drain)
	elapsed := time.Since(start)
	if elapsed < drain {
		t.Fatalf("Stop returned before its drain window elapsed: %v", elapsed)
	}
	if elapsed > 10*time.Second {
		t.Fatalf("Stop is not bounded: %v", elapsed)
	}

	// Job A: committed exactly once during drain.
	a, err := f.store.Get(ctx, "job-drain-commit", time.Now())
	if err != nil || a.Status != StatusCompleted {
		t.Fatalf("job A status = %v err=%v, want completed", a.Status, err)
	}
	if _, err := f.store.Result(ctx, "job-drain-commit"); err != nil {
		t.Fatalf("job A result must exist exactly once: %v", err)
	}
	var aAudits int
	for _, ev := range f.sink.Snapshot() {
		if ev.RequestID == a.FinalRequestID {
			aAudits++
		}
	}
	if aAudits != 1 {
		t.Fatalf("job A audit records = %d, want exactly 1", aAudits)
	}

	// Job B: abort hand-back — queued again, lease cleared, no attempt
	// consumed (abort requeue is free), no result and no audit record.
	b, err := f.store.Get(ctx, "job-abort", time.Now())
	if err != nil {
		t.Fatalf("job B get: %v", err)
	}
	if b.Status != StatusQueued {
		t.Fatalf("aborted job must return to the queue, got %s", b.Status)
	}
	if b.LeaseOwner != "" || !b.LeaseExpiresAt.IsZero() {
		t.Fatalf("aborted job must not hold a lease: %+v", b)
	}
	if b.AttemptCount != 0 {
		t.Fatalf("abort hand-back must not consume an attempt, got %d", b.AttemptCount)
	}
	if _, err := f.store.Result(ctx, "job-abort"); err == nil {
		t.Fatal("aborted job must have no terminal result (no early finalization)")
	}
	for _, ev := range f.sink.Snapshot() {
		if ev.RequestID == "job-abort" {
			t.Fatalf("aborted job must not finalize an audit record: %+v", ev)
		}
	}
	if got := gaugeValue(t, f.reg, "gateway_async_worker_inflight"); got != 0 {
		t.Fatalf("inflight after shutdown = %v, want 0", got)
	}

	// No new claims after Stop: a fresh queued job stays queued forever.
	f2 := newObservabilityFixture(t)
	f2.pool.Stop(time.Second) // stop the fresh pool's redundant worker first
	f2.enqueue(t, "job-after-stop")
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		j, err := f2.store.Get(ctx, "job-after-stop", time.Now())
		if err != nil {
			t.Fatalf("get after stop: %v", err)
		}
		if j.Status != StatusQueued {
			t.Fatalf("stopped pool claimed a job: %s", j.Status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
