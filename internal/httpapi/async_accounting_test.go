package httpapi

// Child-3 settlement handoff tests: the accounting integration of the
// background-job worker. The centerpiece is the cancel-versus-settlement
// review handoff — when a client cancel wins the store transition AFTER the
// provider produced reported usage, the losing worker must settle the job by
// its known usage (ledger row with cost, quota settled to the reported
// total) instead of dropping it, while writing no second audit record. Any
// other lost race (lease recovery → re-execution) still releases, because
// the retry re-reserves and settles.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/accounting"
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

// ledgerStub is the in-memory accounting.Store double used by the worker
// tests: it records the ledger lifecycle and serves one configured price.
type ledgerStub struct {
	mu           sync.Mutex
	price        accounting.Price
	hasPrice     bool
	budgetLimits accounting.BudgetLimits
	reserved     map[string]bool // identity key -> reserved
	settled      []accounting.SettleQuery
	released     []string
	failSettle   atomic.Bool
}

func newLedgerStub(price accounting.Price) *ledgerStub {
	return &ledgerStub{
		price: price, hasPrice: true,
		reserved: map[string]bool{},
	}
}

func identityKey(id accounting.Identity) string { return id.RequestID + "\x00" + id.JobID }

func (s *ledgerStub) EffectivePrice(context.Context, string, string, time.Time) (accounting.Price, bool, error) {
	if s.hasPrice {
		return s.price, true, nil
	}
	return accounting.Price{}, false, nil
}

func (s *ledgerStub) BudgetLimits(context.Context, string, string) (accounting.BudgetLimits, error) {
	return s.budgetLimits, nil
}

func (s *ledgerStub) ReserveLedger(_ context.Context, row accounting.LedgerReservation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reserved[identityKey(row.Identity)] = true
	return nil
}

func (s *ledgerStub) SettleLedger(_ context.Context, q accounting.SettleQuery) (accounting.SettleOutcome, error) {
	if s.failSettle.Load() {
		return accounting.SettleOutcome{}, errors.New("ledger down")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settled = append(s.settled, q)
	delete(s.reserved, identityKey(q.Identity))
	return accounting.SettleOutcome{Settled: true}, nil
}

func (s *ledgerStub) ReleaseLedger(_ context.Context, id accounting.Identity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.released = append(s.released, identityKey(id))
	delete(s.reserved, identityKey(id))
	return nil
}

func (s *ledgerStub) settledFor(jobID string) []accounting.SettleQuery {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []accounting.SettleQuery
	for _, q := range s.settled {
		if q.Identity.JobID == jobID {
			out = append(out, q)
		}
	}
	return out
}

func (s *ledgerStub) releasedFor(jobID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, k := range s.released {
		if strings.HasSuffix(k, "\x00"+jobID) {
			n++
		}
	}
	return n
}

// acctRecordingQuota wraps a quota.Gate and records finalization calls so the
// tests can pin settle/release behavior of the token reservation.
type acctRecordingQuota struct {
	inner quota.Gate
	mu    sync.Mutex
	keep  []*acctRecordingReservation
}

type acctRecordingReservation struct {
	q        *acctRecordingQuota
	inner    quota.Reservation
	settles  []*int64
	releases int
}

func (g *acctRecordingQuota) Reserve(ctx context.Context, subject string, limits quota.Limits, estimate int64, now time.Time) (quota.Reservation, error) {
	inner, err := g.inner.Reserve(ctx, subject, limits, estimate, now)
	if err != nil {
		return nil, err
	}
	r := &acctRecordingReservation{q: g, inner: inner}
	g.mu.Lock()
	g.keep = append(g.keep, r)
	g.mu.Unlock()
	return r, nil
}

func (r *acctRecordingReservation) Settle(total *int64) {
	r.q.mu.Lock()
	r.settles = append(r.settles, total)
	r.q.mu.Unlock()
	r.inner.Settle(total)
}

func (r *acctRecordingReservation) Release() {
	r.q.mu.Lock()
	r.releases++
	r.q.mu.Unlock()
	r.inner.Release()
}

// gatedProvider blocks Complete on a channel and then returns a successful
// response with reported usage — even when its context was cancelled — so
// the test can decide the store race (cancel or recovery) before the worker
// reaches its terminal commit.
type gatedProvider struct {
	inner   provider.Provider
	entered chan struct{}
	release chan struct{}
}

func (g *gatedProvider) Name() string                             { return g.inner.Name() }
func (g *gatedProvider) Capabilities(m string) model.Capabilities { return g.inner.Capabilities(m) }
func (g *gatedProvider) Embeddings(ctx context.Context, req model.EmbeddingsRequest) (model.EmbeddingsResponse, error) {
	return g.inner.Embeddings(ctx, req)
}

func (g *gatedProvider) Complete(ctx context.Context, req model.Request) (model.Response, error) {
	g.entered <- struct{}{}
	select {
	case <-g.release:
	case <-ctx.Done():
		// The cancel registry fired, but the upstream had already produced
		// the billable output: return it, usage included (the at-least-once
		// upstream contract).
	}
	text := req.LastUserText()
	n := int64(len(text))
	resp := model.Response{
		Output:       []model.OutputItem{{Kind: model.OutputText, Text: "done: " + text}},
		FinishReason: model.FinishStop,
		Status:       model.StatusCompleted,
		Usage: &model.Usage{
			PromptTokens: 10, CompletionTokens: int(n), TotalTokens: 10 + int(n), Known: true,
		},
	}
	return resp, nil
}

func (g *gatedProvider) Stream(ctx context.Context, req model.Request, emit func(model.Event) error) error {
	return g.inner.Stream(ctx, req, emit)
}

// accountingFixture wires the background-job stack with the accounting gate.
type accountingFixture struct {
	handler  *ResponsesHandler
	jobsMux  *http.ServeMux
	jobs     async.Store
	sink     *audit.MemorySink
	pool     *async.Pool
	provider *gatedProvider
	ledger   *ledgerStub
	quota    *acctRecordingQuota
}

func newAccountingFixture(t *testing.T, inner provider.Provider, price accounting.Price) *accountingFixture {
	t.Helper()
	gated := &gatedProvider{
		inner: inner, entered: make(chan struct{}, 1), release: make(chan struct{}, 1),
	}
	ledger := newLedgerStub(price)
	gate := &accounting.Gate{
		Store: ledger, Budgets: accounting.NewMemoryBudget(),
		Enforcement: true, Now: time.Now,
	}
	rq := &acctRecordingQuota{inner: quota.NewMemory()}

	keyStore := auth.NewStore()
	salt, err := auth.NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	keyStore.Put(auth.KeyRecord{ID: "key-a", Subject: "subject-r", Salt: salt,
		Hash: auth.HashAPIKey(salt, testKey), Status: auth.StatusActive})
	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: "full-model", Provider: gated.Name(), UpstreamModel: "upstream-full", Enabled: true, Capabilities: fullCaps},
	})
	pol := policy.New()
	pol.Allow("subject-r", "full-model")
	// A configured token budget so the quota gate actually reserves (the
	// same dual-reserve the production worker runs: tokens then money).
	pol.SetLimits("subject-r", policy.Limits{DailyTokens: 1_000_000})
	svc := gateway.New(catalog, map[string]provider.Provider{gated.Name(): gated}, 5*time.Second, 0)
	sink := audit.NewMemorySink(nil)
	cancels := async.NewCancelRegistry()
	jobs := async.NewMemoryStore(nil)
	bundle := &Async{
		Jobs: jobs, Cancels: cancels, Wake: func() {},
		ResultTTL: time.Hour, KeyTTL: time.Hour, MaxResultBytes: 1 << 20, MaxKeyBytes: 256,
	}
	handler := &ResponsesHandler{
		Auth: keyStore, Service: svc, Policy: pol, Limiter: limiter.New(1000, 100),
		Quota: rq, Accounting: gate, Audit: sink, Metrics: metrics.New(),
		MaxBody: 1 << 20, MaxItems: 64, MaxChars: 32_000, Async: bundle,
	}
	jobsAPI := &AsyncJobsHandler{
		Auth: keyStore, Jobs: jobs, Cancels: cancels, Audit: sink, Metrics: metrics.New(),
		PollHint: time.Second,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/responses/{id}", jobsAPI.ServeHTTP)
	mux.HandleFunc("POST /v1/responses/{id}/cancel", jobsAPI.ServeHTTP)
	pool := async.NewPool(async.PoolDeps{
		Store: jobs, Service: svc, Policy: pol,
		Limiter: limiter.New(1000, 100), Quota: rq, Accounting: gate,
		Audit: sink, Metrics: metrics.New(), Cancels: cancels,
		Encoder: NewAsyncEncoder(),
	}, async.PoolConfig{
		WorkerID: "worker-acct", Count: 1, PollInterval: 2 * time.Millisecond,
		Lease: 30 * time.Second, JobTimeout: 5 * time.Second,
		MaxAttempts: 3, ResultTTL: time.Hour, MaxResultBytes: 1 << 20, Drain: 2 * time.Second,
	})
	bundle.Wake = pool.Wake
	pool.Start(context.Background())
	t.Cleanup(func() { pool.Stop(2 * time.Second) })
	return &accountingFixture{handler: handler, jobsMux: mux, jobs: jobs, sink: sink, pool: pool, provider: gated, ledger: ledger, quota: rq}
}

func (f *accountingFixture) create(t *testing.T) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/responses",
		strings.NewReader(`{"model":"full-model","input":"accounting-probe","background":true}`))
	req.Header.Set("Authorization", "Bearer "+testKey)
	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	id := decodeJobID(t, rec)
	<-f.provider.entered // the worker claimed the job and entered the provider
	return id
}

func (f *accountingFixture) cancel(t *testing.T, id string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/responses/"+id+"/cancel", nil)
	req.Header.Set("Authorization", "Bearer "+testKey)
	rec := httptest.NewRecorder()
	f.jobsMux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("cancel: %d %s", rec.Code, rec.Body.String())
	}
}

func (f *accountingFixture) awaitTerminal(t *testing.T, id string, want async.Status) async.Job {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		job, err := f.jobs.Get(context.Background(), id, time.Now())
		if err == nil && job.Status == want {
			return job
		}
		time.Sleep(time.Millisecond)
	}
	job, _ := f.jobs.Get(context.Background(), id, time.Now())
	t.Fatalf("job never reached %s (now %s)", want, job.Status)
	return async.Job{}
}

func (s *ledgerStub) releasesTotal() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.released)
}

// awaitSettlement polls until the job has exactly one settled ledger row:
// the loser's handoff lands asynchronously after the terminal commit loses.
func (f *accountingFixture) awaitSettlement(t *testing.T, id string) []accounting.SettleQuery {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if settled := f.ledger.settledFor(id); len(settled) == 1 {
			return settled
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("job %s never settled exactly once (settled %d)", id, len(f.ledger.settledFor(id)))
	return nil
}

func decodeJobID(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	body := rec.Body.String()
	marker := `"id":"`
	i := strings.Index(body, marker)
	if i < 0 {
		t.Fatalf("no job id in %s", body)
	}
	rest := body[i+len(marker):]
	return rest[:strings.Index(rest, `"`)]
}

// TestAsyncCancelAfterOutputSettlesKnownUsage is the child-3 handoff pin:
// cancel wins while the provider is producing; the worker loses the commit
// race but settles the job by the usage it observed — one settled ledger row
// with the computed cost, the token quota settled to the reported total (not
// released), and exactly one audit record (the cancellation, from the cancel
// winner — the loser writes none).
func TestAsyncCancelAfterOutputSettlesKnownUsage(t *testing.T) {
	price := accounting.Price{Version: 2, Currency: "USD", InputPerToken: 10, OutputPerToken: 20}
	f := newAccountingFixture(t, provider.Fake{}, price)
	id := f.create(t)

	f.cancel(t, id)                  // cancel wins its store transition, aborts the exec ctx
	f.provider.release <- struct{}{} // the provider returns the billable output anyway
	f.awaitTerminal(t, id, async.StatusCancelled)
	settled := f.awaitSettlement(t, id) // the loser's handoff lands asynchronously

	// The loser settled the known usage instead of dropping it.
	q := settled[0]
	if q.Usage.PromptTokens == nil || *q.Usage.PromptTokens != 10 {
		t.Fatalf("usage must be recorded: %+v", q.Usage)
	}
	wantCost := 10*price.InputPerToken + int64(len("accounting-probe"))*price.OutputPerToken
	if q.CostMicros == nil || *q.CostMicros != wantCost {
		t.Fatalf("cost: want %d, got %+v", wantCost, q.CostMicros)
	}
	if q.PriceVersion == nil || *q.PriceVersion != price.Version || q.Currency != price.Currency {
		t.Fatalf("settlement must snapshot the price version: %+v", q)
	}

	// The token quota was settled to the reported total, never released.
	// keep[0] is the creation probe (reserved, released), keep[1] the
	// execution reservation the losing worker settled.
	f.quota.mu.Lock()
	if len(f.quota.keep) != 2 {
		f.quota.mu.Unlock()
		t.Fatalf("probe plus execution reservations expected: %d", len(f.quota.keep))
	}
	res := f.quota.keep[1]
	settles, releases := len(res.settles), res.releases
	var settledTotal *int64
	if settles == 1 {
		settledTotal = res.settles[0]
	}
	f.quota.mu.Unlock()
	if settles != 1 || settledTotal == nil || *settledTotal != 10+int64(len("accounting-probe")) {
		t.Fatalf("quota settle: settles=%d %+v", settles, settledTotal)
	}
	if releases != 0 {
		t.Fatalf("cancel-after-output must not release the quota: %d releases", releases)
	}

	// Exactly one audit record for the job: the cancellation (the winner's).
	var cancelled int
	for _, ev := range f.sink.Snapshot() {
		if ev.ErrorClass == "cancelled" {
			cancelled++
		}
	}
	if cancelled != 1 {
		t.Fatalf("exactly one cancellation audit: %d", cancelled)
	}
}

// TestAsyncRecoveryLossStillReleases: when the lost commit race is a lease
// recovery (the job returns to the queue and will re-execute), the loser
// releases both reservations and writes no settlement — the retry settles.
func TestAsyncRecoveryLossStillReleases(t *testing.T) {
	price := accounting.Price{Version: 2, Currency: "USD", InputPerToken: 10, OutputPerToken: 20}
	f := newAccountingFixture(t, provider.Fake{}, price)
	id := f.create(t)

	// Simulate the recovery sweep winning the race: return the job to the
	// queue (fresh lease) while the worker is inside the provider call, so
	// the worker's CommitSuccess loses and the job re-executes.
	job, err := f.jobs.Get(context.Background(), id, time.Now())
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	ok, err := f.jobs.Requeue(context.Background(), id, job.LeaseOwner, async.RequeueOptions{}, time.Now())
	if err != nil || !ok {
		t.Fatalf("simulate recovery: ok=%v err=%v", ok, err)
	}
	f.provider.release <- struct{}{} // first execution returns; its commit loses
	<-f.provider.entered             // the recovered job re-executes
	f.provider.release <- struct{}{}
	// The recovered job re-executes on the pool; wait for the completed
	// outcome of the retry (usage known, settlement written by the winner).
	f.awaitTerminal(t, id, async.StatusCompleted)
	f.awaitSettlement(t, id)

	if got := f.ledger.settledFor(id); len(got) != 1 {
		t.Fatalf("the retry's winner settles exactly once: %d", len(got))
	}
	// keep[0] creation probe, keep[1] the aborted first execution (the
	// recovery loser), keep[2] the retry (settled by the winner).
	f.quota.mu.Lock()
	defer f.quota.mu.Unlock()
	if len(f.quota.keep) != 3 {
		t.Fatalf("probe plus two executions expected: %d reservations", len(f.quota.keep))
	}
	loser := f.quota.keep[1]
	if loser.releases == 0 {
		t.Fatal("the recovery loser must release its reservation")
	}
	if len(loser.settles) != 0 {
		t.Fatalf("the recovery loser must not settle: %+v", loser.settles)
	}
}

// TestAsyncCreateReleasesAdmissionProbes: creation reserves and immediately
// refunds (the worker re-reserves at execution time), and the probe leaves
// exactly the reserved-then-released ledger evidence.
func TestAsyncCreateReleasesAdmissionProbes(t *testing.T) {
	price := accounting.Price{Version: 2, Currency: "USD", InputPerToken: 10, OutputPerToken: 20}
	f := newAccountingFixture(t, provider.Fake{}, price)
	id := f.create(t)
	f.provider.release <- struct{}{}
	f.awaitTerminal(t, id, async.StatusCompleted)

	settled := f.ledger.settledFor(id)
	if len(settled) != 1 || settled[0].CostMicros == nil {
		t.Fatalf("execution settles exactly once with cost: %+v", settled)
	}
	// The creation probe (identity by request ID, unrelated to the job)
	// released before execution; exactly one release remains as evidence.
	if n := f.ledger.releasesTotal(); n != 1 {
		t.Fatalf("creation probe release: %d", n)
	}
}
