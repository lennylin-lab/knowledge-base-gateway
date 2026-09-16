package httpapi

// Streaming usage settlement and first-token latency tests. Streams whose
// upstream reports usage settle the quota reservation exactly once to the
// reported total; streams without usage keep the conservative reservation;
// audit rows carry known stream tokens and the first-token latency while cost
// stays the staged null contract field.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

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

// settleRecord captures one Settle invocation: exactly-once settlement means
// the handler calls it at most once with the reported total.
type settleRecord struct {
	total *int64
}

// recordingQuota wraps a Gate and records every reservation finalization.
type recordingQuota struct {
	inner   quota.Gate
	mu      sync.Mutex
	settles []settleRecord
	release bool
}

func (g *recordingQuota) Reserve(ctx context.Context, subject string, limits quota.Limits, estimate int64, now time.Time) (quota.Reservation, error) {
	res, err := g.inner.Reserve(ctx, subject, limits, estimate, now)
	if err != nil {
		return nil, err
	}
	return &recordingReservation{g: g, inner: res}, nil
}

func (g *recordingQuota) snapshot() (settles []settleRecord, released bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]settleRecord, len(g.settles))
	copy(out, g.settles)
	return out, g.release
}

type recordingReservation struct {
	g     *recordingQuota
	inner quota.Reservation
}

func (r *recordingReservation) Settle(total *int64) {
	r.g.mu.Lock()
	rec := settleRecord{}
	if total != nil {
		v := *total
		rec.total = &v
	}
	r.g.settles = append(r.g.settles, rec)
	r.g.mu.Unlock()
	r.inner.Settle(total)
}

func (r *recordingReservation) Release() {
	r.g.mu.Lock()
	r.g.release = true
	r.g.mu.Unlock()
	r.inner.Release()
}

// streamUsageBlanket overrides the terminal usage of a stream to simulate an
// upstream that reports no usage on streams (unknown), whatever the inner
// adapter actually parsed.
type streamUsageBlanket struct {
	inner provider.Provider
}

func (u *streamUsageBlanket) Name() string { return u.inner.Name() }

func (u *streamUsageBlanket) Capabilities(m string) model.Capabilities {
	return u.inner.Capabilities(m)
}

func (u *streamUsageBlanket) Embeddings(ctx context.Context, req model.EmbeddingsRequest) (model.EmbeddingsResponse, error) {
	return u.inner.Embeddings(ctx, req)
}

func (u *streamUsageBlanket) Complete(ctx context.Context, req model.Request) (model.Response, error) {
	return u.inner.Complete(ctx, req)
}

func (u *streamUsageBlanket) Stream(ctx context.Context, req model.Request, emit func(model.Event) error) error {
	return u.inner.Stream(ctx, req, func(e model.Event) error {
		if e.Kind == model.EventCompleted && e.Response != nil {
			e.Response.Usage = &model.Usage{Known: false}
		}
		return emit(e)
	})
}

// newStreamUsageChatHandler wires a chat handler with the recording quota
// gate and the given provider.
func newStreamUsageChatHandler(t *testing.T, p provider.Provider) (*ChatHandler, *recordingQuota, *audit.MemorySink) {
	t.Helper()
	store := auth.NewStore()
	salt, err := auth.NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	store.Put(auth.KeyRecord{ID: "key-su", Subject: "subject-su", Salt: salt, Hash: auth.HashAPIKey(salt, testKey), Status: auth.StatusActive})
	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: "gpt-test", Provider: p.Name(), UpstreamModel: "up", Enabled: true, Capabilities: fullCaps},
	})
	pol := policy.New()
	pol.Allow("subject-su", "gpt-test")
	pol.SetLimits("subject-su", policy.Limits{DailyTokens: 1 << 20})
	svc := gateway.New(catalog, map[string]provider.Provider{p.Name(): p}, 2*time.Second, 0)
	sink := audit.NewMemorySink(nil)
	gq := &recordingQuota{inner: quota.NewMemory()}
	h := &ChatHandler{
		Auth: store, Service: svc, Policy: pol,
		Limiter: limiter.New(1000, 100), Quota: gq,
		Audit: sink, Metrics: metrics.New(),
		MaxBody: 1 << 20, MaxMsgs: 64, MaxChars: 32_000,
	}
	return h, gq, sink
}

// newStreamUsageResponsesHandler is the Responses-protocol equivalent.
func newStreamUsageResponsesHandler(t *testing.T, p provider.Provider) (*ResponsesHandler, *recordingQuota, *audit.MemorySink) {
	t.Helper()
	store := auth.NewStore()
	salt, err := auth.NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	store.Put(auth.KeyRecord{ID: "key-sur", Subject: "subject-sur", Salt: salt, Hash: auth.HashAPIKey(salt, testKey), Status: auth.StatusActive})
	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: "full-model", Provider: p.Name(), UpstreamModel: "upstream-full", Enabled: true, Capabilities: fullCaps},
	})
	pol := policy.New()
	pol.Allow("subject-sur", "full-model")
	pol.SetLimits("subject-sur", policy.Limits{DailyTokens: 1 << 20})
	svc := gateway.New(catalog, map[string]provider.Provider{p.Name(): p}, 2*time.Second, 0)
	sink := audit.NewMemorySink(nil)
	gq := &recordingQuota{inner: quota.NewMemory()}
	h := &ResponsesHandler{
		Auth: store, Service: svc, Policy: pol,
		Limiter: limiter.New(1000, 100), Quota: gq,
		Audit: sink, Metrics: metrics.New(),
		MaxBody: 1 << 20, MaxItems: 64, MaxChars: 32_000,
	}
	return h, gq, sink
}

// postResponses posts a raw body to a ResponsesHandler.
func postResponses(t *testing.T, h *ResponsesHandler, body, key, requestID string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	if requestID != "" {
		req.Header.Set("X-Request-ID", requestID)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// The fake provider reports usage deterministically (prompt 10 plus one token
// per output byte), so the "hello" echo ("echo: hello") totals 10 + 11 = 21.
const fakeStreamTotal = int64(21)

// streamEstimate is the reservation the admission pipeline actually charges
// for the plain "hello" stream: the max_tokens clamp from fullCaps, plus the
// deterministic input estimate.
func streamEstimate() int64 {
	capped := fullCaps.MaxOutputTokens
	return int64(quota.Estimate(&capped, 0, len("hello")))
}

func TestChatStreamSettlesQuotaToReportedUsageExactlyOnce(t *testing.T) {
	h, gq, _ := newStreamUsageChatHandler(t, provider.Fake{})
	if rec := doChat(t, h, chatBody("gpt-test", true), testKey, "req-su-1"); rec.Code != http.StatusOK {
		t.Fatalf("stream request: %d %s", rec.Code, rec.Body.String())
	}
	settles, released := gq.snapshot()
	if released {
		t.Fatal("a completed stream must not release its reservation")
	}
	if len(settles) != 1 {
		t.Fatalf("settle calls = %d, want exactly 1", len(settles))
	}
	if settles[0].total == nil || *settles[0].total != fakeStreamTotal {
		t.Fatalf("settled total = %v, want %d", settles[0].total, fakeStreamTotal)
	}
}

func TestResponsesStreamSettlesQuotaToReportedUsageExactlyOnce(t *testing.T) {
	h, gq, _ := newStreamUsageResponsesHandler(t, provider.Fake{})
	if rec := postResponses(t, h, `{"model":"full-model","input":"hello","stream":true}`, testKey, "req-sur-1"); rec.Code != http.StatusOK {
		t.Fatalf("stream request: %d %s", rec.Code, rec.Body.String())
	}
	settles, released := gq.snapshot()
	if released {
		t.Fatal("a completed stream must not release its reservation")
	}
	if len(settles) != 1 {
		t.Fatalf("settle calls = %d, want exactly 1", len(settles))
	}
	// The fake's plain "hello" echo is the text "echo: hello" (11 bytes):
	// 10 + 11 = 21.
	if settles[0].total == nil || *settles[0].total != fakeStreamTotal {
		t.Fatalf("settled total = %v, want %d", settles[0].total, fakeStreamTotal)
	}
}

func TestChatStreamUnknownUsageKeepsConservativeReservation(t *testing.T) {
	// Budget sized so a second estimate never fits once the first conservative
	// reservation is retained (it would fit if the reservation were released).
	// Unknown stream usage must keep the reservation, never settle/release it.
	estimate := streamEstimate()
	store := auth.NewStore()
	salt, err := auth.NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	store.Put(auth.KeyRecord{ID: "k", Subject: "s", Salt: salt, Hash: auth.HashAPIKey(salt, testKey), Status: auth.StatusActive})
	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: "gpt-test", Provider: "fake", UpstreamModel: "up", Enabled: true, Capabilities: fullCaps},
	})
	pol := policy.New()
	pol.Allow("s", "gpt-test")
	pol.SetLimits("s", policy.Limits{DailyTokens: 2*estimate - 1})
	svc := gateway.New(catalog, map[string]provider.Provider{"fake": &streamUsageBlanket{inner: provider.Fake{}}}, 2*time.Second, 0)
	h := &ChatHandler{
		Auth: store, Service: svc, Policy: pol,
		Limiter: limiter.New(1000, 100), Quota: quota.NewMemory(),
		Audit: audit.NewMemorySink(nil), Metrics: metrics.New(),
		MaxBody: 1 << 20, MaxMsgs: 64, MaxChars: 32_000,
	}
	if rec := doChat(t, h, chatBody("gpt-test", true), testKey, "req-su-2"); rec.Code != http.StatusOK {
		t.Fatalf("first stream: %d %s", rec.Code, rec.Body.String())
	}
	if rec := doChat(t, h, chatBody("gpt-test", true), testKey, "req-su-2b"); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("retained conservative reservation must constrain admission: %d %s", rec.Code, rec.Body.String())
	}
}

func TestChatStreamSettledUsageAdmitsNextRequest(t *testing.T) {
	// A budget that fits two streams only when each settles down from the
	// 4096+ estimate to the reported 21 total: retaining the estimate (or
	// skipping settlement) denies the second stream.
	store := auth.NewStore()
	salt, err := auth.NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	store.Put(auth.KeyRecord{ID: "k", Subject: "s", Salt: salt, Hash: auth.HashAPIKey(salt, testKey), Status: auth.StatusActive})
	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: "gpt-test", Provider: "fake", UpstreamModel: "up", Enabled: true, Capabilities: fullCaps},
	})
	pol := policy.New()
	pol.Allow("s", "gpt-test")
	// A budget that admits two streams only when each settles down from the
	// estimate to the reported 21 total: retaining the estimate (or skipping
	// settlement) denies the second stream. Budget = 21 + estimate (the exact
	// post-settlement footprint of two settled streams, with no slack for an
	// unsettled second estimate).
	pol.SetLimits("s", policy.Limits{DailyTokens: fakeStreamTotal + streamEstimate()})
	svc := gateway.New(catalog, map[string]provider.Provider{"fake": provider.Fake{}}, 2*time.Second, 0)
	h := &ChatHandler{
		Auth: store, Service: svc, Policy: pol,
		Limiter: limiter.New(1000, 100), Quota: quota.NewMemory(),
		Audit: audit.NewMemorySink(nil), Metrics: metrics.New(),
		MaxBody: 1 << 20, MaxMsgs: 64, MaxChars: 32_000,
	}
	for i := 0; i < 2; i++ {
		if rec := doChat(t, h, chatBody("gpt-test", true), testKey, ""); rec.Code != http.StatusOK {
			t.Fatalf("stream %d must be admitted after settlement: %d %s", i, rec.Code, rec.Body.String())
		}
	}
}

func TestChatStreamAuditCarriesKnownStreamTokens(t *testing.T) {
	h, _, sink := newStreamUsageChatHandler(t, provider.Fake{})
	if rec := doChat(t, h, chatBody("gpt-test", true), testKey, "req-su-3"); rec.Code != http.StatusOK {
		t.Fatalf("stream request: %d", rec.Code)
	}
	events := sink.Snapshot()
	if len(events) != 1 {
		t.Fatalf("audit events = %d", len(events))
	}
	ev := events[0]
	if !ev.Streaming {
		t.Fatalf("audit event must be streaming: %+v", ev)
	}
	if ev.PromptTokens == nil || *ev.PromptTokens != 10 || ev.CompletionTokens == nil || *ev.CompletionTokens != 11 {
		t.Fatalf("audit tokens = %v/%v, want 10/11 (known, never fabricated)", ev.PromptTokens, ev.CompletionTokens)
	}
	if ev.CostMicros != nil {
		t.Fatalf("cost must stay the staged null field: %+v", ev.CostMicros)
	}
}

func TestChatStreamUnknownUsageAuditStaysNil(t *testing.T) {
	h, _, sink := newStreamUsageChatHandler(t, &streamUsageBlanket{inner: provider.Fake{}})
	if rec := doChat(t, h, chatBody("gpt-test", true), testKey, "req-su-4"); rec.Code != http.StatusOK {
		t.Fatalf("stream request: %d", rec.Code)
	}
	events := sink.Snapshot()
	if len(events) != 1 {
		t.Fatalf("audit events = %d", len(events))
	}
	if events[0].PromptTokens != nil || events[0].CompletionTokens != nil {
		t.Fatalf("unknown stream usage must stay nil in audit: %+v", events[0])
	}
}

func TestResponsesStreamAuditCarriesKnownStreamTokens(t *testing.T) {
	h, _, sink := newStreamUsageResponsesHandler(t, provider.Fake{})
	if rec := postResponses(t, h, `{"model":"full-model","input":"hello","stream":true}`, testKey, "req-sur-2"); rec.Code != http.StatusOK {
		t.Fatalf("stream request: %d", rec.Code)
	}
	events := sink.Snapshot()
	if len(events) != 1 {
		t.Fatalf("audit events = %d", len(events))
	}
	if events[0].PromptTokens == nil || *events[0].PromptTokens != 10 {
		t.Fatalf("audit prompt tokens = %v, want 10", events[0].PromptTokens)
	}
}

func TestChatStreamFirstTokenMillisRecorded(t *testing.T) {
	h, _, sink := newStreamUsageChatHandler(t, provider.Fake{})
	if rec := doChat(t, h, chatBody("gpt-test", true), testKey, "req-su-5"); rec.Code != http.StatusOK {
		t.Fatalf("stream request: %d", rec.Code)
	}
	events := sink.Snapshot()
	if len(events) != 1 {
		t.Fatalf("audit events = %d", len(events))
	}
	if events[0].FirstTokenMillis == nil {
		t.Fatal("a completed stream must record first-token latency")
	}
	if *events[0].FirstTokenMillis < 0 {
		t.Fatalf("first token millis negative: %d", *events[0].FirstTokenMillis)
	}
	if events[0].LatencyMillis < *events[0].FirstTokenMillis {
		t.Fatalf("first token (%d) cannot exceed total latency (%d)", *events[0].FirstTokenMillis, events[0].LatencyMillis)
	}
}

func TestChatNonStreamFirstTokenStaysNull(t *testing.T) {
	// The full-response latency equivalent is LatencyMillis; non-streaming
	// requests record no first-token semantic this task.
	h, _, sink := newStreamUsageChatHandler(t, provider.Fake{})
	if rec := doChat(t, h, chatBody("gpt-test", false), testKey, "req-su-6"); rec.Code != http.StatusOK {
		t.Fatalf("request: %d", rec.Code)
	}
	events := sink.Snapshot()
	if len(events) != 1 || events[0].FirstTokenMillis != nil {
		t.Fatalf("non-streaming first token must stay null: %+v", events)
	}
}

func TestChatStreamedStructuredOutputValidates(t *testing.T) {
	h, _, sink := newStreamUsageChatHandler(t, provider.Fake{})
	body := `{"model":"gpt-test","messages":[{"role":"user","content":"hello"}],"stream":true,` +
		`"response_format":{"type":"json_schema","json_schema":{"name":"answer","schema":{"type":"object","properties":{"echo":{"type":"string"}},"required":["echo"]}}}}`
	rec := doChat(t, h, body, testKey, "req-su-7")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	raw := rec.Body.String()
	if !strings.HasSuffix(raw, "data: [DONE]\n\n") {
		t.Fatalf("stream must end with [DONE]")
	}
	var payload strings.Builder
	for _, line := range strings.Split(raw, "\n\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data: ") || line == "data: [DONE]" {
			continue
		}
		var chunk chatCompletionOut
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &chunk); err != nil {
			t.Fatal(err)
		}
		for _, c := range chunk.Choices {
			if c.Delta != nil {
				payload.WriteString(c.Delta.Content)
			}
		}
	}
	if payload.String() != `{"echo":"hello"}` {
		t.Fatalf("streamed structured output = %q", payload.String())
	}
	for _, ev := range sink.Snapshot() {
		if ev.ErrorClass != "" {
			t.Fatalf("valid streamed structured output must not be audited as a failure: %+v", ev)
		}
	}
}
