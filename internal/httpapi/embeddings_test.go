package httpapi

// Tests for POST /v1/embeddings: admission order, capability gate, quota
// settle semantics on the shared subject token pool, dimension gate, and the
// rollback switch.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

const embedModel = "embed-test"

// embedCaps declares the embeddings matrix for the test catalog: the fake
// provider's deterministic vectors must match the declared dim.
var embedCaps = model.Capabilities{
	Chat: true, Responses: true, Embeddings: true, Stream: true, Usage: true,
	ContextTokens: 8192, MaxOutputTokens: 2048, EmbeddingDim: provider.FakeEmbeddingDim,
}

// unknownUsageEmbeddings replaces the fake's reported usage with unknown
// usage (quota keeps the conservative reservation).
type unknownUsageEmbeddings struct {
	inner provider.Provider
	calls int
}

func (u *unknownUsageEmbeddings) Name() string { return u.inner.Name() }

func (u *unknownUsageEmbeddings) Capabilities(m string) model.Capabilities {
	return u.inner.Capabilities(m)
}

func (u *unknownUsageEmbeddings) Complete(ctx context.Context, req model.Request) (model.Response, error) {
	return u.inner.Complete(ctx, req)
}

func (u *unknownUsageEmbeddings) Stream(ctx context.Context, req model.Request, emit func(model.Event) error) error {
	return u.inner.Stream(ctx, req, emit)
}

func (u *unknownUsageEmbeddings) Embeddings(ctx context.Context, req model.EmbeddingsRequest) (model.EmbeddingsResponse, error) {
	u.calls++
	resp, err := u.inner.Embeddings(ctx, req)
	if err != nil {
		return resp, err
	}
	resp.Usage = &model.Usage{Known: false}
	return resp, nil
}

// fakeEmbeddingsUsage is the deterministic fake embeddings usage for one
// request: 10 prompt tokens plus one token per four input bytes.
func fakeEmbeddingsUsage(inputChars int) int {
	return 10 + (inputChars+3)/4
}

// embedEstimate is the conservative embeddings reservation: the deterministic
// input estimate alone (embeddings usage is input-token only).
func embedEstimate(inputChars int) int64 {
	return quota.InputTokens(inputChars)
}

type embeddingsFixture struct {
	h           *EmbeddingsHandler
	sink        *audit.MemorySink
	embedCalls  int
	lastCatalog *policy.Catalog
}

func newEmbeddingsFixture(t *testing.T, caps model.Capabilities, p provider.Provider, limits policy.Limits) *embeddingsFixture {
	t.Helper()
	store := auth.NewStore()
	salt, err := auth.NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	store.Put(auth.KeyRecord{ID: "key-e", Subject: "subject-e", Salt: salt, Hash: auth.HashAPIKey(salt, testKey), Status: auth.StatusActive})
	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: embedModel, Provider: p.Name(), UpstreamModel: "upstream-embed", Enabled: true, Capabilities: caps},
	})
	pol := policy.New()
	pol.Allow("subject-e", embedModel)
	if limits != (policy.Limits{}) {
		pol.SetLimits("subject-e", limits)
	}
	svc := gateway.New(catalog, map[string]provider.Provider{p.Name(): p}, 2*time.Second, 0)
	sink := audit.NewMemorySink(nil)
	h := &EmbeddingsHandler{
		Auth: store, Service: svc, Policy: pol,
		Limiter: limiter.New(1000, 100), Quota: quota.NewMemory(), Audit: sink, Metrics: metrics.New(),
		MaxBody: 1 << 20, MaxItems: 128, MaxChars: 32_000,
	}
	return &embeddingsFixture{h: h, sink: sink, lastCatalog: catalog}
}

func doEmbeddings(t *testing.T, h *EmbeddingsHandler, body, key, requestID string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/embeddings", strings.NewReader(body))
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

func embeddingsBody(input string) string {
	return `{"model":"` + embedModel + `","input":"` + input + `"}`
}

func TestEmbeddingsHappyPath(t *testing.T) {
	f := newEmbeddingsFixture(t, embedCaps, provider.Fake{}, policy.Limits{})
	rec := doEmbeddings(t, f.h, embeddingsBody("hello"), testKey, "req-emb-1")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp embeddingsResponseOut
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Object != "list" || resp.Model != embedModel {
		t.Fatalf("envelope = %+v", resp)
	}
	if len(resp.Data) != 1 || resp.Data[0].Object != "embedding" || resp.Data[0].Index != 0 {
		t.Fatalf("data = %+v", resp.Data)
	}
	if len(resp.Data[0].Embedding) != provider.FakeEmbeddingDim {
		t.Fatalf("vector width = %d, want %d", len(resp.Data[0].Embedding), provider.FakeEmbeddingDim)
	}
	if resp.Usage == nil || resp.Usage.PromptTokens != fakeEmbeddingsUsage(5) || resp.Usage.TotalTokens != fakeEmbeddingsUsage(5) {
		t.Fatalf("usage = %+v, want prompt-only %d", resp.Usage, fakeEmbeddingsUsage(5))
	}
	if rec.Header().Get("X-Request-ID") != "req-emb-1" {
		t.Error("request id not echoed")
	}
	// Audit records metadata only: resolved model, protocol label, and
	// reported input-token usage — never the vector content.
	events := f.sink.Snapshot()
	if len(events) != 1 {
		t.Fatalf("audit events = %d", len(events))
	}
	ev := events[0]
	if ev.Model != embedModel || ev.Protocol != "embeddings" || ev.Status != 200 || ev.SubjectID != "subject-e" {
		t.Fatalf("audit event = %+v", ev)
	}
	if ev.PromptTokens == nil || *ev.PromptTokens != fakeEmbeddingsUsage(5) {
		t.Fatalf("audit prompt tokens = %v", ev.PromptTokens)
	}
	for _, raw := range []string{"sk-test-plaintext"} {
		if strings.Contains(rec.Body.String(), raw) {
			t.Errorf("response leaks %q", raw)
		}
	}
}

func TestEmbeddingsBatchInput(t *testing.T) {
	f := newEmbeddingsFixture(t, embedCaps, provider.Fake{}, policy.Limits{})
	rec := doEmbeddings(t, f.h, `{"model":"`+embedModel+`","input":["alpha","beta"]}`, testKey, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp embeddingsResponseOut
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Data) != 2 || resp.Data[0].Index != 0 || resp.Data[1].Index != 1 {
		t.Fatalf("batch data = %+v", resp.Data)
	}
}

func TestEmbeddingsCapabilityRejectedPreProvider(t *testing.T) {
	// The catalog matrix omits embeddings: rejection happens before any
	// provider work (the fake would answer anything).
	p := &unknownUsageEmbeddings{inner: provider.Fake{}}
	f := newEmbeddingsFixture(t, testCaps(), p, policy.Limits{})
	rec := doEmbeddings(t, f.h, embeddingsBody("hello"), testKey, "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "capability_not_supported") {
		t.Fatalf("wrong code: %s", rec.Body.String())
	}
	if f.embedCalls != 0 || p.calls != 0 {
		t.Fatalf("provider embeddings calls = %d/%d, want 0", f.embedCalls, p.calls)
	}
}

func TestEmbeddingsAuthBeforeDecode(t *testing.T) {
	f := newEmbeddingsFixture(t, embedCaps, provider.Fake{}, policy.Limits{})
	rec := doEmbeddings(t, f.h, `{"model":"`+embedModel+`","input":not-json`, "sk-wrong", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 before any body decode", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "invalid_api_key") {
		t.Fatalf("wrong envelope: %s", rec.Body.String())
	}
}

func TestEmbeddingsValidation(t *testing.T) {
	f := newEmbeddingsFixture(t, embedCaps, provider.Fake{}, policy.Limits{})
	cases := map[string]string{
		"missing input":    `{"model":"` + embedModel + `"}`,
		"empty input":      `{"model":"` + embedModel + `","input":""}`,
		"empty array":      `{"model":"` + embedModel + `","input":[]}`,
		"non-string item":  `{"model":"` + embedModel + `","input":["ok",5]}`,
		"object input":     `{"model":"` + embedModel + `","input":{"a":1}}`,
		"stream field":     `{"model":"` + embedModel + `","input":"x","stream":true}`,
		"stream null ok":   "",
		"tools field":      `{"model":"` + embedModel + `","input":"x","tools":[]}`,
		"response_format":  `{"model":"` + embedModel + `","input":"x","response_format":{"type":"json_object"}}`,
		"max_tokens":       `{"model":"` + embedModel + `","input":"x","max_tokens":16}`,
		"input too long":   `{"model":"` + embedModel + `","input":"` + strings.Repeat("a", 40_000) + `"}`,
		"unknown ignored":  "",
		"trailing garbage": embeddingsBody("hello") + " tail",
	}
	for name, body := range cases {
		switch name {
		case "stream null ok":
			body = `{"model":"` + embedModel + `","input":"x","stream":null}`
			rec := doEmbeddings(t, f.h, body, testKey, "")
			if rec.Code != http.StatusOK {
				t.Errorf("%s: explicit null must count as absent, got %d", name, rec.Code)
			}
			continue
		case "unknown ignored":
			body = `{"model":"` + embedModel + `","input":"x","unknown_future_field":1}`
			rec := doEmbeddings(t, f.h, body, testKey, "")
			if rec.Code != http.StatusOK {
				t.Errorf("%s: chat unknown-field policy must ignore unknowns, got %d", name, rec.Code)
			}
			continue
		}
		rec := doEmbeddings(t, f.h, body, testKey, "")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400 (%s)", name, rec.Code, rec.Body.String())
			continue
		}
		if !strings.Contains(rec.Body.String(), "invalid_request") {
			t.Errorf("%s: wrong error type: %s", name, rec.Body.String())
		}
	}
}

func TestEmbeddingsRateLimitPerSubject(t *testing.T) {
	f := newEmbeddingsFixture(t, embedCaps, provider.Fake{}, policy.Limits{})
	f.h.Limiter = limiter.New(2, 100)
	for i := 0; i < 2; i++ {
		if rec := doEmbeddings(t, f.h, embeddingsBody("hello"), testKey, ""); rec.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d", i, rec.Code)
		}
	}
	rec := doEmbeddings(t, f.h, embeddingsBody("hello"), testKey, "")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("rate limit denial must carry Retry-After")
	}
}

func TestEmbeddingsQuotaSettlesToReportedInputTokens(t *testing.T) {
	// Budget admits exactly two requests when each settles from the input
	// estimate (2) up to the fake's reported usage (12): after two, 24 of 23
	// tokens are consumed and the third denies. Without settlement the
	// estimates (2+2) would fit trivially and the third would wrongly pass.
	usage := int64(fakeEmbeddingsUsage(5))
	budget := 2*usage - 1
	f := newEmbeddingsFixture(t, embedCaps, provider.Fake{}, policy.Limits{DailyTokens: budget})
	for i := 0; i < 2; i++ {
		if rec := doEmbeddings(t, f.h, embeddingsBody("hello"), testKey, ""); rec.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d body = %s", i, rec.Code, rec.Body.String())
		}
	}
	rec := doEmbeddings(t, f.h, embeddingsBody("hello"), testKey, "")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("third request must exceed the settled pool: status = %d body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "quota_exceeded") {
		t.Fatalf("wrong denial code: %s", rec.Body.String())
	}
}

func TestEmbeddingsSharesChatTokenPool(t *testing.T) {
	// Decision 2026-09-16: embeddings and chat draw from the subject's single
	// daily pool. Chat first: estimate = 2 + policy output clamp 10 = 12,
	// settles to the fake's 21 reported tokens. The remaining pool (1) then
	// denies the embeddings reservation — impossible with an independent
	// embeddings pool.
	limits := policy.Limits{DailyTokens: 22, MaxOutputTokens: 10}
	store := auth.NewStore()
	salt, _ := auth.NewSalt()
	store.Put(auth.KeyRecord{ID: "key-e", Subject: "subject-e", Salt: salt, Hash: auth.HashAPIKey(salt, testKey), Status: auth.StatusActive})
	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: embedModel, Provider: "fake", UpstreamModel: "upstream-embed", Enabled: true, Capabilities: embedCaps},
		{PublicName: "gpt-test", Provider: "fake", UpstreamModel: "up", Enabled: true, Capabilities: testCaps()},
	})
	pol := policy.New()
	pol.AllowAll("subject-e")
	pol.SetLimits("subject-e", limits)
	svc := gateway.New(catalog, map[string]provider.Provider{"fake": provider.Fake{}}, 2*time.Second, 0)
	gate := quota.NewMemory() // one shared pool across protocols
	chat := &ChatHandler{
		Auth: store, Service: svc, Policy: pol, Limiter: limiter.New(1000, 100),
		Quota: gate, Metrics: metrics.New(),
		MaxBody: 1 << 20, MaxMsgs: 64, MaxChars: 32_000,
	}
	emb := &EmbeddingsHandler{
		Auth: store, Service: svc, Policy: pol, Limiter: limiter.New(1000, 100),
		Quota: gate, Metrics: metrics.New(),
		MaxBody: 1 << 20, MaxItems: 128, MaxChars: 32_000,
	}
	if rec := doChat(t, chat, chatBody("gpt-test", false), testKey, ""); rec.Code != http.StatusOK {
		t.Fatalf("chat request: status = %d body = %s", rec.Code, rec.Body.String())
	}
	rec := doEmbeddings(t, emb, embeddingsBody("hello"), testKey, "")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("embeddings must share the chat pool: status = %d body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "quota_exceeded") {
		t.Fatalf("wrong denial code: %s", rec.Body.String())
	}
}

func TestEmbeddingsUnknownUsageKeepsReservation(t *testing.T) {
	p := &unknownUsageEmbeddings{inner: provider.Fake{}}
	// Two estimates (2+2) would fit 3; one less means the second reserve only
	// passes if the first reservation was released (it must not be).
	f := newEmbeddingsFixture(t, embedCaps, p, policy.Limits{DailyTokens: 2*embedEstimate(5) - 1})
	if rec := doEmbeddings(t, f.h, embeddingsBody("hello"), testKey, ""); rec.Code != http.StatusOK {
		t.Fatalf("first request: status = %d body = %s", rec.Code, rec.Body.String())
	}
	rec := doEmbeddings(t, f.h, embeddingsBody("hello"), testKey, "")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("retained reservation must constrain admission: status = %d body = %s", rec.Code, rec.Body.String())
	}
	if p.calls != 1 {
		t.Fatalf("denial reached the provider: calls = %d", p.calls)
	}
}

func TestEmbeddingsDimMismatchIsConfigError(t *testing.T) {
	// The catalog declares a width the upstream does not produce: fail loud
	// with a 500-class config error, return nothing, and never leak a vector.
	mismatch := embedCaps
	mismatch.EmbeddingDim = 100
	f := newEmbeddingsFixture(t, mismatch, provider.Fake{}, policy.Limits{})
	rec := doEmbeddings(t, f.h, embeddingsBody("hello"), testKey, "")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "embedding_dim_mismatch") {
		t.Fatalf("wrong code: %s", rec.Body.String())
	}
	body := rec.Body.String()
	if strings.Contains(body, "[0.") || strings.Contains(body, "0.25") || len(body) > 600 {
		t.Fatalf("config error must not carry vector content: %s", body)
	}
	events := f.sink.Snapshot()
	if len(events) != 1 || events[0].Status != http.StatusInternalServerError || events[0].ErrorClass == "" {
		t.Fatalf("config error must be audited as a failure: %+v", events)
	}
	if events[0].PromptTokens == nil {
		t.Fatal("upstream usage must still be recorded: the provider did the work")
	}
}

// openAIEmbedStub is a minimal OpenAI-compatible embeddings upstream. When
// mrl is true it honors the upstream `dimensions` parameter and returns
// vectors of the requested width (an MRL upstream); otherwise it ignores
// dimensions and always returns its native width. The last request body is
// recorded for request-side assertions.
func openAIEmbedStub(t *testing.T, mrl bool, nativeWidth int) (*httptest.Server, *string) {
	t.Helper()
	var lastBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		lastBody = string(b)
		var req struct {
			Dimensions int `json:"dimensions"`
		}
		_ = json.Unmarshal(b, &req)
		width := nativeWidth
		if mrl && req.Dimensions > 0 {
			width = req.Dimensions
		}
		vec := make([]float64, width)
		for i := range vec {
			vec[i] = 0.125
		}
		vecJSON, _ := json.Marshal(vec)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"object":"list","model":"upstream-embed","data":[{"object":"embedding","index":0,"embedding":%s}],"usage":{"prompt_tokens":3,"total_tokens":3}}`, vecJSON)
	}))
	t.Cleanup(func() { srv.CloseClientConnections(); srv.Close() })
	return srv, &lastBody
}

func TestEmbeddingsInjectsDeclaredDimensions(t *testing.T) {
	// Issue #6: the catalog-declared embedding_dim is injected upstream as
	// `dimensions`, so an MRL upstream returns the declared width and passes
	// the check. A client-passed dimensions field is ignored: not forwarded,
	// not an error — the catalog stays the dimension authority.
	caps := embedCaps
	caps.EmbeddingDim = 4

	t.Run("mrl upstream honors the injected dimension", func(t *testing.T) {
		srv, lastBody := openAIEmbedStub(t, true, 8)
		f := newEmbeddingsFixture(t, caps, provider.NewOpenAI(srv.URL, "sk-upstream-secret"), policy.Limits{})
		rec := doEmbeddings(t, f.h, `{"model":"`+embedModel+`","input":"hello","dimensions":777}`, testKey, "req-emb-dim")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
		}
		var resp embeddingsResponseOut
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		if len(resp.Data) != 1 || len(resp.Data[0].Embedding) != 4 {
			t.Fatalf("data = %+v, want exactly the declared width 4", resp.Data)
		}
		if !strings.Contains(*lastBody, `"dimensions":4`) {
			t.Fatalf("upstream body must carry the declared dimension: %s", *lastBody)
		}
		if strings.Contains(*lastBody, "777") {
			t.Fatalf("client-passed dimensions must never be forwarded: %s", *lastBody)
		}
	})

	t.Run("upstream returning native width fails loud", func(t *testing.T) {
		// An upstream that ignores dimensions returns its native width: the
		// declaration/upstream mismatch stays a loud 500 config error.
		srv, _ := openAIEmbedStub(t, false, 8)
		f := newEmbeddingsFixture(t, caps, provider.NewOpenAI(srv.URL, "sk-upstream-secret"), policy.Limits{})
		rec := doEmbeddings(t, f.h, embeddingsBody("hello"), testKey, "")
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "embedding_dim_mismatch") {
			t.Fatalf("wrong code: %s", rec.Body.String())
		}
	})
}

func TestEmbeddingsRollbackSwitchRemovesRoute(t *testing.T) {
	// The GATEWAY_EMBEDDINGS_ENABLED=false rollback switch unregisters the
	// route by wiring a nil handler.
	mux := NewMux(&ChatHandler{}, Deps{Metrics: http.NotFoundHandler(), Embeddings: nil})
	req := httptest.NewRequest(http.MethodPost, "/v1/embeddings", strings.NewReader(embeddingsBody("hello")))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("disabled endpoint status = %d, want 404", rec.Code)
	}
}

func TestEmbeddingsMethodNotAllowed(t *testing.T) {
	f := newEmbeddingsFixture(t, embedCaps, provider.Fake{}, policy.Limits{})
	mux := NewMux(&ChatHandler{}, Deps{Metrics: http.NotFoundHandler(), Embeddings: f.h})
	req := httptest.NewRequest(http.MethodGet, "/v1/embeddings", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}
