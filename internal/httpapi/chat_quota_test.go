package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
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

// usageOverride replaces the upstream-reported usage to simulate providers
// that omit usage (unknown) or report specific totals.
type usageOverride struct {
	inner provider.Provider
	usage *model.Usage
}

func (u *usageOverride) Name() string { return u.inner.Name() }

func (u *usageOverride) Capabilities(m string) model.Capabilities { return u.inner.Capabilities(m) }

func (u *usageOverride) Embeddings(ctx context.Context, req model.EmbeddingsRequest) (model.EmbeddingsResponse, error) {
	return u.inner.Embeddings(ctx, req)
}

func (u *usageOverride) Complete(ctx context.Context, req model.Request) (model.Response, error) {
	resp, err := u.inner.Complete(ctx, req)
	if err != nil {
		return resp, err
	}
	resp.Usage = u.usage
	return resp, nil
}

func (u *usageOverride) Stream(ctx context.Context, req model.Request, emit func(model.Event) error) error {
	return u.inner.Stream(ctx, req, emit)
}

// streamFailOnce fails streaming before any output on the first call.
type streamFailOnce struct {
	inner provider.Provider
	calls int
}

func (f *streamFailOnce) Name() string { return f.inner.Name() }

func (f *streamFailOnce) Capabilities(m string) model.Capabilities { return f.inner.Capabilities(m) }

func (f *streamFailOnce) Embeddings(ctx context.Context, req model.EmbeddingsRequest) (model.EmbeddingsResponse, error) {
	return f.inner.Embeddings(ctx, req)
}

func (f *streamFailOnce) Complete(ctx context.Context, req model.Request) (model.Response, error) {
	return f.inner.Complete(ctx, req)
}

func (f *streamFailOnce) Stream(ctx context.Context, req model.Request, emit func(model.Event) error) error {
	f.calls++
	if f.calls == 1 {
		return &provider.Error{Class: provider.ClassServer, Msg: "boom"}
	}
	return f.inner.Stream(ctx, req, emit)
}

// outageQuota simulates quota infrastructure failure (Redis unreachable).
type outageQuota struct{}

func (outageQuota) Reserve(context.Context, string, quota.Limits, int64, time.Time) (quota.Reservation, error) {
	return nil, limiter.ErrUnavailable
}

// newQuotaHandler wires a handler with the given token budgets for the
// subject and counts provider Complete invocations.
type countingProvider struct {
	inner provider.Provider
	calls int
}

func (c *countingProvider) Name() string { return c.inner.Name() }

func (c *countingProvider) Capabilities(m string) model.Capabilities { return c.inner.Capabilities(m) }

func (c *countingProvider) Embeddings(ctx context.Context, req model.EmbeddingsRequest) (model.EmbeddingsResponse, error) {
	c.calls++
	return c.inner.Embeddings(ctx, req)
}

func (c *countingProvider) Complete(ctx context.Context, req model.Request) (model.Response, error) {
	c.calls++
	return c.inner.Complete(ctx, req)
}

func (c *countingProvider) Stream(ctx context.Context, req model.Request, emit func(model.Event) error) error {
	c.calls++
	return c.inner.Stream(ctx, req, emit)
}

func newQuotaHandler(t *testing.T, p provider.Provider, limits policy.Limits) (*ChatHandler, *countingProvider) {
	t.Helper()
	store := auth.NewStore()
	salt, err := auth.NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	store.Put(auth.KeyRecord{ID: "key-1", Subject: "subject-a", Salt: salt, Hash: auth.HashAPIKey(salt, testKey), Status: auth.StatusActive})
	catalog := policy.NewCatalog([]policy.ModelInfo{{PublicName: "gpt-test", Provider: p.Name(), UpstreamModel: "up", Enabled: true, Capabilities: testCaps()}})
	pol := policy.New()
	pol.AllowAll("subject-a")
	pol.SetLimits("subject-a", limits)
	counting := &countingProvider{inner: p}
	svc := gateway.New(catalog, map[string]provider.Provider{p.Name(): counting}, 2*time.Second, 0)
	h := &ChatHandler{
		Auth: store, Service: svc, Policy: pol,
		Limiter: limiter.New(1000, 100), Quota: quota.NewMemory(),
		Audit: nil, Metrics: metrics.New(),
		MaxBody: 1 << 20, MaxMsgs: 64, MaxChars: 32_000,
	}
	return h, counting
}

// helloEstimate is the deterministic reservation for the chatBody request:
// 5 content chars plus the default output reserve.
func helloEstimate() int64 {
	return quota.Estimate(nil, 0, len("hello"))
}

func TestChatQuotaDenialIs429WithoutProviderCall(t *testing.T) {
	// Budget admits one reservation (4098) and, after settlement to the
	// reported 21 total tokens, no second reservation.
	h, p := newQuotaHandler(t, provider.Fake{}, policy.Limits{DailyTokens: helloEstimate() + 2})
	if rec := doChat(t, h, chatBody("gpt-test", false), testKey, "req-q-1"); rec.Code != 200 {
		t.Fatalf("first request: status = %d body = %s", rec.Code, rec.Body.String())
	}
	if p.calls != 1 {
		t.Fatalf("provider calls = %d, want 1", p.calls)
	}
	rec := doChat(t, h, chatBody("gpt-test", false), testKey, "req-q-2")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("denied request: status = %d body = %s", rec.Code, rec.Body.String())
	}
	if p.calls != 1 {
		t.Fatalf("quota denial reached the provider: calls = %d", p.calls)
	}
	var env APIError
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if env.Error.Code != "quota_exceeded" || env.Error.Type != "rate_limit_error" {
		t.Fatalf("bad envelope: %+v", env.Error)
	}
	if env.Error.RequestID != "req-q-2" {
		t.Fatalf("request id correlation missing: %+v", env.Error)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("quota denial must carry Retry-After until the UTC boundary")
	}
	for _, leaked := range []string{"daily", "monthly", "remaining", "limit"} {
		if strings.Contains(strings.ToLower(env.Error.Message), leaked) {
			t.Fatalf("quota message leaks policy internals: %q", env.Error.Message)
		}
	}
}

func TestChatQuotaSettlesToReportedTotal(t *testing.T) {
	// Budget admits exactly two requests when the first settles from the
	// 4098-token estimate down to the reported 21 total; if the estimate
	// were retained the second would deny.
	h, _ := newQuotaHandler(t, provider.Fake{}, policy.Limits{DailyTokens: 2*helloEstimate() - 100})
	if rec := doChat(t, h, chatBody("gpt-test", false), testKey, ""); rec.Code != 200 {
		t.Fatalf("first request: %d %s", rec.Code, rec.Body.String())
	}
	if rec := doChat(t, h, chatBody("gpt-test", false), testKey, ""); rec.Code != 200 {
		t.Fatalf("second request must fit after settlement: %d %s", rec.Code, rec.Body.String())
	}
}

func TestChatQuotaReleaseOnProviderFailure(t *testing.T) {
	flaky := &flakyProvider{fails: 1, returnErr: &provider.Error{Class: provider.ClassServer, Msg: "boom"}, inner: provider.Fake{}}
	h, _ := newQuotaHandler(t, flaky, policy.Limits{DailyTokens: helloEstimate()})
	// The failing attempt is released, so the retry (same budget) is admitted.
	if rec := doChat(t, h, chatBody("gpt-test", false), testKey, ""); rec.Code != 503 {
		t.Fatalf("first request must exhaust retries: %d", rec.Code)
	}
	if rec := doChat(t, h, chatBody("gpt-test", false), testKey, ""); rec.Code != 200 {
		t.Fatalf("released budget must admit the next request: %d %s", rec.Code, rec.Body.String())
	}
}

func TestChatQuotaUnknownUsageRetainsEstimate(t *testing.T) {
	unknown := &model.Usage{Known: false}
	// Two estimates would fit exactly; one less means the second reserve
	// only passes if the first estimate was refunded (it must not be).
	h, p := newQuotaHandler(t, &usageOverride{inner: provider.Fake{}, usage: unknown},
		policy.Limits{DailyTokens: 2*helloEstimate() - 1})
	if rec := doChat(t, h, chatBody("gpt-test", false), testKey, ""); rec.Code != 200 {
		t.Fatalf("first request: %d", rec.Code)
	}
	// Unknown usage keeps the conservative reservation charged: the second
	// request no longer fits.
	rec := doChat(t, h, chatBody("gpt-test", false), testKey, "")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("retained estimate must constrain admission: %d %s", rec.Code, rec.Body.String())
	}
	if p.calls != 1 {
		t.Fatalf("denial reached the provider: calls = %d", p.calls)
	}
}

func TestChatQuotaInfraFailureMapsTo503(t *testing.T) {
	h, p := newQuotaHandler(t, provider.Fake{}, policy.Limits{DailyTokens: 100})
	h.Quota = outageQuota{}
	rec := doChat(t, h, chatBody("gpt-test", false), testKey, "")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("quota outage must be 503, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "limiter_unavailable") ||
		strings.Contains(rec.Body.String(), "quota_exceeded") {
		t.Fatalf("outage must use the limiter_unavailable contract: %s", rec.Body.String())
	}
	if rec.Header().Get("Retry-After") != "" {
		t.Fatal("infrastructure failure must not carry Retry-After")
	}
	if p.calls != 0 {
		t.Fatalf("outage must not reach the provider: calls = %d", p.calls)
	}
	if errors.Is(&quota.Error{Code: quota.CodeQuotaExceeded}, limiter.ErrUnavailable) {
		t.Fatal("quota denial must stay distinct from the outage sentinel")
	}
}

func TestChatQuotaStreamingReleasesPreOutputFailure(t *testing.T) {
	inner := provider.Fake{}
	sf := &streamFailOnce{inner: inner}
	h, _ := newQuotaHandler(t, sf, policy.Limits{DailyTokens: helloEstimate()})
	// First stream fails before output: reservation released.
	if rec := doChat(t, h, chatBody("gpt-test", true), testKey, ""); rec.Code != 200 {
		t.Fatalf("streaming headers: %d", rec.Code)
	}
	if sf.calls != 1 {
		t.Fatalf("stream calls = %d", sf.calls)
	}
	// The released budget admits the next stream.
	if rec := doChat(t, h, chatBody("gpt-test", true), testKey, ""); rec.Code != 200 {
		t.Fatalf("second stream must be admitted after release: %d %s", rec.Code, rec.Body.String())
	}
	if sf.calls != 2 {
		t.Fatalf("second stream never reached the provider: calls = %d (release failed)", sf.calls)
	}
}

func TestChatNoTokenQuotaKeepsCurrentBehavior(t *testing.T) {
	// Rate/concurrency limits only; no daily or monthly budget configured.
	h, _ := newQuotaHandler(t, provider.Fake{}, policy.Limits{RatePerMinute: 1000, MaxConcurrent: 100})
	for i := 0; i < 3; i++ {
		if rec := doChat(t, h, chatBody("gpt-test", false), testKey, ""); rec.Code != 200 {
			t.Fatalf("request %d: status = %d body = %s", i, rec.Code, rec.Body.String())
		}
	}
}

func TestChatQuotaMonthlyLimitIndependent(t *testing.T) {
	// Only a monthly ceiling: a fresh day does not lift it, and settlement
	// counts against the month.
	h, _ := newQuotaHandler(t, provider.Fake{}, policy.Limits{MonthlyTokens: helloEstimate() - 1})
	rec := doChat(t, h, chatBody("gpt-test", false), testKey, "")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("monthly-only budget must deny: %d %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "quota_exceeded") {
		t.Fatalf("wrong denial code: %s", rec.Body.String())
	}
}

// TestChatQuotaAuditKeepsUnknownUsage pins the audit contract: usage stays
// nil (unknown) when the upstream reports none, even though enforcement
// retained the conservative reservation.
func TestChatQuotaAuditKeepsUnknownUsage(t *testing.T) {
	unknown := &model.Usage{Known: false}
	h, _ := newQuotaHandler(t, &usageOverride{inner: provider.Fake{}, usage: unknown},
		policy.Limits{DailyTokens: 10 * helloEstimate()})
	sink := audit.NewMemorySink(nil)
	h.Audit = sink
	if rec := doChat(t, h, chatBody("gpt-test", false), testKey, ""); rec.Code != 200 {
		t.Fatalf("request: %d", rec.Code)
	}
	events := sink.Snapshot()
	if len(events) != 1 {
		t.Fatalf("audit events = %d", len(events))
	}
	if events[0].PromptTokens != nil || events[0].CompletionTokens != nil {
		t.Fatalf("unknown usage must stay nil in audit: %+v", events[0])
	}
	if events[0].Status != 200 || events[0].SubjectID != "subject-a" {
		t.Fatalf("audit correlation wrong: %+v", events[0])
	}
}
