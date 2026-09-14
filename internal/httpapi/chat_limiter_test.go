package httpapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/gateway"
	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
)

// failingGate simulates limiter infrastructure failure (e.g. Redis down).
type failingGate struct{ err error }

func (f failingGate) Allow(context.Context, string, time.Time) (bool, time.Duration, func(), error) {
	return false, 0, func() {}, f.err
}

// TestChatLimiterUnavailableMapsTo503 verifies that a limiter outage is
// reported as a 503 service_unavailable (never a 429 rate limit) with a
// non-leaky message and no Retry-After header.
func TestChatLimiterUnavailableMapsTo503(t *testing.T) {
	store := auth.NewStore()
	salt, _ := auth.NewSalt()
	key := "kb_test_key"
	store.Put(auth.KeyRecord{ID: "key_1", Subject: "s", Salt: salt, Hash: auth.HashAPIKey(salt, key), Status: auth.StatusActive, CreatedAt: time.Now()})
	catalog := policy.NewCatalog([]policy.ModelInfo{{PublicName: "m", Provider: "fake", UpstreamModel: "up", Enabled: true}})
	pol := policy.New()
	pol.AllowAll("s")
	chat := &ChatHandler{
		Auth:    store,
		Service: gateway.New(catalog, map[string]provider.Provider{"fake": provider.Fake{}}, time.Second, 0),
		Policy:  pol,
		Limiter: failingGate{err: limiter.ErrUnavailable},
		MaxBody: 1 << 20, MaxMsgs: 8, MaxChars: 1000,
	}
	srv := httptest.NewServer(NewMux(chat, Deps{Metrics: http.NotFoundHandler()}))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"x"}]}`))
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	body := string(raw)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("want 503 for limiter outage, got %d: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, `"code":"limiter_unavailable"`) || !strings.Contains(body, `"type":"service_unavailable"`) {
		t.Fatalf("bad envelope for limiter outage: %s", body)
	}
	if strings.Contains(body, "rate_limit") || resp.Header.Get("Retry-After") != "" {
		t.Fatalf("limiter outage must not look like a rate limit: headers=%v body=%s", resp.Header, body)
	}

	// A genuine rate-limit denial still maps to 429 via the existing path.
	// (Behavior covered end to end in internal/e2e; here we assert the
	// sentinel classification stays distinct.)
	if errors.Is(&limiter.Error{Code: "rate_limit_exceeded"}, limiter.ErrUnavailable) {
		t.Fatal("rate-limit denial must not be classified as limiter outage")
	}
}
