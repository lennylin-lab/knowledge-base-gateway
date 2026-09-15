package httpapi

// Protocol admission validation: the shared tool/schema bounds must reject
// invalid V1.2 tool inputs for both Chat and Responses before any provider
// invocation, and /v1/responses bounded decoding must accept exactly one JSON
// document (no trailing JSON, no trailing garbage).

import (
	"fmt"
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
)

// newCountedChatFixture wires a ChatHandler against the full-capability
// catalog model with a provider call counter, mirroring newResponsesFixture.
func newCountedChatFixture(t *testing.T) (*ChatHandler, *int) {
	t.Helper()
	store := auth.NewStore()
	salt, err := auth.NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	store.Put(auth.KeyRecord{ID: "key-c", Subject: "subject-c", Salt: salt, Hash: auth.HashAPIKey(salt, testKey), Status: auth.StatusActive})
	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: "full-model", Provider: "fake", UpstreamModel: "upstream-full", Enabled: true, Capabilities: fullCaps},
	})
	pol := policy.New()
	pol.Allow("subject-c", "full-model")
	calls := 0
	counting := &callCounter{inner: provider.Fake{}, calls: &calls}
	svc := gateway.New(catalog, map[string]provider.Provider{"fake": counting}, 2*time.Second, 0)
	svc.RetryWait = time.Millisecond
	h := &ChatHandler{
		Auth: store, Service: svc, Policy: pol,
		Limiter: limiter.New(1000, 100), Audit: audit.NewMemorySink(nil), Metrics: metrics.New(),
		MaxBody: 1 << 20, MaxMsgs: 64, MaxChars: 32_000,
	}
	return h, &calls
}

// deepSchema nests plain objects well past model.MaxJSONDepth.
func deepSchema() string {
	s := `{"type":"object"}`
	for i := 0; i < model.MaxJSONDepth; i++ {
		s = `{"type":"object","properties":{"p":` + s + `}}`
	}
	return s
}

func chatToolsBody(toolsJSON string) string {
	return `{"model":"full-model","messages":[{"role":"user","content":"x"}],"tools":[` + toolsJSON + `]}`
}

func responsesToolsBody(toolsJSON string) string {
	return `{"model":"full-model","input":"x","tools":[` + toolsJSON + `]}`
}

func oversizedToolSchema() string {
	return `{"type":"object","description":"` + strings.Repeat("a", 40_000) + `"}`
}

func tooManyToolDefs() string {
	many := make([]string, 0, fullCaps.MaxTools+1)
	for i := 0; i <= fullCaps.MaxTools; i++ {
		many = append(many, fmt.Sprintf(`{"type":"function","function":{"name":"tool_%d","parameters":{"type":"object"}}}`, i))
	}
	return strings.Join(many, ",")
}

// TestChatToolValidationBeforeProvider pins R1/AC1 for Chat Completions:
// invalid tool names, duplicates, excessive count, oversized schemas, and
// over-depth schemas are 400 invalid_request rejections that never reach the
// provider.
func TestChatToolValidationBeforeProvider(t *testing.T) {
	h, calls := newCountedChatFixture(t)
	cases := map[string]string{
		"invalid name": chatToolsBody(`{"type":"function","function":{"name":"bad name!","parameters":{"type":"object"}}}`),
		"empty name":   chatToolsBody(`{"type":"function","function":{"name":"","parameters":{"type":"object"}}}`),
		"duplicate": chatToolsBody(
			`{"type":"function","function":{"name":"get_weather","parameters":{"type":"object"}}},` +
				`{"type":"function","function":{"name":"get_weather","parameters":{"type":"object"}}}`),
		"too many tools": chatToolsBody(tooManyToolDefs()),
		"oversized schema": chatToolsBody(
			`{"type":"function","function":{"name":"big","parameters":` + oversizedToolSchema() + `}}`),
		"too deep schema": chatToolsBody(
			`{"type":"function","function":{"name":"deep","parameters":` + deepSchema() + `}}`),
		"overlong description": chatToolsBody(
			`{"type":"function","function":{"name":"d","description":"` + strings.Repeat("a", 3000) + `"}}`),
	}
	for name, body := range cases {
		rec := doChat(t, h, body, testKey, "req-chat-tools")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400: %s", name, rec.Code, rec.Body.String())
			continue
		}
		if !strings.Contains(rec.Body.String(), "invalid_request") {
			t.Errorf("%s: envelope must be invalid_request: %s", name, rec.Body.String())
		}
	}
	if *calls != 0 {
		t.Errorf("tool-validation rejections reached the provider (%d calls)", *calls)
	}
}

// TestResponsesToolValidationBeforeProvider pins R1/AC1 for /v1/responses.
func TestResponsesToolValidationBeforeProvider(t *testing.T) {
	f := newResponsesFixture(t, fullCaps, provider.Fake{})
	cases := map[string]string{
		"invalid name": responsesToolsBody(`{"type":"function","function":{"name":"bad name!","parameters":{"type":"object"}}}`),
		"empty name":   responsesToolsBody(`{"type":"function","function":{"name":"","parameters":{"type":"object"}}}`),
		"duplicate": responsesToolsBody(
			`{"type":"function","function":{"name":"get_weather","parameters":{"type":"object"}}},` +
				`{"type":"function","function":{"name":"get_weather","parameters":{"type":"object"}}}`),
		"too many tools": responsesToolsBody(tooManyToolDefs()),
		"oversized schema": responsesToolsBody(
			`{"type":"function","function":{"name":"big","parameters":` + oversizedToolSchema() + `}}`),
		"too deep schema": responsesToolsBody(
			`{"type":"function","function":{"name":"deep","parameters":` + deepSchema() + `}}`),
		"overlong description": responsesToolsBody(
			`{"type":"function","function":{"name":"d","description":"` + strings.Repeat("a", 3000) + `"}}`),
	}
	for name, body := range cases {
		rec := doResponses(t, f, body, testKey, "req-resp-tools")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400: %s", name, rec.Code, rec.Body.String())
			continue
		}
		if !strings.Contains(rec.Body.String(), "invalid_request") {
			t.Errorf("%s: envelope must be invalid_request: %s", name, rec.Body.String())
		}
	}
	if *f.calls != 0 {
		t.Errorf("tool-validation rejections reached the provider (%d calls)", *f.calls)
	}
}

// TestResponsesSingleDocumentDecode pins R2/AC2: /v1/responses must reject a
// second concatenated JSON object and any non-whitespace trailing bytes with
// the stable invalid_request envelope, while trailing whitespace alone stays
// a valid single document.
func TestResponsesSingleDocumentDecode(t *testing.T) {
	f := newResponsesFixture(t, fullCaps, provider.Fake{})
	cases := map[string]string{
		"two objects":       `{"model":"full-model","input":"hi"}{"model":"full-model","input":"again"}`,
		"object then array": `{"model":"full-model","input":"hi"}[1,2]`,
		"trailing garbage":  `{"model":"full-model","input":"hi"} garbage`,
		"trailing bracket":  `{"model":"full-model","input":"hi"}]`,
	}
	for name, body := range cases {
		rec := doResponses(t, f, body, testKey, "req-resp-decode")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400: %s", name, rec.Code, rec.Body.String())
			continue
		}
		if !strings.Contains(rec.Body.String(), "invalid_request") {
			t.Errorf("%s: envelope must be invalid_request: %s", name, rec.Body.String())
		}
	}
	if *f.calls != 0 {
		t.Errorf("trailing-document requests reached the provider (%d calls)", *f.calls)
	}

	// Only-whitespace trailing bytes remain a valid single document.
	rec := doResponses(t, f, "{\"model\":\"full-model\",\"input\":\"hi\"}\n\n ", testKey, "req-resp-decode-ok")
	if rec.Code != http.StatusOK {
		t.Fatalf("trailing whitespace must be accepted, got %d: %s", rec.Code, rec.Body.String())
	}
	if *f.calls != 1 {
		t.Errorf("valid single document must reach the provider exactly once (%d calls)", *f.calls)
	}
}
