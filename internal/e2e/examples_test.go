package e2e

// The developer-examples smoke: every request sample under
// docs/examples/requests must run against a gateway wired to the mock
// provider, exactly as the documentation promises. The tests read the same
// files the docs publish, so examples and code cannot drift apart.

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/gateway"
	"github.com/knowledge-base/knowledge-base-gateway/internal/httpapi"
	"github.com/knowledge-base/knowledge-base-gateway/internal/limiter"
	"github.com/knowledge-base/knowledge-base-gateway/internal/metrics"
	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
	"github.com/knowledge-base/knowledge-base-gateway/internal/router"
)

func examplesDir(t *testing.T) string {
	t.Helper()
	abs, err := filepath.Abs("../../docs/examples/requests")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(abs); err != nil {
		t.Fatalf("examples directory missing: %v", err)
	}
	return abs
}

func loadSample(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(examplesDir(t), name))
	if err != nil {
		t.Fatalf("example sample %s missing: %v", name, err)
	}
	return string(raw)
}

// mockGateway is the documented local mock-provider stack: in-memory stores,
// the fake provider with its full capability matrix, and both protocols
// served.
type mockGateway struct {
	srv  *httptest.Server
	key  string
	sink *audit.MemorySink
}

func startMockGateway(t *testing.T) *mockGateway {
	t.Helper()
	gen, err := auth.NewManager(auth.NewStore()).Create(context.Background(), "subject-demo", "tenant_default", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	store := auth.NewStore()
	salt, _ := auth.NewSalt()
	store.Put(auth.KeyRecord{
		ID: gen.Record.ID, Subject: "subject-demo", TenantID: "tenant_default",
		Salt: salt, Hash: auth.HashAPIKey(salt, gen.Plaintext), Status: auth.StatusActive,
	})
	// The seeded capability matrix mirrors migration 0003's declaration for
	// the mock model.
	caps := model.Capabilities{
		Chat: true, Responses: true, Stream: true, Tools: true,
		StructuredOutput: true, JSONMode: true, Usage: true,
		ContextTokens: 8192, MaxOutputTokens: 2048, MaxTools: 8,
	}
	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: "gateway-echo", Provider: "fake", UpstreamModel: "echo-model", Enabled: true, Capabilities: caps},
	})
	pol := policy.New()
	pol.AllowAll("subject-demo")
	svc := gateway.New(catalog, map[string]provider.Provider{"fake": provider.Fake{}}, 5*time.Second, 1)
	svc.Routes.SetRoutes("gateway-echo", []router.Route{
		{ProviderName: "fake", Provider: provider.Fake{}, UpstreamModel: "echo-model", Priority: 10, Enabled: true, Breaker: router.NewBreaker(5, time.Minute)},
	})
	sink := audit.NewMemorySink(nil)
	mreg := metrics.New()
	chat := &httpapi.ChatHandler{
		Auth: store, Service: svc, Policy: pol, Limiter: limiter.New(120, 8),
		Audit: sink, Metrics: mreg,
		MaxBody: 1 << 20, MaxMsgs: 64, MaxChars: 32_000,
	}
	responses := &httpapi.ResponsesHandler{
		Auth: store, Service: svc, Policy: pol, Limiter: limiter.New(120, 8),
		Audit: sink, Metrics: mreg,
		MaxBody: 1 << 20, MaxItems: 64, MaxChars: 32_000,
	}
	models := &httpapi.ModelsHandler{Auth: store, Service: svc, Policy: pol}
	srv := httptest.NewServer(httpapi.NewMux(chat, httpapi.Deps{
		Metrics: mreg.Handler(), Responses: responses, Models: models,
	}))
	t.Cleanup(srv.Close)
	return &mockGateway{srv: srv, key: gen.Plaintext, sink: sink}
}

func (g *mockGateway) post(t *testing.T, path, body string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, g.srv.URL+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+g.key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := g.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var sb strings.Builder
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		sb.WriteString(sc.Text())
		sb.WriteString("\n")
	}
	return resp, sb.String()
}

func TestExampleChatNonStream(t *testing.T) {
	g := startMockGateway(t)
	resp, body := g.post(t, "/v1/chat/completions", loadSample(t, "chat_non_stream.json"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d body = %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, `"object":"chat.completion"`) || !strings.Contains(body, "echo: hello") {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestExampleChatStream(t *testing.T) {
	g := startMockGateway(t)
	resp, body := g.post(t, "/v1/chat/completions", loadSample(t, "chat_stream.json"))
	if resp.StatusCode != http.StatusOK || !strings.HasSuffix(body, "data: [DONE]\n\n") {
		t.Fatalf("status = %d body = %q", resp.StatusCode, body)
	}
}

func TestExampleResponsesNonStream(t *testing.T) {
	g := startMockGateway(t)
	resp, body := g.post(t, "/v1/responses", loadSample(t, "responses_non_stream.json"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d body = %s", resp.StatusCode, body)
	}
	var out struct {
		Object string `json:"object"`
		Status string `json:"status"`
	}
	_ = json.Unmarshal([]byte(body), &out)
	if out.Object != "response" || out.Status != "completed" {
		t.Fatalf("unexpected response: %s", body)
	}
}

func TestExampleResponsesStream(t *testing.T) {
	g := startMockGateway(t)
	resp, body := g.post(t, "/v1/responses", loadSample(t, "responses_stream.json"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if !strings.Contains(body, "event: response.created") || !strings.Contains(body, "event: response.completed") {
		t.Fatalf("missing terminal events: %q", body)
	}
}

func TestExampleResponsesTools(t *testing.T) {
	g := startMockGateway(t)
	// Step 1: tool call comes back.
	resp, body := g.post(t, "/v1/responses", loadSample(t, "responses_tools.json"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("step1 status = %d body = %s", resp.StatusCode, body)
	}
	var out struct {
		Output []struct {
			Type      string `json:"type"`
			CallID    string `json:"call_id"`
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
		} `json:"output"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Output) != 1 || out.Output[0].Type != "function_call" || out.Output[0].Name != "get_weather" {
		t.Fatalf("output = %+v", out.Output)
	}
	if !json.Valid([]byte(out.Output[0].Arguments)) {
		t.Fatalf("arguments invalid: %q", out.Output[0].Arguments)
	}

	// Step 2: the caller sends the tool result back (documented round trip).
	roundTrip := loadSample(t, "responses_tool_result.json")
	resp, body = g.post(t, "/v1/responses", roundTrip)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("step2 status = %d body = %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "tool ok: sunny, 22C") {
		t.Fatalf("tool result lost: %s", body)
	}
}

func TestExampleResponsesStructured(t *testing.T) {
	g := startMockGateway(t)
	resp, body := g.post(t, "/v1/responses", loadSample(t, "responses_structured.json"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d body = %s", resp.StatusCode, body)
	}
	var out struct {
		Output []struct {
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"output"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Output) == 0 {
		t.Fatalf("no output: %s", body)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(out.Output[0].Content[0].Text), &parsed); err != nil {
		t.Fatalf("structured output invalid JSON: %q", out.Output[0].Content[0].Text)
	}
	if _, ok := parsed["echo"]; !ok {
		t.Fatalf("schema requires the echo property: %v", parsed)
	}
}

// TestExamplesDirectoryMatchesDocs guards the shared-fixture contract: the
// examples directory must contain exactly the documented sample set.
func TestExamplesDirectoryMatchesDocs(t *testing.T) {
	want := map[string]bool{
		"chat_non_stream.json":       true,
		"chat_stream.json":           true,
		"responses_non_stream.json":  true,
		"responses_stream.json":      true,
		"responses_tools.json":       true,
		"responses_tool_result.json": true,
		"responses_structured.json":  true,
	}
	entries, err := os.ReadDir(examplesDir(t))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, e := range entries {
		if !e.IsDir() {
			got[e.Name()] = true
		}
	}
	for name := range want {
		if !got[name] {
			t.Errorf("documented sample %s missing", name)
		}
	}
	for name := range got {
		if !want[name] {
			t.Errorf("undeclared sample file %s (update docs/examples)", name)
		}
	}
}
