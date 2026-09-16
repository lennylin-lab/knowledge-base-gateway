package provider

// Embeddings contract tests. The fake's deterministic output and the openai
// adapter's translation are pinned offline against a stub upstream; the
// capability gate (embeddings rejected before any provider work) and the
// dimension check round out the protocol contract.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
)

func embeddingsRequest(inputs ...string) model.EmbeddingsRequest {
	return model.EmbeddingsRequest{Model: "up-model", Input: inputs}
}

func TestFakeEmbeddingsDeterministic(t *testing.T) {
	p := Fake{}
	caps := p.Capabilities("any")
	if !caps.Embeddings || caps.EmbeddingDim != FakeEmbeddingDim {
		t.Fatalf("fake must declare embeddings with a fixed dim, got %+v", caps)
	}
	ctx := context.Background()
	first, err := p.Embeddings(ctx, embeddingsRequest("hello", "world"))
	if err != nil {
		t.Fatal(err)
	}
	if first.Object != "list" || len(first.Data) != 2 {
		t.Fatalf("envelope = %+v", first)
	}
	for i, d := range first.Data {
		if d.Object != "embedding" || d.Index != i {
			t.Fatalf("data[%d] = %+v", i, d)
		}
		if len(d.Embedding) != FakeEmbeddingDim {
			t.Fatalf("data[%d] width = %d, want exactly %d", i, len(d.Embedding), FakeEmbeddingDim)
		}
		for j, v := range d.Embedding {
			if v < -1 || v >= 1 {
				t.Fatalf("data[%d][%d] = %v outside [-1, 1)", i, j, v)
			}
		}
	}
	// Identical inputs produce identical vectors; different inputs do not.
	second, err := p.Embeddings(ctx, embeddingsRequest("hello", "world"))
	if err != nil {
		t.Fatal(err)
	}
	for i := range first.Data {
		for j := range first.Data[i].Embedding {
			if first.Data[i].Embedding[j] != second.Data[i].Embedding[j] {
				t.Fatalf("determinism violated at data[%d][%d]", i, j)
			}
		}
	}
	other, err := p.Embeddings(ctx, embeddingsRequest("hello!", "world"))
	if err != nil {
		t.Fatal(err)
	}
	if other.Data[0].Embedding[0] == first.Data[0].Embedding[0] && other.Data[0].Embedding[1] == first.Data[0].Embedding[1] {
		t.Fatal("different inputs must produce different vectors")
	}
	// Usage is input-token only and always known (quota settles to it).
	if first.Usage == nil || !first.Usage.Known {
		t.Fatalf("usage must be known: %+v", first.Usage)
	}
	if first.Usage.PromptTokens <= 0 || first.Usage.CompletionTokens != 0 || first.Usage.TotalTokens != first.Usage.PromptTokens {
		t.Fatalf("embeddings usage must be prompt-only: %+v", first.Usage)
	}
	// Cancellation surfaces as a timeout-class failure.
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Embeddings(cctx, embeddingsRequest("x")); ClassOf(err) != ClassTimeout {
		t.Fatalf("canceled embeddings class = %v (%v)", ClassOf(err), err)
	}
}

func TestOpenAIEmbeddingsTranslation(t *testing.T) {
	var (
		path  string
		body  string
		auth  string
		creq  *http.Request
		resps = map[string]string{}
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		auth = r.Header.Get("Authorization")
		creq = r
		b, _ := io.ReadAll(r.Body)
		body = string(b)
		var req openaiWireEmbeddingsRequest
		_ = json.Unmarshal(b, &req)
		// Scenario rides on the first input string; the "ok" fixture path is
		// any other input.
		scenario := ""
		if len(req.Input) > 0 {
			scenario = req.Input[0]
		}
		w.Header().Set("Content-Type", "application/json")
		switch scenario {
		case "boom":
			w.WriteHeader(http.StatusInternalServerError)
		case "limited":
			w.WriteHeader(http.StatusTooManyRequests)
		case "reject":
			w.WriteHeader(http.StatusBadRequest)
		case "malformed":
			_, _ = io.WriteString(w, "not-json")
		default:
			_, _ = io.WriteString(w, resps["ok"])
		}
	}))
	t.Cleanup(func() { srv.CloseClientConnections(); srv.Close() })
	resps["ok"] = `{"object":"list","model":"up-model","data":[` +
		`{"object":"embedding","index":0,"embedding":[0.25,-0.5]},` +
		`{"object":"embedding","index":1,"embedding":[1.0]}],"usage":{"prompt_tokens":7,"total_tokens":7}}`

	p := NewOpenAI(srv.URL, "sk-upstream-secret")
	resp, err := p.Embeddings(context.Background(), embeddingsRequest("hello", "world"))
	if err != nil {
		t.Fatal(err)
	}
	if path != "/embeddings" {
		t.Fatalf("request path = %q, want /embeddings", path)
	}
	if auth != "Bearer sk-upstream-secret" {
		t.Fatalf("credentials missing: %q", auth)
	}
	if creq != nil && creq.Method != http.MethodPost {
		t.Fatalf("method = %q", creq.Method)
	}
	// The input must reach the upstream as the string array the API accepts.
	var sent openaiWireEmbeddingsRequest
	if err := json.Unmarshal([]byte(body), &sent); err != nil {
		t.Fatalf("upstream body not the embeddings wire shape: %v (%s)", err, body)
	}
	if sent.Model != "up-model" || len(sent.Input) != 2 || sent.Input[0] != "hello" || sent.Input[1] != "world" {
		t.Fatalf("translated request = %+v (%s)", sent, body)
	}
	if strings.Contains(body, "sk-upstream-secret") {
		t.Fatal("request body must never carry the credential")
	}
	if resp.Object != "list" || len(resp.Data) != 2 {
		t.Fatalf("normalized response = %+v", resp)
	}
	if resp.Data[0].Index != 0 || len(resp.Data[0].Embedding) != 2 || resp.Data[0].Embedding[0] != 0.25 {
		t.Fatalf("vector translation = %+v", resp.Data[0])
	}
	if resp.Data[1].Index != 1 || len(resp.Data[1].Embedding) != 1 {
		t.Fatalf("vector translation = %+v", resp.Data[1])
	}
	if resp.Usage == nil || !resp.Usage.Known || resp.Usage.PromptTokens != 7 ||
		resp.Usage.CompletionTokens != 0 || resp.Usage.TotalTokens != 7 {
		t.Fatalf("usage must be known and prompt-only: %+v", resp.Usage)
	}

	cases := []struct {
		model string
		want  ErrClass
	}{
		{"reject", ClassInvalid},
		{"limited", ClassRateLimited},
		{"boom", ClassServer},
		{"malformed", ClassServer},
	}
	for _, tc := range cases {
		_, err := p.Embeddings(context.Background(), embeddingsRequest(tc.model, "trailing"))
		if ClassOf(err) != tc.want {
			t.Errorf("%s: class = %v, want %v (%v)", tc.model, ClassOf(err), tc.want, err)
		}
	}
	// Timeout class under a deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	t.Cleanup(func() { slow.CloseClientConnections(); slow.Close() })
	sp := NewOpenAI(slow.URL, "sk-upstream-secret")
	if _, err := sp.Embeddings(ctx, embeddingsRequest("x")); ClassOf(err) != ClassTimeout {
		t.Errorf("timeout class = %v (%v)", ClassOf(err), err)
	}
}

func TestAnthropicEmbeddingsUnsupported(t *testing.T) {
	p := NewAnthropic("https://api.anthropic.com", "sk-ant-secret")
	if caps := p.Capabilities("m"); caps.Embeddings {
		t.Fatal("anthropic must declare embeddings false")
	}
	_, err := p.Embeddings(context.Background(), embeddingsRequest("x"))
	if err == nil {
		t.Fatal("anthropic embeddings must fail")
	}
	if !errors.Is(err, model.ErrCapabilityNotSupported) {
		t.Fatalf("must wrap the capability sentinel, got %v", err)
	}
	// And the shared precheck agrees, so requests never reach the adapter.
	if err := model.CheckCapabilities(p.Capabilities("m"), "embeddings", model.Request{}); err == nil {
		t.Fatal("precheck must reject the embeddings protocol for anthropic")
	}
}

func TestCheckEmbeddingDim(t *testing.T) {
	resp := model.EmbeddingsResponse{Data: []model.Embedding{
		{Embedding: make([]float64, 4)}, {Embedding: make([]float64, 4)},
	}}
	if err := model.CheckEmbeddingDim(4, resp); err != nil {
		t.Fatalf("matching dim must pass: %v", err)
	}
	// Undeclared (zero) skips the check: pass-through, server reads what it gets.
	if err := model.CheckEmbeddingDim(0, resp); err != nil {
		t.Fatalf("undeclared dim must skip the check: %v", err)
	}
	err := model.CheckEmbeddingDim(1536, resp)
	if !errors.Is(err, model.ErrEmbeddingDimMismatch) {
		t.Fatalf("mismatch must wrap the sentinel, got %v", err)
	}
	// A single wrong-width vector in a batch fails the whole response.
	resp.Data[1].Embedding = make([]float64, 8)
	if err := model.CheckEmbeddingDim(4, resp); !errors.Is(err, model.ErrEmbeddingDimMismatch) {
		t.Fatalf("partial mismatch must fail: %v", err)
	}
}
