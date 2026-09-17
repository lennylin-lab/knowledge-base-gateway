package httpapi

// V1.2 tool calling and structured output over the Chat Completions protocol.
// The additions are additive: every legacy fixture keeps passing unchanged.

import (
	"encoding/json"
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

func TestChatToolCallContract(t *testing.T) {
	h, _ := newGoldenHandlerCaps(t, fullCaps, provider.Fake{}, 1000)
	body := `{"model":"` + goldenModel + `","messages":[{"role":"user","content":"paris?"}],"tools":[{"type":"function","function":{"name":"get_weather","description":"lookup","parameters":{"type":"object","properties":{"city":{"type":"string"}}}}}]}`
	rec := goldenPost(t, h, "/v1/chat/completions", body, testKey, "req-chat-tool")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp chatCompletionOut
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Object != "chat.completion" {
		t.Fatalf("object = %q", resp.Object)
	}
	msg := resp.Choices[0].Message
	if len(msg.ToolCalls) != 1 {
		t.Fatalf("tool calls = %+v", msg.ToolCalls)
	}
	tc := msg.ToolCalls[0]
	if tc.Function.Name != "get_weather" || !json.Valid([]byte(tc.Function.Arguments)) {
		t.Fatalf("tool call wrong: %+v", tc)
	}
	if resp.Choices[0].FinishReason != model.FinishToolCalls {
		t.Fatalf("finish = %q", resp.Choices[0].FinishReason)
	}
}

func TestChatToolResultRoundTrip(t *testing.T) {
	h, _ := newGoldenHandlerCaps(t, fullCaps, provider.Fake{}, 1000)
	body := `{"model":"` + goldenModel + `","messages":[
		{"role":"user","content":"paris?"},
		{"role":"assistant","tool_calls":[{"id":"call_x","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"paris\"}"}}]},
		{"role":"tool","tool_call_id":"call_x","content":"sunny 22C"}
	],"tools":[{"type":"function","function":{"name":"get_weather","parameters":{"type":"object"}}}]}`
	rec := goldenPost(t, h, "/v1/chat/completions", body, testKey, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp chatCompletionOut
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if !strings.Contains(resp.Choices[0].Message.Content, "tool ok: sunny 22C") {
		t.Fatalf("round trip lost the tool result: %+v", resp.Choices[0].Message)
	}
}

func TestChatStreamingToolCall(t *testing.T) {
	h, _ := newGoldenHandlerCaps(t, fullCaps, provider.Fake{}, 1000)
	body := `{"model":"` + goldenModel + `","messages":[{"role":"user","content":"paris?"}],"stream":true,"tools":[{"type":"function","function":{"name":"get_weather","parameters":{"type":"object"}}}]}`
	rec := goldenPost(t, h, "/v1/chat/completions", body, testKey, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	raw := rec.Body.String()
	if !strings.HasSuffix(raw, "data: [DONE]\n\n") {
		t.Fatalf("stream must end with [DONE]")
	}
	var args strings.Builder
	finish := ""
	firstFragmentSeen := false
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
				for _, tc := range c.Delta.ToolCalls {
					if tc.Index == nil {
						t.Fatal("streaming tool delta must carry an index")
					}
					if !firstFragmentSeen {
						firstFragmentSeen = true
						// OpenAI streaming convention: the first fragment for
						// a call carries id and function name (issue #2).
						if tc.ID == "" || tc.Function.Name == "" {
							t.Fatalf("first tool_call fragment must carry id and name, got id=%q name=%q", tc.ID, tc.Function.Name)
						}
					} else if tc.Function.Name != "" || tc.ID != "" {
						// Clients concatenate name across fragments; repeating
						// identity corrupts dispatch (server retest feedback).
						t.Fatalf("subsequent tool_call fragment must not repeat identity, got id=%q name=%q", tc.ID, tc.Function.Name)
					}
					args.WriteString(tc.Function.Arguments)
				}
			}
			if c.FinishReason != "" {
				finish = c.FinishReason
			}
		}
	}
	if !json.Valid([]byte(args.String())) {
		t.Fatalf("assembled stream args invalid: %q", args.String())
	}
	if finish != model.FinishToolCalls {
		t.Fatalf("finish = %q", finish)
	}
}

func TestChatCapabilityRejectionBeforeProvider(t *testing.T) {
	// The catalog declares no tools/structured output for this model.
	catalogCaps := model.Capabilities{Chat: true, Stream: true, Usage: true}
	store := auth.NewStore()
	salt, _ := auth.NewSalt()
	store.Put(auth.KeyRecord{ID: "k", Subject: "subject-a", Salt: salt, Hash: auth.HashAPIKey(salt, testKey), Status: auth.StatusActive})
	catalog := policy.NewCatalog([]policy.ModelInfo{
		{PublicName: "gpt-test", Provider: "fake", UpstreamModel: "up", Enabled: true, Capabilities: catalogCaps},
	})
	pol := policy.New()
	pol.Allow("subject-a", "gpt-test")
	svc := gateway.New(catalog, map[string]provider.Provider{"fake": provider.Fake{}}, time.Second, 0)
	sink := audit.NewMemorySink(nil)
	h := &ChatHandler{
		Auth: store, Service: svc, Policy: pol,
		Limiter: limiter.New(1000, 100), Audit: sink, Metrics: metrics.New(),
		MaxBody: 1 << 20, MaxMsgs: 64, MaxChars: 32_000,
	}
	cases := map[string]string{
		"tools":     `{"model":"gpt-test","messages":[{"role":"user","content":"x"}],"tools":[{"type":"function","function":{"name":"f"}}]}`,
		"json_mode": `{"model":"gpt-test","messages":[{"role":"user","content":"x"}],"response_format":{"type":"json_object"}}`,
		"schema":    `{"model":"gpt-test","messages":[{"role":"user","content":"x"}],"response_format":{"type":"json_schema","json_schema":{"name":"s","schema":{"type":"object"}}}}`,
	}
	wantCap := map[string]string{
		"tools":     "capability 'tools'",
		"json_mode": "capability 'json_mode'",
		"schema":    "capability 'structured_output'",
	}
	for name, body := range cases {
		rec := doChat(t, h, body, testKey, "")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400: %s", name, rec.Code, rec.Body.String())
			continue
		}
		if !strings.Contains(rec.Body.String(), "capability_not_supported") {
			t.Errorf("%s: wrong code: %s", name, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), wantCap[name]) {
			t.Errorf("%s: message must name the failed capability: %s", name, rec.Body.String())
		}
	}
}

func TestChatSchemaValidationFailureRecorded(t *testing.T) {
	h, sink := newGoldenHandlerCaps(t, fullCaps, provider.Fake{}, 1000)
	// The mock echoes {"echo": "..."}; requiring a different shape fails
	// validation, which audit must record.
	body := `{"model":"` + goldenModel + `","messages":[{"role":"user","content":"hello"}],"response_format":{"type":"json_schema","json_schema":{"name":"s","schema":{"type":"object","required":["different_field"],"properties":{"different_field":{"type":"string"}}}}}}`
	rec := goldenPost(t, h, "/v1/chat/completions", body, testKey, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	ev := sink.Snapshot()
	if len(ev) != 1 || ev[0].ErrorClass != "schema_validation_failed" {
		t.Fatalf("audit must record the schema failure: %+v", ev)
	}
}

func TestChatJSONModeOutput(t *testing.T) {
	h, _ := newGoldenHandlerCaps(t, fullCaps, provider.Fake{}, 1000)
	body := `{"model":"` + goldenModel + `","messages":[{"role":"user","content":"hello"}],"response_format":{"type":"json_object"}}`
	rec := goldenPost(t, h, "/v1/chat/completions", body, testKey, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var resp chatCompletionOut
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if !json.Valid([]byte(resp.Choices[0].Message.Content)) {
		t.Fatalf("json mode output invalid: %q", resp.Choices[0].Message.Content)
	}
}
