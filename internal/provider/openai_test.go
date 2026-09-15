package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
)

func upstreamServer(t *testing.T, handler http.HandlerFunc) *OpenAI {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return NewOpenAI(srv.URL, "sk-upstream-secret")
}

func TestOpenAIErrorClassification(t *testing.T) {
	cases := map[int]ErrClass{
		http.StatusTooManyRequests: ClassRateLimited,
		http.StatusBadGateway:      ClassServer,
		http.StatusBadRequest:      ClassInvalid,
	}
	for code, want := range cases {
		p := upstreamServer(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) })
		_, err := p.Complete(context.Background(), model.Request{Model: "m", Input: []model.InputItem{{Role: "user", Text: "x"}}})
		if ClassOf(err) != want {
			t.Errorf("status %d: class = %v, want %v", code, ClassOf(err), want)
		}
		if want == ClassInvalid && RetryEligible(err) {
			t.Errorf("status %d: must not be retry eligible", code)
		}
		if strings.Contains(err.Error(), "sk-") {
			t.Errorf("error leaks secret: %v", err)
		}
	}
}

// TestOpenAIWireRequestShape pins the vendor request translation: messages,
// tool definitions, tool_choice, and response_format must appear in the exact
// OpenAI Chat Completions shapes.
func TestOpenAIWireRequestShape(t *testing.T) {
	temp := 0.5
	mt := 128
	var got map[string]any
	p := upstreamServer(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusTeapot)
	})
	req := model.Request{
		Model:        "up-model",
		Instructions: "be brief",
		Input: []model.InputItem{
			{Role: model.RoleUser, Text: "weather?"},
			{ToolCall: &model.ToolCall{ID: "call_1", Name: "get_weather", Arguments: `{"city":"paris"}`}},
			{ToolResult: &model.ToolResult{CallID: "call_1", Content: "sunny"}},
			{Role: model.RoleUser, Text: "thanks"},
		},
		Temperature: &temp,
		MaxTokens:   &mt,
		Tools: []model.ToolDefinition{{
			Name: "get_weather", Description: "lookup weather",
			Parameters: json.RawMessage(`{"type":"object","properties":{"city":{"type":"string"}}}`),
		}},
		ToolChoice: "auto",
		ResponseSpec: &model.ResponseSpec{
			Mode: model.ModeJSONSchema, Name: "answer", Strict: true,
			Schema: json.RawMessage(`{"type":"object"}`),
		},
	}
	_, _ = p.Complete(context.Background(), req)

	if got["model"] != "up-model" || got["temperature"] != 0.5 || got["max_tokens"] != float64(mt) {
		t.Errorf("generation controls wrong: %v", got)
	}
	msgs, _ := got["messages"].([]any)
	if len(msgs) != 5 {
		t.Fatalf("messages = %d, want 5 (system + 4 items): %v", len(msgs), got["messages"])
	}
	first, _ := msgs[0].(map[string]any)
	if first["role"] != "system" || first["content"] != "be brief" {
		t.Errorf("instructions not hoisted: %v", first)
	}
	assistant, _ := msgs[2].(map[string]any)
	tcs, _ := assistant["tool_calls"].([]any)
	if assistant["role"] != "assistant" || len(tcs) != 1 {
		t.Errorf("assistant tool call message wrong: %v", assistant)
	}
	tc, _ := tcs[0].(map[string]any)
	fn, _ := tc["function"].(map[string]any)
	if tc["id"] != "call_1" || fn["name"] != "get_weather" || fn["arguments"] != `{"city":"paris"}` {
		t.Errorf("tool call wire wrong: %v", tc)
	}
	toolMsg, _ := msgs[3].(map[string]any)
	if toolMsg["role"] != "tool" || toolMsg["tool_call_id"] != "call_1" || toolMsg["content"] != "sunny" {
		t.Errorf("tool result message wrong: %v", toolMsg)
	}
	tools, _ := got["tools"].([]any)
	if len(tools) != 1 {
		t.Fatalf("tools = %v", got["tools"])
	}
	tool, _ := tools[0].(map[string]any)
	if tool["type"] != "function" {
		t.Errorf("tool type = %v", tool["type"])
	}
	fnDef, _ := tool["function"].(map[string]any)
	if fnDef["name"] != "get_weather" || fnDef["description"] != "lookup weather" {
		t.Errorf("tool definition wrong: %v", tool)
	}
	if got["tool_choice"] != "auto" {
		t.Errorf("tool_choice = %v", got["tool_choice"])
	}
	rf, _ := got["response_format"].(map[string]any)
	if rf == nil || rf["type"] != "json_schema" {
		t.Fatalf("response_format = %v", got["response_format"])
	}
	js, _ := rf["json_schema"].(map[string]any)
	if js["name"] != "answer" || js["strict"] != true {
		t.Errorf("json_schema = %v", js)
	}
}

// TestOpenAIStreamUsageSurfaces pins that stream usage is only surfaced when
// the upstream reports it (never fabricated).
func TestOpenAIStreamUsageSurfaces(t *testing.T) {
	p := upstreamServer(t, func(w http.ResponseWriter, _ *http.Request) {
		sse(w,
			`{"id":"c","choices":[{"delta":{"content":"he"}}]}`,
			`{"id":"c","choices":[{"delta":{"content":"y"}}],"usage":{"prompt_tokens":9,"completion_tokens":2,"total_tokens":11}}`,
			`{"id":"c","choices":[{"delta":{},"finish_reason":"stop"}]}`,
		)
	})
	var completed *model.Response
	err := p.Stream(context.Background(), model.Request{Model: "m", Input: []model.InputItem{{Role: "user", Text: "x"}}},
		func(e model.Event) error {
			if e.Kind == model.EventCompleted {
				completed = e.Response
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if completed == nil || completed.Usage == nil || !completed.Usage.Known || completed.Usage.TotalTokens != 11 {
		t.Errorf("usage not surfaced: %+v", completed)
	}
}

func TestOpenAIUpstreamTimeout(t *testing.T) {
	p := upstreamServer(t, func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(300 * time.Millisecond)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := p.Complete(ctx, model.Request{Model: "m", Input: []model.InputItem{{Role: "user", Text: "x"}}})
	if ClassOf(err) != ClassTimeout {
		t.Errorf("class = %v, want timeout", ClassOf(err))
	}
	if !RetryEligible(err) {
		t.Errorf("timeout should be retry eligible pre-output")
	}
}
