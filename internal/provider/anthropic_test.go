package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
)

func TestValidateBaseURL(t *testing.T) {
	cases := []struct {
		raw       string
		allowHTTP bool
		wantErr   bool
	}{
		{"https://api.openai.com/v1", false, false},
		{"http://api.openai.com/v1", false, true},
		{"http://127.0.0.1:8080/v1", true, false},
		{"ftp://api.openai.com", false, true},
		{"https://user:pass@api.openai.com", false, true},
		{"https://[::1]:8080", true, false},
		{"internal://fake", false, true},
	}
	for _, c := range cases {
		err := ValidateBaseURL(c.raw, c.allowHTTP)
		if (err != nil) != c.wantErr {
			t.Errorf("ValidateBaseURL(%q, %v) err=%v wantErr=%v", c.raw, c.allowHTTP, err, c.wantErr)
		}
	}
}

// TestAnthropicWireRequestShape pins the vendor request translation:
// system hoisting, content blocks, tool definitions with input_schema, and
// tool_choice mapping.
func TestAnthropicWireRequestShape(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages" {
			t.Errorf("path = %s", r.URL.Path)
		}
		if r.Header.Get("x-api-key") != "secret" || r.Header.Get("anthropic-version") == "" {
			t.Error("missing anthropic auth headers")
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusTeapot)
	}))
	defer srv.Close()

	a := NewAnthropic(srv.URL, "secret")
	mt := 64
	req := model.Request{
		Model:        "claude-x",
		Instructions: "be brief",
		Input: []model.InputItem{
			{Role: model.RoleUser, Text: "weather?"},
			{ToolCall: &model.ToolCall{ID: "call_1", Name: "get_weather", Arguments: `{"city":"paris"}`}},
			{ToolResult: &model.ToolResult{CallID: "call_1", Content: "sunny"}},
		},
		MaxTokens: &mt,
		Tools: []model.ToolDefinition{{
			Name:        "get_weather",
			Description: "lookup weather",
			Parameters:  json.RawMessage(`{"type":"object","properties":{"city":{"type":"string"}}}`),
		}},
		ToolChoice: "required",
	}
	_, _ = a.Complete(context.Background(), req)

	if got["system"] != "be brief" || got["max_tokens"] != float64(64) {
		t.Errorf("system/max_tokens wrong: %v", got)
	}
	msgs, _ := got["messages"].([]any)
	if len(msgs) != 3 {
		t.Fatalf("messages = %d, want 3 (user text, assistant tool_use, user tool_result): %v", len(msgs), got["messages"])
	}
	first, _ := msgs[0].(map[string]any)
	if first["role"] != "user" {
		t.Errorf("first message role = %v", first["role"])
	}
	second, _ := msgs[1].(map[string]any)
	if second["role"] != "assistant" {
		t.Errorf("tool call must ride in an assistant turn: %v", second)
	}
	blocks, _ := second["content"].([]any)
	if len(blocks) != 1 {
		t.Fatalf("assistant blocks = %v", second["content"])
	}
	call, _ := blocks[0].(map[string]any)
	if call["type"] != "tool_use" || call["id"] != "call_1" || call["name"] != "get_weather" {
		t.Errorf("tool_use block wrong: %v", call)
	}
	input, _ := call["input"].(map[string]any)
	if input["city"] != "paris" {
		t.Errorf("tool_use input wrong: %v", input)
	}
	third, _ := msgs[2].(map[string]any)
	if third["role"] != "user" {
		t.Errorf("tool result must ride in a user turn: %v", third)
	}
	rblocks, _ := third["content"].([]any)
	res, _ := rblocks[0].(map[string]any)
	if res["type"] != "tool_result" || res["tool_use_id"] != "call_1" || res["content"] != "sunny" {
		t.Errorf("tool_result block wrong: %v", res)
	}
	tools, _ := got["tools"].([]any)
	if len(tools) != 1 {
		t.Fatalf("tools = %v", got["tools"])
	}
	tool, _ := tools[0].(map[string]any)
	if tool["name"] != "get_weather" || tool["input_schema"] == nil {
		t.Errorf("tool definition wrong: %v", tool)
	}
	tc, _ := got["tool_choice"].(map[string]any)
	if tc == nil || tc["type"] != "any" {
		t.Errorf("tool_choice required must map to any: %v", got["tool_choice"])
	}
}

// TestAnthropicRejectsResponseSpec asserts the adapter-level guard for a
// capability the Messages API translation cannot honor.
func TestAnthropicRejectsResponseSpec(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("provider must not be called when the spec cannot be translated")
	}))
	defer srv.Close()
	a := NewAnthropic(srv.URL, "secret")
	req := model.Request{
		Model: "claude-x",
		Input: []model.InputItem{{Role: model.RoleUser, Text: "hi"}},
		ResponseSpec: &model.ResponseSpec{
			Mode:   model.ModeJSONSchema,
			Schema: json.RawMessage(`{"type":"object"}`),
		},
	}
	_, err := a.Complete(context.Background(), req)
	if err == nil {
		t.Fatal("structured output must be rejected by this adapter")
	}
	if err.Error() == "" {
		t.Fatal("error must be descriptive")
	}
}
