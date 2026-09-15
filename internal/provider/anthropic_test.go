package provider

import (
	"context"
	"encoding/json"
	"fmt"
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

// TestAnthropicCompleteWithoutUsageIsUnknown pins R4/AC4: a non-streaming
// Messages response without a usage object leaves the domain usage unknown —
// the adapter must not fabricate known zero usage (which would settle token
// quota to zero and audit fabricated token counts).
func TestAnthropicCompleteWithoutUsageIsUnknown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id":"msg_u1","type":"message","role":"assistant","model":"up-model","content":[{"type":"text","text":"hello"}],"stop_reason":"end_turn"}`)
	}))
	defer srv.Close()
	a := NewAnthropic(srv.URL, "secret")
	resp, err := a.Complete(context.Background(), model.Request{
		Model: "up-model", Input: []model.InputItem{{Role: model.RoleUser, Text: "hi"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Usage != nil {
		t.Fatalf("usage must stay unknown when the upstream omits it, got %+v", resp.Usage)
	}
}

// TestAnthropicStreamWithoutUsageIsUnknown pins R4/AC4 for streams: a
// Messages stream whose message_start and message_delta carry no usage
// objects completes with unknown usage, never fabricated zeros.
func TestAnthropicStreamWithoutUsageIsUnknown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		for _, e := range []string{
			`{"type":"message_start","message":{"id":"msg_u2","model":"up-model"}}`,
			`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"he"}}`,
			`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"y"}}`,
			`{"type":"message_delta","delta":{"stop_reason":"end_turn"}}`,
			`{"type":"message_stop"}`,
		} {
			var typed struct {
				Type string `json:"type"`
			}
			_ = json.Unmarshal([]byte(e), &typed)
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", typed.Type, e)
			f.Flush()
		}
	}))
	defer srv.Close()
	a := NewAnthropic(srv.URL, "secret")
	var completed *model.Response
	err := a.Stream(context.Background(), model.Request{
		Model: "up-model", Input: []model.InputItem{{Role: model.RoleUser, Text: "hi"}},
	}, func(e model.Event) error {
		if e.Kind == model.EventCompleted {
			completed = e.Response
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if completed == nil {
		t.Fatal("stream must complete")
	}
	if completed.Usage != nil {
		t.Fatalf("stream usage must stay unknown when the upstream omits it, got %+v", completed.Usage)
	}
	if completed.Text() != "hey" {
		t.Errorf("text = %q, want %q", completed.Text(), "hey")
	}
}

// TestAnthropicStreamWithUsageStillKnown guards against over-correction: when
// the upstream does report usage, it stays known with the reported values.
func TestAnthropicStreamWithUsageStillKnown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		for _, e := range []string{
			`{"type":"message_start","message":{"id":"msg_u3","model":"up-model","usage":{"input_tokens":5}}}`,
			`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hey"}}`,
			`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}`,
			`{"type":"message_stop"}`,
		} {
			var typed struct {
				Type string `json:"type"`
			}
			_ = json.Unmarshal([]byte(e), &typed)
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", typed.Type, e)
			f.Flush()
		}
	}))
	defer srv.Close()
	a := NewAnthropic(srv.URL, "secret")
	var completed *model.Response
	err := a.Stream(context.Background(), model.Request{
		Model: "up-model", Input: []model.InputItem{{Role: model.RoleUser, Text: "hi"}},
	}, func(e model.Event) error {
		if e.Kind == model.EventCompleted {
			completed = e.Response
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if completed == nil || completed.Usage == nil || !completed.Usage.Known {
		t.Fatalf("reported usage must stay known: %+v", completed)
	}
	if completed.Usage.PromptTokens != 5 || completed.Usage.CompletionTokens != 2 {
		t.Errorf("usage = %+v, want prompt 5 completion 2", completed.Usage)
	}
}
