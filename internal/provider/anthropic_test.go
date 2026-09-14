package provider

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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

func TestAnthropicCompleteContract(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages" {
			t.Errorf("path = %s", r.URL.Path)
		}
		if r.Header.Get("x-api-key") != "secret" || r.Header.Get("anthropic-version") == "" {
			t.Error("missing anthropic auth headers")
		}
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req["system"] != "be brief" || req["max_tokens"].(float64) != 64 {
			t.Errorf("bad request body: %v", req)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "msg_1", "type": "message", "role": "assistant", "model": "claude-x",
			"content":     []map[string]string{{"type": "text", "text": "hello"}},
			"stop_reason": "end_turn",
			"usage":       map[string]int{"input_tokens": 5, "output_tokens": 2},
		})
	}))
	defer srv.Close()

	a := NewAnthropic(srv.URL, "secret")
	mt := 64
	resp, err := a.Complete(context.Background(), ChatRequest{
		Model: "claude-x",
		Messages: []Message{
			{Role: "system", Content: "be brief"},
			{Role: "user", Content: "hi"},
		},
		MaxTokens: &mt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Object != "chat.completion" || resp.Choices[0].Message.Content != "hello" {
		t.Fatalf("unexpected normalized response: %+v", resp)
	}
	if !resp.Usage.Known || resp.Usage.TotalTokens != 7 {
		t.Fatalf("usage not normalized: %+v", resp.Usage)
	}
	if resp.Choices[0].FinishReason != "stop" {
		t.Fatalf("stop_reason not mapped: %q", resp.Choices[0].FinishReason)
	}
}

func TestAnthropicStreamContract(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "event: message_start\n")
		io.WriteString(w, `data: {"type":"message_start","message":{"id":"msg_2","model":"claude-x"}}`+"\n\n")
		io.WriteString(w, "event: content_block_delta\n")
		io.WriteString(w, `data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"he"}}`+"\n\n")
		io.WriteString(w, "event: content_block_delta\n")
		io.WriteString(w, `data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"y"}}`+"\n\n")
		io.WriteString(w, "event: message_delta\n")
		io.WriteString(w, `data: {"type":"message_delta","delta":{"stop_reason":"end_turn"}}`+"\n\n")
		io.WriteString(w, "event: message_stop\n")
		io.WriteString(w, `data: {"type":"message_stop"}`+"\n\n")
	}))
	defer srv.Close()

	a := NewAnthropic(srv.URL, "secret")
	var chunks []string
	err := a.Stream(context.Background(), ChatRequest{Model: "claude-x"}, func(b []byte) error {
		chunks = append(chunks, string(b))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(chunks, "\n")
	for _, want := range []string{`"object":"chat.completion.chunk"`, `"content":"he"`, `"content":"y"`, `"finish_reason":"stop"`} {
		if !strings.Contains(joined, want) {
			t.Errorf("stream chunks missing %s in %s", want, joined)
		}
	}
}
