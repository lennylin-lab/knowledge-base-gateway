package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func upstreamServer(t *testing.T, handler http.HandlerFunc) *OpenAI {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return NewOpenAI(srv.URL, "sk-upstream-secret")
}

func TestOpenAIComplete(t *testing.T) {
	p := upstreamServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer sk-upstream-secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"cmpl-1","object":"chat.completion","created":1700000000,"model":"gpt-x","choices":[{"index":0,"message":{"role":"assistant","content":"hi"}}],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}`))
	})
	resp, err := p.Complete(context.Background(), ChatRequest{Model: "gpt-x", Messages: []Message{{Role: "user", Content: "hello"}}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.ID != "cmpl-1" || len(resp.Choices) != 1 || resp.Choices[0].Message.Content != "hi" {
		t.Errorf("bad response: %+v", resp)
	}
	if !resp.Usage.Known || resp.Usage.TotalTokens != 7 {
		t.Errorf("usage: %+v", resp.Usage)
	}
}

func TestOpenAIErrorClassification(t *testing.T) {
	cases := map[int]ErrClass{
		http.StatusTooManyRequests: ClassRateLimited,
		http.StatusBadGateway:      ClassServer,
		http.StatusBadRequest:      ClassInvalid,
	}
	for code, want := range cases {
		p := upstreamServer(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) })
		_, err := p.Complete(context.Background(), ChatRequest{Model: "m", Messages: []Message{{Role: "user", Content: "x"}}})
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

func TestOpenAIStream(t *testing.T) {
	p := upstreamServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		for _, chunk := range []string{`{"id":"c","choices":[{"delta":{"content":"he"}}]}`, `{"id":"c","choices":[{"delta":{"content":"y"}}]}`} {
			_, _ = fmt.Fprintf(w, "data: %s\n\n", chunk)
			f.Flush()
		}
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
		f.Flush()
	})
	var payloads [][]byte
	err := p.Stream(context.Background(), ChatRequest{Model: "m", Messages: []Message{{Role: "user", Content: "x"}}}, func(pl []byte) error {
		payloads = append(payloads, pl)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(payloads) != 2 {
		t.Fatalf("payloads = %d, want 2", len(payloads))
	}
	var chunk ChatResponse
	if err := json.Unmarshal(payloads[0], &chunk); err != nil {
		t.Fatal(err)
	}
	if chunk.Choices[0].Delta.Content != "he" {
		t.Errorf("chunk = %+v", chunk)
	}
}

func TestOpenAIUpstreamTimeout(t *testing.T) {
	p := upstreamServer(t, func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(300 * time.Millisecond)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := p.Complete(ctx, ChatRequest{Model: "m", Messages: []Message{{Role: "user", Content: "x"}}})
	if ClassOf(err) != ClassTimeout {
		t.Errorf("class = %v, want timeout", ClassOf(err))
	}
	if !RetryEligible(err) {
		t.Errorf("timeout should be retry eligible pre-output")
	}
}

func TestFakeProviderEchoAndStream(t *testing.T) {
	resp, err := Fake{}.Complete(context.Background(), ChatRequest{Model: "m", Messages: []Message{{Role: "user", Content: "abc"}}})
	if err != nil || resp.Choices[0].Message.Content != "echo: abc" {
		t.Fatalf("resp = %+v, err = %v", resp, err)
	}
	var out strings.Builder
	f := Fake{}
	err = f.Stream(context.Background(), ChatRequest{Model: "m", Messages: []Message{{Role: "user", Content: "abcdef"}}}, func(pl []byte) error {
		var c ChatResponse
		if err := json.Unmarshal(pl, &c); err != nil {
			return err
		}
		if c.Choices[0].Delta != nil {
			out.WriteString(c.Choices[0].Delta.Content)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != "echo: abcdef" {
		t.Errorf("stream = %q", out.String())
	}
}
