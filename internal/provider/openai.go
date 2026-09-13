package provider

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// OpenAI is an OpenAI-compatible chat adapter. The API key is held only here
// and never logged or serialized.
type OpenAI struct {
	BaseURL string
	APIKey  string
	Client  *http.Client
}

// NewOpenAI builds the adapter with bounded transport timeouts.
func NewOpenAI(baseURL, apiKey string) *OpenAI {
	return &OpenAI{
		BaseURL: strings.TrimRight(baseURL, "/"),
		APIKey:  apiKey,
		Client:  &http.Client{Timeout: 0}, // deadline comes from request context
	}
}

func (o *OpenAI) Name() string { return "openai" }

func (o *OpenAI) do(ctx context.Context, req ChatRequest, stream bool) (*http.Response, error) {
	req.Stream = stream
	body, err := json.Marshal(req)
	if err != nil {
		return nil, &Error{Class: ClassInternal, Msg: "encode request"}
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, o.BaseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, &Error{Class: ClassInternal, Msg: "build request"}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+o.APIKey)
	resp, err := o.Client.Do(httpReq)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, &Error{Class: ClassTimeout, Msg: "request canceled or deadline exceeded"}
		}
		return nil, &Error{Class: ClassNetwork, Msg: "upstream connection failed"}
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, classifyStatus(resp.StatusCode)
	}
	return resp, nil
}

func classifyStatus(code int) error {
	switch {
	case code == http.StatusTooManyRequests:
		return &Error{Class: ClassRateLimited, Msg: "upstream rate limited"}
	case code >= 500:
		return &Error{Class: ClassServer, Msg: fmt.Sprintf("upstream server error %d", code)}
	case code == http.StatusRequestTimeout || code == http.StatusGatewayTimeout:
		return &Error{Class: ClassTimeout, Msg: "upstream timeout"}
	default:
		return &Error{Class: ClassInvalid, Msg: fmt.Sprintf("upstream rejected request with status %d", code)}
	}
}

// Complete performs a non-streaming completion.
func (o *OpenAI) Complete(ctx context.Context, req ChatRequest) (ChatResponse, error) {
	resp, err := o.do(ctx, req, false)
	if err != nil {
		return ChatResponse{}, err
	}
	defer resp.Body.Close()
	var out ChatResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&out); err != nil {
		return ChatResponse{}, &Error{Class: ClassServer, Msg: "malformed upstream response"}
	}
	if out.Usage == nil {
		out.Usage = &Usage{} // Known stays false; do not fabricate zeros
	} else {
		out.Usage.Known = true
	}
	return out, nil
}

// Stream performs a streaming completion, forwarding each upstream SSE data
// payload to send. Errors after the first send are not retried by callers.
func (o *OpenAI) Stream(ctx context.Context, req ChatRequest, send func(payload []byte) error) error {
	resp, err := o.do(ctx, req, true)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return &Error{Class: ClassTimeout, Msg: "request canceled or deadline exceeded"}
		}
		line := scanner.Bytes()
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		payload := bytes.TrimSpace(line[len("data:"):])
		if bytes.Equal(payload, []byte("[DONE]")) {
			return nil
		}
		if len(payload) == 0 {
			continue
		}
		if err := send(payload); err != nil {
			return &Error{Class: ClassInternal, Msg: "downstream send failed"}
		}
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return &Error{Class: ClassNetwork, Msg: "upstream stream read failed"}
	}
	return nil
}

// Fake is a deterministic in-process provider for development and tests.
type Fake struct{}

func (Fake) Name() string { return "fake" }

// Complete echoes the last user message with fixed usage.
func (Fake) Complete(ctx context.Context, req ChatRequest) (ChatResponse, error) {
	select {
	case <-ctx.Done():
		return ChatResponse{}, &Error{Class: ClassTimeout, Msg: "deadline exceeded"}
	case <-time.After(10 * time.Millisecond):
	}
	last := ""
	for _, m := range req.Messages {
		if m.Role == "user" {
			last = m.Content
		}
	}
	content := "echo: " + last
	n := len(content)
	return ChatResponse{
		ID: "chatcmpl-fake", Object: "chat.completion", Created: time.Now().Unix(), Model: req.Model,
		Choices: []Choice{{Index: 0, Message: &Message{Role: "assistant", Content: content}, FinishReason: "stop"}},
		Usage:   &Usage{PromptTokens: 10, CompletionTokens: n, TotalTokens: 10 + n, Known: true},
	}, nil
}

// Stream emits the echo content in fixed-size chunks, then finishes.
func (Fake) Stream(ctx context.Context, req ChatRequest, send func(payload []byte) error) error {
	resp, _ := Fake{}.Complete(ctx, req)
	chunk := resp
	msg := resp.Choices[0].Message
	const size = 8
	for i := 0; i < len(msg.Content); i += size {
		if err := ctx.Err(); err != nil {
			return &Error{Class: ClassTimeout, Msg: "deadline exceeded"}
		}
		end := min(i+size, len(msg.Content))
		c := chunk
		c.Choices = []Choice{{Index: 0, Delta: &Message{Role: "assistant", Content: msg.Content[i:end]}}}
		b, _ := json.Marshal(c)
		if err := send(b); err != nil {
			return &Error{Class: ClassInternal, Msg: "downstream send failed"}
		}
	}
	final := chunk
	final.Choices = []Choice{{Index: 0, Delta: &Message{}, FinishReason: "stop"}}
	b, _ := json.Marshal(final)
	return send(b)
}
