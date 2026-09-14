package provider

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Anthropic adapts the Anthropic Messages API to the normalized Provider
// interface. Vendor protocol details stay inside this file.
type Anthropic struct {
	BaseURL string
	APIKey  string
	Version string // anthropic-version header; empty uses a pinned default
	Client  *http.Client
}

// NewAnthropic builds the adapter; baseURL should include the API root
// (e.g. https://api.anthropic.com).
func NewAnthropic(baseURL, apiKey string) *Anthropic {
	return &Anthropic{
		BaseURL: strings.TrimRight(baseURL, "/"),
		APIKey:  apiKey,
		Version: "2023-06-01",
		Client:  &http.Client{Timeout: 0},
	}
}

func (a *Anthropic) Name() string { return "anthropic" }

type anthropicRequest struct {
	Model     string             `json:"model"`
	System    string             `json:"system,omitempty"`
	Messages  []anthropicMessage `json:"messages"`
	MaxTokens int                `json:"max_tokens"`
	Stream    bool               `json:"stream"`
}

type anthropicMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type anthropicResponse struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Role    string `json:"role"`
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	Model      string `json:"model"`
	StopReason string `json:"stop_reason"`
	Usage      struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"`
}

// translate converts normalized messages into Anthropic form, hoisting system
// content out of the message list.
func translate(messages []Message) (string, []anthropicMessage) {
	system := ""
	var out []anthropicMessage
	for _, m := range messages {
		switch m.Role {
		case "system":
			if system != "" {
				system += "\n"
			}
			system += m.Content
		case "assistant":
			out = append(out, anthropicMessage{Role: "assistant", Content: m.Content})
		default:
			out = append(out, anthropicMessage{Role: "user", Content: m.Content})
		}
	}
	return system, out
}

func (a *Anthropic) do(ctx context.Context, req ChatRequest, stream bool) (*anthropicResponse, *http.Response, error) {
	system, msgs := translate(req.Messages)
	maxTokens := 1024
	if req.MaxTokens != nil && *req.MaxTokens > 0 {
		maxTokens = *req.MaxTokens
	}
	body, err := json.Marshal(anthropicRequest{
		Model: req.Model, System: system, Messages: msgs, MaxTokens: maxTokens, Stream: stream,
	})
	if err != nil {
		return nil, nil, &Error{Class: ClassInternal, Msg: "encode request"}
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.BaseURL+"/v1/messages", bytes.NewReader(body))
	if err != nil {
		return nil, nil, &Error{Class: ClassInternal, Msg: "build request"}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-api-key", a.APIKey)
	httpReq.Header.Set("anthropic-version", a.Version)
	resp, err := a.Client.Do(httpReq)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, nil, &Error{Class: ClassTimeout, Msg: "request canceled or deadline exceeded"}
		}
		return nil, nil, &Error{Class: ClassNetwork, Msg: "upstream connection failed"}
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, nil, classifyStatus(resp.StatusCode)
	}
	if stream {
		return nil, resp, nil
	}
	defer resp.Body.Close()
	var out anthropicResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&out); err != nil {
		return nil, nil, &Error{Class: ClassServer, Msg: "malformed upstream response"}
	}
	return &out, nil, nil
}

// Complete performs a non-streaming Messages call, normalized to the OpenAI
// compatible response shape used across the gateway.
func (a *Anthropic) Complete(ctx context.Context, req ChatRequest) (ChatResponse, error) {
	out, _, err := a.do(ctx, req, false)
	if err != nil {
		return ChatResponse{}, err
	}
	text := ""
	for _, c := range out.Content {
		if c.Type == "text" {
			text += c.Text
		}
	}
	return ChatResponse{
		ID: out.ID, Object: "chat.completion", Created: 0, Model: out.Model,
		Choices: []Choice{{
			Index: 0, Message: &Message{Role: "assistant", Content: text},
			FinishReason: finishFromStop(out.StopReason),
		}},
		Usage: &Usage{
			PromptTokens: out.Usage.InputTokens, CompletionTokens: out.Usage.OutputTokens,
			TotalTokens: out.Usage.InputTokens + out.Usage.OutputTokens, Known: true,
		},
	}, nil
}

func finishFromStop(reason string) string {
	switch reason {
	case "max_tokens":
		return "length"
	case "":
		return ""
	default:
		return "stop"
	}
}

// Stream forwards the Messages SSE stream, re-emitting each text delta as an
// OpenAI-compatible chat.completion.chunk payload.
func (a *Anthropic) Stream(ctx context.Context, req ChatRequest, send func(payload []byte) error) error {
	_, resp, err := a.do(ctx, req, true)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	var event string
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return &Error{Class: ClassTimeout, Msg: "request canceled or deadline exceeded"}
		}
		line := scanner.Bytes()
		switch {
		case bytes.HasPrefix(line, []byte("event:")):
			event = strings.TrimSpace(string(line[len("event:"):]))
		case bytes.HasPrefix(line, []byte("data:")):
			payload := bytes.TrimSpace(line[len("data:"):])
			if len(payload) == 0 {
				continue
			}
			var evt struct {
				Type  string `json:"type"`
				Delta struct {
					Type       string `json:"type"`
					Text       string `json:"text"`
					StopReason string `json:"stop_reason"`
				} `json:"delta"`
				Message struct {
					ID    string `json:"id"`
					Model string `json:"model"`
				} `json:"message"`
				Usage struct {
					OutputTokens int `json:"output_tokens"`
				} `json:"usage"`
			}
			if err := json.Unmarshal(payload, &evt); err != nil {
				return &Error{Class: ClassServer, Msg: "malformed upstream stream"}
			}
			var chunk ChatResponse
			chunk.ID = evt.Message.ID
			chunk.Object = "chat.completion.chunk"
			chunk.Model = evt.Message.Model
			finish := ""
			switch evt.Type {
			case "content_block_delta":
				chunk.Choices = []Choice{{Index: 0, Delta: &Message{Role: "assistant", Content: evt.Delta.Text}}}
			case "message_delta":
				finish = finishFromStop(evt.Delta.StopReason)
				chunk.Choices = []Choice{{Index: 0, Delta: &Message{}, FinishReason: finish}}
			case "message_stop":
				return nil
			case "error":
				return &Error{Class: ClassServer, Msg: fmt.Sprintf("upstream stream error event %s", event)}
			default:
				continue
			}
			b, _ := json.Marshal(chunk)
			if err := send(b); err != nil {
				return &Error{Class: ClassInternal, Msg: "downstream send failed"}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return &Error{Class: ClassNetwork, Msg: "upstream stream read failed"}
	}
	return nil
}
