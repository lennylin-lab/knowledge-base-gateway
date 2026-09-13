// Package provider defines the provider boundary. No SDK types cross it.
package provider

import (
	"context"
	"errors"
	"fmt"
)

// Message is one chat message in OpenAI-compatible form.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatRequest is the normalized provider request. Model is the upstream name.
type ChatRequest struct {
	Model       string      `json:"model"`
	Messages    []Message   `json:"messages"`
	Temperature *float64    `json:"temperature,omitempty"`
	MaxTokens   *int        `json:"max_tokens,omitempty"`
	Stream      bool        `json:"stream"`
	RequestID   string      `json:"-"`
	Metadata    interface{} `json:"-"`
}

// Usage reports token counts; when the upstream supplies none, Known is false
// and the values must not be treated as zero.
type Usage struct {
	PromptTokens     int  `json:"prompt_tokens"`
	CompletionTokens int  `json:"completion_tokens"`
	TotalTokens      int  `json:"total_tokens"`
	Known            bool `json:"-"`
}

// Choice is one response choice; Message for non-streaming, Delta for streaming.
type Choice struct {
	Index        int      `json:"index"`
	Message      *Message `json:"message,omitempty"`
	Delta        *Message `json:"delta,omitempty"`
	FinishReason string   `json:"finish_reason,omitempty"`
}

// ChatResponse is the normalized provider response.
type ChatResponse struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []Choice `json:"choices"`
	Usage   *Usage   `json:"usage,omitempty"`
}

// ErrClass classifies provider failures for error mapping and retry decisions.
type ErrClass int

const (
	ClassNetwork ErrClass = iota // transient, retry eligible before output
	ClassRateLimited
	ClassServer // upstream 5xx
	ClassTimeout
	ClassInvalid // upstream rejected the request (bad model etc.)
	ClassInternal
)

// Error is a normalized provider error. It never carries provider secrets.
type Error struct {
	Class ErrClass
	Msg   string
}

func (e *Error) Error() string { return fmt.Sprintf("provider %s: %s", e.Class, e.Msg) }

// String names the error class.
func (c ErrClass) String() string {
	switch c {
	case ClassNetwork:
		return "network"
	case ClassRateLimited:
		return "rate_limited"
	case ClassServer:
		return "server"
	case ClassTimeout:
		return "timeout"
	case ClassInvalid:
		return "invalid"
	default:
		return "internal"
	}
}

// ClassOf extracts the ErrClass from an error, defaulting to ClassInternal.
func ClassOf(err error) ErrClass {
	var pe *Error
	if errors.As(err, &pe) {
		return pe.Class
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return ClassTimeout
	}
	return ClassInternal
}

// RetryEligible reports whether a pre-output failure may be retried.
func RetryEligible(err error) bool {
	switch ClassOf(err) {
	case ClassNetwork, ClassRateLimited, ClassServer, ClassTimeout:
		return true
	}
	return false
}

// Provider abstracts one upstream vendor. Stream sends already-encoded SSE
// data payloads (without the "data: " prefix) via send.
type Provider interface {
	Complete(ctx context.Context, req ChatRequest) (ChatResponse, error)
	Stream(ctx context.Context, req ChatRequest, send func(payload []byte) error) error
	Name() string
}
