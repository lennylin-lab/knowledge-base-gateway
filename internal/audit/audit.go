// Package audit records request metadata. Prompt and completion content is
// never recorded, and no secret material reaches this layer.
package audit

import (
	"log/slog"
	"sync"
	"time"
)

// Event is one metadata-only audit record for a request lifecycle. The JSON
// tags define the stable management-API projection; there is no field that
// could carry prompt or completion content.
type Event struct {
	RequestID        string    `json:"request_id"`
	SubjectID        string    `json:"subject_id"`
	KeyID            string    `json:"key_id"`
	Model            string    `json:"model"`
	Provider         string    `json:"provider"`
	Status           int       `json:"status"`
	ErrorClass       string    `json:"error_class,omitempty"`
	LatencyMillis    int64     `json:"latency_ms"`
	PromptTokens     *int      `json:"prompt_tokens"`
	CompletionTokens *int      `json:"completion_tokens"`
	FirstTokenMillis *int64    `json:"first_token_ms"` // time to first streamed output event; nil when not measured
	Streaming        bool      `json:"streaming"`
	CreatedAt        time.Time `json:"created_at"`
	TraceID          string    `json:"trace_id,omitempty"`
	RouteAttempts    int       `json:"route_attempts"`
	CostMicros       *int64    `json:"cost_micros"` // estimated cost; nil when unknown
	Protocol         string    `json:"protocol,omitempty"`
}

// Sink persists audit events. The in-memory sink is development-only; a
// PostgreSQL `llm_requests` writer implements the same interface in production.
type Sink interface {
	Write(Event)
}

// MemorySink keeps events in memory and mirrors them to slog.
type MemorySink struct {
	mu     sync.Mutex
	events []Event
	logger *slog.Logger
}

// NewMemorySink creates a sink that also logs each event.
func NewMemorySink(logger *slog.Logger) *MemorySink {
	return &MemorySink{logger: logger}
}

// Write appends the event and logs it without any prompt content.
func (m *MemorySink) Write(e Event) {
	m.mu.Lock()
	m.events = append(m.events, e)
	m.mu.Unlock()
	if m.logger != nil {
		m.logger.Info("llm_request",
			"request_id", e.RequestID,
			"subject", e.SubjectID,
			"key_id", e.KeyID,
			"model", e.Model,
			"provider", e.Provider,
			"protocol", e.Protocol,
			"status", e.Status,
			"error_class", e.ErrorClass,
			"latency_ms", e.LatencyMillis,
			"streaming", e.Streaming,
			"prompt_tokens", e.PromptTokens,
			"completion_tokens", e.CompletionTokens,
		)
	}
}

// Snapshot returns a copy of recorded events (tests and tooling).
func (m *MemorySink) Snapshot() []Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Event, len(m.events))
	copy(out, m.events)
	return out
}

// Trace correlation fields added for v1.1; both are metadata only.
