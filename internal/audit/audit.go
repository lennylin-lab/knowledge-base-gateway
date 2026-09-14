// Package audit records request metadata. Prompt and completion content is
// never recorded, and no secret material reaches this layer.
package audit

import (
	"log/slog"
	"sync"
	"time"
)

// Event is one metadata-only audit record for a request lifecycle.
type Event struct {
	RequestID        string
	SubjectID        string
	KeyID            string
	Model            string
	Provider         string
	Status           int
	ErrorClass       string
	LatencyMillis    int64
	PromptTokens     *int
	CompletionTokens *int
	Streaming        bool
	CreatedAt        time.Time
	TraceID          string
	RouteAttempts    int
	CostMicros       *int64 // estimated cost; nil when unknown
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
