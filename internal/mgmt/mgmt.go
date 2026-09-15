// Package mgmt defines the management query service behind the admin API:
// model catalog and provider status views, policy metadata, audit and usage
// queries, and the management-operation audit trail. Every view is metadata
// only: no key hashes, provider secrets, base URLs, or prompt/completion
// content. Implementations: PostgreSQL (production) and an in-memory service
// for development mode.
package mgmt

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/router"
)

// ErrNotFound is returned when a management target does not exist.
var ErrNotFound = errors.New("management target not found")

// ModelView is one catalog row as administrators see it: provider bindings
// and enablement included, endpoints and credentials excluded.
type ModelView struct {
	PublicName    string             `json:"public_name"`
	Provider      string             `json:"provider"`
	UpstreamModel string             `json:"upstream_model"`
	Enabled       bool               `json:"enabled"`
	ConfigVersion int                `json:"config_version"`
	Capabilities  model.Capabilities `json:"capabilities"`
}

// ProviderView is one provider registry row's operational status.
type ProviderView struct {
	Name    string `json:"name"`
	Kind    string `json:"kind"`
	Enabled bool   `json:"enabled"`
}

// PolicyView is one subject/model grant with its ceilings.
type PolicyView struct {
	Subject       string `json:"subject_id"`
	PublicModel   string `json:"public_model"`
	RatePerMinute int    `json:"rate_per_minute"`
	MaxConcurrent int    `json:"max_concurrent"`
	DailyTokens   int64  `json:"daily_tokens"`
	MonthlyTokens int64  `json:"monthly_tokens"`
}

// AuditFilter bounds an audit or usage query; zero values are ignored.
type AuditFilter struct {
	RequestID string
	Subject   string
	Model     string
	From      time.Time
	To        time.Time
	Limit     int
}

// UsageRow is one aggregated usage line for dashboards.
type UsageRow struct {
	Model        string `json:"model"`
	Protocol     string `json:"protocol"`
	Requests     int64  `json:"requests"`
	Errors       int64  `json:"errors"`
	PromptTokens int64  `json:"prompt_tokens"`
	OutputTokens int64  `json:"output_tokens"`
	P50Millis    int64  `json:"p50_latency_ms"`
	P95Millis    int64  `json:"p95_latency_ms"`
}

// AdminOp is one management-operation audit record.
type AdminOp struct {
	ID           int64           `json:"id,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
	Action       string          `json:"action"`
	Target       string          `json:"target"`
	AdminSubject string          `json:"admin_subject"`
	Detail       json.RawMessage `json:"detail,omitempty"`
}

// Service is the management query surface consumed by the admin API.
type Service interface {
	Models(ctx context.Context) ([]ModelView, error)
	SetModelEnabled(ctx context.Context, publicModel string, enabled bool) error
	Providers(ctx context.Context) ([]ProviderView, error)
	Policies(ctx context.Context, subject string) ([]PolicyView, error)
	QueryAudit(ctx context.Context, f AuditFilter) ([]audit.Event, error)
	Usage(ctx context.Context, f AuditFilter) ([]UsageRow, error)
	WriteOp(ctx context.Context, op AdminOp) error
	Ops(ctx context.Context, limit int) ([]AdminOp, error)
}

// MemoryService is the development-mode management service: it reads the
// in-memory catalog, route table, and audit sink, and keeps management ops
// in process. Enable/disable decisions are not persisted (development only).
type MemoryService struct {
	Catalog      *policy.Catalog
	Policy       *policy.Policy
	Routes       *router.Routes
	Audit        *audit.MemorySink
	ProviderList []ProviderView

	mu  sync.Mutex
	ops []AdminOp
}

// NewMemoryService builds the dev-mode service.
func NewMemoryService(catalog *policy.Catalog, pol *policy.Policy, routes *router.Routes, sink *audit.MemorySink, providers []ProviderView) *MemoryService {
	return &MemoryService{Catalog: catalog, Policy: pol, Routes: routes, Audit: sink, ProviderList: providers}
}

// Models lists the in-memory catalog (including disabled rows).
func (m *MemoryService) Models(_ context.Context) ([]ModelView, error) {
	out := []ModelView{}
	for _, info := range m.Catalog.All() {
		out = append(out, ModelView{
			PublicName: info.PublicName, Provider: info.Provider,
			UpstreamModel: info.UpstreamModel, Enabled: info.Enabled,
			ConfigVersion: info.ConfigVersion, Capabilities: info.Capabilities,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PublicName < out[j].PublicName })
	return out, nil
}

// SetModelEnabled flips the in-memory catalog flag only.
func (m *MemoryService) SetModelEnabled(_ context.Context, publicModel string, enabled bool) error {
	if !m.Catalog.SetEnabled(publicModel, enabled) {
		return ErrNotFound
	}
	return nil
}

// Providers returns the static startup snapshot.
func (m *MemoryService) Providers(_ context.Context) ([]ProviderView, error) {
	return m.ProviderList, nil
}

// Policies summarizes the in-memory grant set; explicit policy rows are a
// database-mode concept.
func (m *MemoryService) Policies(_ context.Context, subject string) ([]PolicyView, error) {
	if m.Policy == nil {
		return []PolicyView{}, nil
	}
	out := []PolicyView{}
	for _, s := range m.Policy.Subjects() {
		if subject != "" && s != subject {
			continue
		}
		for _, info := range m.Catalog.All() {
			if m.Policy.Permitted(s, info.PublicName) {
				out = append(out, PolicyView{Subject: s, PublicModel: info.PublicName})
			}
		}
	}
	return out, nil
}

// QueryAudit filters the in-memory sink snapshot, newest first.
func (m *MemoryService) QueryAudit(_ context.Context, f AuditFilter) ([]audit.Event, error) {
	limit := f.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var out []audit.Event
	snap := m.Audit.Snapshot()
	for i := len(snap) - 1; i >= 0 && len(out) < limit; i-- {
		e := snap[i]
		if f.RequestID != "" && e.RequestID != f.RequestID {
			continue
		}
		if f.Subject != "" && e.SubjectID != f.Subject {
			continue
		}
		if f.Model != "" && e.Model != f.Model {
			continue
		}
		if !f.From.IsZero() && e.CreatedAt.Before(f.From) {
			continue
		}
		if !f.To.IsZero() && e.CreatedAt.After(f.To) {
			continue
		}
		out = append(out, e)
	}
	return out, nil
}

// Usage aggregates the in-memory snapshot. The latency field is a mean in
// dev mode (documented); the PostgreSQL implementation reports true P50/P95.
func (m *MemoryService) Usage(_ context.Context, f AuditFilter) ([]UsageRow, error) {
	snap := m.Audit.Snapshot()
	type key struct{ model, protocol string }
	agg := map[key]*UsageRow{}
	for _, e := range snap {
		if f.Subject != "" && e.SubjectID != f.Subject {
			continue
		}
		if f.Model != "" && e.Model != f.Model {
			continue
		}
		if !f.From.IsZero() && e.CreatedAt.Before(f.From) {
			continue
		}
		if !f.To.IsZero() && e.CreatedAt.After(f.To) {
			continue
		}
		protocol := e.Protocol
		if strings.TrimSpace(protocol) == "" {
			protocol = "chat"
		}
		k := key{e.Model, protocol}
		row, ok := agg[k]
		if !ok {
			row = &UsageRow{Model: e.Model, Protocol: protocol}
			agg[k] = row
		}
		row.Requests++
		if e.Status >= 400 || e.ErrorClass != "" {
			row.Errors++
		}
		if e.PromptTokens != nil {
			row.PromptTokens += int64(*e.PromptTokens)
		}
		if e.CompletionTokens != nil {
			row.OutputTokens += int64(*e.CompletionTokens)
		}
		row.P50Millis += e.LatencyMillis
	}
	out := []UsageRow{}
	for _, row := range agg {
		if row.Requests > 0 {
			row.P50Millis /= row.Requests
		}
		out = append(out, *row)
	}
	return out, nil
}

// WriteOp appends a management-operation record.
func (m *MemoryService) WriteOp(_ context.Context, op AdminOp) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ops = append(m.ops, op)
	return nil
}

// Ops returns recent management operations, newest first.
func (m *MemoryService) Ops(_ context.Context, limit int) ([]AdminOp, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []AdminOp{}
	for i := len(m.ops) - 1; i >= 0 && len(out) < limit; i-- {
		out = append(out, m.ops[i])
	}
	return out, nil
}
