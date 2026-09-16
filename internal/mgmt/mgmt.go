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
	"fmt"
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

// Provider health values reported on ProviderView.Health. They describe the
// runtime route-breaker state, not an active probe: "serving" (enabled, no
// open breaker), "degraded" (enabled with at least one open breaker),
// "disabled" (registry row disabled), and "unknown" (runtime state not
// wired).
const (
	HealthServing  = "serving"
	HealthDegraded = "degraded"
	HealthDisabled = "disabled"
	HealthUnknown  = "unknown"
)

// Breaker state values reported on ProviderView.BreakerState. They match the
// router breaker vocabulary plus "none" (no live routes in this process) and
// "unknown" (runtime state not wired).
const (
	BreakerClosed   = "closed"
	BreakerHalfOpen = "half-open"
	BreakerOpen     = "open"
	BreakerNone     = "none"
	BreakerUnknown  = "unknown"
)

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

// ProviderView is one provider registry row's operational status. Registry
// enablement and the recent error summary come from the store; live breaker
// state and derived health are overlaid by the running process (see
// ProviderRuntime and ApplyRuntime) because stores cannot see route state.
// No endpoints, credentials, or error message bodies are included.
type ProviderView struct {
	Name           string `json:"name"`
	Kind           string `json:"kind"`
	Enabled        bool   `json:"enabled"`
	Health         string `json:"health"`                     // serving | degraded | disabled | unknown
	BreakerState   string `json:"breaker_state"`              // closed | half-open | open | none | unknown
	RecentErrors   int64  `json:"recent_errors"`              // errored requests in the recent window
	LastErrorClass string `json:"last_error_class,omitempty"` // class of the most recent error
}

// ProviderRuntime is the live per-provider breaker summary injected by the
// serving process wiring; store implementations cannot observe route state.
type ProviderRuntime struct {
	BreakerState string // closed | half-open | open (worst across live routes)
	TotalRoutes  int    // live routes bound to the provider in this process
	OpenRoutes   int    // live routes whose breaker is currently open
}

// ApplyRuntime overlays live breaker state onto a registry view and derives
// health. Disabled stays disabled; any open breaker degrades; other enabled
// providers serve. Providers without live routes report breaker "none" and
// stay "serving" — nothing is failing, there is simply no route bound here.
func (v *ProviderView) ApplyRuntime(rt ProviderRuntime) {
	if !v.Enabled {
		v.Health = HealthDisabled
		v.BreakerState = BreakerNone
		return
	}
	if rt.TotalRoutes == 0 {
		v.BreakerState = BreakerNone
		v.Health = HealthServing
		return
	}
	v.BreakerState = rt.BreakerState
	if rt.OpenRoutes > 0 {
		v.Health = HealthDegraded
		return
	}
	v.Health = HealthServing
}

// PolicyView is one subject/model grant with its ceilings and the subject's
// default-model slots (empty string = unset).
type PolicyView struct {
	Subject       string `json:"subject_id"`
	PublicModel   string `json:"public_model"`
	RatePerMinute int    `json:"rate_per_minute"`
	MaxConcurrent int    `json:"max_concurrent"`
	DailyTokens   int64  `json:"daily_tokens"`
	MonthlyTokens int64  `json:"monthly_tokens"`
	DefaultModel  string `json:"default_model,omitempty"`
	// DefaultEmbeddingModel is the slot backfilled into embeddings requests
	// that omit `model`.
	DefaultEmbeddingModel string `json:"default_embedding_model,omitempty"`
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

// UsageRow is one aggregated usage line for dashboards. P50/P95 latency is a
// true percentile in the PostgreSQL implementation; the development-mode
// service reports a mean and never claims otherwise. First-token percentiles
// cover the rows that recorded one (streams); PostgreSQL reports true
// percentiles and development mode omits the fields (null) rather than
// approximating. Cost remains the staged always-null contract field: pricing
// configuration does not exist yet, so it is never fabricated.
type UsageRow struct {
	Model        string  `json:"model"`
	Protocol     string  `json:"protocol"`
	Requests     int64   `json:"requests"`
	Errors       int64   `json:"errors"`
	ErrorRate    float64 `json:"error_rate"`
	PromptTokens int64   `json:"prompt_tokens"`
	OutputTokens int64   `json:"output_tokens"`
	P50Millis    int64   `json:"p50_latency_ms"`
	P95Millis    int64   `json:"p95_latency_ms"`

	// Contract metric fields. First-token percentiles are null until streams
	// recorded a measurement in the window (dev mode omits them); cost stays
	// null until a pricing decision lands. The field names are contractual so
	// dashboards can detect availability explicitly; null means "not
	// measured", never fabricated zero.
	FirstTokenP50Millis *int64 `json:"first_token_p50_ms"`
	FirstTokenP95Millis *int64 `json:"first_token_p95_ms"`
	CostMicros          *int64 `json:"cost_micros"` // sum of known estimated cost; null when none known
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

// NormalizeOp fills management-audit defaults so every store implementation
// records the same shape.
func NormalizeOp(op AdminOp) AdminOp {
	if op.AdminSubject == "" {
		op.AdminSubject = "admin-token"
	}
	if len(op.Detail) == 0 {
		op.Detail = json.RawMessage(`{}`)
	}
	if op.CreatedAt.IsZero() {
		op.CreatedAt = time.Now()
	}
	return op
}

// Service is the management query surface consumed by the admin API.
type Service interface {
	Models(ctx context.Context) ([]ModelView, error)
	// SetModelEnabledWithAudit persists an enable/disable decision and its
	// management-operation audit record as one atomic operation: either both
	// land or neither does. A returned error means the mutation did not
	// happen; the admin handler treats it as a failed operation.
	SetModelEnabledWithAudit(ctx context.Context, publicModel string, enabled bool, op AdminOp) error
	// SetDefaultModelWithAudit persists the subject's default-model slot
	// (kind "chat" or "embedding") and its management-operation audit record
	// as one atomic operation, with the same all-or-nothing semantics.
	// Unknown subjects or models return ErrNotFound.
	SetDefaultModelWithAudit(ctx context.Context, subject, model, kind string, op AdminOp) error
	Providers(ctx context.Context) ([]ProviderView, error)
	Policies(ctx context.Context, subject string) ([]PolicyView, error)
	QueryAudit(ctx context.Context, f AuditFilter) ([]audit.Event, error)
	Usage(ctx context.Context, f AuditFilter) ([]UsageRow, error)
	// WriteOp appends a management-operation record for mutations that carry
	// their own atomicity. The model enable/disable switch must use
	// SetModelEnabledWithAudit instead.
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

// SetModelEnabledWithAudit flips the in-memory catalog flag and appends the
// management-operation record under one lock, so the dev-mode mutation is
// atomic and immediately live for model resolution.
func (m *MemoryService) SetModelEnabledWithAudit(_ context.Context, publicModel string, enabled bool, op AdminOp) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.Catalog.SetEnabled(publicModel, enabled) {
		return ErrNotFound
	}
	m.ops = append(m.ops, NormalizeOp(op))
	return nil
}

// SetDefaultModelWithAudit flips the in-memory policy's default-model slot
// and appends the management-operation record under one lock, so the
// dev-mode mutation is atomic and immediately live for admission. Unknown
// models return ErrNotFound; unknown subjects (no grants, no wildcard, no
// recorded limits) return ErrNotFound as their row-based store counterpart
// does.
func (m *MemoryService) SetDefaultModelWithAudit(_ context.Context, subject, model, kind string, op AdminOp) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch kind {
	case "chat", "embedding":
	default:
		return fmt.Errorf("unknown default-model kind %q", kind)
	}
	exists := false
	for _, info := range m.Catalog.All() {
		if info.PublicName == model {
			exists = true
			break
		}
	}
	if !exists {
		return ErrNotFound
	}
	known := false
	for _, s := range m.Policy.Subjects() {
		if s == subject {
			known = true
			break
		}
	}
	if !known {
		return ErrNotFound
	}
	switch kind {
	case "chat":
		m.Policy.SetDefault(subject, model, "")
	case "embedding":
		m.Policy.SetDefault(subject, "", model)
	}
	m.ops = append(m.ops, NormalizeOp(op))
	return nil
}

// Providers returns the startup registry snapshot enriched with a recent
// error summary from the in-memory audit sink. Health and breaker state are
// runtime overlays (see AdminDeps.ProviderRuntime wiring), not store data.
func (m *MemoryService) Providers(_ context.Context) ([]ProviderView, error) {
	out := make([]ProviderView, len(m.ProviderList))
	copy(out, m.ProviderList)
	since := time.Now().Add(-24 * time.Hour)
	for i := range out {
		out[i].Health = HealthUnknown
		out[i].BreakerState = BreakerUnknown
		if m.Audit == nil {
			continue
		}
		var last *audit.Event
		for _, e := range m.Audit.Snapshot() {
			if e.Provider != out[i].Name {
				continue
			}
			if e.Status < 400 && e.ErrorClass == "" {
				continue
			}
			if e.CreatedAt.Before(since) {
				continue
			}
			out[i].RecentErrors++
			if last == nil || e.CreatedAt.After(last.CreatedAt) {
				ec := e
				last = &ec
			}
		}
		if last != nil {
			out[i].LastErrorClass = last.ErrorClass
		}
	}
	return out, nil
}

// Policies summarizes the in-memory grant set; explicit policy rows are a
// database-mode concept. The subject's default-model slots ride every row
// (matching the row-based store's view shape).
func (m *MemoryService) Policies(_ context.Context, subject string) ([]PolicyView, error) {
	if m.Policy == nil {
		return []PolicyView{}, nil
	}
	out := []PolicyView{}
	for _, s := range m.Policy.Subjects() {
		if subject != "" && s != subject {
			continue
		}
		defaults, _ := m.Policy.LimitsFor(s)
		for _, info := range m.Catalog.All() {
			if m.Policy.Permitted(s, info.PublicName) {
				out = append(out, PolicyView{
					Subject: s, PublicModel: info.PublicName,
					DefaultModel: defaults.DefaultModel, DefaultEmbeddingModel: defaults.DefaultEmbeddingModel,
				})
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
// Dev mode omits first-token percentiles (they stay null) rather than
// approximating them; PostgreSQL reports true percentiles over measured rows.
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
		if e.CostMicros != nil {
			if row.CostMicros == nil {
				v := *e.CostMicros
				row.CostMicros = &v
			} else {
				*row.CostMicros += *e.CostMicros
			}
		}
		row.P50Millis += e.LatencyMillis
	}
	out := []UsageRow{}
	for _, row := range agg {
		if row.Requests > 0 {
			row.P50Millis /= row.Requests
			row.ErrorRate = float64(row.Errors) / float64(row.Requests)
		}
		out = append(out, *row)
	}
	return out, nil
}

// WriteOp appends a management-operation record.
func (m *MemoryService) WriteOp(_ context.Context, op AdminOp) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ops = append(m.ops, NormalizeOp(op))
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
