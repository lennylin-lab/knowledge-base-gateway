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

// PolicyView is one subject/model grant with its row ceilings and the
// subject's default-model slots (empty string = unset). The row ceilings are
// what the access_policies row declares; EffectiveLimits is what the gateway
// actually enforces after folding all of the subject's rows.
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
	// EffectiveLimits is the subject's folded policy (identical on every row
	// of the same subject): each ceiling is the minimum declared across the
	// subject's access_policies rows (issue #8), so a tightening row is
	// visible to operators instead of silently resizing the subject. Zero
	// means the subject has no cap for that field (unlimited). Metadata only.
	EffectiveLimits EffectiveLimits `json:"effective_limits"`
}

// EffectiveLimits is the per-subject folded ceiling block exposed on every
// PolicyView row. Build it with EffectiveLimitsFrom so the view can never
// drift from the enforcement semantics in policy.Limits.FoldPolicyRow.
type EffectiveLimits struct {
	RatePerMinute   int   `json:"rate_per_minute"`
	MaxConcurrent   int   `json:"max_concurrent"`
	DailyTokens     int64 `json:"daily_tokens"`
	MonthlyTokens   int64 `json:"monthly_tokens"`
	MaxInputTokens  int   `json:"max_input_tokens"`
	MaxOutputTokens int   `json:"max_output_tokens"`
}

// EffectiveLimitsFrom projects folded enforcement limits into the view block.
func EffectiveLimitsFrom(l policy.Limits) EffectiveLimits {
	return EffectiveLimits{
		RatePerMinute:   l.RatePerMinute,
		MaxConcurrent:   l.MaxConcurrent,
		DailyTokens:     l.DailyTokens,
		MonthlyTokens:   l.MonthlyTokens,
		MaxInputTokens:  l.MaxInputTokens,
		MaxOutputTokens: l.MaxOutputTokens,
	}
}

// AuditFilter bounds an audit or usage query; zero values are ignored.
// Tenant is the caller's tenant boundary: empty means platform-global (no
// restriction), non-empty is a mandatory query predicate that limits results
// to subjects of that tenant — never a post-query filter.
type AuditFilter struct {
	RequestID string
	Subject   string
	Model     string
	From      time.Time
	To        time.Time
	Limit     int
	Tenant    string
}

// ErrTenantBoundary is returned when a tenant-bound query is issued against a
// management service that cannot apply tenant predicates (development mode
// without tenant data). Handlers must deny the request rather than widen it.
var ErrTenantBoundary = errors.New("tenant boundary not supported by this management service")

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
	CostMicros          *int64 `json:"cost_micros"` // sum of known settled cost; null when none known

	// V1.4 ledger-derived cost fields: UnknownCostRequests counts settled
	// ledger rows in the window whose cost stayed unknown (never counted as
	// zero), and PriceVersions names the distinct price versions that
	// produced the known cost, so every reported cost is explainable.
	UnknownCostRequests int64 `json:"unknown_cost_requests"`
	PriceVersions       []int `json:"price_versions,omitempty"`
}

// AdminOp is one management-operation audit record.
type AdminOp struct {
	ID           int64           `json:"id,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
	Action       string          `json:"action"`
	Target       string          `json:"target"`
	AdminSubject string          `json:"admin_subject"`
	Detail       json.RawMessage `json:"detail,omitempty"`
	// Actor is the authenticated identity behind the mutation (V1.4): the
	// credential used, its tenant boundary, and its effective scopes. It is
	// merged into Detail under the "actor" key by NormalizeOp so every store
	// records the same shape without a schema change. Never contains secret
	// material.
	Actor AdminActor `json:"actor,omitempty"`
}

// AdminActor identifies the authenticated admin behind a management mutation.
type AdminActor struct {
	CredentialID string   `json:"credential_id,omitempty"`
	TenantID     string   `json:"tenant,omitempty"` // empty = platform-global
	Scopes       []string `json:"scopes,omitempty"`
	Bootstrap    bool     `json:"bootstrap,omitempty"`
}

// PriceView is one pricing_catalog row: micros per token per class, in one
// currency, effective from the given instant. Money is integer micros only.
type PriceView struct {
	Provider                  string    `json:"provider"`
	PublicModel               string    `json:"public_model"`
	PriceVersion              int       `json:"price_version"`
	Currency                  string    `json:"currency"`
	InputMicrosPerToken       int64     `json:"input_micros_per_token"`
	OutputMicrosPerToken      int64     `json:"output_micros_per_token"`
	ReasoningMicrosPerToken   *int64    `json:"reasoning_micros_per_token"`
	CachedInputMicrosPerToken *int64    `json:"cached_input_micros_per_token"`
	EffectiveFrom             time.Time `json:"effective_from"`
	CreatedAt                 time.Time `json:"created_at"`
}

// PriceInput upserts one price version (keyed by provider, public model,
// version).
type PriceInput struct {
	Provider                  string
	PublicModel               string
	PriceVersion              int
	Currency                  string
	InputMicrosPerToken       int64
	OutputMicrosPerToken      int64
	ReasoningMicrosPerToken   *int64
	CachedInputMicrosPerToken *int64
	EffectiveFrom             time.Time // zero means now
}

// BudgetView is one budget_policies row: a monetary cap per scope/period in
// one currency.
type BudgetView struct {
	ID           int64     `json:"id"`
	Scope        string    `json:"scope"`
	SubjectID    string    `json:"subject_id,omitempty"`
	TenantID     string    `json:"tenant_id"`
	Period       string    `json:"period"`
	Currency     string    `json:"currency"`
	AmountMicros int64     `json:"amount_micros"`
	Enabled      bool      `json:"enabled"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// BudgetInput upserts one budget (keyed by scope, target, period, currency).
// Enabled nil defaults to true.
type BudgetInput struct {
	Scope        string
	SubjectID    string
	TenantID     string
	Period       string
	Currency     string
	AmountMicros int64
	Enabled      *bool
}

// BudgetUsageView is one budget's current-period utilization, computed from
// settled ledger rows: the sum of known cost and the number of settlements
// whose cost stayed unknown. Unknown-cost settlements are surfaced
// separately — they are never counted as zero spend.
type BudgetUsageView struct {
	Scope                  string `json:"scope"`
	SubjectID              string `json:"subject_id,omitempty"`
	TenantID               string `json:"tenant_id"`
	Period                 string `json:"period"`
	Currency               string `json:"currency"`
	LimitMicros            int64  `json:"limit_micros"`
	UsedMicros             int64  `json:"used_micros"`
	UnknownCostSettlements int64  `json:"unknown_cost_settlements"`
}

// NormalizeOp fills management-audit defaults so every store implementation
// records the same shape. The actor block (subject via AdminSubject, plus
// credential, tenant boundary, and effective scopes) is merged into Detail
// under the "actor" key: actor ID, tenant, and scopes travel with every audit
// summary without weakening the redaction contract — Detail is metadata only
// and never carries secrets or content.
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
	op.Detail = mergeActor(op.Detail, op.AdminSubject, op.Actor)
	return op
}

// mergeActor embeds the actor block into an audit detail object. The detail
// must be a JSON object (handlers marshal maps); anything that does not
// unmarshal into an object is passed through untouched so the store boundary
// — not the merge — decides: malformed JSON then fails the audit insert and
// rolls the whole mutation back (the atomic mutation/audit rule), instead of
// being silently rewritten, which would hide handler bugs and weaken the
// evidence chain.
func mergeActor(detail json.RawMessage, subject string, actor AdminActor) json.RawMessage {
	fields := map[string]any{}
	if len(detail) > 0 {
		if err := json.Unmarshal(detail, &fields); err != nil {
			return detail
		}
	}
	if fields == nil {
		fields = map[string]any{}
	}
	bootstrap := false
	scopes := []string{}
	if actor.Bootstrap {
		bootstrap = true
	}
	if len(actor.Scopes) > 0 {
		scopes = actor.Scopes
	}
	fields["actor"] = map[string]any{
		"subject":       subject,
		"credential_id": actor.CredentialID,
		"tenant":        actor.TenantID,
		"scopes":        scopes,
		"bootstrap":     bootstrap,
	}
	out, err := json.Marshal(fields)
	if err != nil {
		return json.RawMessage(`{"actor":{"subject":"unmarshalable"}}`)
	}
	return out
}

// MergeDetail enriches a mutation's audit detail with redacted before/after
// summaries (the store implementations read the previous state inside their
// transaction). Values must be metadata only — never secrets, key material,
// prompts, or completions. The base detail stays intact; new keys win on
// collision. A base that does not unmarshal into an object is passed through
// untouched: the malformed JSON then fails the audit insert and rolls the
// mutation back rather than being silently rewritten.
func MergeDetail(base json.RawMessage, summary map[string]any) json.RawMessage {
	fields := map[string]any{}
	if len(base) > 0 {
		if err := json.Unmarshal(base, &fields); err != nil {
			return base
		}
	}
	if fields == nil {
		fields = map[string]any{}
	}
	for k, v := range summary {
		fields[k] = v
	}
	out, err := json.Marshal(fields)
	if err != nil {
		return base
	}
	return out
}

// Service is the management query surface consumed by the admin API. The
// tenant parameter on Policies / SetDefaultModelWithAudit (and AuditFilter.
// Tenant) is the caller's tenant boundary: empty for platform-global
// principals, otherwise a mandatory predicate or ownership check enforced
// inside the implementation.
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
	// Unknown subjects or models — including subjects outside the caller's
	// tenant boundary — return ErrNotFound.
	SetDefaultModelWithAudit(ctx context.Context, subject, model, kind string, op AdminOp, tenant string) error
	Providers(ctx context.Context) ([]ProviderView, error)
	Policies(ctx context.Context, subject, tenant string) ([]PolicyView, error)
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
// Tenant boundaries are only enforceable when TenantOfSubject is wired;
// tenant-bound queries without the hook fail closed with ErrTenantBoundary.
type MemoryService struct {
	Catalog      *policy.Catalog
	Policy       *policy.Policy
	Routes       *router.Routes
	Audit        *audit.MemorySink
	ProviderList []ProviderView
	// TenantOfSubject reports a subject's authoritative tenant. Nil means
	// development mode has no tenant data and tenant-bound queries are
	// denied (fail closed), matching the mandatory-predicate rule.
	TenantOfSubject func(subject string) (string, bool)

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
// and appends the management-operation record under one lock, so the dev-mode
// mutation is atomic and immediately live for admission. Unknown models
// return ErrNotFound; unknown subjects (no grants, no wildcard, no recorded
// limits) — and subjects outside the caller's tenant boundary — return
// ErrNotFound as their row-based store counterpart does.
func (m *MemoryService) SetDefaultModelWithAudit(_ context.Context, subject, model, kind string, op AdminOp, tenant string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if tenant != "" && !m.subjectInTenant(subject, tenant) {
		return ErrNotFound
	}
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

// subjectInTenant checks the tenant boundary via the hook; without the hook
// no subject can be proven inside any tenant, so the answer is false (fail
// closed) for a bounded caller. Callers holding the lock invoke this.
func (m *MemoryService) subjectInTenant(subject, tenant string) bool {
	if m.TenantOfSubject == nil {
		return false
	}
	t, ok := m.TenantOfSubject(subject)
	return ok && t == tenant
}

// tenantBound reports ErrTenantBoundary when a bounded caller queries a
// service that has no tenant data (cannot predicate → must deny).
func (m *MemoryService) tenantBound(tenant string) error {
	if tenant != "" && m.TenantOfSubject == nil {
		return ErrTenantBoundary
	}
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
// (matching the row-based store's view shape), and the effective-limits
// block carries the subject's folded ceilings — in this mode the policy
// service already holds the folded Limits. A non-empty tenant is a mandatory
// predicate: only subjects bound to that tenant are included.
func (m *MemoryService) Policies(_ context.Context, subject, tenant string) ([]PolicyView, error) {
	if err := m.tenantBound(tenant); err != nil {
		return nil, err
	}
	if m.Policy == nil {
		return []PolicyView{}, nil
	}
	inTenant := func(s string) bool {
		if tenant == "" {
			return true
		}
		return m.subjectInTenant(s, tenant)
	}
	out := []PolicyView{}
	for _, s := range m.Policy.Subjects() {
		if subject != "" && s != subject {
			continue
		}
		if !inTenant(s) {
			continue
		}
		defaults, _ := m.Policy.LimitsFor(s)
		effective := EffectiveLimitsFrom(defaults)
		for _, info := range m.Catalog.All() {
			if m.Policy.Permitted(s, info.PublicName) {
				out = append(out, PolicyView{
					Subject: s, PublicModel: info.PublicName,
					DefaultModel: defaults.DefaultModel, DefaultEmbeddingModel: defaults.DefaultEmbeddingModel,
					EffectiveLimits: effective,
				})
			}
		}
	}
	return out, nil
}

// QueryAudit filters the in-memory sink snapshot, newest first. A non-empty
// filter tenant is a mandatory predicate over the subject dimension.
func (m *MemoryService) QueryAudit(_ context.Context, f AuditFilter) ([]audit.Event, error) {
	if err := m.tenantBound(f.Tenant); err != nil {
		return nil, err
	}
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
		if f.Tenant != "" && !m.subjectInTenant(e.SubjectID, f.Tenant) {
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
	if err := m.tenantBound(f.Tenant); err != nil {
		return nil, err
	}
	snap := m.Audit.Snapshot()
	type key struct{ model, protocol string }
	agg := map[key]*UsageRow{}
	for _, e := range snap {
		if f.Subject != "" && e.SubjectID != f.Subject {
			continue
		}
		if f.Tenant != "" && !m.subjectInTenant(e.SubjectID, f.Tenant) {
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
