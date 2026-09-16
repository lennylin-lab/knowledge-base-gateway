// Package policy enforces model whitelist and per-subject permissions.
// Denials must not leak which models other subjects may access.
package policy

import (
	"encoding/json"
	"maps"
	"slices"
	"sync"

	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
)

// Policy is the checked access decision source. It reports whether a subject
// may call a model that exists in the catalog.
type Policy struct {
	mu        sync.RWMutex
	allowed   map[string]map[string]struct{} // subject -> set of public model names
	wildcards map[string]struct{}            // subjects allowed any catalog model
	limits    map[string]Limits              // subject -> persisted ceilings
}

// New creates an empty policy.
func New() *Policy {
	return &Policy{allowed: map[string]map[string]struct{}{}, wildcards: map[string]struct{}{}, limits: map[string]Limits{}}
}

// Allow grants a subject access to one model.
func (p *Policy) Allow(subject, model string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.allowed[subject] == nil {
		p.allowed[subject] = map[string]struct{}{}
	}
	p.allowed[subject][model] = struct{}{}
}

// AllowAll grants a subject access to every catalog model.
func (p *Policy) AllowAll(subject string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.wildcards[subject] = struct{}{}
}

// Subjects returns all configured subjects (for readiness reporting).
func (p *Policy) Subjects() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := slices.Collect(maps.Keys(p.allowed))
	for s := range p.wildcards {
		out = append(out, s)
	}
	return out
}

// Catalog answers model-existence questions.
type Catalog struct {
	mu     sync.RWMutex
	models map[string]ModelInfo
}

// ModelInfo describes one catalog entry. Capabilities is the public model
// capability matrix; when Declared is false the gateway derives the matrix
// from the provider adapter. ConfigVersion versions the capability/route
// configuration for audit correlation. RetrievalProfile is opaque catalog
// JSON (retrieval thresholds consumed by clients); nil when the row declares
// none.
type ModelInfo struct {
	PublicName       string
	Provider         string
	UpstreamModel    string
	Enabled          bool
	Capabilities     model.Capabilities
	ConfigVersion    int
	RetrievalProfile json.RawMessage
}

// NewCatalog builds a catalog from entries.
func NewCatalog(entries []ModelInfo) *Catalog {
	c := &Catalog{models: map[string]ModelInfo{}}
	for _, e := range entries {
		c.models[e.PublicName] = e
	}
	return c
}

// Lookup returns the entry and whether the public model exists and is enabled.
func (c *Catalog) Lookup(publicModel string) (ModelInfo, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	m, ok := c.models[publicModel]
	return m, ok && m.Enabled
}

// SetEnabled flips a catalog entry's enabled flag (management operation).
// It reports whether the model exists.
func (c *Catalog) SetEnabled(publicModel string, enabled bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	m, ok := c.models[publicModel]
	if !ok {
		return false
	}
	m.Enabled = enabled
	c.models[publicModel] = m
	return true
}

// SetEntry inserts or replaces one catalog entry. The management runtime
// refresh uses it so a persisted enable/disable (with fresh capabilities and
// configuration version) becomes visible to the running process without a
// restart. Lookup treats an entry with Enabled=false as absent, so replacing
// an entry with a disabled row hides the model from resolution.
func (c *Catalog) SetEntry(info ModelInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.models[info.PublicName] = info
}

// Remove drops a catalog entry and reports whether it existed. The runtime
// refresh uses it when the persisted row disappeared underneath the process.
func (c *Catalog) Remove(publicModel string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.models[publicModel]
	delete(c.models, publicModel)
	return ok
}

// Permitted reports whether a subject may use a model that has already been
// confirmed to exist. The result is identical for "model missing" and
// "model forbidden" from the caller's perspective (non-leaky).
func (p *Policy) Permitted(subject, publicModel string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if _, ok := p.wildcards[subject]; ok {
		return true
	}
	_, ok := p.allowed[subject][publicModel]
	return ok
}

// Limits are per-subject rate, concurrency, and token ceilings, plus the
// subject's default-model slots. Zero-value ceilings mean unset.
type Limits struct {
	RatePerMinute   int
	MaxConcurrent   int
	DailyTokens     int64 // zero means unset/unknown
	MonthlyTokens   int64
	MaxInputTokens  int // zero means unset; rejects oversized inputs before any provider work
	MaxOutputTokens int // zero means unset; caps request max_tokens

	// DefaultModel / DefaultEmbeddingModel are the subject's default-model
	// slots ("" = unset). Requests omitting `model` backfill the matching
	// slot before model resolution: chat/responses use DefaultModel,
	// embeddings uses DefaultEmbeddingModel. An explicit model always wins.
	DefaultModel          string
	DefaultEmbeddingModel string
}

// FoldPolicyRow folds one access_policies row into the subject's collapsed
// limits (issue #8 decision). Rows must arrive in ascending id order (the
// loader's ORDER BY) for the default slots; the ceiling fold itself is
// order-independent. Ceiling fields take the **minimum declared value**
// across the subject's rows — intersection semantics, so adding a row can
// never raise a quota. Per the Limits zero-means-unset convention, a ceiling
// constrains only when the row declares it (> 0): the NOT NULL columns
// (rate_per_minute, max_concurrent) declare a value on every row, while the
// nullable columns (daily/monthly tokens, max_input/output tokens) declare
// nothing after their NULL→0 COALESCE — a row without a cap does not cap the
// subject, and if no row declares a cap the field stays zero (uncapped).
// The default-model slots take the first non-empty value in id order across
// the subject's rows — a row with an empty slot never erases a default
// declared on an earlier row, and the chat and embedding slots are judged
// independently. The PostgreSQL loader and any in-memory row source share
// this helper so the collapse semantics cannot drift between modes.
func (l *Limits) FoldPolicyRow(row Limits) {
	if row.RatePerMinute > 0 && (l.RatePerMinute == 0 || row.RatePerMinute < l.RatePerMinute) {
		l.RatePerMinute = row.RatePerMinute
	}
	if row.MaxConcurrent > 0 && (l.MaxConcurrent == 0 || row.MaxConcurrent < l.MaxConcurrent) {
		l.MaxConcurrent = row.MaxConcurrent
	}
	if row.DailyTokens > 0 && (l.DailyTokens == 0 || row.DailyTokens < l.DailyTokens) {
		l.DailyTokens = row.DailyTokens
	}
	if row.MonthlyTokens > 0 && (l.MonthlyTokens == 0 || row.MonthlyTokens < l.MonthlyTokens) {
		l.MonthlyTokens = row.MonthlyTokens
	}
	if row.MaxInputTokens > 0 && (l.MaxInputTokens == 0 || row.MaxInputTokens < l.MaxInputTokens) {
		l.MaxInputTokens = row.MaxInputTokens
	}
	if row.MaxOutputTokens > 0 && (l.MaxOutputTokens == 0 || row.MaxOutputTokens < l.MaxOutputTokens) {
		l.MaxOutputTokens = row.MaxOutputTokens
	}
	if l.DefaultModel == "" {
		l.DefaultModel = row.DefaultModel
	}
	if l.DefaultEmbeddingModel == "" {
		l.DefaultEmbeddingModel = row.DefaultEmbeddingModel
	}
}

// All returns every catalog entry (for router and readiness wiring).
func (c *Catalog) All() []ModelInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]ModelInfo, 0, len(c.models))
	for _, m := range c.models {
		out = append(out, m)
	}
	return out
}

// LimitsFor returns configured limits for a subject; ok is false when no
// explicit policy exists and defaults apply.
func (p *Policy) LimitsFor(subject string) (Limits, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	l, ok := p.limits[subject]
	return l, ok
}

// DefaultModelFor returns the subject's configured default model for the
// protocol: chat/responses resolve default_model, embeddings resolves
// default_embedding_model. ok is false when the slot is unset.
func (p *Policy) DefaultModelFor(subject, protocol string) (string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	l, ok := p.limits[subject]
	if !ok {
		return "", false
	}
	switch protocol {
	case "embeddings":
		if l.DefaultEmbeddingModel == "" {
			return "", false
		}
		return l.DefaultEmbeddingModel, true
	default:
		if l.DefaultModel == "" {
			return "", false
		}
		return l.DefaultModel, true
	}
}

// SetDefault records the subject's default-model slots without touching its
// ceilings (used by the local development wiring; the PostgreSQL loader
// carries both slots on Limits directly).
func (p *Policy) SetDefault(subject, chatModel, embeddingModel string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.limits == nil {
		p.limits = map[string]Limits{}
	}
	l := p.limits[subject]
	if chatModel != "" {
		l.DefaultModel = chatModel
	}
	if embeddingModel != "" {
		l.DefaultEmbeddingModel = embeddingModel
	}
	p.limits[subject] = l
}

// SetLimits records per-subject limits loaded from persisted policy.
func (p *Policy) SetLimits(subject string, l Limits) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.limits == nil {
		p.limits = map[string]Limits{}
	}
	p.limits[subject] = l
}
