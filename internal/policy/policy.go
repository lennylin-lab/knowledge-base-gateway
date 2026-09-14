// Package policy enforces model whitelist and per-subject permissions.
// Denials must not leak which models other subjects may access.
package policy

import (
	"maps"
	"slices"
	"sync"
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

// ModelInfo describes one catalog entry.
type ModelInfo struct {
	PublicName    string
	Provider      string
	UpstreamModel string
	Enabled       bool
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

// Limits are per-subject rate, concurrency, and token ceilings.
type Limits struct {
	RatePerMinute   int
	MaxConcurrent   int
	DailyTokens     int64 // zero means unset/unknown
	MonthlyTokens   int64
	MaxOutputTokens int // zero means unset; caps request max_tokens
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

// SetLimits records per-subject limits loaded from persisted policy.
func (p *Policy) SetLimits(subject string, l Limits) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.limits == nil {
		p.limits = map[string]Limits{}
	}
	p.limits[subject] = l
}
