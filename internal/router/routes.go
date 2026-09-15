package router

import (
	"sort"
	"sync"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
)

// Route is one configured provider binding for a public model.
type Route struct {
	ProviderName  string
	Provider      provider.Provider
	UpstreamModel string
	Priority      int // lower wins
	Timeout       time.Duration
	Enabled       bool
	Breaker       *Breaker
}

// Routes holds the ordered primary/backup route table per public model.
type Routes struct {
	mu      sync.RWMutex
	byModel map[string][]Route
}

// NewRoutes creates an empty route table.
func NewRoutes() *Routes {
	return &Routes{byModel: map[string][]Route{}}
}

// SetRoutes replaces the routes for a public model. Routes are stored sorted
// by priority (lower first).
func (r *Routes) SetRoutes(publicModel string, routes []Route) {
	cp := append([]Route(nil), routes...)
	sort.SliceStable(cp, func(i, j int) bool { return cp[i].Priority < cp[j].Priority })
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byModel[publicModel] = cp
}

// Available returns at most one route for a public model: the
// highest-priority enabled route whose breaker grants a permit. The caller
// must attempt the returned route and report the outcome through Record.
// Admitting one route per call keeps permits 1:1 with attempts, so extra
// half-open permits are never taken that a plan might not use: when every
// breaker is open, no route is returned until one of the breakers itself
// transitions to half-open after its cool-down and grants exactly one probe
// through Allow. Recovery stays delegated to Record.
func (r *Routes) Available(publicModel string) []Route {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, rt := range r.byModel[publicModel] {
		if rt.Enabled && rt.Breaker.Allow() {
			return []Route{rt}
		}
	}
	return nil
}

// AdmitRoute acquires the breaker permit of the named enabled route,
// immediately before the caller's attempt. The caller must attempt the route
// and report the outcome through Record, so an acquired permit is always
// accounted for and a half-open probe can never be orphaned by a plan that
// stops at an earlier candidate. It returns false when the route is unknown,
// disabled, or its breaker refused (open with the cool-down not elapsed, or
// its half-open probe already pending).
func (r *Routes) AdmitRoute(publicModel, providerName string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, rt := range r.byModel[publicModel] {
		if rt.Enabled && rt.ProviderName == providerName {
			return rt.Breaker.Allow()
		}
	}
	return false
}

// EnabledRoutes returns the enabled routes for a public model in priority
// order without acquiring breaker permits. Plan building uses it so no
// permit is held across admission-side work; actual admission happens per
// attempt through AdmitRoute.
func (r *Routes) EnabledRoutes(publicModel string) []Route {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []Route
	for _, rt := range r.byModel[publicModel] {
		if rt.Enabled {
			out = append(out, rt)
		}
	}
	return out
}

// Record reports the outcome of a route attempt for breaker bookkeeping.
func (r *Routes) Record(publicModel, providerName string, success bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, rt := range r.byModel[publicModel] {
		if rt.ProviderName == providerName {
			rt.Breaker.Record(success)
		}
	}
}
