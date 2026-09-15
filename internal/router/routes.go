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

// BreakerSummary aggregates the breaker state of every live route bound to
// one provider, for management health reporting. State is the worst state
// across those routes ("open" > "half-open" > "closed").
type BreakerSummary struct {
	TotalRoutes int    // routes bound to the provider across all models
	OpenRoutes  int    // routes whose breaker is currently open
	State       string // worst state: "closed" | "half-open" | "open"
}

// breakerRank orders breaker severity so the worst state wins the summary:
// closed < half-open < open.
var breakerRank = map[string]int{"closed": 0, "half-open": 1, "open": 2}

// ProviderBreakers summarizes breaker state per provider name across every
// model's route table in one consistent snapshot. Providers without live
// routes are absent from the result.
func (r *Routes) ProviderBreakers() map[string]BreakerSummary {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := map[string]BreakerSummary{}
	for _, routes := range r.byModel {
		for _, rt := range routes {
			s := out[rt.ProviderName]
			state := rt.Breaker.State()
			s.TotalRoutes++
			if state == "open" {
				s.OpenRoutes++
			}
			// >= also seeds the zero-value empty state with the first route's
			// state ("closed" ranks equal to "").
			if breakerRank[state] >= breakerRank[s.State] {
				s.State = state
			}
			out[rt.ProviderName] = s
		}
	}
	return out
}
