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

// Available returns the enabled, non-open routes for a public model in
// priority order. An open breaker excludes a route unless it is the only one,
// in which case a half-open probe is admitted so the route can recover.
func (r *Routes) Available(publicModel string) []Route {
	r.mu.RLock()
	defer r.mu.RUnlock()
	candidates := append([]Route(nil), r.byModel[publicModel]...)
	var healthy []Route
	for _, rt := range candidates {
		if !rt.Enabled {
			continue
		}
		if rt.Breaker.Allow() {
			healthy = append(healthy, rt)
		}
	}
	if len(healthy) == 0 && len(candidates) > 0 {
		// Force a half-open probe of the best route so failures cannot wedge
		// the model permanently when every route is open.
		for _, rt := range candidates {
			if rt.Enabled {
				healthy = append(healthy, rt)
				break
			}
		}
	}
	return healthy
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
