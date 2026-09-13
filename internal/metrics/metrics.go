// Package metrics provides a minimal Prometheus text-format exposition for
// gateway request counters, avoiding heavyweight dependencies in the MVP.
package metrics

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
)

// Registry holds labeled counters and exposes them at /metrics.
type Registry struct {
	mu       sync.Mutex
	requests map[string]int // label set -> count
}

// New creates an empty registry.
func New() *Registry { return &Registry{requests: map[string]int{}} }

// IncRequest increments gateway_requests_total for the given labels.
func (r *Registry) IncRequest(model, status string) {
	r.mu.Lock()
	r.requests[fmt.Sprintf("model=%q,status=%q", model, status)]++
	r.mu.Unlock()
}

// Handler serves the Prometheus text exposition.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		r.mu.Lock()
		keys := make([]string, 0, len(r.requests))
		total := 0
		for k, v := range r.requests {
			keys = append(keys, k)
			total += v
		}
		r.mu.Unlock()
		sort.Strings(keys)

		var b strings.Builder
		b.WriteString("# HELP gateway_requests_total Total chat completion requests.\n")
		b.WriteString("# TYPE gateway_requests_total counter\n")
		b.WriteString(fmt.Sprintf("gateway_requests_total %d\n", total))
		for _, k := range keys {
			b.WriteString(fmt.Sprintf("gateway_requests_total{%s} %d\n", k, r.requests[k]))
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = io.WriteString(w, b.String())
	})
}
