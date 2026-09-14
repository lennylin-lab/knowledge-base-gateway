// Package metrics provides a minimal Prometheus text-format exposition for
// gateway request counters, avoiding heavyweight dependencies in the MVP.
package metrics

import (
	"fmt"
	"io"
	"maps"
	"net/http"
	"sort"
	"strings"
	"sync"
)

// Registry holds labeled counters and exposes them at /metrics.
type Registry struct {
	mu        sync.Mutex
	requests  map[string]int // label set -> count
	upstreams map[string]int
	tokens    map[string]int
	rateLimit map[string]int
}

// New creates an empty registry.
func New() *Registry {
	return &Registry{
		requests: map[string]int{}, upstreams: map[string]int{},
		tokens: map[string]int{}, rateLimit: map[string]int{},
	}
}

// IncUpstreamError increments gateway_upstream_errors_total.
func (r *Registry) IncUpstreamError(model, providerName, class string) {
	r.mu.Lock()
	r.upstreams[fmt.Sprintf("model=%q,provider=%q,class=%q", model, providerName, class)]++
	r.mu.Unlock()
}

// AddTokens increments gateway_tokens_total for prompt/completion kinds.
func (r *Registry) AddTokens(model string, prompt, completion int) {
	r.mu.Lock()
	r.tokens[fmt.Sprintf("model=%q,kind=%q", model, "prompt")] += prompt
	r.tokens[fmt.Sprintf("model=%q,kind=%q", model, "completion")] += completion
	r.mu.Unlock()
}

// IncRateLimit increments gateway_rate_limit_total.
func (r *Registry) IncRateLimit(model string) {
	r.mu.Lock()
	r.rateLimit[fmt.Sprintf("model=%q", model)]++
	r.mu.Unlock()
}

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
		reqs := maps.Clone(r.requests)
		upstreams := maps.Clone(r.upstreams)
		tokens := maps.Clone(r.tokens)
		rateLimit := maps.Clone(r.rateLimit)
		r.mu.Unlock()

		var b strings.Builder
		writeSeries := func(name, help string, m map[string]int) {
			fmt.Fprintf(&b, "# HELP %s %s\n", name, help)
			fmt.Fprintf(&b, "# TYPE %s counter\n", name)
			total := 0
			for _, v := range m {
				total += v
			}
			fmt.Fprintf(&b, "%s %d\n", name, total)
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				fmt.Fprintf(&b, "%s{%s} %d\n", name, k, m[k])
			}
		}
		writeSeries("gateway_requests_total", "Total chat completion requests.", reqs)
		writeSeries("gateway_upstream_errors_total", "Total upstream provider errors.", upstreams)
		writeSeries("gateway_tokens_total", "Total tokens reported by upstreams.", tokens)
		writeSeries("gateway_rate_limit_total", "Total rate/limit denials.", rateLimit)

		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = io.WriteString(w, b.String())
	})
}
