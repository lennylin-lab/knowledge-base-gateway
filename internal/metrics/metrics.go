// Package metrics owns the gateway's Prometheus collectors and serves the
// /metrics endpoint. Metric definitions and label dimensions stay local;
// exposition, label-value escaping, and aggregation are delegated to the
// official Prometheus Go client.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// requestDurationBuckets covers interactive LLM latencies, which routinely
// span hundreds of milliseconds up to minutes for long completions.
var requestDurationBuckets = []float64{
	0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120,
}

// Registry holds the labeled gateway metrics and exposes them at /metrics.
type Registry struct {
	reg                *prometheus.Registry
	requests           *prometheus.CounterVec
	upstreamErrors     *prometheus.CounterVec
	tokens             *prometheus.CounterVec
	rateLimit          *prometheus.CounterVec
	duration           *prometheus.HistogramVec
	asyncJobs          *prometheus.CounterVec
	queueDepth         prometheus.Gauge
	budgetDenials      *prometheus.CounterVec
	settlementFailures prometheus.Counter
}

// New creates a registry with the gateway collectors plus the standard Go
// runtime and process collectors.
func New() *Registry {
	reg := prometheus.NewRegistry()
	r := &Registry{reg: reg}
	r.requests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_requests_total",
		Help: "Total chat completion requests.",
	}, []string{"model", "status"})
	r.upstreamErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_upstream_errors_total",
		Help: "Total upstream provider errors.",
	}, []string{"model", "provider", "class"})
	r.tokens = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_tokens_total",
		Help: "Total tokens reported by upstreams.",
	}, []string{"model", "kind"})
	r.rateLimit = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_rate_limit_total",
		Help: "Total rate/limit denials.",
	}, []string{"model"})
	r.duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gateway_request_duration_seconds",
		Help:    "Chat completion request duration in seconds.",
		Buckets: requestDurationBuckets,
	}, []string{"model", "status"})
	r.asyncJobs = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_async_jobs_total",
		Help: "Background jobs by terminal status (completed, failed, cancelled, expired).",
	}, []string{"status"})
	r.queueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gateway_async_queue_depth",
		Help: "Background jobs currently queued.",
	})
	r.budgetDenials = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_budget_denials_total",
		Help: "Monetary budget denials by scope (subject, tenant).",
	}, []string{"scope"})
	r.settlementFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gateway_ledger_settlement_failures_total",
		Help: "Usage-ledger settlements that failed and remain retryable (reserved rows kept).",
	})

	reg.MustRegister(
		r.requests, r.upstreamErrors, r.tokens, r.rateLimit, r.duration,
		r.asyncJobs, r.queueDepth, r.budgetDenials, r.settlementFailures,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return r
}

// IncUpstreamError increments gateway_upstream_errors_total.
func (r *Registry) IncUpstreamError(model, providerName, class string) {
	r.upstreamErrors.WithLabelValues(model, providerName, class).Inc()
}

// AddTokens increments gateway_tokens_total for prompt/completion kinds.
func (r *Registry) AddTokens(model string, prompt, completion int) {
	r.tokens.WithLabelValues(model, "prompt").Add(float64(prompt))
	r.tokens.WithLabelValues(model, "completion").Add(float64(completion))
}

// IncRateLimit increments gateway_rate_limit_total.
func (r *Registry) IncRateLimit(model string) {
	r.rateLimit.WithLabelValues(model).Inc()
}

// IncRequest increments gateway_requests_total for the given labels.
func (r *Registry) IncRequest(model, status string) {
	r.requests.WithLabelValues(model, status).Inc()
}

// ObserveDuration records the request duration in
// gateway_request_duration_seconds for the given labels.
func (r *Registry) ObserveDuration(model, status string, d time.Duration) {
	r.duration.WithLabelValues(model, status).Observe(d.Seconds())
}

// IncAsyncJob increments gateway_async_jobs_total for a terminal status.
func (r *Registry) IncAsyncJob(status string) {
	r.asyncJobs.WithLabelValues(status).Inc()
}

// SetAsyncQueueDepth records gateway_async_queue_depth from the worker sweep.
func (r *Registry) SetAsyncQueueDepth(n int) {
	r.queueDepth.Set(float64(n))
}

// IncBudgetDenial increments gateway_budget_denials_total for the denying
// scope (subject or tenant).
func (r *Registry) IncBudgetDenial(scope string) {
	r.budgetDenials.WithLabelValues(scope).Inc()
}

// IncSettlementFailure increments gateway_ledger_settlement_failures_total.
func (r *Registry) IncSettlementFailure() {
	r.settlementFailures.Inc()
}

// Handler serves the Prometheus text exposition for this registry.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{})
}
