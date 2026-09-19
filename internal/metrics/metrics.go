// Package metrics owns the gateway's Prometheus collectors and serves the
// /metrics endpoint. Metric definitions and label dimensions stay local;
// exposition, label-value escaping, and aggregation are delegated to the
// official Prometheus Go client.
package metrics

import (
	"context"
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
	budgetDenials      *prometheus.CounterVec
	settlementFailures prometheus.Counter
	settlementRetries  prometheus.Counter
	costUnknown        prometheus.Counter
	lifecycleArchived  *prometheus.CounterVec
	lifecycleDeleted   *prometheus.CounterVec
	lifecycleExports   prometheus.Counter
	queueWait          prometheus.Histogram
	leaseExpired       prometheus.Counter
	resultsExpired     prometheus.Counter
	workerHealthy      prometheus.Gauge
	workerInflight     prometheus.Gauge
}

// QueueSampler produces one scrape-time queue sample (depth and oldest queued
// job age). Implemented by callers over the async store so the metrics
// package stays leaf-clean. A scrape that samples errors emits no queue
// series at all — never a fabricated zero.
type QueueSampler func(ctx context.Context) (depth int, oldestAge time.Duration, err error)

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
	// Queue depth/age are scrape-time collector samples (registered only when
	// a QueueSampler is wired), so the DB is polled once per scrape and no
	// per-job label ever exists.
	r.budgetDenials = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_budget_denials_total",
		Help: "Monetary budget denials by scope (subject, tenant).",
	}, []string{"scope"})
	r.settlementFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gateway_ledger_settlement_failures_total",
		Help: "Usage-ledger settlements that failed and remain retryable (reserved rows kept).",
	})
	r.settlementRetries = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gateway_ledger_settlement_retries_total",
		Help: "Settlement attempts re-driven after a previous failure (bounded in-process retry).",
	})
	r.costUnknown = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gateway_ledger_cost_unknown_total",
		Help: "Settlements whose cost stayed unknown (unknown usage or no applicable price), never fabricated as zero.",
	})
	r.lifecycleArchived = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_lifecycle_rows_archived_total",
		Help: "Rows moved to the archive sink by retention sweeps, per table.",
	}, []string{"table"})
	r.lifecycleDeleted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_lifecycle_rows_deleted_total",
		Help: "Rows deleted after a verified archive, per table.",
	}, []string{"table"})
	r.lifecycleExports = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gateway_lifecycle_exports_total",
		Help: "Completed data exports.",
	})
	r.queueWait = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "gateway_async_queue_wait_seconds",
		Help:    "Time a claimed job spent queued before its first claim.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 600},
	})
	r.leaseExpired = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gateway_async_lease_expired_total",
		Help: "Worker leases that lapsed and were recovered by the sweep (requeued or terminal-failed).",
	})
	r.resultsExpired = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gateway_async_results_expired_total",
		Help: "Terminal job results dropped after their retention TTL passed.",
	})
	r.workerHealthy = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gateway_async_worker_healthy",
		Help: "1 when the worker pool is accepting jobs and its sweep is live, 0 during/after shutdown.",
	})
	r.workerInflight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gateway_async_worker_inflight",
		Help: "Background jobs currently executing in this process.",
	})

	reg.MustRegister(
		r.requests, r.upstreamErrors, r.tokens, r.rateLimit, r.duration,
		r.asyncJobs, r.budgetDenials, r.settlementFailures, r.settlementRetries,
		r.costUnknown, r.lifecycleArchived, r.lifecycleDeleted, r.lifecycleExports,
		r.queueWait, r.leaseExpired, r.resultsExpired, r.workerHealthy, r.workerInflight,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	r.workerHealthy.Set(0) // flips to 1 only after Start reports a live pool
	return r
}

// queueSampleTimeout bounds one scrape-time queue sample so a slow database
// can never wedge /metrics.
const queueSampleTimeout = 2 * time.Second

// RegisterQueueCollector installs the custom collectors for
// gateway_async_queue_depth and gateway_async_queue_oldest_seconds. Both are
// sampled once per scrape through the injected function (DB queue depth and
// oldest queued age), so they exist only where a queue exists and carry no
// per-job labels. A failed sample emits no series for that scrape — a gap on
// the dashboard, never a fabricated zero. Call at most once per registry.
func (r *Registry) RegisterQueueCollector(sample QueueSampler) {
	r.reg.MustRegister(queueCollector{sample: sample})
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

// ObserveQueueWait records the enqueue→first-claim latency of a claimed job.
func (r *Registry) ObserveQueueWait(d time.Duration) {
	r.queueWait.Observe(d.Seconds())
}

// AddLeaseExpired adds n to gateway_async_lease_expired_total (leases the
// recovery sweep reclaimed).
func (r *Registry) AddLeaseExpired(n int) {
	r.leaseExpired.Add(float64(n))
}

// AddResultsExpired adds n to gateway_async_results_expired_total.
func (r *Registry) AddResultsExpired(n int) {
	r.resultsExpired.Add(float64(n))
}

// SetWorkerHealthy records gateway_async_worker_healthy (1 live, 0 stopping).
func (r *Registry) SetWorkerHealthy(v bool) {
	if v {
		r.workerHealthy.Set(1)
		return
	}
	r.workerHealthy.Set(0)
}

// SetWorkerInflight records gateway_async_worker_inflight.
func (r *Registry) SetWorkerInflight(n int) {
	r.workerInflight.Set(float64(n))
}

// queueCollector samples the queue once per scrape and emits the depth and
// oldest-age gauges from that single sample. On sample error nothing is
// emitted, so a database outage shows a gap rather than a stale or fake
// value. No label dimensions exist: aggregate depth/age only, never per-job.
type queueCollector struct {
	sample QueueSampler
}

var (
	queueDepthDesc = prometheus.NewDesc("gateway_async_queue_depth",
		"Background jobs currently queued (sampled at scrape time).", nil, nil)
	queueOldestDesc = prometheus.NewDesc("gateway_async_queue_oldest_seconds",
		"Age of the oldest queued job in seconds (0 when the queue is empty; sampled at scrape time).", nil, nil)
)

func (c queueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- queueDepthDesc
	ch <- queueOldestDesc
}

func (c queueCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), queueSampleTimeout)
	defer cancel()
	depth, oldest, err := c.sample(ctx)
	if err != nil {
		return
	}
	ch <- prometheus.MustNewConstMetric(queueDepthDesc, prometheus.GaugeValue, float64(depth))
	ch <- prometheus.MustNewConstMetric(queueOldestDesc, prometheus.GaugeValue, oldest.Seconds())
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

// IncSettlementRetry increments gateway_ledger_settlement_retries_total for
// one re-driven settlement attempt.
func (r *Registry) IncSettlementRetry() {
	r.settlementRetries.Inc()
}

// IncCostUnknown increments gateway_ledger_cost_unknown_total: a settlement
// whose cost stayed unknown (never fabricated as zero).
func (r *Registry) IncCostUnknown() {
	r.costUnknown.Inc()
}

// IncLifecycleArchived adds n to gateway_lifecycle_rows_archived_total for
// one governed table.
func (r *Registry) IncLifecycleArchived(table string, n int64) {
	r.lifecycleArchived.WithLabelValues(table).Add(float64(n))
}

// IncLifecycleDeleted adds n to gateway_lifecycle_rows_deleted_total for one
// governed table.
func (r *Registry) IncLifecycleDeleted(table string, n int64) {
	r.lifecycleDeleted.WithLabelValues(table).Add(float64(n))
}

// IncLifecycleExport increments gateway_lifecycle_exports_total.
func (r *Registry) IncLifecycleExport() {
	r.lifecycleExports.Inc()
}

// Handler serves the Prometheus text exposition for this registry.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{})
}
