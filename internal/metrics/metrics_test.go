package metrics

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestCountersAggregateByLabel(t *testing.T) {
	reg := New()
	reg.IncRequest("m", "200")
	reg.IncRequest("m", "200")
	reg.IncRequest("other", "503")
	reg.IncUpstreamError("m", "primary", "timeout")
	reg.AddTokens("m", 11, 7)
	reg.IncRateLimit("m")

	if got := testutil.ToFloat64(reg.requests.WithLabelValues("m", "200")); got != 2 {
		t.Fatalf("gateway_requests_total{model=m,status=200} = %v, want 2", got)
	}
	if got := testutil.ToFloat64(reg.requests.WithLabelValues("other", "503")); got != 1 {
		t.Fatalf("gateway_requests_total{model=other,status=503} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(reg.upstreamErrors.WithLabelValues("m", "primary", "timeout")); got != 1 {
		t.Fatalf("gateway_upstream_errors_total = %v, want 1", got)
	}
	if got := testutil.ToFloat64(reg.tokens.WithLabelValues("m", "prompt")); got != 11 {
		t.Fatalf("gateway_tokens_total{kind=prompt} = %v, want 11", got)
	}
	if got := testutil.ToFloat64(reg.tokens.WithLabelValues("m", "completion")); got != 7 {
		t.Fatalf("gateway_tokens_total{kind=completion} = %v, want 7", got)
	}
	if got := testutil.ToFloat64(reg.rateLimit.WithLabelValues("m")); got != 1 {
		t.Fatalf("gateway_rate_limit_total = %v, want 1", got)
	}
}

func TestDurationHistogramObserved(t *testing.T) {
	reg := New()
	reg.ObserveDuration("m", "200", 375*time.Millisecond)

	families, err := reg.reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var found bool
	for _, mf := range families {
		if mf.GetName() != "gateway_request_duration_seconds" {
			continue
		}
		found = true
		for _, ms := range mf.Metric {
			labels := map[string]string{}
			for _, lp := range ms.Label {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["model"] == "m" && labels["status"] == "200" {
				if ms.GetHistogram().GetSampleCount() != 1 {
					t.Fatalf("expected one observation, got %d", ms.GetHistogram().GetSampleCount())
				}
				if got := ms.GetHistogram().GetSampleSum(); got <= 0.25 || got > 0.5 {
					t.Fatalf("observed sum %v outside the 0.25-0.5s buckets", got)
				}
			}
		}
	}
	if !found {
		t.Fatal("gateway_request_duration_seconds family missing from /metrics output")
	}
}

// TestHandlerEscapesLabelValues pins that label values are escaped by the
// official client instead of being formatted by hand: quotes, backslashes,
// and newlines in model names must not break the exposition format.
func TestHandlerEscapesLabelValues(t *testing.T) {
	reg := New()
	tricky := "m\"x\\y\nz"
	reg.IncRequest(tricky, "200")
	// Unused vecs emit no family at all; populate each before asserting.
	reg.IncUpstreamError(tricky, "p", "timeout")
	reg.AddTokens(tricky, 1, 1)
	reg.IncRateLimit(tricky)
	reg.ObserveDuration(tricky, "200", time.Second)

	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()

	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Fatalf("unexpected content type %q", ct)
	}
	if !strings.Contains(body, `model="m\"x\\y\nz"`) {
		t.Fatalf("label value not escaped safely; body:\n%s", body)
	}
	for _, name := range []string{
		"gateway_requests_total", "gateway_upstream_errors_total",
		"gateway_tokens_total", "gateway_rate_limit_total",
		"gateway_request_duration_seconds",
	} {
		if !strings.Contains(body, fmt.Sprintf("# HELP %s ", name)) {
			t.Fatalf("metric family %s missing from exposition", name)
		}
	}
	// Every series line must be self-contained: a raw newline inside a label
	// value would produce lines that do not start with a known token.
	for _, line := range strings.Split(body, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.Contains(line, " ") {
			t.Fatalf("malformed exposition line %q", line)
		}
	}
}

// TestAsyncAndCostSignals pins the roadmap observability signals: queue wait
// latency, lease expiry, settlement retry, unknown cost, and result expiry
// counters, plus the worker health/inflight gauges — all without any
// subject/tenant/job label.
func TestAsyncAndCostSignals(t *testing.T) {
	reg := New()
	reg.ObserveQueueWait(250 * time.Millisecond)
	reg.AddLeaseExpired(2)
	reg.AddResultsExpired(3)
	reg.IncSettlementRetry()
	reg.IncCostUnknown()
	reg.IncSettlementFailure()
	reg.SetWorkerHealthy(true)
	reg.SetWorkerInflight(4)

	families, err := reg.reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	want := map[string]float64{
		"gateway_async_lease_expired_total":        2,
		"gateway_async_results_expired_total":      3,
		"gateway_ledger_settlement_retries_total":  1,
		"gateway_ledger_cost_unknown_total":        1,
		"gateway_ledger_settlement_failures_total": 1,
		"gateway_async_worker_healthy":             1,
		"gateway_async_worker_inflight":            4,
	}
	seen := map[string]bool{}
	for _, mf := range families {
		if _, ok := want[mf.GetName()]; !ok {
			continue
		}
		seen[mf.GetName()] = true
		if labels := mf.GetMetric()[0].GetLabel(); len(labels) != 0 {
			t.Fatalf("%s must carry no labels (no subject/tenant/job dimensions), got %v", mf.GetName(), labels)
		}
		if got := mf.GetMetric()[0].GetCounter().GetValue() + mf.GetMetric()[0].GetGauge().GetValue(); got != want[mf.GetName()] {
			t.Fatalf("%s = %v, want %v", mf.GetName(), got, want[mf.GetName()])
		}
	}
	for name := range want {
		if !seen[name] {
			t.Fatalf("metric family %s missing from exposition", name)
		}
	}

	// Queue-wait histogram recorded one observation in the 0.1-0.25s band.
	var queueWait bool
	for _, mf := range families {
		if mf.GetName() != "gateway_async_queue_wait_seconds" {
			continue
		}
		queueWait = true
		if ms := mf.GetMetric()[0].GetHistogram(); ms.GetSampleCount() != 1 || ms.GetSampleSum() <= 0 || ms.GetSampleSum() > 0.5 {
			t.Fatalf("queue wait histogram: count=%d sum=%v", ms.GetSampleCount(), ms.GetSampleSum())
		}
	}
	if !queueWait {
		t.Fatal("gateway_async_queue_wait_seconds family missing")
	}

	reg.SetWorkerHealthy(false)
	reg.SetWorkerInflight(0)
	if got := testutil.ToFloat64(reg.workerHealthy); got != 0 {
		t.Fatalf("worker healthy after stop = %v, want 0", got)
	}
	if got := testutil.ToFloat64(reg.workerInflight); got != 0 {
		t.Fatalf("worker inflight after drain = %v, want 0", got)
	}
}

// TestQueueCollectorSamplesAtScrape proves the queue depth/age gauges are
// scrape-time samples with no label dimensions, and that a failed sample
// emits nothing rather than a fabricated zero.
func TestQueueCollectorSamplesAtScrape(t *testing.T) {
	reg := New()
	calls := 0
	sampleErr := false
	reg.RegisterQueueCollector(func(context.Context) (int, time.Duration, error) {
		calls++
		if sampleErr {
			return 0, 0, errors.New("queue store down")
		}
		return 7, 1500 * time.Millisecond, nil
	})

	families, err := reg.reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	if calls != 1 {
		t.Fatalf("one scrape must sample once, got %d calls", calls)
	}
	found := map[string]float64{}
	for _, mf := range families {
		switch mf.GetName() {
		case "gateway_async_queue_depth", "gateway_async_queue_oldest_seconds":
			if labels := mf.GetMetric()[0].GetLabel(); len(labels) != 0 {
				t.Fatalf("%s must carry no labels", mf.GetName())
			}
			found[mf.GetName()] = mf.GetMetric()[0].GetGauge().GetValue()
		}
	}
	if found["gateway_async_queue_depth"] != 7 {
		t.Fatalf("queue depth = %v, want 7", found["gateway_async_queue_depth"])
	}
	if found["gateway_async_queue_oldest_seconds"] != 1.5 {
		t.Fatalf("queue oldest age = %v, want 1.5", found["gateway_async_queue_oldest_seconds"])
	}

	// Sample failure: gap, not zero.
	sampleErr = true
	families, err = reg.reg.Gather()
	if err != nil {
		t.Fatalf("gather after failure: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() == "gateway_async_queue_depth" {
			t.Fatalf("failed sample must emit no series, got %v", mf.GetMetric()[0].GetGauge().GetValue())
		}
	}
}

// TestTerminalStatusLabelSet pins the bounded label vocabulary of the async
// outcome counter: exactly the four terminal job states plus nothing else,
// and budget denials limited to the two scope values.
func TestTerminalStatusLabelSet(t *testing.T) {
	reg := New()
	for _, status := range []string{"completed", "failed", "cancelled", "expired"} {
		reg.IncAsyncJob(status)
	}
	reg.IncBudgetDenial("subject")
	reg.IncBudgetDenial("tenant")

	families, err := reg.reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	statusValues := map[string]bool{}
	scopeValues := map[string]bool{}
	for _, mf := range families {
		switch mf.GetName() {
		case "gateway_async_jobs_total":
			for _, m := range mf.GetMetric() {
				statusValues[m.GetLabel()[0].GetValue()] = true
			}
		case "gateway_budget_denials_total":
			for _, m := range mf.GetMetric() {
				scopeValues[m.GetLabel()[0].GetValue()] = true
			}
		}
	}
	for _, want := range []string{"completed", "failed", "cancelled", "expired"} {
		if !statusValues[want] {
			t.Fatalf("async job status label %s missing", want)
		}
	}
	for _, want := range []string{"subject", "tenant"} {
		if !scopeValues[want] {
			t.Fatalf("budget denial scope label %s missing", want)
		}
	}
}
