package metrics

import (
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
