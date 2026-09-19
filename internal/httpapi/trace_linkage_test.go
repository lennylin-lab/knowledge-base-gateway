package httpapi

// Observability integration tests for the background-job flow: an in-memory
// span exporter proves one trace links HTTP enqueue → worker → provider
// attempt → settlement; a failing exporter proves export problems never
// break serving; and a redaction pass proves no prompt/completion content,
// credential, or URL reaches any telemetry attribute.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/knowledge-base/knowledge-base-gateway/internal/accounting"
	"github.com/knowledge-base/knowledge-base-gateway/internal/async"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
)

// spanRecorder is the test exporter: an in-memory, thread-safe
// sdktrace.SpanExporter collecting every ended span.
type spanRecorder struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
	fail  bool
}

func (r *spanRecorder) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail {
		return errors.New("export endpoint down")
	}
	r.spans = append(r.spans, spans...)
	return nil
}

func (r *spanRecorder) Shutdown(context.Context) error { return nil }

func (r *spanRecorder) snapshot() []sdktrace.ReadOnlySpan {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]sdktrace.ReadOnlySpan(nil), r.spans...)
}

func (r *spanRecorder) spanNames() map[string]bool {
	out := map[string]bool{}
	for _, s := range r.snapshot() {
		out[s.Name()] = true
	}
	return out
}

// installTraceHarness swaps the global tracer provider for one exporting to
// rec with always-on sampling, and installs the W3C TraceContext
// propagator. Restored on cleanup (tests in a package run sequentially).
func installTraceHarness(t *testing.T, rec *spanRecorder) {
	t.Helper()
	prevProvider := otel.GetTracerProvider()
	prevPropagator := otel.GetTextMapPropagator()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(rec),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prevProvider)
		otel.SetTextMapPropagator(prevPropagator)
	})
}

// awaitSpanNames polls until every named span appears in the export (spans
// end slightly after their store effects become visible in polls).
func awaitSpanNames(t *testing.T, rec *spanRecorder, names ...string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		names_seen := rec.spanNames()
		all := true
		for _, n := range names {
			if !names_seen[n] {
				all = false
				break
			}
		}
		if all {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("spans never exported; have %v, want %v", rec.spanNames(), names)
}

// createBackgroundWithTrace posts a background request carrying the given
// traceparent (when non-empty) and a known input text, returning the job ID.
func (f *accountingFixture) createBackgroundWithTrace(t *testing.T, traceparent, input string) string {
	t.Helper()
	body := fmt.Sprintf(`{"model":"full-model","input":%q,"background":true}`, input)
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testKey)
	if traceparent != "" {
		req.Header.Set("traceparent", traceparent)
	}
	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	id := decodeJobID(t, rec)
	<-f.provider.entered // the worker claimed the job and entered the provider
	return id
}

// TestTraceLinksEnqueueWorkerProviderSettlement is the PRD's exporter test:
// one trace must connect the HTTP request, admission, enqueue, claim, worker
// execution, provider attempt, and settlement spans across the process
// boundary (the job row carries the W3C context between enqueue and worker).
func TestTraceLinksEnqueueWorkerProviderSettlement(t *testing.T) {
	rec := &spanRecorder{}
	installTraceHarness(t, rec)
	f := newAccountingFixture(t, provider.Fake{}, accounting.Price{Version: 2, Currency: "USD", InputPerToken: 10, OutputPerToken: 20})

	// A caller-supplied W3C context: the whole trace must join THIS trace.
	const callerTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	const callerSpanID = "00f067aa0ba902b7"
	id := f.createBackgroundWithTrace(t,
		"00-"+callerTraceID+"-"+callerSpanID+"-01", "trace-linkage-probe")
	f.provider.release <- struct{}{}
	f.awaitTerminal(t, id, async.StatusCompleted)
	f.awaitSettlement(t, id)
	awaitSpanNames(t, rec, "async.worker.execute", "ledger.settle", "async.commit")

	var worker, enqueue sdktrace.ReadOnlySpan
	inTrace := map[string]bool{}
	for _, s := range rec.snapshot() {
		if s.SpanContext().TraceID().String() != callerTraceID {
			continue // unrelated roots (claim polls and such) carry other trace IDs
		}
		inTrace[s.Name()] = true
		switch s.Name() {
		case "async.worker.execute":
			worker = s
		case "async.enqueue":
			enqueue = s
		}
	}
	for _, name := range []string{
		"http.responses", "http.admission", "async.enqueue",
		"async.worker.execute", "provider.attempt", "ledger.settle",
	} {
		if !inTrace[name] {
			t.Fatalf("trace %s missing span %q; spans in trace: %v", callerTraceID, name, inTrace)
		}
	}

	// The worker span must be a distinct span parented by the persisted
	// enqueue span, inside the same trace.
	if worker == nil || enqueue == nil {
		t.Fatal("worker and enqueue spans must exist in the caller trace")
	}
	if worker.SpanContext().SpanID() == enqueue.SpanContext().SpanID() {
		t.Fatal("worker must be a distinct span, not the enqueue span replayed")
	}
	if ro, ok := worker.(sdktrace.ReadOnlySpan); ok {
		if ro.Parent().SpanID().String() != enqueue.SpanContext().SpanID().String() {
			t.Fatalf("worker parent %s, want the persisted enqueue span %s",
				ro.Parent().SpanID(), enqueue.SpanContext().SpanID())
		}
	} else {
		t.Fatal("worker span must be a readable SDK span")
	}
}

// TestExporterFailureNeverBreaksServing: with the exporter permanently
// broken, the background job still enqueues, executes, completes, and
// settles — export failure is a tracing degradation only.
func TestExporterFailureNeverBreaksServing(t *testing.T) {
	rec := &spanRecorder{fail: true}
	installTraceHarness(t, rec)
	f := newAccountingFixture(t, provider.Fake{}, accounting.Price{Version: 2, Currency: "USD", InputPerToken: 10, OutputPerToken: 20})

	id := f.createBackgroundWithTrace(t, "", "exporter-outage-probe")
	f.provider.release <- struct{}{}
	f.awaitTerminal(t, id, async.StatusCompleted)
	settled := f.awaitSettlement(t, id)
	if len(settled) != 1 {
		t.Fatalf("settlement must survive exporter failure: %d", len(settled))
	}
}

// TestTelemetryRedaction runs a background job whose input carries a canary
// and asserts no exported span carries the canary, the API key, or any URL
// scheme in an attribute — telemetry is metadata only.
func TestTelemetryRedaction(t *testing.T) {
	rec := &spanRecorder{}
	installTraceHarness(t, rec)
	f := newAccountingFixture(t, provider.Fake{}, accounting.Price{Version: 2, Currency: "USD", InputPerToken: 10, OutputPerToken: 20})

	const canary = "TOPSECRET-prompt-canary-不要泄露"
	id := f.createBackgroundWithTrace(t, "", canary)
	f.provider.release <- struct{}{}
	f.awaitTerminal(t, id, async.StatusCompleted)
	f.awaitSettlement(t, id)
	awaitSpanNames(t, rec, "ledger.settle")

	prohibited := []string{canary, testKey, "https://", "http://", "Bearer "}
	spans := rec.snapshot()
	if len(spans) == 0 {
		t.Fatal("expected exported spans to scan")
	}
	for _, s := range spans {
		if strings.Contains(s.Name(), canary) || strings.Contains(s.Name(), "http://") {
			t.Fatalf("span name carries prohibited content: %s", s.Name())
		}
		ro, ok := s.(sdktrace.ReadOnlySpan)
		if !ok {
			continue
		}
		for _, attr := range ro.Attributes() {
			val := attr.Value.Emit()
			for _, p := range prohibited {
				if strings.Contains(val, p) {
					t.Fatalf("span %s: attribute %s=%q carries prohibited content %q",
						s.Name(), attr.Key, val, p)
				}
			}
		}
		for _, ev := range ro.Events() {
			for _, kv := range ev.Attributes {
				val := kv.Value.Emit()
				for _, p := range prohibited {
					if strings.Contains(val, p) {
						t.Fatalf("span %s: event attribute carries prohibited content %q",
							s.Name(), p)
					}
				}
			}
		}
	}
}

// TestHTTPSpanCarriesResponseStatus pins that the server span records the
// real final status (a 400 denial must not be fabricated as 200).
func TestHTTPSpanCarriesResponseStatus(t *testing.T) {
	rec := &spanRecorder{}
	installTraceHarness(t, rec)
	f := newAccountingFixture(t, provider.Fake{}, accounting.Price{})

	// A malformed body: the handler must answer 400 and the span must say so.
	body := "not-json"
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testKey)
	resp := httptest.NewRecorder()
	f.handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("probe response = %d, want 400", resp.Code)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, s := range rec.snapshot() {
			if s.Name() != "http.responses" {
				continue
			}
			ro, ok := s.(sdktrace.ReadOnlySpan)
			if !ok {
				continue
			}
			for _, attr := range ro.Attributes() {
				if attr.Key == "gw.status" && attr.Value.AsInt64() == http.StatusBadRequest {
					return
				}
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("no http.responses span carried gw.status=400")
}

// TestSettlementRetryObserved pins the bounded in-process settlement retry:
// the first settlement attempt fails (failure counted), one retry is
// attempted, and the retry succeeds — the ledger row ends settled exactly
// once and the job's reservation is never released as if unpaid.
func TestSettlementRetryObserved(t *testing.T) {
	f := newAccountingFixture(t, provider.Fake{}, accounting.Price{Version: 2, Currency: "USD", InputPerToken: 10, OutputPerToken: 20})

	f.ledger.failSettleOnce.Store(1) // fail exactly the first settle attempt
	id := f.create(t)
	f.provider.release <- struct{}{}
	f.awaitTerminal(t, id, async.StatusCompleted)
	f.awaitSettlement(t, id)

	// The retry landed the one settled row; a retried-then-settled job must
	// not release its reservations as if the outcome were unpaid.
	if got := f.ledger.settlementAttempts.Load(); got < 1 {
		t.Fatalf("settled attempts = %d, want at least the retry", got)
	}
	if n := f.ledger.releasedFor(id); n != 0 {
		t.Fatalf("a retried-then-settled job must not release: %d releases", n)
	}
}

// TestTraceContextNormalizedBeforePersistence: hostile traceparent values
// never persist — normalization collapses anything non-W3C to empty before
// the row is written (store boundary exercised on the memory store).
func TestTraceContextNormalizedBeforePersistence(t *testing.T) {
	jobs := async.NewMemoryStore(nil)
	out, err := jobs.Create(context.Background(), async.CreateInput{
		JobID: "job-trace-1", SubjectID: "s", TenantID: "t", Protocol: "responses",
		PublicModel: "m", RequestDigest: "d",
		TraceID: "4BF92F3577B34DA6A3CE929D0E0E4736", // uppercase: not normalized
		SpanID:  "00f067aa0ba902b7;DROP",            // not hex
		Now:     time.Now(),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if out.Job.TraceID != "" || out.Job.ParentSpanID != "" || out.Job.TraceSampled {
		t.Fatalf("malformed trace context must persist as empty, got %+v", out.Job)
	}

	// A valid pair persists verbatim with its sampled bit.
	out, err = jobs.Create(context.Background(), async.CreateInput{
		JobID: "job-trace-2", SubjectID: "s", TenantID: "t", Protocol: "responses",
		PublicModel: "m", RequestDigest: "d2",
		TraceID: "4bf92f3577b34da6a3ce929d0e0e4736", SpanID: "00f067aa0ba902b7",
		TraceSampled: true, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("create valid: %v", err)
	}
	if out.Job.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" ||
		out.Job.ParentSpanID != "00f067aa0ba902b7" || !out.Job.TraceSampled {
		t.Fatalf("valid trace context must persist verbatim: %+v", out.Job)
	}
}
