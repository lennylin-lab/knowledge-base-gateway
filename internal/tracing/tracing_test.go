package tracing

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// withTestProvider installs a real SDK provider so span identity behaves
// exactly as in production (the default no-op provider produces invalid
// span contexts). Tests in this package run sequentially, so the global
// swap is safe; the provider is shut down on cleanup.
func withTestProvider(t *testing.T) {
	t.Helper()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	otel.SetTracerProvider(tp)
}

// TestNormalize accepts only well-formed lowercase-hex W3C pairs.
func TestNormalize(t *testing.T) {
	tid := "0af7651916cd43dd8448eb211c80319c"
	sid := "b7ad6b7169203331"

	if tc, ok := Normalize(tid, sid); !ok || tc.TraceID != tid || tc.SpanID != sid {
		t.Fatalf("valid pair rejected: %+v ok=%v", tc, ok)
	}
	for name, pair := range map[string][2]string{
		"uppercase trace":  {"0AF7651916CD43DD8448EB211C80319C", sid},
		"short trace":      {"0af7651916cd43dd8448eb211c80319", sid},
		"long trace":       {tid + "0", sid},
		"short span":       {"b7ad6b71692033", sid},
		"non-hex":          {"0af7651916cd43dd8448eb211c80319z", sid},
		"empty":            {"", ""},
		"half empty":       {tid, ""},
		"script injection": {"<script>alert(1)</script>aaaa", sid},
		"spaces":           {tid + " ", sid},
	} {
		if _, ok := Normalize(pair[0], pair[1]); ok {
			t.Fatalf("%s: malformed pair must be rejected", name)
		}
	}
}

// TestCurrentAndWithRemote proves the persisted-pair round trip: a span's
// identity survives normalization and grafts as a remote parent, so the
// worker span joins the enqueue trace with the sampled bit carried through.
func TestCurrentAndWithRemote(t *testing.T) {
	withTestProvider(t)

	ctx, span := Start(context.Background(), "test.enqueue")
	tc := Current(ctx)
	if tc.Empty() {
		t.Fatal("expected non-empty trace context from a real span")
	}
	if !validHex(tc.TraceID, 32) || !validHex(tc.SpanID, 16) {
		t.Fatalf("persisted identity not normalized: %+v", tc)
	}
	if !tc.Sampled {
		t.Fatal("sampled bit must be captured")
	}
	span.End()

	// Graft onto a fresh context: the child must join the same trace with
	// the enqueue span as its remote parent.
	workerCtx, workerSpan := Start(WithRemote(context.Background(), tc), "test.worker")
	got := trace.SpanContextFromContext(workerCtx)
	if got.TraceID().String() != tc.TraceID {
		t.Fatalf("worker trace %s, want enqueue trace %s", got.TraceID(), tc.TraceID)
	}
	if ro, ok := workerSpan.(sdktrace.ReadOnlySpan); ok {
		if ro.Parent().SpanID().String() != tc.SpanID {
			t.Fatalf("worker parent %s, want persisted span %s", ro.Parent().SpanID(), tc.SpanID)
		}
		if !ro.Parent().IsSampled() {
			t.Fatal("remote parent must stay sampled so ParentBased keeps the worker span")
		}
	} else {
		t.Fatal("expected a recording SDK span")
	}
	workerSpan.SetAttributes(attribute.String("probe", "value"))
	workerSpan.End()

	// Invalid persisted values graft nothing (fresh trace, never an error).
	bad := WithRemote(context.Background(), TraceContext{TraceID: "not-hex", SpanID: "b7ad6b7169203331"})
	if trace.SpanContextFromContext(bad).IsValid() {
		t.Fatal("malformed pair must not graft a span context")
	}
}

// TestEmptyContextBehavior: absent context is a no-op, never an error.
func TestEmptyContextBehavior(t *testing.T) {
	if !(TraceContext{}.Empty()) {
		t.Fatal("zero value must be empty")
	}
	if (TraceContext{TraceID: "a", SpanID: "b"}).Empty() {
		t.Fatal("a filled context must not be empty")
	}
	ctx := WithRemote(context.Background(), TraceContext{})
	if trace.SpanContextFromContext(ctx).IsValid() {
		t.Fatal("empty context must not graft")
	}
	if NewTraceID() == "" || len(NewTraceID()) != 32 {
		t.Fatal("generated trace id must be 32 hex chars")
	}
}
