// Package tracing is the gateway's narrow observability helper over the
// OpenTelemetry API. Domain packages record spans through these helpers and
// never see SDK or exporter types: with the default global no-op provider
// every call is a cheap no-op, and internal/telemetry (wired only in
// cmd/gateway) installs the real TracerProvider when OTLP is enabled.
//
// Redaction invariant: span attributes are metadata only — public model,
// provider name, protocol, status/error classes, attempt counts, IDs. Never
// prompt/completion/tool-argument text, credentials, URLs, SQL, or any other
// request content.
package tracing

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// tracerName is the single instrumentation scope for gateway spans.
const tracerName = "kb-gateway"

// Tracer returns the gateway tracer from the globally installed provider
// (a no-op until internal/telemetry installs one).
func Tracer() trace.Tracer { return otel.Tracer(tracerName) }

// Start begins a span named name as a child of the context's current span,
// injecting the returned context so downstream calls join the trace.
func Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return Tracer().Start(ctx, name, trace.WithSpanKind(trace.SpanKindInternal), trace.WithAttributes(attrs...))
}

// StartServer begins a server-side root span, joining the incoming W3C
// traceparent when the caller's context already carries extracted identity.
func StartServer(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return Tracer().Start(ctx, name, trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(attrs...))
}

// Extract joins the incoming W3C trace context from request headers. Only
// trace identity is extracted; baggage is never read, so it can never reach
// a persisted job or log line.
func Extract(ctx context.Context, header http.Header) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(header))
}

// Semantic span attribute keys. Values are always metadata-only; tests pin
// that request content never appears under any attribute.
const (
	AttrRequestID   = "gw.request_id"
	AttrJobID       = "gw.job_id"
	AttrProtocol    = "gw.protocol"
	AttrModel       = "gw.model"
	AttrProvider    = "gw.provider"
	AttrAttempt     = "gw.attempt"
	AttrStatus      = "gw.status"
	AttrErrorClass  = "gw.error_class"
	AttrOutcome     = "gw.outcome"
	AttrWorkerID    = "gw.worker_id"
	AttrTraceSource = "gw.trace_source"
)

// String safely appends a string attribute only when the value is non-empty.
func String(key, value string) attribute.KeyValue {
	return attribute.String(key, value)
}

// Attr is a small typed builder so call sites stay terse.
func Int(key string, v int) attribute.KeyValue { return attribute.Int(key, v) }

// --- Persisted trace context -------------------------------------------------
//
// The enqueue path records the current span context as two normalized hex
// strings on the job row; the worker rebuilds a remote SpanContext from them.
// Normalization (exact length, lowercase hex) runs before persistence so an
// arbitrary or oversized value can never reach the database, and baggage is
// never persisted at all.

// TraceContext is the persisted W3C trace identity of a job's enqueue span.
// Empty strings mean absent. Sampled carries the caller's sampling decision
// so the worker's ParentBased sampler keeps execution spans inside the same
// sampled trace instead of silently dropping them.
type TraceContext struct {
	TraceID string // 32 lowercase hex chars
	SpanID  string // 16 lowercase hex chars
	Sampled bool
}

// Empty reports whether no trace context is carried.
func (tc TraceContext) Empty() bool { return tc.TraceID == "" || tc.SpanID == "" }

// validHex reports whether s is exactly n lowercase hex characters.
func validHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// Normalize validates and canonicalizes raw trace/span ID pair. ok is false
// for anything that is not a well-formed lowercase-hex W3C pair — the caller
// then persists an empty TraceContext (trace context is best-effort
// correlation, never a correctness dependency).
func Normalize(traceID, spanID string) (TraceContext, bool) {
	tc := TraceContext{TraceID: traceID, SpanID: spanID}
	if tc.Empty() {
		return TraceContext{}, false
	}
	if !validHex(tc.TraceID, 32) || !validHex(tc.SpanID, 16) {
		return TraceContext{}, false
	}
	return tc, true
}

// FromSpanContext captures sc as the persisted pair (zero when invalid).
func FromSpanContext(sc trace.SpanContext) TraceContext {
	if !sc.IsValid() {
		return TraceContext{}
	}
	return TraceContext{
		TraceID: sc.TraceID().String(),
		SpanID:  sc.SpanID().String(),
		Sampled: sc.IsSampled(),
	}
}

// Current captures the context's current span context as the persisted pair.
func Current(ctx context.Context) TraceContext {
	return FromSpanContext(trace.SpanContextFromContext(ctx))
}

// WithRemote grafts a remote SpanContext built from a persisted pair onto
// ctx so a child span joins the original trace. Invalid or empty values
// return ctx unchanged (the caller's span then roots a fresh trace).
func WithRemote(ctx context.Context, tc TraceContext) context.Context {
	if tc.Empty() {
		return ctx
	}
	norm, ok := Normalize(tc.TraceID, tc.SpanID)
	if !ok {
		return ctx
	}
	var tid trace.TraceID
	if _, err := hex.Decode(tid[:], []byte(norm.TraceID)); err != nil {
		return ctx
	}
	var sid trace.SpanID
	if _, err := hex.Decode(sid[:], []byte(norm.SpanID)); err != nil {
		return ctx
	}
	// The sampling decision rides the persisted context: the worker's
	// ParentBased sampler then keeps execution spans in the sampled trace.
	flags := trace.TraceFlags(0).WithSampled(tc.Sampled)
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid, SpanID: sid, Remote: true, TraceFlags: flags,
	})
	return trace.ContextWithRemoteSpanContext(ctx, sc)
}

// NewTraceID generates a random 32-hex trace ID (used by the worker when a
// claimed job carries no persisted context, so every execution is still
// correlatable).
func NewTraceID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return ""
	}
	return hex.EncodeToString(b[:])
}
