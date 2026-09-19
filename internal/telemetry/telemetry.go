// Package telemetry owns the OpenTelemetry SDK lifecycle: the OTLP trace
// exporter, the sampler, and the batch processor. It is wired only in
// cmd/gateway; every other package observes tracing through the API-level
// helpers in internal/tracing, so with the default no-op provider all
// instrumentation is inert.
//
// Kill-switch contract: GATEWAY_OTLP_ENABLED=false (the default) leaves the
// global no-op provider installed — serving, Prometheus metrics, and the
// async worker are completely unaffected, and no exporter goroutine exists.
// When enabled, exporter failure degrades tracing only: the batch processor
// drops spans after its bounded queue, export errors surface through the OTel
// error handler (logged), and the serving path never blocks on export.
package telemetry

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
)

// Options configures the OTLP exporter. Produced from internal/config.
type Options struct {
	Enabled  bool          // GATEWAY_OTLP_ENABLED — the independent kill switch
	Endpoint string        // OTLP/HTTP endpoint, host:port (default localhost:4318)
	Insecure bool          // plain-HTTP export (dev/collector-on-loopback)
	Ratio    float64       // TraceIDRatioBased sampling for root spans (0..1)
	Timeout  time.Duration // per-export timeout (bounded export)
}

// Shutdown flushes and releases the installed provider. It is always safe to
// call; the no-op setup returns a function that does nothing.
type Shutdown func(context.Context) error

// Setup installs the global TracerProvider and W3C TraceContext propagator.
// The propagator carries trace identity only — baggage is never propagated or
// persisted. When opts.Enabled is false the no-op provider stays installed
// and the returned Shutdown is a no-op.
func Setup(ctx context.Context, opts Options, logger *slog.Logger) (Shutdown, error) {
	noop := func(context.Context) error { return nil }
	if !opts.Enabled {
		// Kill switch: keep whatever default is installed (the SDK-neutral
		// no-op) and touch nothing else.
		return noop, nil
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 10 * time.Second
	}

	opts2 := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(opts.Endpoint),
		otlptracehttp.WithTimeout(opts.Timeout),
		// The exporter never blocks serving: batch drops, export fails into
		// the error handler, and the HTTP client has no retry storm (bounded
		// attempts, then the batch moves on).
		otlptracehttp.WithRetry(otlptracehttp.RetryConfig{
			Enabled:         true,
			InitialInterval: 500 * time.Millisecond,
			MaxInterval:     5 * time.Second,
			MaxElapsedTime:  opts.Timeout,
		}),
	}
	if opts.Insecure {
		opts2 = append(opts2, otlptracehttp.WithInsecure())
	}
	exp, err := otlptracehttp.New(ctx, opts2...)
	if err != nil {
		return noop, err
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName("kb-gateway")),
	)
	if err != nil {
		return noop, err
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(opts.Ratio))),
	)
	otel.SetTracerProvider(provider)
	// Trace context only: identity correlation across processes, never baggage.
	otel.SetTextMapPropagator(propagation.TraceContext{})
	if logger != nil {
		logger.Info("otlp trace export enabled",
			"endpoint", opts.Endpoint, "insecure", opts.Insecure, "sampling_ratio", opts.Ratio)
	}
	return func(ctx context.Context) error {
		// Flush bounded by the caller's context; after shutdown the global
		// provider keeps working as a live-but-unflushed provider, which is
		// fine at process exit.
		return provider.Shutdown(ctx)
	}, nil
}
