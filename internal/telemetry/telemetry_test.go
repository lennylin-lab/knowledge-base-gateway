package telemetry

// Telemetry setup tests. The OTLP exporter is never dialed here (an
// unreachable endpoint is fine — exporter construction is lazy), so these
// run offline. The exporter-failure-tolerance behavior (exporter down never
// breaks serving) is proven end to end in internal/httpapi's trace-linkage
// test with a deliberately failing exporter.

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// TestSetupDisabledKeepsNoop pins the kill-switch contract: with Enabled
// false (the default), the global no-op provider stays installed — spans are
// non-recording, nothing dialable exists, and Shutdown is a no-op.
func TestSetupDisabledKeepsNoop(t *testing.T) {
	shutdown, err := Setup(context.Background(), Options{Enabled: false,
		Endpoint: "127.0.0.1:1", Insecure: true, Ratio: 1, Timeout: time.Second}, slog.Default())
	if err != nil {
		t.Fatalf("disabled setup must not fail: %v", err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("disabled shutdown must be a no-op: %v", err)
	}
	_, span := otel.Tracer("test").Start(context.Background(), "probe")
	if span.IsRecording() {
		t.Fatal("kill switch must leave the no-op provider installed")
	}
	span.End()
}

// TestSetupEnabledInstallsProvider proves the enabled path installs a real
// provider: spans become recording, and the unreachable exporter endpoint
// must NOT fail setup (exporter failure never blocks startup or serving).
func TestSetupEnabledInstallsProvider(t *testing.T) {
	t.Cleanup(func() { otel.SetTracerProvider(trace.TracerProvider(nil)) })
	shutdown, err := Setup(context.Background(), Options{
		Enabled: true,
		// A blackholed endpoint: construction is lazy, so setup succeeds and
		// export failures surface later through the batch processor only.
		Endpoint: "127.0.0.1:1", Insecure: true, Ratio: 1, Timeout: 500 * time.Millisecond,
	}, slog.Default())
	if err != nil {
		t.Fatalf("enabled setup with unreachable collector must not fail: %v", err)
	}
	if shutdown == nil {
		t.Fatal("enabled setup must return a shutdown func")
	}
	_, span := otel.Tracer("test").Start(context.Background(), "probe")
	if !span.IsRecording() {
		t.Fatal("enabled setup must install a recording provider")
	}
	span.End()

	// Bounded shutdown: must return promptly even with nothing exported.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		// Flushing to a dead endpoint may error; the contract is that it
		// returns (bounded), which it did — record but do not fail.
		t.Logf("shutdown flush reported: %v", err)
	}
}
