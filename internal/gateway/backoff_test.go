package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
	"github.com/knowledge-base/knowledge-base-gateway/internal/router"
)

// TestNewBackoffBoundedGrowth pins the retry delay contract: delays start at
// RetryWait, grow by the multiplier, stay inside the ±jitter window, and are
// capped at retryBackoffCap * RetryWait.
func TestNewBackoffBoundedGrowth(t *testing.T) {
	svc := &Service{RetryWait: 20 * time.Millisecond}
	bo := svc.newBackoff()
	if bo == nil {
		t.Fatal("backoff must be created when RetryWait > 0")
	}

	interval := svc.RetryWait
	capInterval := time.Duration(retryBackoffCap * float64(svc.RetryWait))
	for i := 0; i < 20; i++ {
		d := bo.NextBackOff()
		lo := time.Duration(retryBackoffJitter * float64(interval))
		hi := time.Duration((1 + retryBackoffJitter) * float64(interval))
		if d < lo || d > hi {
			t.Fatalf("delay %d = %v outside jitter window [%v, %v]", i, d, lo, hi)
		}
		if float64(interval) >= float64(capInterval)/retryBackoffGrowth {
			interval = capInterval
		} else {
			interval = time.Duration(float64(interval) * retryBackoffGrowth)
		}
	}
	if interval != capInterval {
		t.Fatalf("interval %v must be capped at %v", interval, capInterval)
	}
}

func TestNewBackoffDisabledWhenRetryWaitZero(t *testing.T) {
	svc := &Service{RetryWait: 0}
	if bo := svc.newBackoff(); bo != nil {
		t.Fatal("RetryWait = 0 must disable backoff waits")
	}
}

// TestCompleteRetriesAreBoundedAndDelayed pins that MaxRetries bounds the
// attempts on the primary and that backoff delays actually elapse between
// them (sum of jitter lower bounds).
func TestCompleteRetriesAreBoundedAndDelayed(t *testing.T) {
	primary := &scriptedProvider{name: "primary", errs: []error{
		retryableErr(), retryableErr(), retryableErr(), retryableErr(),
	}}
	backup := &scriptedProvider{name: "backup"}
	catalog := policy.NewCatalog([]policy.ModelInfo{{PublicName: "m", Provider: "primary", UpstreamModel: "up", Enabled: true}})
	svc := New(catalog, map[string]provider.Provider{"primary": primary}, time.Second, 3)
	svc.RetryWait = 5 * time.Millisecond
	svc.Routes.SetRoutes("m", []router.Route{{
		ProviderName: "primary", Provider: primary, UpstreamModel: "up",
		Priority: 10, Enabled: true, Breaker: router.NewBreaker(5, time.Minute),
	}})

	start := time.Now()
	_, name, err := svc.Complete(context.Background(), planFor(t, svc), model.Request{})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("want exhaustion error")
	}
	if name != "primary" {
		t.Fatalf("backup must not be attempted when retries are bounded to primary, got %s", name)
	}
	if primary.calls != 4 || backup.calls != 0 {
		t.Fatalf("want 4 primary attempts (1+MaxRetries) and 0 backup, got %d and %d", primary.calls, backup.calls)
	}
	// Three waits: >= 5ms*0.5 + 7.5ms*0.5 + 11.25ms*0.5 ≈ 11.9ms.
	if elapsed < 10*time.Millisecond {
		t.Fatalf("backoff delays did not elapse between attempts: %v", elapsed)
	}
}

// TestCompleteWaitHonorsTotalDeadline pins that a long backoff wait is cut
// short by the request deadline instead of extending it.
func TestCompleteWaitHonorsTotalDeadline(t *testing.T) {
	primary := &scriptedProvider{name: "primary", errs: []error{retryableErr(), retryableErr()}}
	catalog := policy.NewCatalog([]policy.ModelInfo{{PublicName: "m", Provider: "primary", UpstreamModel: "up", Enabled: true}})
	svc := New(catalog, map[string]provider.Provider{"primary": primary}, 40*time.Millisecond, 5)
	svc.RetryWait = 5 * time.Second // far beyond the total deadline
	start := time.Now()
	_, _, err := svc.Complete(context.Background(), planFor(t, svc), model.Request{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline error from interrupted wait, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("wait was not bounded by the total deadline: %v", elapsed)
	}
}

// TestStreamDelaysFailoverWithBackoff covers the wait path on the streaming
// failover loop: a pre-output failure waits before switching to the backup.
func TestStreamDelaysFailoverWithBackoff(t *testing.T) {
	primary := &scriptedProvider{name: "primary", errs: []error{retryableErr()}}
	backup := &scriptedProvider{name: "backup"}
	svc := failoverService(t, primary, backup)
	svc.RetryWait = 10 * time.Millisecond
	var got model.Event
	name, err := svc.Stream(context.Background(), planFor(t, svc), model.Request{}, func(e model.Event) error { got = e; return nil })
	if err != nil || got.Kind == "" {
		t.Fatalf("want backup stream after backoff wait, got err=%v event=%v", err, got)
	}
	if name != "backup" || primary.calls != 1 {
		t.Fatalf("want exactly one primary attempt then backup, got primary=%d name=%s", primary.calls, name)
	}
}

func retryableErr() error {
	return &provider.Error{Class: provider.ClassNetwork, Msg: "flaky"}
}
