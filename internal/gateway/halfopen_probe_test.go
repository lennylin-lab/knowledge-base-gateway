package gateway

// Half-open probe tests (issue #1 AC4 recovery guarantee): breaker permits
// are taken per attempt, so a request that never reaches an admitted backup
// cannot orphan the backup's half-open probe, and a failed-over pair converges
// through normal probe recovery instead of wedging.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
	"github.com/knowledge-base/knowledge-base-gateway/internal/router"
)

// halfOpenService rebuilds the failover fixture with fast breakers
// (threshold 1, 25 ms cool-down) so tests can drive open → half-open
// transitions quickly.
func halfOpenService(t *testing.T, primary, backup provider.Provider) *Service {
	t.Helper()
	svc := failoverService(t, primary, backup)
	svc.Routes.SetRoutes("m", []router.Route{
		{ProviderName: "primary", Provider: primary, UpstreamModel: "up-p", Priority: 10, Enabled: true, Breaker: router.NewBreaker(1, 25*time.Millisecond)},
		{ProviderName: "backup", Provider: backup, UpstreamModel: "up-b", Priority: 20, Enabled: true, Breaker: router.NewBreaker(1, 25*time.Millisecond)},
	})
	return svc
}

// TestCompletePrimarySuccessDoesNotOrphanBackupProbe pins the per-attempt
// admission contract: a primary-success request must leave the backup's
// half-open probe available, and a subsequent failover must converge on both
// breakers through normal probe recovery.
func TestCompletePrimarySuccessDoesNotOrphanBackupProbe(t *testing.T) {
	primary := &scriptedProvider{name: "primary"}
	backup := &scriptedProvider{name: "backup"}
	svc := halfOpenService(t, primary, backup)

	// Trip both breakers, then let the cool-down elapse.
	svc.Routes.Record("m", "primary", false)
	svc.Routes.Record("m", "backup", false)
	time.Sleep(60 * time.Millisecond)

	// The primary serves; the backup must not be attempted or admitted.
	resp, name, err := svc.Complete(context.Background(), planFor(t, svc), model.Request{})
	if err != nil || resp.ID != "ok-primary" || name != "primary" {
		t.Fatalf("want primary success, got name=%s resp=%v err=%v", name, resp, err)
	}
	if backup.calls != 0 {
		t.Fatalf("backup must not be attempted, calls=%d", backup.calls)
	}

	// The backup's half-open permit must still be available for a real probe.
	if !svc.Routes.AdmitRoute("m", "backup") {
		t.Fatal("backup half-open probe was orphaned by the primary-success request")
	}
	svc.Routes.Record("m", "backup", true) // complete the probe; backup closes

	// Failover: the primary fails retryably on its second call (call 0 was
	// the phase-2 success), the backup serves, and the attempt counts stay
	// bounded on both sides.
	primary.errs = []error{nil, &provider.Error{Class: provider.ClassNetwork, Msg: "down"}}
	_, name, err = svc.Complete(context.Background(), planFor(t, svc), model.Request{})
	if err != nil || name != "backup" {
		t.Fatalf("want failover to backup, got name=%s err=%v", name, err)
	}
	if primary.calls != 2 || backup.calls != 1 {
		t.Fatalf("attempt counts: primary=%d backup=%d", primary.calls, backup.calls)
	}

	// The opened primary recovers through a normal half-open probe instead of
	// wedging: after the cool-down the next request is served by the primary.
	time.Sleep(60 * time.Millisecond)
	_, name, err = svc.Complete(context.Background(), planFor(t, svc), model.Request{})
	if err != nil || name != "primary" {
		t.Fatalf("primary must recover via half-open probe, got name=%s err=%v", name, err)
	}
}

// TestStreamPrimarySuccessDoesNotOrphanBackupProbe pins the same per-attempt
// admission contract for the streaming loop.
func TestStreamPrimarySuccessDoesNotOrphanBackupProbe(t *testing.T) {
	primary := &scriptedProvider{name: "primary"}
	backup := &scriptedProvider{name: "backup"}
	svc := halfOpenService(t, primary, backup)
	svc.Routes.Record("m", "primary", false)
	svc.Routes.Record("m", "backup", false)
	time.Sleep(60 * time.Millisecond)

	name, err := svc.Stream(context.Background(), planFor(t, svc), model.Request{}, func(model.Event) error { return nil })
	if err != nil || name != "primary" {
		t.Fatalf("want primary stream, got name=%s err=%v", name, err)
	}
	if backup.calls != 0 {
		t.Fatalf("backup must not be attempted, calls=%d", backup.calls)
	}
	if !svc.Routes.AdmitRoute("m", "backup") {
		t.Fatal("backup half-open probe was orphaned by the primary-success stream")
	}
}

// TestCompleteAllRoutesOpenReturnsNoRoute pins AC4 at the execution stage:
// with every breaker open and no cool-down elapsed, no provider is attempted
// and the request fails with ErrNoRoute.
func TestCompleteAllRoutesOpenReturnsNoRoute(t *testing.T) {
	primary := &scriptedProvider{name: "primary"}
	backup := &scriptedProvider{name: "backup"}
	svc := halfOpenService(t, primary, backup)
	svc.Routes.Record("m", "primary", false)
	svc.Routes.Record("m", "backup", false)

	if _, _, err := svc.Complete(context.Background(), planFor(t, svc), model.Request{}); !errors.Is(err, ErrNoRoute) {
		t.Fatalf("want ErrNoRoute, got %v", err)
	}
	if primary.calls != 0 || backup.calls != 0 {
		t.Fatalf("no provider may be attempted while all breakers are open: primary=%d backup=%d", primary.calls, backup.calls)
	}
}
