package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
)

type slowProvider struct{ delay time.Duration }

func (slowProvider) Name() string { return "slow" }

func (s slowProvider) Capabilities(string) model.Capabilities {
	return model.Capabilities{Chat: true, Responses: true, Stream: true}
}

func (s slowProvider) Complete(ctx context.Context, _ model.Request) (model.Response, error) {
	select {
	case <-ctx.Done():
		return model.Response{}, ctx.Err()
	case <-time.After(s.delay):
		return model.Response{ID: "ok", Status: model.StatusCompleted}, nil
	}
}

func (s slowProvider) Stream(ctx context.Context, _ model.Request, emit func(model.Event) error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(s.delay):
		return emit(model.Event{Kind: model.EventTextDelta, Delta: "{}"})
	}
}

func newTestService(delay time.Duration, timeout time.Duration) *Service {
	catalog := policy.NewCatalog([]policy.ModelInfo{{PublicName: "m", Provider: "slow", UpstreamModel: "up", Enabled: true}})
	return New(catalog, map[string]provider.Provider{"slow": slowProvider{delay: delay}}, timeout, 0)
}

func TestCompleteEnforcesTotalDeadline(t *testing.T) {
	svc := newTestService(500*time.Millisecond, 50*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	plan, rerr := svc.Resolve("s", "m")
	if rerr != nil {
		t.Fatalf("resolve: %v", rerr)
	}
	if _, _, err := svc.Complete(ctx, plan, model.Request{}); err == nil {
		t.Fatal("expected deadline error")
	}
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Fatalf("complete was not bounded by configured timeout: %v", elapsed)
	}
}

func TestStreamEnforcesTotalDeadline(t *testing.T) {
	svc := newTestService(500*time.Millisecond, 50*time.Millisecond)
	plan, rerr := svc.Resolve("s", "m")
	if rerr != nil {
		t.Fatalf("resolve: %v", rerr)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := svc.Stream(ctx, plan, model.Request{}, func(model.Event) error { return nil }); err == nil {
		t.Fatal("expected deadline error")
	}
}

func TestResolveUnknownModelNonLeaky(t *testing.T) {
	svc := newTestService(time.Millisecond, time.Second)
	if _, err := svc.Resolve("subject-a", "nope"); err != ErrUnknownModel {
		t.Fatalf("want ErrUnknownModel, got %v", err)
	}
}
