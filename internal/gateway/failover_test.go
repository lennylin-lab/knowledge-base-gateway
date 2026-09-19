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

// scriptedProvider fails according to a script before succeeding.
type scriptedProvider struct {
	name   string
	errs   []error // returned in order; nil entries succeed
	calls  int
	stream func(s *scriptedProvider, emit func(model.Event) error) error
}

func (p *scriptedProvider) Name() string { return p.name }

func (p *scriptedProvider) Capabilities(string) model.Capabilities {
	return model.Capabilities{Chat: true, Responses: true, Stream: true, Tools: true, Usage: true}
}

func (p *scriptedProvider) Embeddings(context.Context, model.EmbeddingsRequest) (model.EmbeddingsResponse, error) {
	return model.EmbeddingsResponse{}, errors.New("embeddings not implemented by this test stub")
}

func (p *scriptedProvider) Complete(_ context.Context, _ model.Request) (model.Response, error) {
	i := p.calls
	p.calls++
	if i < len(p.errs) && p.errs[i] != nil {
		return model.Response{}, p.errs[i]
	}
	return model.Response{ID: "ok-" + p.name, Status: model.StatusCompleted}, nil
}

func (p *scriptedProvider) Stream(ctx context.Context, _ model.Request, emit func(model.Event) error) error {
	if p.stream != nil {
		return p.stream(p, emit)
	}
	i := p.calls
	p.calls++
	if i < len(p.errs) && p.errs[i] != nil {
		return p.errs[i]
	}
	return emit(model.Event{Kind: model.EventTextDelta, Delta: "ok"})
}

func failoverService(t *testing.T, primary, backup provider.Provider) *Service {
	t.Helper()
	catalog := policy.NewCatalog([]policy.ModelInfo{{PublicName: "m", Provider: "primary", UpstreamModel: "up-p", Enabled: true}})
	svc := New(catalog, map[string]provider.Provider{"primary": primary}, time.Second, 0)
	svc.RetryWait = 0
	svc.Routes.SetRoutes("m", []router.Route{
		{ProviderName: "primary", Provider: primary, UpstreamModel: "up-p", Priority: 10, Enabled: true, Breaker: router.NewBreaker(5, time.Minute)},
		{ProviderName: "backup", Provider: backup, UpstreamModel: "up-b", Priority: 20, Enabled: true, Breaker: router.NewBreaker(5, time.Minute)},
	})
	return svc
}

func planFor(t *testing.T, svc *Service) Plan {
	t.Helper()
	plan, err := svc.Resolve("s", "m")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	return plan
}

func TestCompleteFailsOverOnTimeout(t *testing.T) {
	primary := &scriptedProvider{name: "primary", errs: []error{&provider.Error{Class: provider.ClassTimeout, Msg: "slow"}}}
	backup := &scriptedProvider{name: "backup"}
	svc := failoverService(t, primary, backup)
	resp, name, err := svc.Complete(context.Background(), planFor(t, svc), model.Request{})
	if err != nil || resp.ID != "ok-backup" {
		t.Fatalf("want backup success, got resp=%v err=%v", resp, err)
	}
	if name != "backup" {
		t.Fatalf("want serving provider backup, got %s", name)
	}
}

func TestCompleteFailsOverOn429And5xx(t *testing.T) {
	for _, class := range []provider.ErrClass{provider.ClassRateLimited, provider.ClassServer, provider.ClassNetwork} {
		primary := &scriptedProvider{name: "primary", errs: []error{&provider.Error{Class: class, Msg: "x"}}}
		backup := &scriptedProvider{name: "backup"}
		svc := failoverService(t, primary, backup)
		if _, name, err := svc.Complete(context.Background(), planFor(t, svc), model.Request{}); err != nil || name != "backup" {
			t.Fatalf("class %v: want backup, got %s err=%v", class, name, err)
		}
	}
}

func TestCompleteDoesNotRetryInvalid(t *testing.T) {
	primary := &scriptedProvider{name: "primary", errs: []error{&provider.Error{Class: provider.ClassInvalid, Msg: "bad"}}}
	backup := &scriptedProvider{name: "backup"}
	svc := failoverService(t, primary, backup)
	_, name, err := svc.Complete(context.Background(), planFor(t, svc), model.Request{})
	if err == nil {
		t.Fatal("want error")
	}
	if name != "primary" {
		t.Fatalf("non-retryable must not fail over, served by %s", name)
	}
	if backup.calls != 0 {
		t.Fatal("backup must not be called for non-retryable errors")
	}
}

func TestStreamNoSwitchAfterOutput(t *testing.T) {
	primary := &scriptedProvider{name: "primary", stream: func(p *scriptedProvider, emit func(model.Event) error) error {
		if err := emit(model.Event{Kind: model.EventTextDelta, Delta: "1"}); err != nil {
			return err
		}
		return &provider.Error{Class: provider.ClassNetwork, Msg: "stream broke"}
	}}
	backup := &scriptedProvider{name: "backup"}
	svc := failoverService(t, primary, backup)
	_, err := svc.Stream(context.Background(), planFor(t, svc), model.Request{}, func(model.Event) error { return nil })
	if err == nil {
		t.Fatal("want stream error")
	}
	if backup.calls != 0 {
		t.Fatal("streaming must not switch providers after output began")
	}
}

func TestStreamFailsOverBeforeOutput(t *testing.T) {
	primary := &scriptedProvider{name: "primary", errs: []error{&provider.Error{Class: provider.ClassNetwork, Msg: "connect failed"}}}
	backup := &scriptedProvider{name: "backup"}
	svc := failoverService(t, primary, backup)
	var got model.Event
	name, err := svc.Stream(context.Background(), planFor(t, svc), model.Request{}, func(e model.Event) error { got = e; return nil })
	if err != nil || got.Kind == "" {
		t.Fatalf("want backup stream, got err=%v event=%v", err, got)
	}
	if name != "backup" {
		t.Fatalf("want backup, got %s", name)
	}
}

// stallErr mimics the provider adapters' frame-gap watchdog classification.
var stallErr = &provider.Error{Class: provider.ClassTimeout, Msg: "upstream stalled: no frame within the stall window"}

func TestStreamStallRetriesPrimaryUpToBudget(t *testing.T) {
	primary := &scriptedProvider{name: "primary", errs: []error{stallErr, stallErr, stallErr}}
	backup := &scriptedProvider{name: "backup"}
	svc := failoverService(t, primary, backup)
	svc.StreamMaxRetries = 3
	if _, err := svc.Stream(context.Background(), planFor(t, svc), model.Request{}, func(model.Event) error { return nil }); err != nil {
		t.Fatalf("want success once the stall clears, got %v", err)
	}
	if primary.calls != 4 {
		t.Fatalf("primary attempts = %d, want 4 (1 + StreamMaxRetries)", primary.calls)
	}
	if backup.calls != 0 {
		t.Fatal("stall retries must stay on the primary until the budget is spent")
	}
}

func TestStreamStallExhaustsBudgetThenTerminates(t *testing.T) {
	alwaysStall := func(p *scriptedProvider, _ func(model.Event) error) error {
		p.calls++
		return stallErr
	}
	primary := &scriptedProvider{name: "primary", stream: alwaysStall}
	backup := &scriptedProvider{name: "backup", stream: alwaysStall}
	svc := failoverService(t, primary, backup)
	svc.StreamMaxRetries = 3
	_, err := svc.Stream(context.Background(), planFor(t, svc), model.Request{}, func(model.Event) error { return nil })
	if err == nil {
		t.Fatal("want termination after the retry budget is spent")
	}
	if primary.calls != 4 {
		t.Fatalf("primary attempts = %d, want 4 (1 + StreamMaxRetries)", primary.calls)
	}
	if backup.calls != 1 {
		t.Fatalf("backup attempts = %d, want 1 (failover, no retry budget)", backup.calls)
	}
}

func TestStreamNoStallRetryAfterOutput(t *testing.T) {
	primary := &scriptedProvider{name: "primary", stream: func(p *scriptedProvider, emit func(model.Event) error) error {
		p.calls++
		if err := emit(model.Event{Kind: model.EventTextDelta, Delta: "1"}); err != nil {
			return err
		}
		return stallErr
	}}
	backup := &scriptedProvider{name: "backup"}
	svc := failoverService(t, primary, backup)
	svc.StreamMaxRetries = 5
	_, err := svc.Stream(context.Background(), planFor(t, svc), model.Request{}, func(model.Event) error { return nil })
	if err == nil {
		t.Fatal("want the post-output stall surfaced as-is")
	}
	if primary.calls != 1 {
		t.Fatalf("primary attempts = %d, want 1: retrying after output would duplicate content", primary.calls)
	}
	if backup.calls != 0 {
		t.Fatal("backup must not be attempted after output began")
	}
}

func TestFailoverHonorsTotalDeadline(t *testing.T) {
	block := func(ctx context.Context, _ model.Request) (model.Response, error) {
		<-ctx.Done()
		return model.Response{}, ctx.Err()
	}
	slowPrimary := &deadlineProvider{name: "primary", fn: block}
	backup := &scriptedProvider{name: "backup"}
	catalog := policy.NewCatalog([]policy.ModelInfo{{PublicName: "m", Provider: "primary", UpstreamModel: "up", Enabled: true}})
	svc := New(catalog, map[string]provider.Provider{"primary": slowPrimary}, 80*time.Millisecond, 0)
	svc.RetryWait = 0
	svc.Routes.SetRoutes("m", []router.Route{
		{ProviderName: "primary", Provider: slowPrimary, UpstreamModel: "up", Priority: 10, Enabled: true,
			Breaker: router.NewBreaker(5, time.Minute), Timeout: 5 * time.Second},
		{ProviderName: "backup", Provider: backup, UpstreamModel: "up", Priority: 20, Enabled: true,
			Breaker: router.NewBreaker(5, time.Minute), Timeout: 5 * time.Second},
	})
	start := time.Now()
	_, _, err := svc.Complete(context.Background(), planFor(t, svc), model.Request{})
	if err == nil {
		t.Fatal("want deadline error despite backup (total deadline exceeded)")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("total deadline not honored: %v", elapsed)
	}
	_ = errors.Is
}

type deadlineProvider struct {
	name string
	fn   func(ctx context.Context, req model.Request) (model.Response, error)
}

func (p *deadlineProvider) Name() string { return p.name }
func (p *deadlineProvider) Capabilities(string) model.Capabilities {
	return model.Capabilities{Chat: true, Responses: true, Stream: true}
}
func (p *deadlineProvider) Embeddings(context.Context, model.EmbeddingsRequest) (model.EmbeddingsResponse, error) {
	return model.EmbeddingsResponse{}, errors.New("embeddings not implemented by this test stub")
}

func (p *deadlineProvider) Complete(ctx context.Context, req model.Request) (model.Response, error) {
	return p.fn(ctx, req)
}
func (p *deadlineProvider) Stream(ctx context.Context, _ model.Request, _ func(model.Event) error) error {
	<-ctx.Done()
	return ctx.Err()
}
