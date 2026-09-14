// Package gateway orchestrates provider routing, timeouts, and retry policy.
// Retries are finite, deadline-bounded, and only eligible before any output.
// Primary/backup failover uses the router route table; streaming never
// switches providers once output has reached the client.
package gateway

import (
	"context"
	"errors"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
	"github.com/knowledge-base/knowledge-base-gateway/internal/router"
)

// ErrUnknownModel is returned when the public model is not in the catalog or
// the subject lacks permission. Callers must not distinguish the two.
var ErrUnknownModel = errors.New("model not available")

// ErrNotPermitted is a distinct internal signal; HTTP layers may map it the
// same as ErrUnknownModel to avoid leaking policy details.
var ErrNotPermitted = errors.New("model not permitted")

// ErrNoRoute is returned when every route for a model is disabled or open.
var ErrNoRoute = errors.New("no provider route available")

// Candidate is one attemptable route for a request.
type Candidate struct {
	Provider      provider.Provider
	ProviderName  string
	UpstreamModel string
	Timeout       time.Duration
}

// Plan is the ordered set of candidates for one request.
type Plan struct {
	PublicModel string
	Candidates  []Candidate
}

// Primary returns the provider name of the first candidate.
func (p Plan) Primary() string {
	if len(p.Candidates) == 0 {
		return ""
	}
	return p.Candidates[0].ProviderName
}

// Service routes normalized chat requests across route candidates with
// per-route timeouts, bounded retries, failover, and a total deadline.
type Service struct {
	Catalog    *policy.Catalog
	Routes     *router.Routes
	Timeout    time.Duration // total deadline for the whole request
	MaxRetries int           // additional attempts on the primary candidate
	RetryWait  time.Duration
	Now        func() time.Time
}

// New builds a Service with defaults. providers maps provider names to
// adapters; each catalog entry becomes a single-candidate route.
func New(catalog *policy.Catalog, providers map[string]provider.Provider, timeout time.Duration, maxRetries int) *Service {
	routes := router.NewRoutes()
	for _, info := range catalog.All() {
		if p, ok := providers[info.Provider]; ok {
			routes.SetRoutes(info.PublicName, []router.Route{{
				ProviderName: info.Provider, Provider: p,
				UpstreamModel: info.UpstreamModel, Priority: 10, Enabled: true,
				Breaker: router.NewBreaker(5, 30*time.Second),
			}})
		}
	}
	return &Service{
		Catalog: catalog, Routes: routes, Timeout: timeout,
		MaxRetries: maxRetries, RetryWait: 100 * time.Millisecond, Now: time.Now,
	}
}

// Resolve checks catalog existence, returning the ordered attempt plan.
// Errors are non-leaky: callers cannot tell whether a model is missing or
// forbidden.
func (s *Service) Resolve(_, publicModel string) (Plan, error) {
	if _, ok := s.Catalog.Lookup(publicModel); !ok {
		return Plan{}, ErrUnknownModel
	}
	now := s.now()
	var cands []Candidate
	for _, rt := range s.Routes.Available(publicModel, now) {
		cands = append(cands, Candidate{
			Provider: rt.Provider, ProviderName: rt.ProviderName,
			UpstreamModel: rt.UpstreamModel, Timeout: rt.Timeout,
		})
	}
	if len(cands) == 0 {
		return Plan{}, ErrNoRoute
	}
	return Plan{PublicModel: publicModel, Candidates: cands}, nil
}

func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// withDeadline applies the configured total deadline on top of the caller's
// context so a slow upstream can never outlive the configured limit.
func (s *Service) withDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if s.Timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, s.Timeout)
}

// attemptTimeout clamps a candidate's own timeout by the remaining total.
func attemptTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, d)
}

// Complete performs a non-streaming completion. Attempts proceed through the
// candidate order; only pre-output network/429/5xx/timeout failures advance
// to the next candidate or retry, always within the total deadline. The
// second return value names the provider that served (or last attempted) the
// request for audit purposes.
func (s *Service) Complete(ctx context.Context, plan Plan, req provider.ChatRequest) (provider.ChatResponse, string, error) {
	ctx, cancel := s.withDeadline(ctx)
	defer cancel()
	var lastErr error
	attempts := 0
	for i, cand := range plan.Candidates {
		creq := req
		creq.Model = cand.UpstreamModel
		maxTries := 1
		if i == 0 {
			maxTries = 1 + s.MaxRetries // bounded retries on the primary only
		}
		for try := 0; try < maxTries; try++ {
			attempts++
			if attempts > 1 && s.RetryWait > 0 {
				select {
				case <-time.After(s.RetryWait):
				case <-ctx.Done():
					return provider.ChatResponse{}, cand.ProviderName, ctx.Err()
				}
			}
			actx, acancel := attemptTimeout(ctx, cand.Timeout)
			resp, err := cand.Provider.Complete(actx, creq)
			acancel()
			if err == nil {
				s.Routes.Record(plan.PublicModel, cand.ProviderName, s.now(), true)
				return resp, cand.ProviderName, nil
			}
			s.Routes.Record(plan.PublicModel, cand.ProviderName, s.now(), false)
			lastErr = err
			if ctx.Err() != nil {
				return provider.ChatResponse{}, cand.ProviderName, ctx.Err()
			}
			if !provider.RetryEligible(err) {
				return provider.ChatResponse{}, cand.ProviderName, err
			}
		}
	}
	if lastErr == nil {
		lastErr = ErrNoRoute
	}
	return provider.ChatResponse{}, plan.Primary(), lastErr
}

// Stream performs a streaming completion under the configured total deadline.
// Failover is permitted only while send has never succeeded; once output
// reached the client the error is returned as-is with no retry.
func (s *Service) Stream(ctx context.Context, plan Plan, req provider.ChatRequest, send func(payload []byte) error) (string, error) {
	ctx, cancel := s.withDeadline(ctx)
	defer cancel()
	outputStarted := false
	wrapped := func(payload []byte) error {
		if err := send(payload); err != nil {
			return err
		}
		outputStarted = true
		return nil
	}
	var lastErr error
	attempts := 0
	for _, cand := range plan.Candidates {
		creq := req
		creq.Model = cand.UpstreamModel
		creq.Stream = true
		if attempts > 0 && s.RetryWait > 0 {
			select {
			case <-time.After(s.RetryWait):
			case <-ctx.Done():
				return cand.ProviderName, ctx.Err()
			}
		}
		attempts++
		actx, acancel := attemptTimeout(ctx, cand.Timeout)
		err := cand.Provider.Stream(actx, creq, wrapped)
		acancel()
		if err == nil {
			s.Routes.Record(plan.PublicModel, cand.ProviderName, s.now(), true)
			return cand.ProviderName, nil
		}
		s.Routes.Record(plan.PublicModel, cand.ProviderName, s.now(), false)
		lastErr = err
		if outputStarted || ctx.Err() != nil || !provider.RetryEligible(err) {
			return cand.ProviderName, err
		}
	}
	if lastErr == nil {
		lastErr = ErrNoRoute
	}
	return plan.Primary(), lastErr
}
