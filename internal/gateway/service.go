// Package gateway orchestrates provider routing, timeouts, and retry policy.
// Retries are finite, deadline-bounded, and only eligible before any output.
// Primary/backup failover uses the router route table; streaming never
// switches providers once output has reached the client.
package gateway

import (
	"context"
	"errors"
	"time"

	"github.com/cenkalti/backoff/v5"

	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
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

// ErrNoRoute is returned when a model has no enabled route, or when no
// route's breaker admits an attempt at execution time.
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

// Service routes normalized domain requests across route candidates with
// per-route timeouts, bounded retries, failover, and a total deadline.
type Service struct {
	Catalog    *policy.Catalog
	Routes     *router.Routes
	Timeout    time.Duration // total deadline for the whole request
	MaxRetries int           // additional attempts on the primary candidate
	RetryWait  time.Duration // initial retry delay; grows exponentially, zero disables waits

	providers map[string]provider.Provider // retained for capability lookups
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
		MaxRetries: maxRetries, RetryWait: 100 * time.Millisecond,
		providers: providers,
	}
}

// Capabilities resolves the effective capability matrix for a public model:
// the catalog declaration when one exists, otherwise the primary provider's
// adapter-level matrix. ok is false for unknown or disabled models and for
// models whose provider is not registered.
func (s *Service) Capabilities(publicModel string) (model.Capabilities, bool) {
	info, ok := s.Catalog.Lookup(publicModel)
	if !ok {
		return model.Capabilities{}, false
	}
	if info.Capabilities.Declared() {
		return info.Capabilities, true
	}
	p, ok := s.providers[info.Provider]
	if !ok {
		return model.Capabilities{}, false
	}
	return p.Capabilities(info.UpstreamModel), true
}

// Resolve checks catalog existence, returning the ordered enabled-candidate
// plan. No breaker permit is taken here: admission happens per attempt in
// Complete/Stream, immediately before each provider call, so a permit can
// never be orphaned by a plan that is not fully attempted or by requests
// rejected between resolution and execution. Errors are non-leaky: callers
// cannot tell whether a model is missing or forbidden.
func (s *Service) Resolve(_, publicModel string) (Plan, error) {
	if _, ok := s.Catalog.Lookup(publicModel); !ok {
		return Plan{}, ErrUnknownModel
	}
	var cands []Candidate
	for _, rt := range s.Routes.EnabledRoutes(publicModel) {
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

// retryBackoffGrowth and retryBackoffJitter keep the mature library defaults
// for exponential growth and randomization; they are declared here so the
// retry timing contract has a single documented home.
const (
	retryBackoffGrowth = 1.5  // multiplier per attempt (cenkalti/backoff default)
	retryBackoffJitter = 0.5  // ±50% full-jitter window (cenkalti/backoff default)
	retryBackoffCap    = 10.0 // MaxInterval = RetryWait * cap, bounding each delay
)

// newBackoff returns the bounded exponential backoff delay generator used
// between attempts. Delays grow from RetryWait by retryBackoffGrowth with
// ±retryBackoffJitter randomization, capped at retryBackoffCap * RetryWait.
// Total retry time is bounded by the request deadline, so the backoff itself
// carries no elapsed-time stop. Returns nil when RetryWait is disabled.
func (s *Service) newBackoff() backoff.BackOff {
	if s.RetryWait <= 0 {
		return nil
	}
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = s.RetryWait
	b.RandomizationFactor = retryBackoffJitter
	b.Multiplier = retryBackoffGrowth
	b.MaxInterval = time.Duration(retryBackoffCap * float64(s.RetryWait))
	b.Reset()
	return b
}

// waitBackoff sleeps for the next backoff interval, returning early with the
// context error when the total request deadline elapses first.
func waitBackoff(ctx context.Context, bo backoff.BackOff) error {
	if bo == nil {
		return nil
	}
	d := bo.NextBackOff()
	if d == backoff.Stop || d <= 0 {
		return nil // nothing to wait for; the attempt loop bounds retries
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
// candidate order; each attempt admits its route's breaker permit
// immediately before the provider call and records the outcome immediately
// after, so half-open probes are never consumed by requests that stop at an
// earlier candidate. Only pre-output network/429/5xx/timeout failures
// advance to the next candidate or retry, always within the total deadline.
// Retry delays follow bounded exponential backoff with jitter, starting at
// RetryWait. The second return value names the provider that served (or last
// attempted) the request for audit purposes.
func (s *Service) Complete(ctx context.Context, plan Plan, req model.Request) (model.Response, string, error) {
	ctx, cancel := s.withDeadline(ctx)
	defer cancel()
	var lastErr error
	bo := s.newBackoff()
	attempts := 0
	for i, cand := range plan.Candidates {
		creq := req
		creq.Model = cand.UpstreamModel
		maxTries := 1
		if i == 0 {
			maxTries = 1 + s.MaxRetries // bounded retries on the primary only
		}
		for try := 0; try < maxTries; try++ {
			if attempts > 0 {
				if err := waitBackoff(ctx, bo); err != nil {
					return model.Response{}, cand.ProviderName, err
				}
			}
			// Admit immediately before the attempt; Record always follows the
			// call below, so an acquired permit is never orphaned.
			if !s.Routes.AdmitRoute(plan.PublicModel, cand.ProviderName) {
				break // breaker refused: fail over to the next candidate
			}
			attempts++
			actx, acancel := attemptTimeout(ctx, cand.Timeout)
			resp, err := cand.Provider.Complete(actx, creq)
			acancel()
			s.Routes.Record(plan.PublicModel, cand.ProviderName, err == nil)
			if err == nil {
				return resp, cand.ProviderName, nil
			}
			lastErr = err
			if ctx.Err() != nil {
				return model.Response{}, cand.ProviderName, ctx.Err()
			}
			if !provider.RetryEligible(err) {
				return model.Response{}, cand.ProviderName, err
			}
		}
	}
	if lastErr == nil {
		lastErr = ErrNoRoute
	}
	return model.Response{}, plan.Primary(), lastErr
}

// Stream performs a streaming completion under the configured total deadline.
// Each attempt admits its route's breaker permit immediately before the
// provider call and records the outcome immediately after, so half-open
// probes are never consumed by requests that stop at an earlier candidate.
// Failover is permitted only while emit has never succeeded; once output
// reached the client the error is returned as-is with no retry.
func (s *Service) Stream(ctx context.Context, plan Plan, req model.Request, emit func(model.Event) error) (string, error) {
	ctx, cancel := s.withDeadline(ctx)
	defer cancel()
	outputStarted := false
	wrapped := func(e model.Event) error {
		if err := emit(e); err != nil {
			return err
		}
		outputStarted = true
		return nil
	}
	var lastErr error
	bo := s.newBackoff()
	attempts := 0
	for _, cand := range plan.Candidates {
		creq := req
		creq.Model = cand.UpstreamModel
		creq.Stream = true
		if attempts > 0 {
			if err := waitBackoff(ctx, bo); err != nil {
				return cand.ProviderName, err
			}
		}
		// Admit immediately before the attempt; Record always follows the
		// call below, so an acquired permit is never orphaned.
		if !s.Routes.AdmitRoute(plan.PublicModel, cand.ProviderName) {
			continue // breaker refused: fail over to the next candidate
		}
		attempts++
		actx, acancel := attemptTimeout(ctx, cand.Timeout)
		err := cand.Provider.Stream(actx, creq, wrapped)
		acancel()
		s.Routes.Record(plan.PublicModel, cand.ProviderName, err == nil)
		if err == nil {
			return cand.ProviderName, nil
		}
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
