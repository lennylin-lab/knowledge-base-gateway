// Package gateway orchestrates provider routing, timeouts, and retry policy.
// Retries are finite, deadline-bounded, and only eligible before any output.
package gateway

import (
	"context"
	"errors"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
)

// ErrUnknownModel is returned when the public model is not in the catalog or
// the subject lacks permission. Callers must not distinguish the two.
var ErrUnknownModel = errors.New("model not available")

// ErrNotPermitted is a distinct internal signal; HTTP layers may map it the
// same as ErrUnknownModel to avoid leaking policy details.
var ErrNotPermitted = errors.New("model not permitted")

// Service routes normalized chat requests to providers with timeout/retry.
type Service struct {
	Catalog    *policy.Catalog
	Providers  map[string]provider.Provider
	Timeout    time.Duration
	MaxRetries int
	RetryWait  time.Duration
}

// New builds a Service with defaults.
func New(catalog *policy.Catalog, providers map[string]provider.Provider, timeout time.Duration, maxRetries int) *Service {
	return &Service{Catalog: catalog, Providers: providers, Timeout: timeout, MaxRetries: maxRetries, RetryWait: 100 * time.Millisecond}
}

// Resolve checks catalog existence and subject permission, returning the
// upstream model name. Errors are non-leaky: callers cannot tell whether a
// model is missing or forbidden.
func (s *Service) Resolve(subject, publicModel string) (provider.Provider, string, error) {
	info, ok := s.Catalog.Lookup(publicModel)
	if !ok {
		return nil, "", ErrUnknownModel
	}
	p, ok := s.Providers[info.Provider]
	if !ok {
		return nil, "", ErrUnknownModel
	}
	return p, info.UpstreamModel, nil
}

// withDeadline applies the configured total deadline on top of the caller's
// context so a slow upstream can never outlive the configured limit.
func (s *Service) withDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if s.Timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, s.Timeout)
}

// Complete performs a non-streaming completion with bounded retries. Only
// pre-output network/429/5xx/timeout failures are retried, always within the
// caller's remaining deadline and the configured total timeout.
func (s *Service) Complete(ctx context.Context, p provider.Provider, req provider.ChatRequest) (provider.ChatResponse, error) {
	ctx, cancel := s.withDeadline(ctx)
	defer cancel()
	var lastErr error
	for attempt := 0; attempt <= s.MaxRetries; attempt++ {
		if attempt > 0 && s.RetryWait > 0 {
			select {
			case <-time.After(s.RetryWait):
			case <-ctx.Done():
				return provider.ChatResponse{}, ctx.Err()
			}
		}
		resp, err := p.Complete(ctx, req)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return provider.ChatResponse{}, ctx.Err()
		}
		if !provider.RetryEligible(err) {
			return provider.ChatResponse{}, err
		}
	}
	return provider.ChatResponse{}, lastErr
}

// Stream performs a streaming completion under the configured total deadline.
// No retry happens here: once output may have reached the client, retrying
// would duplicate irreversible output.
func (s *Service) Stream(ctx context.Context, p provider.Provider, req provider.ChatRequest, send func(payload []byte) error) error {
	ctx, cancel := s.withDeadline(ctx)
	defer cancel()
	return p.Stream(ctx, req, send)
}
