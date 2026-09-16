// Package provider defines the provider boundary. No SDK types cross it:
// adapters translate the internal domain protocol (internal/model) into
// vendor-specific HTTP protocols, and normalize vendor errors and events
// back into domain types.
package provider

import (
	"context"
	"errors"
	"fmt"

	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
)

// ErrClass classifies provider failures for error mapping and retry decisions.
type ErrClass int

const (
	ClassNetwork ErrClass = iota // transient, retry eligible before output
	ClassRateLimited
	ClassServer // upstream 5xx
	ClassTimeout
	ClassInvalid // upstream rejected the request (bad model etc.)
	ClassInternal
)

// Error is a normalized provider error. It never carries provider secrets.
type Error struct {
	Class ErrClass
	Msg   string
}

func (e *Error) Error() string { return fmt.Sprintf("provider %s: %s", e.Class, e.Msg) }

// String names the error class.
func (c ErrClass) String() string {
	switch c {
	case ClassNetwork:
		return "network"
	case ClassRateLimited:
		return "rate_limited"
	case ClassServer:
		return "server"
	case ClassTimeout:
		return "timeout"
	case ClassInvalid:
		return "invalid"
	default:
		return "internal"
	}
}

// ClassOf extracts the ErrClass from an error, defaulting to ClassInternal.
func ClassOf(err error) ErrClass {
	var pe *Error
	if errors.As(err, &pe) {
		return pe.Class
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return ClassTimeout
	}
	return ClassInternal
}

// RetryEligible reports whether a pre-output failure may be retried.
func RetryEligible(err error) bool {
	switch ClassOf(err) {
	case ClassNetwork, ClassRateLimited, ClassServer, ClassTimeout:
		return true
	}
	return false
}

// Provider abstracts one upstream vendor under the unified domain protocol.
// Complete and Stream consume normalized domain requests; Stream emits domain
// events (created, deltas, done, completed/failed) through emit. Adapters
// must honor context cancellation and never surface vendor request/response
// types through this interface.
type Provider interface {
	// Name is the adapter identifier used in audit records and routing.
	Name() string
	// Capabilities reports what the adapter can do for an upstream model.
	// Catalog declarations may only narrow this matrix.
	Capabilities(upstreamModel string) model.Capabilities
	// Complete performs a non-streaming completion.
	Complete(ctx context.Context, req model.Request) (model.Response, error)
	// Stream performs a streaming completion. If emit returns an error the
	// adapter stops and reports it as a downstream failure; callers never
	// retry after the first successful emit.
	Stream(ctx context.Context, req model.Request, emit func(model.Event) error) error
	// Embeddings performs a non-streaming embeddings call. Vectors are
	// returned exactly as the upstream produced them; the HTTP layer checks
	// their width against the catalog-declared embedding_dim. Adapters whose
	// upstream has no embeddings API return an error wrapping
	// model.ErrCapabilityNotSupported (the capability matrix declares
	// embeddings false for them by default).
	Embeddings(ctx context.Context, req model.EmbeddingsRequest) (model.EmbeddingsResponse, error)
}
