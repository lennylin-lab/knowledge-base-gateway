// Package store declares persistence interfaces for PostgreSQL/Redis
// implementations. The MVP runs with in-memory substitutes in development;
// production implementations must use parameterized queries, explicit
// contexts, and versioned forward migrations under migrations/.
package store

import (
	"context"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/audit"
	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
)

// KeyStore persists API key metadata (hash, salt, subject, status, expiry).
type KeyStore interface {
	// GetByHash resolves a key hash to its metadata record.
	GetByHash(ctx context.Context, hash []byte) (auth.KeyRecord, error)
	// TouchLastUsed updates last_used_at for an authenticated key.
	TouchLastUsed(ctx context.Context, keyID string, now time.Time) error
}

// CatalogStore loads the model catalog.
type CatalogStore interface {
	ListModels(ctx context.Context) ([]policy.ModelInfo, error)
}

// LimiterState is the Redis-backed rate/concurrency state interface. It must
// be used for multi-instance deployments; the in-memory limiter is
// development-only.
type LimiterState interface {
	// Allow consumes one rate/concurrency slot atomically.
	Allow(ctx context.Context, subject string, ratePerMinute, maxConcurrent int, now time.Time) (bool, func(), error)
}

// AuditSink persists audit metadata into llm_requests.
type AuditSink interface {
	Write(e audit.Event)
}

// Readiness reports component health for /readyz.
type Readiness interface {
	Ready(ctx context.Context) error
}
