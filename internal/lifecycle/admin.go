package lifecycle

// Admin is the operator-facing lifecycle service behind the admin API:
// retention policy views/mutations (each mutation commits with its
// management-audit record at the store boundary), operator-triggered sweep
// runs, recent run views, and exports. The maintenance command and the admin
// surface share the same Sweeper so triggered and scheduled sweeps cannot
// drift. Metadata only everywhere: policies, run records, and export
// records carry query metadata and verification data, never exported
// content or secrets.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/metrics"
	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
)

// AdminDeps wires the admin service. Zero bounds fall back to defaults.
type AdminDeps struct {
	Store      Store
	Export     ExportStore
	Sink       ArchiveSink
	Metrics    *metrics.Registry
	Logger     *slog.Logger
	BatchSize  int
	MaxBatches int
	Now        func() time.Time
}

// Admin serves the lifecycle management surface.
type Admin struct {
	store   Store
	export  ExportStore
	sweep   *Sweeper
	logger  *slog.Logger
	metrics *metrics.Registry
	now     func() time.Time
}

// NewAdmin assembles the admin service.
func NewAdmin(deps AdminDeps) (*Admin, error) {
	if deps.Store == nil || deps.Export == nil {
		return nil, fmt.Errorf("lifecycle: admin requires store and export store")
	}
	if deps.Sink == nil {
		return nil, fmt.Errorf("lifecycle: admin requires an archive sink")
	}
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.DiscardHandler)
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}
	sw, err := NewSweeper(SweeperDeps{
		Store: deps.Store, Sink: deps.Sink, Metrics: deps.Metrics, Logger: deps.Logger,
		BatchSize: deps.BatchSize, MaxBatches: deps.MaxBatches, Now: deps.Now,
	})
	if err != nil {
		return nil, err
	}
	return &Admin{store: deps.Store, export: deps.Export, sweep: sw, logger: deps.Logger,
		metrics: deps.Metrics, now: deps.Now}, nil
}

// Policies lists the configured retention policies.
func (a *Admin) Policies(ctx context.Context) ([]Policy, error) {
	return a.store.RetentionPolicies(ctx)
}

// UpsertPolicy persists one policy change; the store commits the mutation
// and its management-audit record atomically.
func (a *Admin) UpsertPolicy(ctx context.Context, in PolicyInput, op mgmt.AdminOp) error {
	if err := in.Validate(); err != nil {
		return err
	}
	up, ok := a.store.(PolicyWriter)
	if !ok {
		return fmt.Errorf("lifecycle: policy mutations are not supported by this store")
	}
	return up.UpsertRetentionPolicy(ctx, in, op)
}

// PolicyWriter is the optional policy-mutation capability of a Store.
type PolicyWriter interface {
	UpsertRetentionPolicy(ctx context.Context, in PolicyInput, op mgmt.AdminOp) error
}

// Runs lists recent archive runs (platform operational metadata).
func (a *Admin) Runs(ctx context.Context, limit int) ([]RunView, error) {
	runs, ok := a.store.(RunReader)
	if !ok {
		return nil, fmt.Errorf("lifecycle: run history is not supported by this store")
	}
	return runs.RecentRuns(ctx, limit)
}

// Exports lists export records within the caller's tenant boundary.
func (a *Admin) Exports(ctx context.Context, tenant string, limit int) ([]ExportView, error) {
	return a.export.ListExports(ctx, tenant, limit)
}

// RunReader is the optional run-history capability of a Store.
type RunReader interface {
	RecentRuns(ctx context.Context, limit int) ([]RunView, error)
}

// StartRun triggers one bounded sweep cycle in-process and records the
// operator's management-audit op before running (the run itself writes its
// own archive_runs row for observability). Audit failures abort the
// trigger: an unaudited operator action must not run.
func (a *Admin) StartRun(ctx context.Context, opts Options, op mgmt.AdminOp) ([]TableResult, error) {
	if opts.Only != "" {
		if _, err := ParseTable(string(opts.Only)); err != nil {
			return nil, err
		}
	}
	detail, _ := json.Marshal(map[string]any{"dry_run": opts.DryRun, "table": string(opts.Only)})
	op.Action = "retention_run"
	op.Detail = detail
	if err := a.store.WriteOp(ctx, op); err != nil {
		return nil, fmt.Errorf("lifecycle: run audit: %w", err)
	}
	return a.sweep.Run(ctx, opts)
}

// Sweep exposes the sweeper for the maintenance command path (no operator
// audit: the archive_runs rows are the record of system-operated sweeps).
func (a *Admin) Sweep() *Sweeper { return a.sweep }

// Store exposes the wired store for command plumbing that needs the KeyTTL
// sweep outside the retention cycle.
func (a *Admin) Store() Store { return a.store }
