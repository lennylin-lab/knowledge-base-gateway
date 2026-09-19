package lifecycle

// The Sweeper runs the retention phases per governed table: policy lookup
// (absent row or enabled=false is a hold), stale-run recovery, then bounded
// archive → verify → delete batches. Every phase is idempotent, deletes are
// eligibility-re-checked in SQL, and the sink's verified manifest is the
// only thing that can unlock a delete — a failed or partial archive leaves
// every source row in place.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/metrics"
)

// SweeperDeps wires the sweeper. Zero-value bounds fall back to the package
// defaults.
type SweeperDeps struct {
	Store         Store
	Sink          ArchiveSink
	Metrics       *metrics.Registry // optional
	Logger        *slog.Logger      // optional
	BatchSize     int               // rows per batch (default DefaultBatchSize)
	MaxBatches    int               // batches per run per table (default DefaultMaxBatches)
	StaleRunAfter time.Duration     // running runs older than this are failed on startup (default DefaultStaleRunAfter)
	Now           func() time.Time
}

// Sweeper executes retention cycles.
type Sweeper struct {
	deps SweeperDeps
}

// NewSweeper validates the wiring.
func NewSweeper(deps SweeperDeps) (*Sweeper, error) {
	if deps.Store == nil {
		return nil, fmt.Errorf("lifecycle: sweeper requires a store")
	}
	if deps.Sink == nil {
		return nil, fmt.Errorf("lifecycle: sweeper requires an archive sink")
	}
	if deps.BatchSize <= 0 {
		deps.BatchSize = DefaultBatchSize
	}
	if deps.MaxBatches <= 0 {
		deps.MaxBatches = DefaultMaxBatches
	}
	if deps.StaleRunAfter <= 0 {
		deps.StaleRunAfter = DefaultStaleRunAfter
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.DiscardHandler)
	}
	return &Sweeper{deps: deps}, nil
}

func (s *Sweeper) now() time.Time { return s.deps.Now().UTC() }

// Run executes one sweep cycle and reports each table's outcome. The first
// return value is per-table detail; the error is the first table failure
// (later tables still run — one table's failure never blocks the others).
func (s *Sweeper) Run(ctx context.Context, opts Options) ([]TableResult, error) {
	now := s.now()
	if _, err := s.deps.Store.MarkStaleRunsFailed(ctx, s.deps.StaleRunAfter, now); err != nil {
		return nil, fmt.Errorf("lifecycle: stale run recovery: %w", err)
	}
	// Manifests must name the migration head they were produced from; a
	// store that cannot report it fails the cycle (an interpretable archive
	// is a precondition for deletion).
	schemaVersion, err := s.deps.Store.SchemaVersion(ctx)
	if err != nil {
		return nil, fmt.Errorf("lifecycle: schema version: %w", err)
	}

	tables := Tables
	if opts.Only != "" {
		t, err := ParseTable(string(opts.Only))
		if err != nil {
			return nil, err
		}
		tables = []TableName{t}
	}

	policies, err := s.deps.Store.RetentionPolicies(ctx)
	if err != nil {
		return nil, fmt.Errorf("lifecycle: load policies: %w", err)
	}
	byTable := map[TableName]Policy{}
	for _, p := range policies {
		byTable[p.Table] = p
	}

	var (
		out    []TableResult
		firstE error
	)
	for _, table := range tables {
		res := s.runTable(ctx, table, byTable[table], opts, now, schemaVersion)
		out = append(out, res)
		if res.Status == "failed" && firstE == nil {
			firstE = fmt.Errorf("lifecycle: %s: %s", table, res.Err)
		}
	}
	return out, firstE
}

// runTable sweeps one table under its policy.
func (s *Sweeper) runTable(ctx context.Context, table TableName, p Policy, opts Options, now time.Time, schemaVersion int) TableResult {
	res := TableResult{Table: table}
	// No row (keep forever) or enabled=false (legal hold) deletes nothing.
	if p.Table == "" || !p.Enabled {
		res.Status = "skipped"
		res.Detail = "no policy (keep forever)"
		if p.Table != "" && !p.Enabled {
			res.Detail = "policy disabled (legal hold)"
		}
		return res
	}

	cutoff := now.Add(-time.Duration(p.TTL) * time.Second)
	runID, err := s.deps.Store.StartRun(ctx, table, opts.DryRun, now)
	if err != nil {
		res.Status = "failed"
		res.Err = fmt.Sprintf("start run: %v", err)
		return res
	}

	finish := func(status RunStatus, detail map[string]any) {
		raw, _ := json.Marshal(detail)
		if err := s.deps.Store.FinishRun(ctx, runID, status, res.Archived, res.Deleted, raw); err != nil {
			s.deps.Logger.Error("lifecycle: finish run failed", "table", string(table), "run_id", runID, "error", err)
		}
	}

	if opts.DryRun {
		n, err := s.deps.Store.CountEligible(ctx, table, cutoff)
		if err != nil {
			res.Status = "failed"
			res.Err = fmt.Sprintf("count eligible: %v", err)
			finish(RunFailed, map[string]any{"dry_run": true, "error": res.Err})
			return res
		}
		res.Status = "dry-run"
		res.Eligible = n
		finish(RunCompleted, map[string]any{"dry_run": true, "eligible": n, "cutoff": cutoff})
		return res
	}

	var manifests []Manifest
	for batch := 0; batch < s.deps.MaxBatches; batch++ {
		if err := ctx.Err(); err != nil {
			res.Status = "failed"
			res.Err = err.Error()
			finish(RunFailed, map[string]any{"error": res.Err})
			return res
		}
		b, err := s.deps.Store.SelectBatch(ctx, table, cutoff, s.deps.BatchSize)
		if err != nil {
			res.Status = "failed"
			res.Err = fmt.Sprintf("select batch: %v", err)
			finish(RunFailed, map[string]any{"error": res.Err})
			return res
		}
		if b.Count == 0 {
			break
		}
		res.Eligible += int64(b.Count)

		data := concatLines(b.Lines)
		archived, err := s.deps.Sink.Write(ctx, Manifest{
			ManifestVersion: manifestVersion,
			SchemaVersion:   schemaVersion,
			Table:           table,
			TimeColumn:      "created_at",
			TimeFrom:        b.Oldest,
			TimeTo:          b.Newest,
		}, data)
		if err != nil {
			// A failed archive deletes nothing; the run is marked failed and
			// the next cycle re-selects the same rows (idempotent retry).
			res.Status = "failed"
			res.Err = fmt.Sprintf("archive: %v", err)
			finish(RunFailed, map[string]any{"error": res.Err, "batch_rows": b.Count})
			return res
		}
		manifests = append(manifests, archived)
		res.Archived += int64(b.Count)

		deleted, err := s.deps.Store.DeleteBatch(ctx, table, b)
		if err != nil {
			res.Status = "failed"
			res.Err = fmt.Sprintf("delete batch: %v", err)
			finish(RunFailed, map[string]any{"error": res.Err, "manifests": manifestSummaries(manifests)})
			return res
		}
		res.Deleted += deleted
		if s.deps.Metrics != nil {
			s.deps.Metrics.IncLifecycleArchived(string(table), int64(b.Count))
			s.deps.Metrics.IncLifecycleDeleted(string(table), deleted)
		}
	}

	res.Status = "completed"
	finish(RunCompleted, map[string]any{
		"cutoff": cutoff, "archived": res.Archived, "deleted": res.Deleted,
		"manifests": manifestSummaries(manifests),
	})
	return res
}

// manifestSummaries reduces manifests to the fields an operator needs in the
// run detail (ids, counts, checksums).
func manifestSummaries(ms []Manifest) []map[string]any {
	out := make([]map[string]any, 0, len(ms))
	for _, m := range ms {
		out = append(out, map[string]any{
			"archive_id": m.ArchiveID, "rows": m.RowCount, "sha256": m.SHA256,
		})
	}
	return out
}

// concatLines joins NDJSON lines into the artifact bytes.
func concatLines(lines [][]byte) []byte {
	n := 0
	for _, l := range lines {
		n += len(l)
	}
	out := make([]byte, 0, n)
	for _, l := range lines {
		out = append(out, l...)
	}
	return out
}

// SweepExpiredIdempotencyKeys runs the KeyTTL reclamation sweep and logs the
// outcome. Enforcement of the TTL already happens at lookup time; this only
// reclaims the rows.
func (s *Sweeper) SweepExpiredIdempotencyKeys(ctx context.Context) (int, error) {
	n, err := s.deps.Store.SweepExpiredIdempotencyKeys(ctx, s.now())
	if err != nil {
		return 0, err
	}
	if n > 0 {
		s.deps.Logger.Info("lifecycle: swept expired idempotency keys", "count", n)
	}
	return n, nil
}
