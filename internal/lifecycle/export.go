package lifecycle

// The export pipeline: filtered, tenant-predicated keyset pages over the
// governed tables, streamed as NDJSON with a header line, typed record
// lines, and a trailing summary line carrying the row count and SHA-256
// completion marker. Projections are redacted by construction at the store
// boundary (the async_job_results response column — completion content — is
// never selected). The summary hashes exactly the record-line bytes, so a
// consumer can verify the artifact by recounting and re-hashing.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
)

// ExportRequest carries one export operation: the streaming target, the
// caller attribution, the caller's tenant boundary, and the filter.
type ExportRequest struct {
	RequestedBy string
	// Tenant is the caller's boundary (empty = platform-global). A non-empty
	// tenant is a mandatory predicate over every source and — with the
	// management-log rule below — makes the platform audit trail
	// unexportable.
	Tenant string
	Filter ExportFilter
	// Op is the management-audit op for the operation; the Admin fills the
	// target and verification detail before writing.
	Op mgmt.AdminOp
}

// exportArtifact emits the NDJSON artifact and tracks its verification
// summary. Record lines are hashed exactly as emitted; header and summary
// lines frame the hashed region.
type exportArtifact struct {
	w      io.Writer
	digest hash.Hash
	rows   int64
	limit  int64
}

func newExportArtifact(w io.Writer, maxRows int) *exportArtifact {
	return &exportArtifact{w: w, digest: sha256.New(), limit: int64(maxRows)}
}

// header emits the artifact header line (not hashed, not counted).
func (e *exportArtifact) header(id, requestedBy string, f ExportFilter, at time.Time) error {
	raw, err := json.Marshal(map[string]any{
		"export": map[string]any{
			"id": id, "requested_by": requestedBy,
			"generated_at": at.UTC().Format(time.RFC3339),
			"filters":      f.Metadata(),
		},
	})
	if err != nil {
		return err
	}
	_, err = e.w.Write(append(raw, '\n'))
	return err
}

// write emits one record line while row budget remains; emitted=false when
// the artifact hit its bound.
func (e *exportArtifact) write(rec ExportRecord) (emitted bool, err error) {
	if e.rows >= e.limit {
		return false, nil
	}
	if _, err := e.digest.Write(rec.Line); err != nil {
		return false, err
	}
	if _, err := e.w.Write(rec.Line); err != nil {
		return false, err
	}
	e.rows++
	return true, nil
}

// trailer emits the summary line and returns the summary value.
func (e *exportArtifact) trailer() (ExportSummary, error) {
	s := ExportSummary{Rows: e.rows, SHA256: hex.EncodeToString(e.digest.Sum(nil)), Complete: true}
	raw, err := json.Marshal(map[string]any{"summary": s})
	if err != nil {
		return s, err
	}
	_, err = e.w.Write(append(raw, '\n'))
	return s, err
}

func (e *exportArtifact) truncated() bool { return e.rows >= e.limit }

// prepareFilter clamps the row budget and resolves the tenant rule. The
// tenant rule is enforced here, not only at the HTTP boundary: a non-empty
// request tenant makes the export tenant-scoped and excludes the
// platform-scope management log; the management log is exported only for
// platform-global callers that explicitly asked for it.
func prepareFilter(f ExportFilter) ExportFilter {
	if f.MaxRows <= 0 {
		f.MaxRows = DefaultExportMaxRows
	}
	if f.MaxRows > MaxExportRows {
		f.MaxRows = MaxExportRows
	}
	if f.Tenant != "" && f.IncludeManagementLog {
		// Defensive: the handler rejects this earlier; the domain never
		// widens a tenant-scoped export to the platform audit trail.
		f.IncludeManagementLog = false
	}
	return f
}

// BeginExport creates the queued data_exports record and validates the
// request — notably the tenant existence — before any artifact bytes are
// produced, so a 400-class rejection never races a committed 200 stream.
// Callers then stream with StreamExport.
func (a *Admin) BeginExport(ctx context.Context, in ExportRequest) (string, error) {
	f := prepareFilter(in.Filter)
	id := newExportID()
	if err := a.export.CreateExport(ctx, id, in.RequestedBy, f.Tenant, f); err != nil {
		return "", err
	}
	return id, nil
}

// StreamExport walks every requested source in deterministic order, streams
// the NDJSON artifact, moves the data_exports row to its terminal state, and
// writes the management-audit op.
func (a *Admin) StreamExport(ctx context.Context, w io.Writer, id string, in ExportRequest) (ExportSummary, error) {
	f := prepareFilter(in.Filter)
	scoped := f.Tenant != ""

	artifact := newExportArtifact(w, f.MaxRows)
	if err := artifact.header(id, in.RequestedBy, f, a.now()); err != nil {
		return ExportSummary{}, a.failExport(ctx, id, in, fmt.Errorf("write header: %w", err))
	}
	if err := a.exportPages(ctx, artifact, f, scoped); err != nil {
		return ExportSummary{}, a.failExport(ctx, id, in, err)
	}
	summary, err := artifact.trailer()
	if err != nil {
		return ExportSummary{}, a.failExport(ctx, id, in, fmt.Errorf("write summary: %w", err))
	}
	if err := a.export.CompleteExport(ctx, id, "completed", summary.Rows, summary.SHA256); err != nil {
		return summary, fmt.Errorf("lifecycle: complete export record: %w", err)
	}
	a.auditExport(ctx, in, id, summary)
	if a.metrics != nil {
		a.metrics.IncLifecycleExport()
	}
	return summary, nil
}

// Export composes BeginExport + StreamExport for callers that write to a
// buffer and can treat a rejection as a plain error.
func (a *Admin) Export(ctx context.Context, w io.Writer, in ExportRequest) (ExportSummary, error) {
	id, err := a.BeginExport(ctx, in)
	if err != nil {
		return ExportSummary{}, err
	}
	return a.StreamExport(ctx, w, id, in)
}

// exportPages walks the sources with keyset cursors until the row budget or
// the sources are exhausted.
func (a *Admin) exportPages(ctx context.Context, artifact *exportArtifact, f ExportFilter, scoped bool) error {
	sources := exportSources
	if f.IncludeManagementLog && !scoped {
		sources = append(append([]ExportSource{}, exportSources...), SourceManagementLog)
	}
	for _, source := range sources {
		cur := ExportCursor{}
		for {
			recs, next, err := a.export.Page(ctx, source, f, cur, exportPageSize)
			if err != nil {
				return fmt.Errorf("page %s: %w", source, err)
			}
			for _, rec := range recs {
				emitted, err := artifact.write(rec)
				if err != nil {
					return fmt.Errorf("write %s record: %w", source, err)
				}
				if !emitted {
					return nil // row budget exhausted; max_rows in the filter explains the bound
				}
			}
			if next.Zero() || len(recs) == 0 {
				break
			}
			cur = next
			if artifact.truncated() {
				return nil
			}
		}
	}
	return nil
}

// failExport marks the export record failed and returns the error.
func (a *Admin) failExport(ctx context.Context, id string, in ExportRequest, err error) error {
	_ = a.export.CompleteExport(ctx, id, "failed", 0, "")
	a.auditExport(ctx, in, id, ExportSummary{Complete: false, Error: "failed"})
	return fmt.Errorf("lifecycle: export %s: %w", id, err)
}

// auditExport writes the management-audit record (metadata only: the filter
// block and verification summary, never exported content).
func (a *Admin) auditExport(ctx context.Context, in ExportRequest, id string, s ExportSummary) {
	detail, _ := json.Marshal(map[string]any{
		"export_id": id, "filters": in.Filter.Metadata(),
		"rows": s.Rows, "sha256": s.SHA256, "complete": s.Complete,
	})
	op := in.Op
	op.Target = id
	op.Detail = detail
	if err := a.export.WriteExportAudit(ctx, op); err != nil {
		a.logger.Error("lifecycle: export audit write failed", "export_id", id, "error", err)
	}
}

// newExportID mints the public export identifier.
func newExportID() string {
	var rnd [8]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return fmt.Sprintf("exp_%d", time.Now().UnixNano())
	}
	return "exp_" + hex.EncodeToString(rnd[:])
}
