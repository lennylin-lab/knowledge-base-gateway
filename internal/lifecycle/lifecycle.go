// Package lifecycle implements the V1.4 data lifecycle: configurable
// retention with archive-before-delete, verified archive manifests, bounded
// sweep phases that never block online requests, and tenant-safe redacted
// exports. It is deliberately a maintenance surface separate from request
// serving (the design's rollback point: stop the maintenance process first —
// verified archives are never destroyed, and a disabled or absent policy is
// a hold that deletes nothing).
//
// Invariants pinned here and in the store implementations:
//   - Eligibility never touches live state: terminal jobs only, and ledger
//     rows only after settlement has reached a terminal state (reserved rows
//     are the accounting child's exactly-once settlement input and are never
//     eligible).
//   - Source deletion happens only after the sink returned a verified,
//     completed manifest (row count + SHA-256 re-read from storage), so a
//     partial or failed archive deletes nothing and every phase retries
//     idempotently.
//   - Physical partitioning of existing tables is intentionally NOT part of
//     this package: converting llm_requests would rewrite the table and break
//     rolling upgrades (the migration-0009 decision). The lifecycle is
//     archive-based instead: bounded primary-key ranges move expired rows to
//     the archive tier online; the declared partition intent lives in
//     retention_policies.
package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
)

// TableName is the closed set of tables this package governs. It mirrors the
// retention_policies CHECK constraint (migration 0009); the two must move
// together.
type TableName string

// Governed tables.
const (
	TableRequests TableName = "llm_requests"
	TableJobs     TableName = "async_jobs"
	TableResults  TableName = "async_job_results"
	TableLedger   TableName = "usage_ledger"
)

// Tables lists the governed tables in sweep order: results before the jobs
// that own them, so an async_jobs deletion cascade never removes a result
// that has not been archived (the cascade target is either already deleted
// by its own policy or archived with the job batch).
var Tables = []TableName{TableRequests, TableResults, TableJobs, TableLedger}

// ErrUnknownTable rejects a table name outside the closed set.
var ErrUnknownTable = errors.New("lifecycle: unknown table")

// ErrUnknownTenant reports an export filter naming a tenant that does not
// exist (the data_exports row would violate its foreign key).
var ErrUnknownTenant = errors.New("lifecycle: unknown tenant")

// ParseTable validates one table name.
func ParseTable(raw string) (TableName, error) {
	t := TableName(raw)
	for _, known := range Tables {
		if t == known {
			return t, nil
		}
	}
	return "", fmt.Errorf("%w: %q", ErrUnknownTable, raw)
}

// Policy is one retention_policies row. Enabled=false is the legal-hold
// state: the sweep skips the table entirely. Absence of a row is also a hold
// ("no policy" never means a default TTL — it means keep forever).
type Policy struct {
	Table               TableName `json:"table"`
	TTL                 int64     `json:"ttl_seconds"` // positive; seconds granularity matches the schema
	ArchiveBeforeDelete bool      `json:"archive_before_delete"`
	Enabled             bool      `json:"enabled"`
	UpdatedAt           time.Time `json:"updated_at"`
}

// PolicyInput upserts one policy. TTL must be positive (a zero/negative TTL
// is a configuration error, not "keep forever").
type PolicyInput struct {
	Table               TableName
	TTL                 int64
	ArchiveBeforeDelete bool
	Enabled             bool
}

// Validate enforces the domain rules the schema CHECKs also encode.
func (in PolicyInput) Validate() error {
	if _, err := ParseTable(string(in.Table)); err != nil {
		return err
	}
	if in.TTL <= 0 {
		return fmt.Errorf("ttl_seconds must be positive, got %d", in.TTL)
	}
	return nil
}

// RunStatus is the closed archive_runs state set (mirrors migration 0009).
type RunStatus string

// Run states.
const (
	RunRunning   RunStatus = "running"
	RunCompleted RunStatus = "completed"
	RunFailed    RunStatus = "failed"
)

// RunView is one archive_runs row for the admin surface.
type RunView struct {
	ID           int64           `json:"id"`
	Table        TableName       `json:"table"`
	Status       string          `json:"status"`
	RowsArchived int64           `json:"rows_archived"`
	RowsDeleted  int64           `json:"rows_deleted"`
	StartedAt    time.Time       `json:"started_at"`
	FinishedAt   *time.Time      `json:"finished_at"`
	Detail       json.RawMessage `json:"detail,omitempty"`
}

// Batch is one bounded set of eligible rows selected for archive/delete.
// Lines are pre-encoded NDJSON record lines (see EncodeRecord); Keys carries
// the primary keys the delete phase removes (for async_jobs this is the job
// ID — its results and request snapshot are removed by cascade, and any
// remaining result rows are archived by this same batch first).
type Batch struct {
	Table  TableName
	Lines  [][]byte
	Keys   []string
	Count  int
	Oldest time.Time
	Newest time.Time
}

// Append adds one encoded line and its delete key, tracking the batch's
// time range.
func (b *Batch) Append(line []byte, key string, createdAt time.Time) {
	b.Lines = append(b.Lines, line)
	b.Keys = append(b.Keys, key)
	b.Count++
	if b.Oldest.IsZero() || createdAt.Before(b.Oldest) {
		b.Oldest = createdAt
	}
	if createdAt.After(b.Newest) {
		b.Newest = createdAt
	}
}

// Manifest describes one verified archive artifact. Complete=true is the
// completion marker: it is only ever persisted after the stored bytes were
// re-read and their row count and checksum matched, so a partial archive can
// never advertise itself as complete and become deletion-eligible.
type Manifest struct {
	ManifestVersion int       `json:"manifest_version"`
	SchemaVersion   int       `json:"schema_version"` // migration head the archive was produced from
	Table           TableName `json:"table"`
	ArchiveID       string    `json:"archive_id"`
	CreatedAt       time.Time `json:"created_at"`
	RowCount        int       `json:"row_count"`
	SHA256          string    `json:"sha256"` // hex digest over the concatenated data bytes
	DataFile        string    `json:"data_file"`
	TimeColumn      string    `json:"time_column"`
	TimeFrom        time.Time `json:"time_from"`
	TimeTo          time.Time `json:"time_to"`
	Complete        bool      `json:"complete"`
}

// manifestVersion is bumped only on a breaking manifest-format change.
const manifestVersion = 1

// RecordType discriminates NDJSON record lines. Archive and export share one
// projection contract so consumers and redaction rules cannot drift.
type RecordType string

// Record types.
const (
	RecordRequest       RecordType = "llm_request"
	RecordJob           RecordType = "async_job"
	RecordResult        RecordType = "async_job_result"
	RecordLedger        RecordType = "usage_ledger"
	RecordManagementLog RecordType = "management_log"
)

// Record is one typed NDJSON line.
type Record struct {
	Type   RecordType      `json:"type"`
	Record json.RawMessage `json:"record"`
}

// EncodeRecord renders one record line (with trailing newline) in the shared
// archive/export projection contract.
func EncodeRecord(t RecordType, record any) ([]byte, error) {
	raw, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	line, err := json.Marshal(Record{Type: t, Record: raw})
	if err != nil {
		return nil, err
	}
	return append(line, '\n'), nil
}

// ArchiveSink persists archive artifacts. The initial sink is filesystem;
// production configuration must choose a durable location for the root.
type ArchiveSink interface {
	// Write stores the batch bytes as one artifact, verifies the stored
	// bytes by reading them back (row count and SHA-256), and only then
	// returns the completed manifest. On any failure the partial artifact is
	// removed and the returned manifest is the zero value: callers must
	// treat an error as "nothing was archived" and delete no source rows.
	Write(ctx context.Context, m Manifest, data []byte) (Manifest, error)
}

// Options bounds one sweep cycle.
type Options struct {
	DryRun bool
	Only   TableName // empty sweeps every governed table
}

// TableResult reports one table's sweep outcome. Status values: "skipped"
// (no policy or legal hold), "dry-run", "completed", "failed".
type TableResult struct {
	Table     TableName  `json:"table"`
	Status    string     `json:"status"`
	Eligible  int64      `json:"eligible_rows"`
	Archived  int64      `json:"rows_archived"`
	Deleted   int64      `json:"rows_deleted"`
	Detail    string     `json:"detail,omitempty"` // skip reason or failure class
	Err       string     `json:"error,omitempty"`
	Manifests []Manifest `json:"manifests,omitempty"`
}

// Store is the persistence boundary for retention sweeps. Implementations
// must keep every statement parameterized and bounded, and re-check
// eligibility inside the delete so a row that became ineligible between
// select and delete is kept.
type Store interface {
	// RetentionPolicies lists all configured policies.
	RetentionPolicies(ctx context.Context) ([]Policy, error)
	// CountEligible reports rows older than cutoff that the policy's
	// eligibility rules admit (terminal jobs, settled/released ledger rows).
	CountEligible(ctx context.Context, table TableName, cutoff time.Time) (int64, error)
	// SelectBatch returns one bounded batch of eligible rows as redacted
	// NDJSON lines plus their primary keys.
	SelectBatch(ctx context.Context, table TableName, cutoff time.Time, limit int) (Batch, error)
	// DeleteBatch removes the batch's rows and returns how many were
	// actually deleted (re-checking eligibility in the WHERE).
	DeleteBatch(ctx context.Context, table TableName, b Batch) (int64, error)
	// MarkStaleRunsFailed closes runs left 'running' by a crashed process.
	// Nothing needs undoing: deletes only ever follow a verified archive
	// inside the same run.
	MarkStaleRunsFailed(ctx context.Context, staleAfter time.Duration, now time.Time) (int, error)
	// StartRun opens a run row and returns its ID.
	StartRun(ctx context.Context, table TableName, dryRun bool, now time.Time) (int64, error)
	// FinishRun closes a run with its terminal status and counters.
	FinishRun(ctx context.Context, runID int64, status RunStatus, archived, deleted int64, detail json.RawMessage) error
	// SchemaVersion reports the migration head stamps into manifests.
	SchemaVersion(ctx context.Context) (int, error)
	// WriteOp records a management-audit op (operator-triggered runs and
	// policy mutations commit their audit through the same boundary).
	WriteOp(ctx context.Context, op mgmt.AdminOp) error
	// SweepExpiredIdempotencyKeys deletes idempotency mappings whose
	// expires_at passed (the KeyTTL sweep). The lookup path enforces the
	// same TTL, so the sweep is reclamation, not correctness-critical.
	SweepExpiredIdempotencyKeys(ctx context.Context, now time.Time) (int, error)
}

// ExportSource is one exportable record family. Management log is platform
// scope: the Admin surface only ever requests it for platform-global
// callers, and it is never tenant-exportable.
type ExportSource string

// Export sources, in deterministic emission order.
const (
	SourceRequests      ExportSource = "llm_requests"
	SourceJobs          ExportSource = "async_jobs"
	SourceResults       ExportSource = "async_job_results"
	SourceLedger        ExportSource = "usage_ledger"
	SourceManagementLog ExportSource = "management_log"
)

// exportSources is the fixed iteration order (results before jobs mirrors
// the archive order and keeps exports deterministic).
var exportSources = []ExportSource{SourceRequests, SourceResults, SourceJobs, SourceLedger}

// ExportFilter bounds an export query. Every field is metadata: an export
// never carries prompts, completions, or stored response bodies (the
// async_job_results response column is excluded by construction in the
// projection).
type ExportFilter struct {
	RequestID string
	TraceID   string
	JobID     string
	// Tenant is a mandatory predicate for tenant-bound callers and a
	// scoping choice for platform-global ones (empty = all tenants).
	Tenant  string
	Subject string
	Model   string
	From    time.Time
	To      time.Time
	// IncludeManagementLog requests the platform-scope management audit
	// trail. Honored only for platform-global callers.
	IncludeManagementLog bool
	// MaxRows bounds the artifact; the handler clamps it.
	MaxRows int
}

// Metadata renders the filter as the redacted query metadata stored on the
// data_exports row (never exported content).
func (f ExportFilter) Metadata() map[string]any {
	m := map[string]any{}
	if f.RequestID != "" {
		m["request_id"] = f.RequestID
	}
	if f.TraceID != "" {
		m["trace_id"] = f.TraceID
	}
	if f.JobID != "" {
		m["job_id"] = f.JobID
	}
	if f.Tenant != "" {
		m["tenant"] = f.Tenant
	}
	if f.Subject != "" {
		m["subject"] = f.Subject
	}
	if f.Model != "" {
		m["model"] = f.Model
	}
	if !f.From.IsZero() {
		m["from"] = f.From.UTC().Format(time.RFC3339)
	}
	if !f.To.IsZero() {
		m["to"] = f.To.UTC().Format(time.RFC3339)
	}
	if f.IncludeManagementLog {
		m["include_management_log"] = true
	}
	m["max_rows"] = f.MaxRows
	return m
}

// ExportCursor is the keyset continuation: the last emitted (created_at, pk)
// pair of one source. Key string form is source-typed at the store boundary.
type ExportCursor struct {
	At  time.Time
	Key string
}

// Zero reports whether the cursor starts a fresh page walk.
func (c ExportCursor) Zero() bool { return c.At.IsZero() && c.Key == "" }

// ExportRecord is one page row.
type ExportRecord struct {
	Line []byte
}

// ExportView is one data_exports row projection.
type ExportView struct {
	ID          string          `json:"id"`
	RequestedBy string          `json:"requested_by"`
	TenantID    string          `json:"tenant_id,omitempty"`
	Filters     json.RawMessage `json:"filters"`
	Status      string          `json:"status"`
	CreatedAt   time.Time       `json:"created_at"`
	CompletedAt *time.Time      `json:"completed_at"`
}

// ExportSummary is the verification block of a finished export: the row
// count and SHA-256 cover exactly the emitted record lines, and Complete
// marks the artifact's completion marker (a partial stream is never
// complete and must fail consumer verification).
type ExportSummary struct {
	Rows     int64  `json:"rows"`
	SHA256   string `json:"sha256"`
	Complete bool   `json:"complete"`
	Error    string `json:"error,omitempty"`
}

// ExportStore is the persistence boundary for exports: redacted, tenant-
// predicated keyset pages plus the data_exports record lifecycle.
type ExportStore interface {
	// CreateExport records the request (status queued). A filter naming an
	// unknown tenant returns ErrUnknownTenant.
	CreateExport(ctx context.Context, id, requestedBy string, tenant string, f ExportFilter) error
	// CompleteExport stamps the terminal state and verification metadata
	// into the record's filter block.
	CompleteExport(ctx context.Context, id, status string, rows int64, sha256 string) error
	// Page returns one keyset page of redacted records for one source. The
	// tenant inside the filter is a mandatory SQL predicate.
	Page(ctx context.Context, source ExportSource, f ExportFilter, cur ExportCursor, limit int) ([]ExportRecord, ExportCursor, error)
	// ListExports returns export records within the caller's tenant boundary
	// (empty = platform view of all).
	ListExports(ctx context.Context, tenant string, limit int) ([]ExportView, error)
	// WriteExportAudit records the export operation in the management audit
	// trail (actor attribution rides the op).
	WriteExportAudit(ctx context.Context, op mgmt.AdminOp) error
}

// Default bounds shared by the sweeper, the admin surface, and the
// maintenance command.
const (
	DefaultBatchSize     = 200
	DefaultMaxBatches    = 8
	DefaultStaleRunAfter = time.Hour
	DefaultExportMaxRows = 5000
	MaxExportRows        = 20000
	exportPageSize       = 500
)
