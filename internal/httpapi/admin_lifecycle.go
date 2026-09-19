package httpapi

// V1.4 data-lifecycle admin surface: retention policies, archive-run
// history/triggering, and scoped streaming exports. Endpoints are disabled
// (404) unless a LifecycleAdmin implementation is wired (database mode with
// the lifecycle flag on — the documented rollback point). Scope matrix
// (adminRoutePolicy): policy views/run history are viewer+global (platform
// operational metadata, like the management log); policy mutations and run
// triggering are platform-admin+global; exports are viewer with the caller's
// tenant boundary as a mandatory predicate. Management-log rows are
// platform-scope and never tenant-exportable (child-4 ruling).

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/lifecycle"
	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
)

// LifecycleAdmin is the management surface for retention, archive runs, and
// exports. The tenant parameters are the caller's tenant boundary (empty =
// platform-global), enforced inside the implementation as mandatory
// predicates. Exports are two-phase: BeginExport creates the queued record
// and validates the request (a 400-class rejection happens before any
// artifact bytes), StreamExport then writes the NDJSON artifact.
type LifecycleAdmin interface {
	Policies(ctx context.Context) ([]lifecycle.Policy, error)
	UpsertPolicy(ctx context.Context, in lifecycle.PolicyInput, op mgmt.AdminOp) error
	Runs(ctx context.Context, limit int) ([]lifecycle.RunView, error)
	StartRun(ctx context.Context, opts lifecycle.Options, op mgmt.AdminOp) ([]lifecycle.TableResult, error)
	BeginExport(ctx context.Context, in lifecycle.ExportRequest) (string, error)
	StreamExport(ctx context.Context, w io.Writer, id string, in lifecycle.ExportRequest) (lifecycle.ExportSummary, error)
	Exports(ctx context.Context, tenant string, limit int) ([]lifecycle.ExportView, error)
}

// registerLifecycleAdmin mounts the lifecycle routes. Called from
// NewAdminMux only when deps.Lifecycle is non-nil.
func registerLifecycleAdmin(mux *http.ServeMux, guard func(http.HandlerFunc) http.HandlerFunc, deps AdminDeps) {
	mux.HandleFunc("/admin/lifecycle/policies", guard(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			policies, err := deps.Lifecycle.Policies(r.Context())
			if err != nil {
				writeError(w, newRequestID(), http.StatusInternalServerError, "internal_error", "query_failed", "could not query retention policies")
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"policies": policies})
		case http.MethodPost:
			handleUpsertPolicy(w, r, deps)
		default:
			writeError(w, newRequestID(), http.StatusMethodNotAllowed, "invalid_request_error", "method_not_allowed", "use GET or POST")
		}
	}))
	mux.HandleFunc("/admin/lifecycle/runs", guard(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			runs, err := deps.Lifecycle.Runs(r.Context(), queryInt(r.URL.Query().Get("limit")))
			if err != nil {
				writeError(w, newRequestID(), http.StatusInternalServerError, "internal_error", "query_failed", "could not query archive runs")
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"runs": runs})
		case http.MethodPost:
			handleTriggerRun(w, r, deps)
		default:
			writeError(w, newRequestID(), http.StatusMethodNotAllowed, "invalid_request_error", "method_not_allowed", "use GET or POST")
		}
	}))
	mux.HandleFunc("/admin/exports", guard(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			principal, _ := principalFromContext(r.Context())
			exports, err := deps.Lifecycle.Exports(r.Context(), principal.TenantID, queryInt(r.URL.Query().Get("limit")))
			if err != nil {
				writeError(w, newRequestID(), http.StatusInternalServerError, "internal_error", "query_failed", "could not query export records")
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"exports": exports})
		case http.MethodPost:
			handleExport(w, r, deps)
		default:
			writeError(w, newRequestID(), http.StatusMethodNotAllowed, "invalid_request_error", "method_not_allowed", "use GET or POST")
		}
	}))
}

// policyBody is the retention-policy upsert payload. ttl_seconds must be
// positive (the schema CHECK agrees); enabled=false is the legal hold.
type policyBody struct {
	Table               string `json:"table"`
	TTLSeconds          int64  `json:"ttl_seconds"`
	ArchiveBeforeDelete *bool  `json:"archive_before_delete"` // defaults true
	Enabled             *bool  `json:"enabled"`               // defaults true
}

func handleUpsertPolicy(w http.ResponseWriter, r *http.Request, deps AdminDeps) {
	requestID := newRequestID()
	var body policyBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&body); err != nil {
		writeError(w, requestID, http.StatusBadRequest, "invalid_request_error", "invalid_request", "invalid retention policy payload")
		return
	}
	in := lifecycle.PolicyInput{
		Table: lifecycle.TableName(body.Table), TTL: body.TTLSeconds,
		ArchiveBeforeDelete: body.ArchiveBeforeDelete == nil || *body.ArchiveBeforeDelete,
		Enabled:             body.Enabled == nil || *body.Enabled,
	}
	// Handler-side validation for the 400 mapping; the service re-validates
	// as the authority.
	if err := in.Validate(); err != nil {
		writeError(w, requestID, http.StatusBadRequest, "invalid_request_error", "invalid_policy",
			"table must be one of the governed lifecycle tables and ttl_seconds must be positive")
		return
	}
	detail, _ := json.Marshal(map[string]any{"table": body.Table, "ttl_seconds": body.TTLSeconds})
	if err := deps.Lifecycle.UpsertPolicy(r.Context(), in, adminOpFrom(r, "retention_policy_upsert", body.Table, detail)); err != nil {
		if errors.Is(err, lifecycle.ErrUnknownTable) {
			writeError(w, requestID, http.StatusBadRequest, "invalid_request_error", "unknown_table", "table must be one of the governed lifecycle tables")
			return
		}
		writeError(w, requestID, http.StatusBadRequest, "invalid_request_error", "invalid_policy", "ttl_seconds must be positive")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "upserted", "table": body.Table})
}

// runBody is the sweep-trigger payload; every field is optional.
type runBody struct {
	DryRun bool   `json:"dry_run"`
	Table  string `json:"table"`
}

func handleTriggerRun(w http.ResponseWriter, r *http.Request, deps AdminDeps) {
	requestID := newRequestID()
	var body runBody
	if r.Body != nil {
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, requestID, http.StatusBadRequest, "invalid_request_error", "invalid_request", "invalid run payload")
			return
		}
	}
	opts := lifecycle.Options{DryRun: body.DryRun, Only: lifecycle.TableName(body.Table)}
	// Handler-side validation for the 400 mapping; the service re-validates.
	if opts.Only != "" {
		if _, err := lifecycle.ParseTable(string(opts.Only)); err != nil {
			writeError(w, requestID, http.StatusBadRequest, "invalid_request_error", "unknown_table", "table must be one of the governed lifecycle tables")
			return
		}
	}
	results, err := deps.Lifecycle.StartRun(r.Context(), opts, adminOpFrom(r, "retention_run", body.Table, nil))
	if err != nil {
		if errors.Is(err, lifecycle.ErrUnknownTable) {
			writeError(w, requestID, http.StatusBadRequest, "invalid_request_error", "unknown_table", "table must be one of the governed lifecycle tables")
			return
		}
		writeError(w, requestID, http.StatusInternalServerError, "internal_error", "run_failed", "the retention sweep failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

// exportBody is the export request payload. Every field is metadata; the
// tenant field is a scoping choice for platform callers and overridden for
// tenant-bound ones.
type exportBody struct {
	RequestID            string `json:"request_id"`
	TraceID              string `json:"trace_id"`
	JobID                string `json:"job_id"`
	Tenant               string `json:"tenant"`
	Subject              string `json:"subject"`
	Model                string `json:"model"`
	From                 string `json:"from"` // RFC3339
	To                   string `json:"to"`   // RFC3339
	IncludeManagementLog bool   `json:"include_management_log"`
	MaxRows              int    `json:"max_rows"`
}

func handleExport(w http.ResponseWriter, r *http.Request, deps AdminDeps) {
	requestID := newRequestID()
	principal, _ := principalFromContext(r.Context())
	var body exportBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, requestID, http.StatusBadRequest, "invalid_request_error", "invalid_request", "invalid export payload")
		return
	}
	// Tenant boundary first: a tenant-bound caller is confined to its own
	// tenant, and the platform-scope management log is never
	// tenant-exportable (uniform 403 before any store access).
	if !principal.Global() {
		if body.IncludeManagementLog {
			adminInsufficientScope(w, requestID)
			return
		}
		body.Tenant = principal.TenantID
	}
	from, okFrom := timeOrZero(body.From)
	to, okTo := timeOrZero(body.To)
	if !okFrom || !okTo {
		writeError(w, requestID, http.StatusBadRequest, "invalid_request_error", "invalid_request", "from/to must be RFC3339 timestamps")
		return
	}
	f := lifecycle.ExportFilter{
		RequestID: body.RequestID, TraceID: body.TraceID, JobID: body.JobID,
		Tenant: body.Tenant, Subject: body.Subject, Model: body.Model,
		From:                 from,
		To:                   to,
		IncludeManagementLog: body.IncludeManagementLog,
		MaxRows:              body.MaxRows,
	}

	in := lifecycle.ExportRequest{
		RequestedBy: principal.AdminSubject, Tenant: principal.TenantID,
		Filter: f,
		Op:     adminOpFrom(r, "data_export", "", nil),
	}
	// Phase 1 — record creation and validation: a 400-class rejection
	// (unknown tenant) happens before any artifact bytes are committed.
	exportID, err := deps.Lifecycle.BeginExport(r.Context(), in)
	if err != nil {
		if errors.Is(err, lifecycle.ErrUnknownTenant) {
			writeError(w, requestID, http.StatusBadRequest, "invalid_request_error", "unknown_tenant", "tenant filter names an unknown tenant")
			return
		}
		writeError(w, requestID, http.StatusInternalServerError, "internal_error", "export_failed", "could not start the export")
		return
	}
	// Phase 2 — stream. Headers are committed; failures travel in the
	// artifact's incomplete summary and the failed data_exports record.
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	summary, err := deps.Lifecycle.StreamExport(r.Context(), w, exportID, in)
	if err != nil {
		deps.Logger.Error("lifecycle export failed", "export_id", exportID, "error", err)
		return
	}
	if summary.Complete {
		deps.Logger.Info("lifecycle export completed", "export_id", exportID,
			"rows", summary.Rows, "sha256", summary.SHA256)
	}
}

// timeOrZero parses an RFC3339 body timestamp.
func timeOrZero(raw string) (time.Time, bool) {
	if raw == "" {
		return time.Time{}, true
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}
