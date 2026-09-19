package lifecycle

// Export pipeline tests with a scripted ExportStore: artifact framing
// (header, record lines, summary trailer), checksum/count verification,
// tenant scoping, the management-log platform-scope rule, and failure
// handling.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
)

// fakeExportStore scripts pages per source and records the data_exports
// lifecycle calls.
type fakeExportStore struct {
	pages   map[ExportSource][][]byte // source -> record lines (one page each)
	sources []ExportSource            // order sources were paged
	created []string                  // CreateExport tenant args
	tenant  string                    // tenant seen by Page (predicate check)
	done    []string                  // CompleteExport status args
	failAt  ExportSource              // source that errors ("" = none)
	failErr error
}

func (f *fakeExportStore) CreateExport(_ context.Context, _, _ string, tenant string, _ ExportFilter) error {
	f.created = append(f.created, tenant)
	return nil
}

func (f *fakeExportStore) CompleteExport(_ context.Context, _, status string, _ int64, _ string) error {
	f.done = append(f.done, status)
	return nil
}

func (f *fakeExportStore) Page(_ context.Context, source ExportSource, fl ExportFilter, _ ExportCursor, _ int) ([]ExportRecord, ExportCursor, error) {
	f.sources = append(f.sources, source)
	f.tenant = fl.Tenant
	if f.failAt == source {
		return nil, ExportCursor{}, f.failErr
	}
	var lines [][]byte
	if l, ok := f.pages[source]; ok {
		lines = l
	}
	out := make([]ExportRecord, 0, len(lines))
	for _, l := range lines {
		out = append(out, ExportRecord{Line: l})
	}
	return out, ExportCursor{}, nil
}

func (f *fakeExportStore) ListExports(context.Context, string, int) ([]ExportView, error) {
	return nil, nil
}

func (f *fakeExportStore) WriteExportAudit(_ context.Context, op mgmt.AdminOp) error {
	if op.Target == "" {
		return errors.New("export audit target must be set")
	}
	return nil
}

func newAdmin(t *testing.T, store Store, ex ExportStore) *Admin {
	t.Helper()
	a, err := NewAdmin(AdminDeps{Store: store, Export: ex, Sink: &countingSink{}, Now: func() time.Time {
		return time.Unix(1_800_000_000, 0).UTC()
	}})
	if err != nil {
		t.Fatalf("new admin: %v", err)
	}
	return a
}

func splitNDJSON(t *testing.T, body *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, l := range strings.Split(strings.TrimSuffix(body.String(), "\n"), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("ndjson line %q: %v", l, err)
		}
		out = append(out, m)
	}
	return out
}

func TestExportArtifactFramingAndVerification(t *testing.T) {
	recA := line(t, RecordRequest, "a")
	recB := line(t, RecordLedger, "b")
	ex := &fakeExportStore{pages: map[ExportSource][][]byte{
		SourceRequests: {recA},
		SourceLedger:   {recB},
	}}
	a := newAdmin(t, &fakeStore{schemaVer: 11}, ex)

	var buf bytes.Buffer
	summary, err := a.Export(context.Background(), &buf, ExportRequest{
		RequestedBy: "admin-1", Filter: ExportFilter{Tenant: "t1", MaxRows: 100},
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if !summary.Complete || summary.Rows != 2 {
		t.Fatalf("summary = %+v", summary)
	}
	sum := sha256.Sum256(append(append([]byte{}, recA...), recB...))
	if summary.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("summary sha %s does not verify the emitted bytes", summary.SHA256)
	}

	lines := splitNDJSON(t, &buf)
	if len(lines) != 4 { // header + 2 records + summary
		t.Fatalf("artifact lines = %d, want 4", len(lines))
	}
	if _, ok := lines[0]["export"]; !ok {
		t.Fatalf("first line must be the header: %v", lines[0])
	}
	if lines[0]["export"].(map[string]any)["requested_by"] != "admin-1" {
		t.Fatalf("header attribution missing: %v", lines[0])
	}
	if _, ok := lines[len(lines)-1]["summary"]; !ok {
		t.Fatalf("last line must be the summary: %v", lines[len(lines)-1])
	}
	if got := lines[len(lines)-1]["summary"].(map[string]any)["rows"]; got != float64(2) {
		t.Fatalf("summary rows = %v", got)
	}
	if got := lines[1]["type"]; got != string(RecordRequest) {
		t.Fatalf("record type = %v", got)
	}
	// The data_exports record completes with the verification metadata.
	if len(ex.done) != 1 || ex.done[0] != "completed" {
		t.Fatalf("export record status calls = %v", ex.done)
	}
	// The tenant predicate reached the pages and the record was tenant-scoped.
	if ex.tenant != "t1" || len(ex.created) != 1 || ex.created[0] != "t1" {
		t.Fatalf("tenant handling wrong: created=%v tenant=%q", ex.created, ex.tenant)
	}
}

func TestExportTenantScopeExcludesManagementLog(t *testing.T) {
	ex := &fakeExportStore{pages: map[ExportSource][][]byte{}}
	a := newAdmin(t, &fakeStore{}, ex)

	var buf bytes.Buffer
	_, err := a.Export(context.Background(), &buf, ExportRequest{
		Filter: ExportFilter{Tenant: "t1", IncludeManagementLog: true},
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	for _, s := range ex.sources {
		if s == SourceManagementLog {
			t.Fatal("tenant-scoped export must never include the management log")
		}
	}
	if _, ok := ex.pages[SourceManagementLog]; ok {
		t.Fatal("management log must not be paged for tenant exports")
	}
}

func TestExportGlobalCanIncludeManagementLog(t *testing.T) {
	ml := line(t, RecordManagementLog, "op1")
	ex := &fakeExportStore{pages: map[ExportSource][][]byte{
		SourceManagementLog: {ml},
	}}
	a := newAdmin(t, &fakeStore{}, ex)

	var buf bytes.Buffer
	summary, err := a.Export(context.Background(), &buf, ExportRequest{
		RequestedBy: "platform-admin",
		Filter:      ExportFilter{IncludeManagementLog: true},
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if summary.Rows != 1 {
		t.Fatalf("rows = %d", summary.Rows)
	}
	found := false
	for _, s := range ex.sources {
		if s == SourceManagementLog {
			found = true
		}
	}
	if !found {
		t.Fatal("global export with the flag must include the management log")
	}
}

func TestExportMaxRowsBound(t *testing.T) {
	var pages [][]byte
	for i := 0; i < 5; i++ {
		pages = append(pages, line(t, RecordRequest, strings.Repeat("x", i+1)))
	}
	ex := &fakeExportStore{pages: map[ExportSource][][]byte{SourceRequests: pages}}
	a := newAdmin(t, &fakeStore{}, ex)

	var buf bytes.Buffer
	summary, err := a.Export(context.Background(), &buf, ExportRequest{
		Filter: ExportFilter{MaxRows: 3},
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if summary.Rows != 3 {
		t.Fatalf("rows = %d, want the 3-row bound", summary.Rows)
	}
	if !summary.Complete {
		t.Fatal("a bounded-but-finished artifact is still complete (max_rows is metadata)")
	}
}

func TestExportFailureMarksRecordFailed(t *testing.T) {
	ex := &fakeExportStore{
		failAt: SourceRequests, failErr: errors.New("db down"),
	}
	a := newAdmin(t, &fakeStore{}, ex)

	var buf bytes.Buffer
	_, err := a.Export(context.Background(), &buf, ExportRequest{})
	if err == nil {
		t.Fatal("page failure must surface")
	}
	if len(ex.done) != 1 || ex.done[0] != "failed" {
		t.Fatalf("export record must be failed: %v", ex.done)
	}
}

func TestExportUnknownTenantPropagates(t *testing.T) {
	ex := &errCreateStore{}
	a := newAdmin(t, &fakeStore{}, ex)
	var buf bytes.Buffer
	if _, err := a.Export(context.Background(), &buf, ExportRequest{}); !errors.Is(err, ErrUnknownTenant) {
		t.Fatalf("err = %v", err)
	}
}

type errCreateStore struct{ fakeExportStore }

func (errCreateStore) CreateExport(context.Context, string, string, string, ExportFilter) error {
	return ErrUnknownTenant
}
