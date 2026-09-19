package lifecycle

// Offline tests for the filesystem archive sink and the sweeper's phase
// discipline: archive before delete, a failed/partial archive deletes
// nothing, dry runs touch nothing, absent or disabled policies (legal holds)
// skip entirely, and retries are idempotent.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
)

func line(tb testing.TB, t RecordType, id string) []byte {
	tb.Helper()
	raw, err := EncodeRecord(t, map[string]any{"id": id})
	if err != nil {
		tb.Fatalf("encode record: %v", err)
	}
	return raw
}

// resultFor picks one table's outcome from a cycle report.
func resultFor(t *testing.T, res []TableResult, table TableName) TableResult {
	t.Helper()
	for _, r := range res {
		if r.Table == table {
			return r
		}
	}
	t.Fatalf("no result for %s in %+v", table, res)
	return TableResult{}
}

func TestFSArchiveSinkWritesVerifiedManifest(t *testing.T) {
	root := t.TempDir()
	sink := NewFSArchiveSink(root)
	data := append(line(t, RecordRequest, "a"), line(t, RecordRequest, "b")...)

	m, err := sink.Write(context.Background(), Manifest{
		ManifestVersion: manifestVersion, SchemaVersion: 11, Table: TableRequests,
		TimeColumn: "created_at", TimeFrom: time.Now().Add(-time.Hour), TimeTo: time.Now(),
	}, data)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if !m.Complete {
		t.Fatal("manifest must be complete after verification")
	}
	if m.RowCount != 2 {
		t.Fatalf("row count = %d, want 2", m.RowCount)
	}
	sum := sha256.Sum256(data)
	if m.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("sha256 mismatch: %s", m.SHA256)
	}
	if m.ManifestVersion != manifestVersion || m.SchemaVersion != 11 ||
		m.Table != TableRequests || m.TimeColumn != "created_at" {
		t.Fatalf("manifest metadata wrong: %+v", m)
	}

	stored, err := os.ReadFile(filepath.Join(root, m.DataFile))
	if err != nil {
		t.Fatalf("read data file: %v", err)
	}
	if string(stored) != string(data) {
		t.Fatal("stored bytes differ from the archived batch")
	}
	var manifest Manifest
	raw, err := os.ReadFile(filepath.Join(root, m.ArchiveID+".manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	if !manifest.Complete || manifest.RowCount != 2 || manifest.SHA256 != m.SHA256 {
		t.Fatalf("persisted manifest wrong: %+v", manifest)
	}
}

func TestFSArchiveSinkLeavesNoPartialArtifact(t *testing.T) {
	// A root path that collides with a regular file makes MkdirAll fail;
	// the sink must not leave artifacts behind and must report failure.
	root := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(root, []byte("x"), 0o640); err != nil {
		t.Fatal(err)
	}
	_, err := NewFSArchiveSink(root).Write(context.Background(), Manifest{Table: TableLedger}, line(t, RecordLedger, "a"))
	if err == nil {
		t.Fatal("write to unusable root must fail")
	}
	entries, err := os.ReadFile(root)
	if err != nil || string(entries) != "x" {
		t.Fatalf("root file must be untouched: %v %q", err, entries)
	}
}

// fakeStore is a scripted lifecycle.Store for sweeper tests.
type fakeStore struct {
	mu sync.Mutex

	policies   []Policy
	eligible   int64
	batches    []Batch // popped in order per select
	runID      int64
	runs       map[int64]RunView
	deleted    [][]string
	selects    int
	staleFails int
	sweptKeys  int
	schemaVer  int
}

func (f *fakeStore) RetentionPolicies(context.Context) ([]Policy, error) {
	return f.policies, nil
}

func (f *fakeStore) CountEligible(context.Context, TableName, time.Time) (int64, error) {
	return f.eligible, nil
}

func (f *fakeStore) SelectBatch(_ context.Context, table TableName, _ time.Time, _ int) (Batch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.selects++
	if len(f.batches) == 0 {
		return Batch{Table: table}, nil
	}
	b := f.batches[0]
	f.batches = f.batches[1:]
	return b, nil
}

func (f *fakeStore) DeleteBatch(_ context.Context, _ TableName, b Batch) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, b.Keys)
	return int64(len(b.Keys)), nil
}

func (f *fakeStore) MarkStaleRunsFailed(context.Context, time.Duration, time.Time) (int, error) {
	f.staleFails++
	return 0, nil
}

func (f *fakeStore) StartRun(_ context.Context, table TableName, dryRun bool, now time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.runs == nil {
		f.runs = map[int64]RunView{}
	}
	f.runID++
	f.runs[f.runID] = RunView{ID: f.runID, Table: table, Status: "running", StartedAt: now}
	_ = dryRun
	return f.runID, nil
}

func (f *fakeStore) FinishRun(_ context.Context, runID int64, status RunStatus, archived, deleted int64, detail json.RawMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.runs[runID]
	if !ok {
		return fmt.Errorf("unknown run %d", runID)
	}
	r.Status = string(status)
	r.RowsArchived, r.RowsDeleted = archived, deleted
	r.Detail = detail
	f.runs[runID] = r
	return nil
}

func (f *fakeStore) SchemaVersion(context.Context) (int, error) { return f.schemaVer, nil }

func (f *fakeStore) WriteOp(_ context.Context, _ mgmt.AdminOp) error { return nil }

func (f *fakeStore) SweepExpiredIdempotencyKeys(context.Context, time.Time) (int, error) {
	f.sweptKeys++
	return 0, nil
}

// countingSink counts Write calls and can be scripted to fail.
type countingSink struct {
	mu     sync.Mutex
	writes int
	failAt int // fail the n-th write (1-based); 0 = never
}

func (c *countingSink) Write(_ context.Context, m Manifest, data []byte) (Manifest, error) {
	c.mu.Lock()
	c.writes++
	n := c.writes
	c.mu.Unlock()
	if c.failAt == n {
		return Manifest{}, errors.New("sink failure")
	}
	sum := sha256.Sum256(data)
	m.SHA256 = hex.EncodeToString(sum[:])
	m.RowCount = strings.Count(string(data), "\n")
	m.Complete = true
	return m, nil
}

func newSweeper(t *testing.T, store Store, sink ArchiveSink) *Sweeper {
	t.Helper()
	sw, err := NewSweeper(SweeperDeps{Store: store, Sink: sink, Now: func() time.Time { return time.Unix(1_800_000_000, 0).UTC() }})
	if err != nil {
		t.Fatalf("new sweeper: %v", err)
	}
	return sw
}

func TestSweeperArchivesBeforeDeleteAndIsIdempotent(t *testing.T) {
	store := &fakeStore{
		policies:  []Policy{{Table: TableRequests, TTL: 3600, ArchiveBeforeDelete: true, Enabled: true}},
		schemaVer: 11,
		batches: []Batch{
			{Table: TableRequests, Count: 2, Keys: []string{"a", "b"},
				Lines: [][]byte{line(t, RecordRequest, "a"), line(t, RecordRequest, "b")}},
		},
	}
	sink := &countingSink{}
	sw := newSweeper(t, store, sink)

	res, err := sw.Run(context.Background(), Options{})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	r := resultFor(t, res, TableRequests)
	if r.Status != "completed" {
		t.Fatalf("results = %+v err=%v", res, err)
	}
	if r.Archived != 2 || r.Deleted != 2 {
		t.Fatalf("archived=%d deleted=%d, want 2/2", r.Archived, r.Deleted)
	}
	if len(store.deleted) != 1 || len(store.deleted[0]) != 2 {
		t.Fatalf("delete calls = %+v", store.deleted)
	}
	if sink.writes != 1 {
		t.Fatalf("sink writes = %d, want 1", sink.writes)
	}
	if store.runs[1].Status != "completed" {
		t.Fatalf("run = %+v", store.runs[1])
	}

	// Idempotent rerun: no eligible rows remain, nothing is archived or
	// deleted again.
	res, err = sw.Run(context.Background(), Options{})
	if err != nil {
		t.Fatalf("rerun: %v", err)
	}
	r = resultFor(t, res, TableRequests)
	if r.Archived != 0 || r.Deleted != 0 {
		t.Fatalf("rerun must be a no-op: %+v", r)
	}
	if store.staleFails != 2 {
		t.Fatalf("stale recovery ran %d times, want 2", store.staleFails)
	}
}

func TestSweeperDryRunTouchesNothing(t *testing.T) {
	store := &fakeStore{
		policies: []Policy{{Table: TableLedger, TTL: 60, ArchiveBeforeDelete: true, Enabled: true}},
		eligible: 42,
	}
	sink := &countingSink{}
	sw := newSweeper(t, store, sink)

	res, err := sw.Run(context.Background(), Options{DryRun: true})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	r := resultFor(t, res, TableLedger)
	if r.Status != "dry-run" || r.Eligible != 42 {
		t.Fatalf("dry-run result = %+v", r)
	}
	if sink.writes != 0 || len(store.deleted) != 0 || store.selects != 0 {
		t.Fatalf("dry run must archive and delete nothing: writes=%d deletes=%d selects=%d",
			sink.writes, len(store.deleted), store.selects)
	}
}

func TestSweeperLegalHoldSkipsTable(t *testing.T) {
	store := &fakeStore{
		policies: []Policy{
			{Table: TableRequests, TTL: 60, Enabled: false}, // legal hold
			{Table: TableJobs, TTL: 60, ArchiveBeforeDelete: true, Enabled: true},
		},
		batches: []Batch{{Table: TableJobs, Count: 1, Keys: []string{"j"}, Lines: [][]byte{line(t, RecordJob, "j")}}},
	}
	sink := &countingSink{}
	sw := newSweeper(t, store, sink)

	res, err := sw.Run(context.Background(), Options{})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	byTable := map[TableName]TableResult{}
	for _, r := range res {
		byTable[r.Table] = r
	}
	if byTable[TableRequests].Status != "skipped" || !strings.Contains(byTable[TableRequests].Detail, "legal hold") {
		t.Fatalf("disabled policy must be a hold: %+v", byTable[TableRequests])
	}
	if byTable[TableJobs].Status != "completed" {
		t.Fatalf("enabled policy must sweep: %+v", byTable[TableJobs])
	}
	if sink.writes != 1 || len(store.deleted) != 1 {
		t.Fatalf("held table must not be touched: writes=%d deletes=%d", sink.writes, len(store.deleted))
	}
}

func TestSweeperNoPolicyMeansKeepForever(t *testing.T) {
	store := &fakeStore{}
	sw := newSweeper(t, store, &countingSink{})
	res, err := sw.Run(context.Background(), Options{})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(res) != len(Tables) {
		t.Fatalf("every governed table must be reported: %+v", res)
	}
	for _, r := range res {
		if r.Status != "skipped" {
			t.Fatalf("absent policy must skip %s: %+v", r.Table, r)
		}
	}
}

func TestSweeperFailedArchiveDeletesNothing(t *testing.T) {
	store := &fakeStore{
		policies: []Policy{{Table: TableResults, TTL: 60, ArchiveBeforeDelete: true, Enabled: true}},
		batches:  []Batch{{Table: TableResults, Count: 1, Keys: []string{"j"}, Lines: [][]byte{line(t, RecordResult, "j")}}},
	}
	sink := &countingSink{failAt: 1}
	sw := newSweeper(t, store, sink)

	res, err := sw.Run(context.Background(), Options{})
	if err == nil {
		t.Fatal("failed archive must surface an error")
	}
	if resultFor(t, res, TableResults).Status != "failed" {
		t.Fatalf("table result = %+v", res)
	}
	if len(store.deleted) != 0 {
		t.Fatalf("failed archive must delete nothing: %+v", store.deleted)
	}
	if store.runs[1].Status != "failed" {
		t.Fatalf("run must be failed: %+v", store.runs[1])
	}
}

func TestSweeperRespectsBatchBound(t *testing.T) {
	store := &fakeStore{
		policies: []Policy{{Table: TableRequests, TTL: 60, Enabled: true}},
	}
	// More batches available than MaxBatches: the run stops at the bound.
	for i := 0; i < 5; i++ {
		store.batches = append(store.batches, Batch{
			Table: TableRequests, Count: 1, Keys: []string{fmt.Sprintf("k%d", i)},
			Lines: [][]byte{line(t, RecordRequest, fmt.Sprintf("k%d", i))},
		})
	}
	sw, err := NewSweeper(SweeperDeps{
		Store: store, Sink: &countingSink{}, MaxBatches: 3, BatchSize: 10,
		Now: func() time.Time { return time.Unix(1_800_000_000, 0).UTC() },
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := sw.Run(context.Background(), Options{Only: TableRequests})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if resultFor(t, res, TableRequests).Archived != 3 ||
		resultFor(t, res, TableRequests).Deleted != 3 {
		t.Fatalf("max-batches bound: %+v", res)
	}
}

func TestSweeperRunOrdersResultsBeforeJobs(t *testing.T) {
	store := &fakeStore{
		policies: []Policy{
			{Table: TableRequests, TTL: 1, Enabled: true},
			{Table: TableResults, TTL: 1, Enabled: true},
			{Table: TableJobs, TTL: 1, Enabled: true},
			{Table: TableLedger, TTL: 1, Enabled: true},
		},
	}
	sw := newSweeper(t, store, &countingSink{})
	res, err := sw.Run(context.Background(), Options{})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res[0].Table != TableRequests || res[1].Table != TableResults || res[2].Table != TableJobs || res[3].Table != TableLedger {
		t.Fatalf("sweep order must keep results before their owning jobs: %+v", res)
	}
}

func TestParseTableRejectsUnknown(t *testing.T) {
	if _, err := ParseTable("nope"); !errors.Is(err, ErrUnknownTable) {
		t.Fatalf("err = %v", err)
	}
	if _, err := ParseTable("tenants"); !errors.Is(err, ErrUnknownTable) {
		t.Fatalf("ungoverned tables must be rejected: %v", err)
	}
	for _, known := range Tables {
		if _, err := ParseTable(string(known)); err != nil {
			t.Fatalf("known table %s rejected: %v", known, err)
		}
	}
}

func TestPolicyInputValidation(t *testing.T) {
	if err := (PolicyInput{Table: TableRequests, TTL: 0}).Validate(); err == nil {
		t.Fatal("zero TTL must be rejected")
	}
	if err := (PolicyInput{Table: "tenants", TTL: 10}).Validate(); !errors.Is(err, ErrUnknownTable) {
		t.Fatal("ungoverned table must be rejected")
	}
	if err := (PolicyInput{Table: TableLedger, TTL: 1}).Validate(); err != nil {
		t.Fatalf("valid input rejected: %v", err)
	}
}
