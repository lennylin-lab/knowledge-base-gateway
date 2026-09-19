package pg

// Env-gated tests for the accounting persistence boundary on the real
// schema: effective price-version selection, the exactly-once settlement
// contract (conditional updates plus the partial unique index), unknown-cost
// preservation with the schema CHECKs as the last line of defense, budget
// folding, and the admin mutation/audit atomicity. Run with
// TEST_DATABASE_URL set; the tests skip otherwise and are serialized against
// the other schema-driving suites with the same advisory lock.

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/accounting"
	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
)

func newLedgerTestStore(t *testing.T) (*LedgerStore, *sql.DB) {
	t.Helper()
	dsn := testDatabaseURL(t)
	ensureAsyncSchema(t, dsn)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, stmt := range []string{
		`INSERT INTO tenants (id, name) VALUES ('tenant_default', 'Default Tenant') ON CONFLICT (id) DO NOTHING`,
		`INSERT INTO tenants (id, name) VALUES ('tenant_eu', 'EU Tenant') ON CONFLICT (id) DO NOTHING`,
		`INSERT INTO subjects (id, tenant_id) VALUES ('subject_default', 'tenant_default') ON CONFLICT (id) DO NOTHING`,
		`INSERT INTO subjects (id, tenant_id) VALUES ('subject_eu', 'tenant_eu') ON CONFLICT (id) DO NOTHING`,
		`UPDATE model_catalog SET enabled = true WHERE public_name = 'gateway-echo'`,
		`DELETE FROM usage_ledger`,
		`DELETE FROM budget_policies`,
		`DELETE FROM pricing_catalog`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("seed: %v: %v", stmt, err)
		}
	}
	t.Cleanup(func() {
		for _, stmt := range []string{
			`DELETE FROM usage_ledger`, `DELETE FROM budget_policies`, `DELETE FROM pricing_catalog`,
		} {
			_, _ = db.Exec(stmt)
		}
	})
	ctx := context.Background()
	pool, err := Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pool connect: %v", err)
	}
	t.Cleanup(pool.Close)
	return &LedgerStore{DB: pool}, db
}

func mustExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("exec %s: %v", query, err)
	}
}

func i64p(v int64) *int64 { return &v }

// TestEffectivePriceSelection pins effective-at-time resolution: the greatest
// effective_from not after the requested instant, ties to the highest
// version; a time before every price is the (false, nil) unknown outcome.
func TestEffectivePriceSelection(t *testing.T) {
	s, db := newLedgerTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, row := range []struct {
		version int
		from    time.Time
		input   int64
	}{
		{1, base, 100},
		{2, base.AddDate(0, 1, 0), 200},
		{3, base.AddDate(0, 1, 0), 300}, // same instant as v2: highest version wins
	} {
		mustExec(t, db, `
			INSERT INTO pricing_catalog (provider, public_model, price_version, currency,
				input_micros_per_token, output_micros_per_token, effective_from)
			VALUES ('fake', 'gateway-echo', $1, 'USD', $2, $2, $3)`,
			row.version, row.input, row.from)
	}
	cases := []struct {
		at      time.Time
		version int
		input   int64
	}{
		{base, 1, 100},
		{base.AddDate(0, 1, -1), 1, 100},
		{base.AddDate(0, 1, 0), 3, 300},
		{base.AddDate(0, 2, 0), 3, 300},
	}
	for _, tc := range cases {
		p, ok, err := s.EffectivePrice(ctx, "fake", "gateway-echo", tc.at)
		if err != nil || !ok {
			t.Fatalf("at %s: ok=%v err=%v", tc.at, ok, err)
		}
		if p.Version != tc.version || p.InputPerToken != tc.input || p.Currency != "USD" {
			t.Fatalf("at %s: got v%d/%d, want v%d/%d", tc.at, p.Version, p.InputPerToken, tc.version, tc.input)
		}
	}
	if _, ok, err := s.EffectivePrice(ctx, "fake", "gateway-echo", base.AddDate(0, 0, -1)); ok || err != nil {
		t.Fatalf("before every price must be unknown: ok=%v err=%v", ok, err)
	}
	if _, ok, err := s.EffectivePrice(ctx, "fake", "other-model", base.AddDate(0, 2, 0)); ok || err != nil {
		t.Fatalf("unknown model must be unknown: ok=%v err=%v", ok, err)
	}
}

// TestLedgerExactlyOnceSettlement drives the reserve/settle/release lifecycle
// through the store: exactly one settled row per identity, repeated and
// racing finalizations are no-ops, and settle-after-release does nothing.
func TestLedgerExactlyOnceSettlement(t *testing.T) {
	s, db := newLedgerTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	mustExec(t, db, `
		INSERT INTO pricing_catalog (provider, public_model, price_version, currency,
			input_micros_per_token, output_micros_per_token, effective_from)
		VALUES ('fake', 'gateway-echo', 4, 'USD', 10, 20, now() - interval '1 hour')`)

	id := accounting.Identity{RequestID: "req-ledger-1"}
	if err := s.ReserveLedger(ctx, accounting.LedgerReservation{
		Identity: id, SubjectID: "subject_default",
		// Deliberately wrong: the stored tenant comes from the subject's
		// persisted binding, never from the (possibly empty) caller value.
		TenantID: "not-a-tenant",
		Protocol: "chat", PublicModel: "gateway-echo", CreatedAt: now,
	}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	var tenant string
	if err := db.QueryRow(`SELECT tenant_id FROM usage_ledger WHERE request_id = $1`, id.RequestID).Scan(&tenant); err != nil {
		t.Fatalf("read reserved row: %v", err)
	}
	if tenant != "tenant_default" {
		t.Fatalf("the ledger tenant must be the subject's persisted tenant: %q", tenant)
	}

	prompt, completion := int64(100), int64(50)
	reasoning := int64(10)
	out, err := s.SettleLedger(ctx, accounting.SettleQuery{
		Identity: id,
		Usage: accounting.Usage{
			PromptTokens: &prompt, CompletionTokens: &completion, ReasoningTokens: &reasoning,
		},
		CostMicros: i64p(100*10 + 50*20 + 10*5), PriceVersion: i64pInt(4), Currency: "USD",
		SettledAt: now,
	})
	if err != nil || !out.Settled {
		t.Fatalf("settle: out=%+v err=%v", out, err)
	}
	// The stored row carries the exact tokens, version, and cost.
	var cost, version sql.NullInt64
	var pt, ct, rt sql.NullInt64
	if err := db.QueryRow(`
		SELECT cost_micros, price_version, prompt_tokens, completion_tokens, reasoning_tokens
		FROM usage_ledger WHERE request_id = $1 AND settle_status = 'settled'`, id.RequestID).
		Scan(&cost, &version, &pt, &ct, &rt); err != nil {
		t.Fatalf("read settled row: %v", err)
	}
	if !cost.Valid || cost.Int64 != 100*10+50*20+10*5 || !version.Valid || version.Int64 != 4 {
		t.Fatalf("settled row: cost=%+v version=%+v", cost, version)
	}
	if !pt.Valid || pt.Int64 != 100 || !ct.Valid || ct.Int64 != 50 || !rt.Valid || rt.Int64 != 10 {
		t.Fatalf("token columns: %+v %+v %+v", pt, ct, rt)
	}

	// Repeated settlement is the no-op outcome, never a second row.
	out, err = s.SettleLedger(ctx, accounting.SettleQuery{Identity: id, SettledAt: now})
	if err != nil {
		t.Fatalf("repeat settle: %v", err)
	}
	if out.Settled {
		t.Fatal("repeat settlement must report Settled=false")
	}
	if n := countRows(t, db, `SELECT count(*) FROM usage_ledger WHERE request_id = $1`, id.RequestID); n != 1 {
		t.Fatalf("exactly one row per identity: %d", n)
	}

	// A reservation for an unknown subject fails loudly (the authentication
	// layer guarantees existence, so this would be an invariant violation).
	err = s.ReserveLedger(ctx, accounting.LedgerReservation{
		Identity:  accounting.Identity{RequestID: "req-ledger-ghost"},
		SubjectID: "no-such-subject", TenantID: "tenant_default",
		Protocol: "chat", PublicModel: "gateway-echo", CreatedAt: now,
	})
	if err == nil {
		t.Fatal("an unknown subject must fail the reservation")
	}

	// Release after settle is a no-op (nothing reserved anymore).
	if err := s.ReleaseLedger(ctx, id); err != nil {
		t.Fatalf("release after settle: %v", err)
	}

	// A second identity: reserve then release, then settle-after-release is
	// the no-op outcome and the row stays released.
	id2 := accounting.Identity{RequestID: "req-ledger-2"}
	if err := s.ReserveLedger(ctx, accounting.LedgerReservation{
		Identity: id2, SubjectID: "subject_default", TenantID: "tenant_default",
		Protocol: "chat", PublicModel: "gateway-echo", CreatedAt: now,
	}); err != nil {
		t.Fatalf("reserve 2: %v", err)
	}
	if err := s.ReleaseLedger(ctx, id2); err != nil {
		t.Fatalf("release: %v", err)
	}
	out, err = s.SettleLedger(ctx, accounting.SettleQuery{Identity: id2, SettledAt: now})
	if err != nil || out.Settled {
		t.Fatalf("settle-after-release must be a no-op: out=%+v err=%v", out, err)
	}
	if got := countRows(t, db, `SELECT count(*) FROM usage_ledger WHERE request_id = $1 AND settle_status = 'released'`, id2.RequestID); got != 1 {
		t.Fatalf("released row: %d", got)
	}
}

func i64pInt(v int) *int { return &v }

func countRows(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

// TestLedgerUniqueIndexBlocksDoubleSettlement: the schema's partial unique
// index is the last line of defense — two settled rows for one identity are
// rejected at the database even when written by hand.
func TestLedgerUniqueIndexBlocksDoubleSettlement(t *testing.T) {
	s, db := newLedgerTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	id := accounting.Identity{JobID: "11111111-1111-1111-1111-111111111111"}
	// The ledger FK requires a real async job; create one through the store.
	jobs := &AsyncStore{DB: s.DB}
	if _, err := jobs.Create(ctx, asyncCreateInput(id.JobID, now)); err != nil {
		t.Fatalf("seed job: %v", err)
	}
	if err := s.ReserveLedger(ctx, accounting.LedgerReservation{
		Identity: id, SubjectID: "subject_default", TenantID: "tenant_default",
		Protocol: "responses", PublicModel: "gateway-echo", CreatedAt: now,
	}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if err := s.ReleaseLedger(ctx, id); err != nil {
		t.Fatalf("release: %v", err)
	}
	// A released row keeps its identity: inserting a hand-settled row for the
	// same job identity succeeds once and must be rejected twice.
	mustExec(t, db, `
		INSERT INTO usage_ledger (job_id, subject_id, tenant_id, protocol, public_model,
			settle_status, settled_at, created_at)
		VALUES ($1, 'subject_default', 'tenant_default', 'responses', 'gateway-echo',
			'settled', now(), now())`, id.JobID)
	if _, err := db.Exec(`
		INSERT INTO usage_ledger (job_id, subject_id, tenant_id, protocol, public_model,
			settle_status, settled_at, created_at)
		VALUES ($1, 'subject_default', 'tenant_default', 'responses', 'gateway-echo',
			'settled', now(), now())`, id.JobID); err == nil {
		t.Fatal("the partial unique index must reject a second settled row per identity")
	}
}

// TestLedgerUnknownCostPreservedBySchema: settlement without usage keeps
// every money/token column NULL, and the schema CHECKs reject a cost without
// a price version or a negative amount — application bugs cannot fabricate.
func TestLedgerUnknownCostPreservedBySchema(t *testing.T) {
	s, db := newLedgerTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	id := accounting.Identity{RequestID: "req-ledger-unknown"}
	if err := s.ReserveLedger(ctx, accounting.LedgerReservation{
		Identity: id, SubjectID: "subject_default", TenantID: "tenant_default",
		Protocol: "embeddings", PublicModel: "gateway-echo", CreatedAt: now,
	}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := s.SettleLedger(ctx, accounting.SettleQuery{Identity: id, SettledAt: now}); err != nil {
		t.Fatalf("settle without usage: %v", err)
	}
	var cost, version sql.NullInt64
	if err := db.QueryRow(`
		SELECT cost_micros, price_version FROM usage_ledger
		WHERE request_id = $1 AND settle_status = 'settled'`, id.RequestID).
		Scan(&cost, &version); err != nil {
		t.Fatalf("read: %v", err)
	}
	if cost.Valid || version.Valid {
		t.Fatalf("unknown settlement must stay NULL: cost=%+v version=%+v", cost, version)
	}

	// Cost without price version violates the CHECK.
	if _, err := db.Exec(`
		INSERT INTO usage_ledger (request_id, subject_id, tenant_id, protocol, public_model,
			settle_status, settled_at, cost_micros, created_at)
		VALUES ('req-check-1', 'subject_default', 'tenant_default', 'chat', 'gateway-echo',
			'settled', now(), 5, now())`); err == nil {
		t.Fatal("a priced settlement must be forced to name its price version")
	}
	// Negative cost violates the CHECK.
	if _, err := db.Exec(`
		INSERT INTO usage_ledger (request_id, subject_id, tenant_id, protocol, public_model,
			settle_status, settled_at, price_version, currency, cost_micros, created_at)
		VALUES ('req-check-2', 'subject_default', 'tenant_default', 'chat', 'gateway-echo',
			'settled', now(), 1, 'USD', -5, now())`); err == nil {
		t.Fatal("negative money must be rejected by the schema")
	}
}

// TestBudgetLimitsFoldAndDimensions: enabled subject and tenant rows fold
// through the shared helper; disabled rows are excluded.
func TestBudgetLimitsFoldAndDimensions(t *testing.T) {
	s, db := newLedgerTestStore(t)
	ctx := context.Background()
	mustExec(t, db, `
		INSERT INTO budget_policies (scope, subject_id, tenant_id, period, currency, amount_micros)
		VALUES ('subject', 'subject_default', 'tenant_default', 'daily', 'USD', 5_000_000),
		       ('subject', 'subject_default', 'tenant_default', 'monthly', 'USD', 90_000_000),
		       ('tenant', NULL, 'tenant_default', 'monthly', 'USD', 40_000_000)`)
	// A disabled row must not constrain.
	mustExec(t, db, `
		INSERT INTO budget_policies (scope, subject_id, tenant_id, period, currency, amount_micros, enabled)
		VALUES ('tenant', NULL, 'tenant_default', 'daily', 'USD', 1, false)`)

	limits, err := s.BudgetLimits(ctx, "subject_default", "tenant_default")
	if err != nil {
		t.Fatalf("budget limits: %v", err)
	}
	// The min-of-declared fold is pinned on the pure helper
	// (accounting.FoldBudgetRows); the schema's unique target index keeps one
	// row per target/period/currency, so the store fold is identity here.
	if limits.Currency != "USD" || limits.SubjectDaily != 5_000_000 ||
		limits.SubjectMonthly != 90_000_000 || limits.TenantMonthly != 40_000_000 || limits.TenantDaily != 0 {
		t.Fatalf("folded limits: %+v", limits)
	}
	// Another tenant's budgets must not leak into the subject's evaluation.
	limits, err = s.BudgetLimits(ctx, "subject_eu", "tenant_eu")
	if err != nil || limits.Configured() {
		t.Fatalf("foreign tenant must be uncapped: %+v err=%v", limits, err)
	}
	// The tenant dimension is derived from the subject's persisted binding:
	// even an empty caller-side tenant resolves tenant_default's budgets.
	limits, err = s.BudgetLimits(ctx, "subject_default", "")
	if err != nil || limits.TenantMonthly != 40_000_000 || limits.SubjectDaily != 5_000_000 {
		t.Fatalf("subject-derived tenant fold: %+v err=%v", limits, err)
	}
}

// TestUpsertPriceAndBudgetAtomicWithAudit: the management mutations write
// their audit record in the same transaction and are idempotent upserts.
func TestUpsertPriceAndBudgetAtomicWithAudit(t *testing.T) {
	s, db := newLedgerTestStore(t)
	ctx := context.Background()
	now := time.Now()

	in := mgmt.PriceInput{
		Provider: "fake", PublicModel: "gateway-echo", PriceVersion: 9, Currency: "USD",
		InputMicrosPerToken: 1, OutputMicrosPerToken: 2,
		ReasoningMicrosPerToken: i64p(3), EffectiveFrom: now,
	}
	if err := s.UpsertPrice(ctx, in, mgmt.AdminOp{CreatedAt: now, Action: "price_upsert", Target: "fake/gateway-echo/v9"}); err != nil {
		t.Fatalf("upsert price: %v", err)
	}
	// Idempotent update of the same version.
	in.OutputMicrosPerToken = 5
	if err := s.UpsertPrice(ctx, in, mgmt.AdminOp{CreatedAt: now, Action: "price_upsert", Target: "fake/gateway-echo/v9"}); err != nil {
		t.Fatalf("re-upsert price: %v", err)
	}
	prices, err := s.ListPrices(ctx)
	if err != nil {
		t.Fatalf("list prices: %v", err)
	}
	var found int
	for _, p := range prices {
		if p.Provider == "fake" && p.PriceVersion == 9 {
			found++
			if p.OutputMicrosPerToken != 5 || p.ReasoningMicrosPerToken == nil || *p.ReasoningMicrosPerToken != 3 {
				t.Fatalf("upsert did not update: %+v", p)
			}
		}
	}
	if found != 1 {
		t.Fatalf("exactly one row per version: %d", found)
	}

	enabled := true
	budget := mgmt.BudgetInput{
		Scope: "subject", SubjectID: "subject_default", TenantID: "tenant_default",
		Period: "daily", Currency: "USD", AmountMicros: 7_000_000, Enabled: &enabled,
	}
	if err := s.UpsertBudget(ctx, budget, mgmt.AdminOp{CreatedAt: now, Action: "budget_upsert", Target: "ledger-test-subject"}); err != nil {
		t.Fatalf("upsert budget: %v", err)
	}
	budget.AmountMicros = 8_000_000
	if err := s.UpsertBudget(ctx, budget, mgmt.AdminOp{CreatedAt: now, Action: "budget_upsert", Target: "ledger-test-subject"}); err != nil {
		t.Fatalf("re-upsert budget: %v", err)
	}
	budgets, err := s.ListBudgets(ctx, "")
	if err != nil {
		t.Fatalf("list budgets: %v", err)
	}
	found = 0
	for _, b := range budgets {
		if b.Scope == "subject" && b.SubjectID == "subject_default" {
			found++
			if b.AmountMicros != 8_000_000 || !b.Enabled {
				t.Fatalf("budget upsert did not update: %+v", b)
			}
		}
	}
	if found != 1 {
		t.Fatalf("the unique target index must collapse duplicates: %d", found)
	}

	// Both mutations wrote their audit rows (scoped to this test's targets,
	// since admin_audit is shared across suites).
	if n := countRows(t, db, `SELECT count(*) FROM admin_audit
		WHERE (action = 'price_upsert' AND target = 'fake/gateway-echo/v9')
		   OR (action = 'budget_upsert' AND target = 'ledger-test-subject')`); n != 4 {
		t.Fatalf("each mutation must carry exactly one audit row: %d", n)
	}
}

// TestBudgetUsageCurrentPeriod: utilization is computed from settled ledger
// cost in the current UTC period; unknown-cost settlements are counted
// separately and never as zero spend.
func TestBudgetUsageCurrentPeriod(t *testing.T) {
	s, db := newLedgerTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	mustExec(t, db, `
		INSERT INTO budget_policies (scope, subject_id, tenant_id, period, currency, amount_micros)
		VALUES ('subject', 'subject_default', 'tenant_default', 'daily', 'USD', 1_000_000),
		       ('tenant', NULL, 'tenant_default', 'monthly', 'USD', 10_000_000)`)

	insertSettled := func(requestID string, cost *int64) {
		t.Helper()
		mustExec(t, db, `
			INSERT INTO usage_ledger (request_id, subject_id, tenant_id, protocol, public_model,
				settle_status, settled_at, price_version, currency, cost_micros, created_at)
			VALUES ($1, 'subject_default', 'tenant_default', 'chat', 'gateway-echo',
				'settled', now(), 1, 'USD', $2, now())`, requestID, cost)
	}
	insertSettled("req-usage-known-1", i64p(120))
	insertSettled("req-usage-known-2", i64p(80))
	insertSettled("req-usage-unknown", nil)

	usage, err := s.BudgetUsage(ctx, now, "")
	if err != nil {
		t.Fatalf("budget usage: %v", err)
	}
	var subjectDaily, tenantMonthly *mgmt.BudgetUsageView
	for i := range usage {
		v := usage[i]
		switch {
		case v.Scope == "subject" && v.Period == "daily":
			subjectDaily = &usage[i]
		case v.Scope == "tenant" && v.Period == "monthly":
			tenantMonthly = &usage[i]
		}
	}
	if subjectDaily == nil || subjectDaily.UsedMicros != 200 || subjectDaily.UnknownCostSettlements != 1 {
		t.Fatalf("subject daily usage: %+v", subjectDaily)
	}
	if tenantMonthly == nil || tenantMonthly.UsedMicros != 200 || tenantMonthly.UnknownCostSettlements != 1 {
		t.Fatalf("tenant monthly usage: %+v", tenantMonthly)
	}
}

// TestLedgerReservedBacklogReadinessSignal proves the settlement-backlog
// readiness query on the real schema: fresh reserved rows never count, rows
// older than the window do, and settled/released rows are invisible.
func TestLedgerReservedBacklogReadinessSignal(t *testing.T) {
	s, db := newLedgerTestStore(t)
	ctx := context.Background()
	now := time.Now()

	if n, err := s.ReservedBacklog(ctx, 10*time.Minute); err != nil || n != 0 {
		t.Fatalf("empty ledger backlog = %d err=%v, want 0", n, err)
	}

	// A fresh reserved row: in-flight, not backlog.
	fresh := accounting.Identity{RequestID: "req-fresh"}
	if err := s.ReserveLedger(ctx, accounting.LedgerReservation{
		Identity: fresh, SubjectID: "subject_default", TenantID: "tenant_default",
		Protocol: "responses", PublicModel: "gateway-echo", CreatedAt: now,
	}); err != nil {
		t.Fatalf("reserve fresh: %v", err)
	}
	// A stale reserved row: backdated past the window (request-id identity:
	// job_id carries a foreign key into async_jobs, so sync rows are the
	// simplest fixture here).
	stale := accounting.Identity{RequestID: "req-stale-backlog"}
	if err := s.ReserveLedger(ctx, accounting.LedgerReservation{
		Identity: stale, SubjectID: "subject_default", TenantID: "tenant_default",
		Protocol: "responses", PublicModel: "gateway-echo", CreatedAt: now.Add(-30 * time.Minute),
	}); err != nil {
		t.Fatalf("reserve stale: %v", err)
	}
	mustExec(t, db, `UPDATE usage_ledger SET created_at = $1 WHERE request_id IS NOT DISTINCT FROM $2`,
		now.Add(-30*time.Minute), "req-stale-backlog")

	n, err := s.ReservedBacklog(ctx, 10*time.Minute)
	if err != nil {
		t.Fatalf("backlog: %v", err)
	}
	if n != 1 {
		t.Fatalf("backlog = %d, want exactly the one stale reserved row", n)
	}

	// Settling the stale row clears the backlog signal.
	if _, err := s.SettleLedger(ctx, accounting.SettleQuery{
		Identity: stale, SettledAt: now,
	}); err != nil {
		t.Fatalf("settle stale: %v", err)
	}
	if n, err := s.ReservedBacklog(ctx, 10*time.Minute); err != nil || n != 0 {
		t.Fatalf("backlog after settle = %d err=%v, want 0", n, err)
	}
}
