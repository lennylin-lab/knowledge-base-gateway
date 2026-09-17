package policy

import (
	"context"
	"testing"
)

// TestLimitsRoundTripIncludingMaxInputTokens pins AC2: the in-memory policy
// limits can represent the persisted access_policies.max_input_tokens column
// and round-trip it together with the existing ceilings.
func TestLimitsRoundTripIncludingMaxInputTokens(t *testing.T) {
	p := New()
	if _, ok := p.LimitsFor("subject-1"); ok {
		t.Fatal("no limits must exist before SetLimits")
	}
	want := Limits{
		RatePerMinute:   120,
		MaxConcurrent:   8,
		DailyTokens:     1_000_000,
		MonthlyTokens:   10_000_000,
		MaxInputTokens:  4096,
		MaxOutputTokens: 512,
	}
	p.SetLimits("subject-1", want)
	got, ok := p.LimitsFor("subject-1")
	if !ok {
		t.Fatal("limits must exist after SetLimits")
	}
	if got != want {
		t.Fatalf("limits round-trip mismatch: got %+v, want %+v", got, want)
	}
}

// TestUnsetInputCeilingStaysZero pins the zero-means-unset convention: a
// subject without a configured input ceiling keeps MaxInputTokens zero so
// admission skips the subject-level check.
func TestUnsetInputCeilingStaysZero(t *testing.T) {
	p := New()
	p.SetLimits("subject-1", Limits{RatePerMinute: 10, MaxConcurrent: 2})
	got, ok := p.LimitsFor("subject-1")
	if !ok || got.MaxInputTokens != 0 || got.MaxOutputTokens != 0 {
		t.Fatalf("unset ceilings must stay zero, got ok=%v %+v", ok, got)
	}
}

// TestFoldPolicyRowsFirstNonEmptyDefaults pins issue #7: when a subject's
// access_policies rows are folded in id order, the default-model slots take
// the first non-empty value — a row with an empty slot never erases a default
// declared on an earlier row, and the chat and embedding slots are judged
// independently. Ceilings fold to the minimum declared value (issue #8).
func TestFoldPolicyRowsFirstNonEmptyDefaults(t *testing.T) {
	rows := []Limits{
		{RatePerMinute: 30, MaxConcurrent: 3, DailyTokens: 300,
			DefaultModel: "chat-a"},
		{RatePerMinute: 20, MaxConcurrent: 2},
		{RatePerMinute: 10, MaxConcurrent: 1, DailyTokens: 100, MonthlyTokens: 1000,
			MaxInputTokens: 512, MaxOutputTokens: 64,
			DefaultEmbeddingModel: "embed-c"},
	}
	var got Limits
	for _, row := range rows {
		got.FoldPolicyRow(row)
	}
	want := Limits{
		RatePerMinute: 10, MaxConcurrent: 1, DailyTokens: 100,
		MonthlyTokens: 1000, MaxInputTokens: 512, MaxOutputTokens: 64,
		DefaultModel: "chat-a", DefaultEmbeddingModel: "embed-c",
	}
	if got != want {
		t.Fatalf("folded limits = %+v, want %+v", got, want)
	}
}

// TestFoldPolicyRowsMinOfDeclaredCeilings pins the issue #8 ceiling fold:
// each ceiling is the minimum declared across the subject's rows regardless
// of row order; a row that leaves a nullable cap unset does not cap the
// subject (passthrough), and a field no row declares stays zero (uncapped).
func TestFoldPolicyRowsMinOfDeclaredCeilings(t *testing.T) {
	rows := []Limits{
		{RatePerMinute: 30, MaxConcurrent: 4, DailyTokens: 500, MonthlyTokens: 9000},
		{RatePerMinute: 10, MaxConcurrent: 2},
		{RatePerMinute: 20, MaxConcurrent: 8, MaxInputTokens: 2048},
	}
	want := Limits{
		RatePerMinute: 10, MaxConcurrent: 2, DailyTokens: 500,
		MonthlyTokens: 9000, MaxInputTokens: 2048, MaxOutputTokens: 0,
	}
	// Ascending id order and a shuffled order must fold identically: the
	// ceiling fold is order-independent.
	shuffled := []Limits{rows[2], rows[0], rows[1]}
	for name, set := range map[string][]Limits{"id-order": rows, "shuffled": shuffled} {
		var got Limits
		for _, row := range set {
			got.FoldPolicyRow(row)
		}
		if got != want {
			t.Fatalf("%s: folded limits = %+v, want %+v", name, got, want)
		}
	}
	// A tighter row arriving after looser ones must tighten: adding a row can
	// never raise a quota.
	var tightened Limits
	tightened.FoldPolicyRow(rows[0])
	tightened.FoldPolicyRow(Limits{RatePerMinute: 5, MaxConcurrent: 1, DailyTokens: 100})
	if tightened.RatePerMinute != 5 || tightened.MaxConcurrent != 1 || tightened.DailyTokens != 100 {
		t.Fatalf("later tighter row must tighten every declared ceiling, got %+v", tightened)
	}
	if tightened.MonthlyTokens != 9000 {
		t.Fatalf("ceilings the tighter row does not declare must keep earlier values, got %+v", tightened)
	}
	if tightened.MaxInputTokens != 0 || tightened.MaxOutputTokens != 0 {
		t.Fatalf("fields no row declares must stay zero, got %+v", tightened)
	}
}

// TestFoldPolicyRowsAllNullDefaultsStayEmpty pins the unchanged 400 path: a
// subject whose rows all leave the slots NULL keeps no default, while the
// ceilings fold to the minimum declared value.
func TestFoldPolicyRowsAllNullDefaultsStayEmpty(t *testing.T) {
	var got Limits
	got.FoldPolicyRow(Limits{RatePerMinute: 5})
	got.FoldPolicyRow(Limits{RatePerMinute: 7})
	if got.DefaultModel != "" || got.DefaultEmbeddingModel != "" {
		t.Fatalf("all-NULL rows must keep both slots empty, got %+v", got)
	}
	if got.RatePerMinute != 5 {
		t.Fatalf("ceilings must fold to the minimum declared, got %+v", got)
	}
}

// TestFoldPolicyRowSingleRowIdentity pins that a one-row subject folds to
// itself — the fix must not change single-row subjects at all.
func TestFoldPolicyRowSingleRowIdentity(t *testing.T) {
	row := Limits{
		RatePerMinute: 9, MaxConcurrent: 4, DailyTokens: 50,
		DefaultModel: "m", DefaultEmbeddingModel: "e",
	}
	var got Limits
	got.FoldPolicyRow(row)
	if got != row {
		t.Fatalf("single row must fold to itself, got %+v want %+v", got, row)
	}
}

// TestResolverLimitsForPooledBehavior pins that Resolver.LimitsFor — the
// admission layer's single limit-resolution seam — returns exactly the
// subject's pooled limits today: identical for every public model (quota is
// subject-pooled, per-model overrides are reserved, not implemented), and the
// zero Limits with a nil error for subjects without an explicit policy, so
// callers skip gating exactly as before the seam existed.
func TestResolverLimitsForPooledBehavior(t *testing.T) {
	p := New()
	r := NewResolver(p)
	ctx := context.Background()

	got, err := r.LimitsFor(ctx, "subject-1", "model-a")
	if err != nil {
		t.Fatalf("no-policy subject must resolve without error, got %v", err)
	}
	if got != (Limits{}) {
		t.Fatalf("no-policy subject must resolve to zero limits, got %+v", got)
	}

	want := Limits{RatePerMinute: 10, MaxConcurrent: 2, DailyTokens: 1000, MonthlyTokens: 9000, MaxOutputTokens: 64}
	p.SetLimits("subject-1", want)
	for _, m := range []string{"model-a", "model-b", "embed-c"} {
		got, err = r.LimitsFor(ctx, "subject-1", m)
		if err != nil {
			t.Fatalf("%s: resolver error %v", m, err)
		}
		if got != want {
			t.Fatalf("%s: limits = %+v, want the subject's pooled %+v (model-independent)", m, got, want)
		}
	}
}

// TestCatalogSetEntryAndRemove pins the management runtime-refresh primitives:
// SetEntry inserts unknown models and replaces known ones (fresh capabilities
// and configuration version), a disabled entry hides the model from Lookup,
// and Remove drops entries.
func TestCatalogSetEntryAndRemove(t *testing.T) {
	c := NewCatalog([]ModelInfo{{PublicName: "m", Provider: "p1", UpstreamModel: "u", Enabled: true, ConfigVersion: 1}})
	if _, ok := c.Lookup("m"); !ok {
		t.Fatal("seed entry must resolve")
	}

	// Replace with fresh configuration; resolution keeps working.
	c.SetEntry(ModelInfo{PublicName: "m", Provider: "p2", UpstreamModel: "u2", Enabled: true, ConfigVersion: 7})
	info, ok := c.Lookup("m")
	if !ok || info.Provider != "p2" || info.ConfigVersion != 7 {
		t.Fatalf("SetEntry must replace the row, got %+v ok=%v", info, ok)
	}

	// A disabled entry hides the model from resolution but stays listed.
	c.SetEntry(ModelInfo{PublicName: "m", Provider: "p2", Enabled: false})
	if _, ok := c.Lookup("m"); ok {
		t.Fatal("disabled entry must not resolve")
	}
	if len(c.All()) != 1 {
		t.Fatalf("disabled entry must stay in the catalog, got %d rows", len(c.All()))
	}

	// Unknown models can be added for the re-enable path.
	c.SetEntry(ModelInfo{PublicName: "late", Provider: "p", Enabled: true})
	if _, ok := c.Lookup("late"); !ok {
		t.Fatal("SetEntry must add unknown models")
	}

	// Remove drops entries and reports whether they existed.
	if !c.Remove("late") || c.Remove("late") {
		t.Fatal("Remove must report prior existence exactly once")
	}
	if _, ok := c.Lookup("late"); ok {
		t.Fatal("removed entry must not resolve")
	}
}
