package policy

import "testing"

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
