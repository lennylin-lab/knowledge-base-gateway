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
