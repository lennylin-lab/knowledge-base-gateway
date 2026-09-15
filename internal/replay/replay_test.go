package replay

// The replay suite is itself part of the validation gate: every bundled
// fixture must pass offline, and the bundled set must stay non-empty so the
// standalone command never silently becomes a no-op.

import (
	"context"
	"testing"
)

func TestBundledFixturesReplayOffline(t *testing.T) {
	fixtures, err := Bundled()
	if err != nil {
		t.Fatalf("load bundled fixtures: %v", err)
	}
	if len(fixtures) < 8 {
		t.Fatalf("bundled fixture set unexpectedly small: %d", len(fixtures))
	}
	seen := map[string]bool{}
	for _, f := range fixtures {
		if seen[f.Name] {
			t.Errorf("duplicate fixture name %q", f.Name)
		}
		seen[f.Name] = true
		if err := Run(context.Background(), f); err != nil {
			t.Errorf("%s: %v", f.Name, err)
		}
	}
}

func TestRunRejectsUnknownProvider(t *testing.T) {
	err := Run(context.Background(), Fixture{Name: "bad", Provider: "gopher"})
	if err == nil {
		t.Fatal("unknown provider must fail")
	}
}

func TestRunRejectsNetworkAdapterWithoutStub(t *testing.T) {
	err := Run(context.Background(), Fixture{Name: "no-stub", Provider: "openai"})
	if err == nil {
		t.Fatal("network adapter without an upstream stub must fail")
	}
}
