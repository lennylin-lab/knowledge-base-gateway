package async

// KeyTTL contract tests (in-memory store, mirrored by the PostgreSQL
// implementation): an idempotency key replays only inside its expires_at
// window, a different digest inside the window is a conflict, and the sweep
// removes exactly the expired mappings. The data-lifecycle child owns this
// enforcement (previously expires_at was persisted but never enforced).

import (
	"context"
	"testing"
	"time"
)

func TestIdempotencyKeyTTLReplayWindow(t *testing.T) {
	s := NewMemoryStore(nil)
	ctx := context.Background()
	base := time.Unix(1_800_000_000, 0)
	in := CreateInput{
		JobID: "resp_ttl_1", SubjectID: "subject-a", TenantID: "tenant-a",
		Protocol: "responses", PublicModel: "model-x",
		RequestDigest: "digest-1", Request: []byte(`{}`),
		KeyHash: "hash-1", KeyTTL: time.Hour, Now: base,
	}
	if _, err := s.Create(ctx, in); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Inside the window: same key + digest replays the original job.
	out, err := s.Create(ctx, func() CreateInput {
		c := in
		c.JobID = "resp_ttl_2"
		c.Now = base.Add(30 * time.Minute)
		return c
	}())
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !out.Replay || out.Job.ID != "resp_ttl_1" {
		t.Fatalf("inside-window replay = %+v", out)
	}

	// Inside the window: a different digest under the same key conflicts.
	_, err = s.Create(ctx, func() CreateInput {
		c := in
		c.JobID = "resp_ttl_3"
		c.RequestDigest = "digest-other"
		c.Now = base.Add(45 * time.Minute)
		return c
	}())
	if err != ErrConflict {
		t.Fatalf("inside-window conflict err = %v", err)
	}

	// Outside the window: the expired key no longer replays — the request
	// creates a fresh job.
	out, err = s.Create(ctx, func() CreateInput {
		c := in
		c.JobID = "resp_ttl_4"
		c.Now = base.Add(2 * time.Hour)
		return c
	}())
	if err != nil {
		t.Fatalf("post-expiry create: %v", err)
	}
	if out.Replay || out.Job.ID != "resp_ttl_4" {
		t.Fatalf("expired key must not replay: %+v", out)
	}
}

func TestSweepExpiredIdempotencyKeys(t *testing.T) {
	s := NewMemoryStore(nil)
	ctx := context.Background()
	base := time.Unix(1_800_000_000, 0)

	for i, ttl := range []time.Duration{time.Hour, 2 * time.Hour} {
		in := CreateInput{
			JobID: string(rune('a'+i)) + "-job", SubjectID: "subject-a", TenantID: "tenant-a",
			Protocol: "responses", PublicModel: "model-x",
			RequestDigest: "digest", Request: []byte(`{}`),
			KeyHash: "hash-" + string(rune('a'+i)), KeyTTL: ttl, Now: base,
		}
		if _, err := s.Create(ctx, in); err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
	}

	// At +90m: only the 1h key is expired; the sweep removes exactly it.
	n, err := s.SweepExpiredIdempotencyKeys(ctx, base.Add(90*time.Minute))
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n != 1 {
		t.Fatalf("swept = %d, want 1", n)
	}

	// The surviving key still replays; the swept one does not.
	out, err := s.Create(ctx, CreateInput{
		JobID: "b-new", SubjectID: "subject-a", TenantID: "tenant-a",
		Protocol: "responses", PublicModel: "model-x",
		RequestDigest: "digest", Request: []byte(`{}`),
		KeyHash: "hash-b", KeyTTL: time.Hour, Now: base.Add(90 * time.Minute),
	})
	if err != nil || !out.Replay || out.Job.ID != "b-job" {
		t.Fatalf("surviving key must replay: %+v err=%v", out, err)
	}
	out, err = s.Create(ctx, CreateInput{
		JobID: "a-new", SubjectID: "subject-a", TenantID: "tenant-a",
		Protocol: "responses", PublicModel: "model-x",
		RequestDigest: "digest", Request: []byte(`{}`),
		KeyHash: "hash-a", KeyTTL: time.Hour, Now: base.Add(90 * time.Minute),
	})
	if err != nil || out.Replay {
		t.Fatalf("swept key must not replay: %+v err=%v", out, err)
	}
}
