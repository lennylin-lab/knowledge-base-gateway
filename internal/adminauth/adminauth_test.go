package adminauth

import (
	"context"
	"strings"
	"testing"
	"time"
)

func testManager() (*Manager, *MemoryStore) {
	s := NewMemoryStore()
	return NewManager(s), s
}

func TestScopeImplications(t *testing.T) {
	// platform-admin is the superset; operator and billing carry viewer.
	if !(ScopePlatformAdmin).Satisfies(ScopeViewer) ||
		!(ScopePlatformAdmin).Satisfies(ScopeOperator) ||
		!(ScopePlatformAdmin).Satisfies(ScopeBilling) {
		t.Fatal("platform-admin must imply every other scope")
	}
	if !(ScopeOperator).Satisfies(ScopeViewer) || !(ScopeBilling).Satisfies(ScopeViewer) {
		t.Fatal("operator and billing must imply viewer")
	}
	if (ScopeViewer).Satisfies(ScopeOperator) || (ScopeOperator).Satisfies(ScopeBilling) ||
		(ScopeBilling).Satisfies(ScopeOperator) {
		t.Fatal("no upward implication is allowed")
	}
}

func TestParseScopesClosedVocabulary(t *testing.T) {
	scopes, err := ParseScopes([]string{"viewer", "billing", "billing"})
	if err != nil {
		t.Fatalf("valid scopes rejected: %v", err)
	}
	if len(scopes) != 2 || scopes[0] != ScopeBilling || scopes[1] != ScopeViewer {
		t.Fatalf("scopes must dedupe and sort, got %v", scopes)
	}
	if _, err := ParseScopes([]string{"superuser"}); err == nil {
		t.Fatal("scope outside the closed vocabulary must be rejected")
	}
	if _, err := ParseScopes(nil); err == nil {
		t.Fatal("empty grant set must be rejected")
	}
}

func TestHashCredentialDomainSeparated(t *testing.T) {
	salt := []byte("0123456789abcdef")
	h1 := HashCredential(salt, "secret")
	h2 := HashCredential([]byte("0123456789abcde0"), "secret")
	if string(h1) == string(h2) {
		t.Fatal("different salts must produce different digests")
	}
	if len(h1) != 32 {
		t.Fatalf("digest must be sha-256 sized, got %d", len(h1))
	}
}

func TestCredentialLifecycle(t *testing.T) {
	mgr, mem := testManager()
	ctx := context.Background()
	now := time.Now()
	mgr.Now = func() time.Time { return now }

	gen, err := mgr.Create(ctx, CreateInput{
		AdminSubject: "ops-team", Scopes: []string{"operator"}, TenantID: "tenant_eu",
	}, AuditOp{Action: "admin_credential_create"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !strings.HasPrefix(gen.Plaintext, CredentialPrefix) {
		t.Fatalf("plaintext must carry the admin marker: %q", gen.Plaintext)
	}
	if gen.Record.Salt != nil || gen.Record.Hash != nil {
		t.Fatal("returned record must be redacted")
	}

	// Resolve succeeds for the plaintext.
	rec, err := mgr.Store.Resolve(ctx, gen.Plaintext, now)
	if err != nil || rec.AdminSubject != "ops-team" {
		t.Fatalf("resolve: %v %+v", err, rec)
	}
	if rec.TenantID != "tenant_eu" || len(rec.Scopes) != 1 || rec.Scopes[0] != ScopeOperator {
		t.Fatalf("resolved record lost its grants: %+v", rec)
	}
	// A near-miss plaintext fails.
	if _, err := mgr.Store.Resolve(ctx, gen.Plaintext+"x", now); err != ErrInvalid {
		t.Fatalf("modified credential must be invalid, got %v", err)
	}

	// Rotate: successor works, predecessor fails immediately.
	rot, old, err := mgr.Rotate(ctx, rec.ID, AuditOp{Action: "admin_credential_rotate"})
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if old.ID != rec.ID || old.Status != StatusRevoked {
		t.Fatalf("rotate reply must report the revoked predecessor: %+v", old)
	}
	if _, err := mgr.Store.Resolve(ctx, gen.Plaintext, now); err == nil {
		t.Fatal("rotated-away credential must fail immediately")
	}
	if _, err := mgr.Store.Resolve(ctx, rot.Plaintext, now); err != nil {
		t.Fatalf("successor must resolve: %v", err)
	}
	if rot.Record.RotatedFrom != rec.ID {
		t.Fatalf("lineage must point at the predecessor, got %q", rot.Record.RotatedFrom)
	}

	// Revoke: idempotent, second revoke is a no-op.
	if err := mgr.Revoke(ctx, rot.Record.ID, AuditOp{Action: "admin_credential_revoke"}); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if err := mgr.Revoke(ctx, rot.Record.ID, AuditOp{Action: "admin_credential_revoke"}); err != nil {
		t.Fatalf("revoke must be idempotent: %v", err)
	}
	if _, err := mgr.Store.Resolve(ctx, rot.Plaintext, now); err == nil {
		t.Fatal("revoked credential must fail immediately")
	}

	// Every mutation carried exactly one audit row (no-op revoke: none).
	audits := mem.AuditSnapshot()
	if len(audits) != 3 {
		t.Fatalf("want 3 audit rows (create, rotate, revoke), got %d", len(audits))
	}
}

func TestCredentialExpiry(t *testing.T) {
	mgr, _ := testManager()
	ctx := context.Background()
	now := time.Now()
	mgr.Now = func() time.Time { return now }

	gen, err := mgr.Create(ctx, CreateInput{
		AdminSubject: "temp", Scopes: []string{"viewer"}, ExpiresAt: now.Add(time.Hour),
	}, AuditOp{Action: "admin_credential_create"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := mgr.Store.Resolve(ctx, gen.Plaintext, now); err != nil {
		t.Fatalf("unexpired credential must resolve: %v", err)
	}
	_, err = mgr.Store.Resolve(ctx, gen.Plaintext, now.Add(2*time.Hour))
	if err != ErrExpired {
		t.Fatalf("expired credential must answer ErrExpired, got %v", err)
	}
	// Advance the clock past expiry: the credential fails and cannot rotate.
	mgr.Now = func() time.Time { return now.Add(2 * time.Hour) }
	if _, _, err := mgr.Rotate(ctx, gen.Record.ID, AuditOp{}); err == nil {
		t.Fatal("rotating an expired credential must fail")
	}
}

func TestRotateUnknownCredential(t *testing.T) {
	mgr, _ := testManager()
	if _, _, err := mgr.Rotate(context.Background(), "adm_missing", AuditOp{}); err != ErrNotFound {
		t.Fatalf("unknown credential rotate must be ErrNotFound, got %v", err)
	}
	if err := mgr.Revoke(context.Background(), "adm_missing", AuditOp{}); err != ErrNotFound {
		t.Fatalf("unknown credential revoke must be ErrNotFound, got %v", err)
	}
}

func TestListTenantPredicate(t *testing.T) {
	mgr, _ := testManager()
	ctx := context.Background()
	for _, in := range []CreateInput{
		{AdminSubject: "a", Scopes: []string{"viewer"}, TenantID: "tenant_eu"},
		{AdminSubject: "b", Scopes: []string{"viewer"}, TenantID: "tenant_eu"},
		{AdminSubject: "c", Scopes: []string{"viewer"}, TenantID: ""},
	} {
		if _, err := mgr.Create(ctx, in, AuditOp{Action: "admin_credential_create"}); err != nil {
			t.Fatalf("create %s: %v", in.AdminSubject, err)
		}
	}
	all, err := mgr.List(ctx, "", "")
	if err != nil || len(all) != 3 {
		t.Fatalf("global list: %v %d", err, len(all))
	}
	eu, err := mgr.List(ctx, "", "tenant_eu")
	if err != nil || len(eu) != 2 {
		t.Fatalf("tenant-predicated list must return only the tenant's rows: %v %d", err, len(eu))
	}
	for _, rec := range eu {
		if rec.TenantID != "tenant_eu" {
			t.Fatalf("tenant predicate leaked a foreign row: %+v", rec)
		}
	}
}

func TestAuthenticatorDualPath(t *testing.T) {
	mgr, mem := testManager()
	now := time.Now()
	mgr.Now = func() time.Time { return now }
	authz := &Authenticator{Store: mgr.Store, LegacyToken: "legacy-secret", Limiter: NewAuthLimiter(), Now: func() time.Time { return now }}

	gen, err := mgr.Create(context.Background(), CreateInput{
		AdminSubject: "ops", Scopes: []string{"operator"},
	}, AuditOp{Action: "admin_credential_create"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Credential path.
	p, post, err := authz.Authenticate(context.Background(), gen.Plaintext, "10.0.0.1")
	if err != nil {
		t.Fatalf("credential auth: %v", err)
	}
	if p.AdminSubject != "ops" || p.Bootstrap || !p.Has(ScopeViewer) || p.Has(ScopeBilling) {
		t.Fatalf("wrong credential principal: %+v", p)
	}
	post()
	events := mem.AuthSnapshot()
	if len(events) != 1 || !events[0].Success || events[0].Principal.CredentialID != p.CredentialID {
		t.Fatalf("authentication audit missing: %+v", events)
	}

	// Legacy path: explicit platform-admin bootstrap behavior.
	p, post, err = authz.Authenticate(context.Background(), "legacy-secret", "10.0.0.1")
	if err != nil || !p.Bootstrap || !p.Global() {
		t.Fatalf("legacy auth: %v %+v", err, p)
	}
	for _, s := range AllScopes {
		if !p.Has(s) {
			t.Fatalf("bootstrap principal must hold %s", s)
		}
	}

	// Unknown and revoked answer the same error (uniform, non-enumerable).
	if _, _, err := authz.Authenticate(context.Background(), "wrong", "10.0.0.1"); err != ErrInvalid {
		t.Fatalf("unknown token must be ErrInvalid, got %v", err)
	}
	mgr.Revoke(context.Background(), gen.Record.ID, AuditOp{})
	if _, _, err := authz.Authenticate(context.Background(), gen.Plaintext, "10.0.0.1"); err != ErrInvalid {
		t.Fatal("revoked credential must surface as the same ErrInvalid")
	}
}

func TestAuthenticatorFailureRateLimit(t *testing.T) {
	authz := &Authenticator{LegacyToken: "legacy-secret", Limiter: NewAuthLimiter(), Now: time.Now}
	client := "192.168.1.50"
	for i := 0; i < DefaultFailureLimit; i++ {
		if _, _, err := authz.Authenticate(context.Background(), "bad", client); err != ErrInvalid {
			t.Fatalf("attempt %d: want ErrInvalid, got %v", i, err)
		}
	}
	_, _, err := authz.Authenticate(context.Background(), "bad", client)
	rl, ok := err.(*RateLimitError)
	if !ok {
		t.Fatalf("after the failure budget the caller must be rate limited, got %v", err)
	}
	if rl.RetryAfter <= 0 || rl.RetryAfter > time.Minute {
		t.Fatalf("retry-after out of bounds: %v", rl.RetryAfter)
	}
	// Even the correct token is refused while limited (brute force pays).
	if _, _, err := authz.Authenticate(context.Background(), "legacy-secret", client); err == nil {
		t.Fatal("rate-limited client must not authenticate even with the right token")
	}
	// Failures are per client; another source is unaffected.
	if _, _, err := authz.Authenticate(context.Background(), "legacy-secret", "10.9.9.9"); err != nil {
		t.Fatalf("independent client must authenticate: %v", err)
	}
	// Successes never consume the failure budget.
	authz2 := &Authenticator{LegacyToken: "s", Limiter: NewAuthLimiter(), Now: time.Now}
	for i := 0; i < DefaultFailureLimit+10; i++ {
		if _, _, err := authz2.Authenticate(context.Background(), "s", "10.1.1.1"); err != nil {
			t.Fatalf("successful authentication %d must never be limited: %v", i, err)
		}
	}
}

func TestAuthenticatorLegacyOnlyWithoutStore(t *testing.T) {
	authz := &Authenticator{LegacyToken: "t"}
	p, _, err := authz.Authenticate(context.Background(), "t", "c")
	if err != nil || !p.Bootstrap || !p.Has(ScopePlatformAdmin) {
		t.Fatalf("legacy-only mode: %v %+v", err, p)
	}
	if _, _, err := authz.Authenticate(context.Background(), CredentialPrefix+"deadbeef", "c"); err != ErrInvalid {
		t.Fatal("credential-shaped tokens must fail without a store")
	}
}
