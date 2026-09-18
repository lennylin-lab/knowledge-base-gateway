-- V1.4 admin identity schema: hashed management credentials with roles/
-- scopes and lifecycle metadata. Schema only: issuance, rotation, revocation
-- endpoints and scope enforcement are feature work. GATEWAY_ADMIN_TOKEN
-- remains the development-mode entry and is unaffected. Additive only.
--
-- Invariants encoded at the database boundary:
--   - only irreversible hashes and short prefixes are stored (never the
--     plaintext credential, which exists only in the create/rotate reply);
--   - roles/scope sets are closed: every scope must be one of the documented
--     roles (viewer, operator, billing, platform-admin) and a credential
--     carries at least one;
--   - lifecycle states are closed (active/revoked) with revocation stamped;
--   - rotation lineage is a self-reference; tenant scoping is a tenants FK.

CREATE TABLE admin_credentials (
    id                TEXT PRIMARY KEY,
    admin_subject     TEXT NOT NULL,
    credential_hash   BYTEA NOT NULL UNIQUE,
    credential_prefix TEXT NOT NULL,
    -- Closed scope vocabulary: viewer / operator / billing / platform-admin.
    -- The subset check rejects any scope outside the set (malformed or
    -- privileged-invented names fail at the database boundary), and the
    -- cardinality check rejects empty grant sets (cardinality, unlike
    -- array_length, is 0 — not NULL — for the empty array).
    scopes            TEXT[] NOT NULL CHECK (
                          scopes <@ ARRAY['viewer', 'operator', 'billing', 'platform-admin']::text[]
                          AND cardinality(scopes) >= 1
                      ),
    status            TEXT NOT NULL CHECK (status IN ('active', 'revoked')),
    -- Tenant-scoped credentials may only see their tenant; NULL means the
    -- credential is platform-wide (allowed for any role; enforcement is a
    -- feature concern, the FK here is the ownership boundary).
    tenant_id         TEXT REFERENCES tenants (id),
    expires_at        TIMESTAMPTZ,
    last_used_at      TIMESTAMPTZ,
    revoked_at        TIMESTAMPTZ,
    rotated_from      TEXT REFERENCES admin_credentials (id),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Revocation is stamped.
    CHECK ((status = 'revoked') = (revoked_at IS NOT NULL))
);

CREATE INDEX idx_admin_credentials_subject ON admin_credentials (admin_subject);
CREATE INDEX idx_admin_credentials_status ON admin_credentials (status);
