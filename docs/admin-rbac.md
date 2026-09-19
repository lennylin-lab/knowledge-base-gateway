# Admin identities, scopes, and audit (V1.4)

The admin surface evolves from one all-powerful static token to lifecycle-
managed, least-privilege identities. This document is the annotated version of
the enforced policy; the single enforcement points are
`adminRoutePolicy` (route → scope/tenant matrix) in `internal/httpapi/
admin_auth.go` and the implication rules in `internal/adminauth`. Handlers
never compare role names.

## Scopes

The closed vocabulary (`internal/adminauth`): `viewer`, `operator`, `billing`,
`platform-admin`. A credential carries at least one scope; the database CHECK
(migration 0008) rejects anything outside the set and empty grant sets, so
application validation is defense in depth, not the boundary.

Implication rules (one owner, pinned by `TestScopeImplications`):

| Scope | Implies | Typical use |
|---|---|---|
| `viewer` | — | Read models, providers, policies, audit, usage metadata |
| `operator` | `viewer` | Model/provider switches, subject default-model slots |
| `billing` | `viewer` | Prices, budgets, usage/cost views |
| `platform-admin` | `viewer`, `operator`, `billing` | Credentials, tenants, API keys, everything else |

## Route → scope matrix

Every admin route is checked against this matrix after authentication and
before any store access. `global` = platform-global resource: tenant-bound
identities are denied even with the scope. `tenant-checkable` = the operation
targets a tenant-ownable resource; tenant-bound principals are confined to
their tenant (denials are non-leaky, see below).

| Route | Methods | Required scope | Tenant rule |
|---|---|---|---|
| `/admin/keys` | POST, GET | `platform-admin` | tenant-checkable |
| `/admin/keys/{id}/rotate`, `/revoke` | POST | `platform-admin` | tenant-checkable |
| `/admin/admins` | POST, GET | `platform-admin` | tenant-checkable |
| `/admin/admins/{id}/rotate`, `/revoke` | POST | `platform-admin` | tenant-checkable |
| `/admin/models` | GET | `viewer` | global |
| `/admin/models/{name}/enable`, `/disable` | POST | `operator` | global |
| `/admin/providers` | GET | `viewer` | global |
| `/admin/policies` | GET | `viewer` | tenant-predicated |
| `/admin/policies/{subject}/default-model` | POST | `operator` | tenant-checkable |
| `/admin/audit` | GET | `viewer` | tenant-predicated |
| `/admin/usage` | GET | `viewer` | tenant-predicated |
| `/admin/management-log` | GET | `viewer` | global |
| `/admin/prices` | GET, POST | `billing` | global |
| `/admin/budgets` | GET, POST | `billing` | tenant-checkable |
| any unlisted route | any | `platform-admin` | global (strictest default) |

The matrix is pinned cell-by-cell by `TestAdminScopeMatrix`; adding a route or
changing a scope must update that table deliberately.

### Background-response visibility

`GET /v1/responses/{id}` accepts scoped admin credentials: the owner keeps
access, and an admin principal with the `viewer` scope (directly or by
implication) may read any job within its tenant reach — global for platform
identities. `POST /v1/responses/{id}/cancel` stays with the owning subject;
admin credentials authenticate but are denied (visibility is not control).
Foreign-tenant and absent jobs answer the same non-leaky `response_not_found`.

## Authentication

Two paths coexist during the migration (`internal/adminauth.Authenticator`):

1. **Stored credentials.** Plaintexts start with the `kba_` marker (the
   underscore cannot occur in a `kb_` API key, so the namespaces never
   collide) and route to the credential store: salted, domain-separated
   SHA-256 digests compared in constant time. Expired, revoked, and unknown
   credentials are indistinguishable.
2. **Legacy bootstrap token.** `GATEWAY_ADMIN_TOKEN` authenticates as the
   explicit platform-admin bootstrap identity (`bootstrap-token`,
   `bootstrap: true` in the audit actor block). It works only when
   configured, is never logged, and never appears in a response.

Uniform failure envelopes: every authentication failure is the same 401
(`invalid_admin_token`); scope or tenant-boundary denials are the same 403
(`insufficient_scope`) raised before any store call; rate-limited callers get
429 with `Retry-After`. Failed authentication attempts are rate limited
independently of the model-traffic limiter (20 failures per client per
minute; successes never count). `TestAdminAuthenticationRateLimited` and
`TestAdminAuthenticationAudited` pin both.

## Credential lifecycle

- **Create** (`POST /admin/admins`): subject, non-empty scope subset, optional
  tenant binding and expiry. The plaintext is returned exactly once and never
  persisted, logged, or echoed in listings — only the digest and a display
  prefix (`kba_` + 4 hex characters) survive.
- **Rotate** (`POST /admin/admins/{id}/rotate`): mints the successor (same
  subject, scopes, tenant, expiry) and revokes the predecessor in one
  transaction; the old plaintext fails immediately after the response.
- **Revoke** (`POST /admin/admins/{id}/revoke`): immediate; revoking an
  already-revoked credential is a no-op success with no audit row.
- **Expire**: optional `expires_in_hours`; expiry is checked at
  authentication time.
- **Last use**: stamped at most once per credential per minute, off the
  request path, and never fails a request.

Hashing: `SHA-256("kbgw-admin-credential-v1" || salt || secret)` with a
per-credential 16-byte salt, stored packed as `salt || digest` in the single
UNIQUE `credential_hash` column (the schema has no separate salt column). The
domain separation means a captured API-key digest can never be replayed as an
admin credential or vice versa.

## Tenant boundaries

The authoritative subject → tenant binding is `subjects.tenant_id`. The
optional `api_keys.tenant_id` principal column is never the boundary. All
tenant-bound reads are mandatory SQL predicates (never post-query filters):
policies, audit, usage, budgets, admin-credential listings, key listings, and
background-job visibility. Tenant-bound mutations verify the target's
authoritative tenant inside the transaction (default-model) or before the
mutation (keys, credentials). Unknown and foreign targets answer identical
non-leaky envelopes (`TestTenantBoundaryNonLeaky`), so existence never leaks
across tenants. A tenant-bound platform-admin can only mint tenant-bound
credentials of its own tenant — a tenant-scoped identity is never upgraded
into a global one. In development mode without tenant data, tenant-bound
queries fail closed (`ErrTenantBoundary` → 403), never widen.

## Audit contract

- Every mutation (model enable/disable, default-model, prices, budgets, API
  keys, admin credentials) commits together with its `admin_audit` row in one
  transaction; a rolled-back mutation leaves no audit row and a committed
  mutation is always audited. The API-key lifecycle (whose store and audit
  sink are separate) records its audit row after the change — key rows are
  append-only and revocation idempotent, so the evidence gap is bounded.
- Each row carries the actor block: admin subject, credential ID, tenant
  boundary, effective scopes, and the bootstrap marker.
- Detail carries redacted old/new summaries (previous enabled state, previous
  model slot, previous price/budget row) — never secrets, digests, prompts,
  completions, or tool parameters.
- Authentication itself is audited: one row per attempt (success and failure
  with a reason class only — never the presented secret).

## Migration path from the legacy token

The parent rollback point: the legacy token stays available until scoped
credentials are production-verified. Both paths work simultaneously.

1. **Bootstrap (today).** Deploy with `GATEWAY_ADMIN_TOKEN` set. Use it to
   mint scoped credentials per operator via `POST /admin/admins`; start with
   `viewer`/`operator`/`billing` and grant `platform-admin` narrowly. The
   admin listener also starts in database mode without the token once
   credentials exist.
2. **Observe.** Every mutation's audit row names its actor; bootstrap-token
   usage is visible as `bootstrap: true`. Migrate operators and automation to
   their scoped credentials; rotate any credential that was shared.
3. **Deprecate.** Unset `GATEWAY_ADMIN_TOKEN` in production. Stored
   credentials keep the admin API available; the listener stays up.
4. **Rollback.** Re-set `GATEWAY_ADMIN_TOKEN` at any time — the legacy path
   is independent of stored credentials and restores the bootstrap identity
   immediately. Rollbacks never touch stored credentials, and a tenant-scoped
   credential is never widened into a global token.

Token-only deployments (development, smoke) behave exactly as before: with no
credential store wired, the guard falls back to the historical constant-time
token comparison.
