# Design: admin identity and authorization

`adminauth.Principal` carries credential ID, tenant boundary, roles/scopes, and
bootstrap marker. Middleware authenticates before body decode, applies a central
route-to-scope policy, and passes the principal to management services. Scope
constants and implication rules have one owner; handlers do not compare roles.

Credential hashing follows the existing API-key lifecycle patterns with a
distinct prefix/domain. Mutations use PostgreSQL transactions that lock the
target, capture redacted before/after summaries, apply the change, and insert
`admin_audit`. Platform-admin is global; all other tenant-scoped identities have
mandatory query predicates, not post-query filtering.
