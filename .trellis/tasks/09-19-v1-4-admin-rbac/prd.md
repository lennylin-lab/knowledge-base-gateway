# Admin identities roles and scopes

## Goal

Replace production reliance on one all-powerful token with lifecycle-managed,
least-privilege admin identities and tenant-safe management APIs.

## Requirements

- Create, rotate, revoke, expire, and record last use for admin credentials;
  persist only salted irreversible hashes and reveal plaintext once.
- Implement viewer, operator, billing, and platform-admin permissions as named
  scopes checked by every management route. Tenant admins are tenant-bound.
- Retain `GATEWAY_ADMIN_TOKEN` as a development/bootstrap compatibility identity
  with explicit platform-admin behavior and a deprecation path.
- Rate-limit admin authentication independently and avoid credential enumeration.
- Every mutation atomically records actor ID, tenant, effective scopes, action,
  target, and redacted old/new summaries; never secrets or model content.

## Acceptance Criteria

- [ ] Complete route-by-scope and tenant-boundary matrix tests deny every
  unauthorized read/mutation and do not reveal target existence.
- [ ] Plaintext appears only in create/rotate response; rotation revokes old
  credentials atomically; revoked/expired credentials fail immediately.
- [ ] Every successful mutation has one audit row and every rolled-back mutation
  has none; last-used updates are bounded and do not block requests.
- [ ] Legacy token works only when configured and is clearly observable as the
  bootstrap actor without appearing in logs.

## Out of Scope

- Browser login, OIDC/SAML, interactive sessions, and customer user management.
