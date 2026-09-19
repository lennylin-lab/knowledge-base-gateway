# Implementation plan: admin RBAC

1. Inventory every admin route and write the explicit scope/tenant matrix.
2. Implement credential domain/store lifecycle and constant-time auth.
3. Add centralized middleware, independent limiter, and bootstrap-token adapter.
4. Thread principals into management queries/mutations with SQL tenant predicates.
5. Expand atomic audit summaries and add lifecycle/security documentation.
6. Run matrix, timing/error, transaction rollback, real PostgreSQL, race, and
   existing admin regression tests.

Rollback: keep the legacy token adapter available while disabling stored admin
credentials; never downgrade tenant-scoped credentials into global tokens.
