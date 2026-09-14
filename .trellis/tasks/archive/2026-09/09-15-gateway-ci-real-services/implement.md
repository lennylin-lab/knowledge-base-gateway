# CI Real-Service Verification Implementation Plan

1. Add the CI workflow and pinned PostgreSQL/Redis service definitions.
2. Add bounded health waits and create an isolated test database.
3. Run format, race test, vet, and build checks.
4. Run migration up/version/down/up and export integration variables.
5. Run PostgreSQL/Redis integration tests and fail on unexpected skips.
6. Add failure log collection and secret masking; validate workflow syntax.

Validation: execute the workflow on a clean runner or equivalent local
container environment, then run the existing local Go checks to ensure no
developer workflow regressed.
