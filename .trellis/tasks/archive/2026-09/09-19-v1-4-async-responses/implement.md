# Implementation plan: async Responses

1. Specify state-transition, ownership, digest, lease, and race tests first.
2. Add repository interfaces and PostgreSQL implementation from child-1 schema.
3. Extract shared Responses decode/admission; add create/get/cancel wire handlers.
4. Implement worker claim, heartbeat, recovery, retry, cancellation, and shutdown.
5. Add result size/TTL configuration, stable errors, metrics hooks, and docs.
6. Run race-heavy unit tests, real PostgreSQL multi-worker/fault tests, V1.3
   goldens, provider contracts, replay, and smoke.

Rollback: disable async acceptance, drain workers, retain rows/results until TTL,
then remove runtime wiring; synchronous Responses remains untouched.
