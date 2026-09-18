# Implementation plan: compatibility and migrations

1. Add missing V1.3 golden/contract cases and prove no fixture drift.
2. Write paired migrations and schema-level constraints/indexes.
3. Extend parse and real PostgreSQL lifecycle tests, including version-5 upgrade.
4. Update CI migration version and compose migration smoke assertions.
5. Run unit, race, vet, real PostgreSQL up/down/re-up, replay, and container smoke.

Rollback gate: no feature writer may ship before its down migration is rehearsed;
never run a down migration while V1.4 writers are enabled.
