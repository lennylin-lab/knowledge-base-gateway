# Implementation plan: lifecycle and export

1. Define retention eligibility, legal holds, archive manifest, and redacted
   export projection tests.
2. Add partition/lifecycle schema and an online migration rehearsal fixture.
3. Implement archive sink, idempotent maintenance phases, locks, and dry-run.
4. Implement scoped streaming export and management endpoints/commands.
5. Add metrics/audit hooks, operator runbooks, restore verification, and config.
6. Load-test concurrent serving, partition maintenance, export, failure recovery,
   real PostgreSQL migration, and rollback.

Rollback: stop maintenance first. Never delete verified archives; restore table
routing before removing shadow/original tables during partition migration.
