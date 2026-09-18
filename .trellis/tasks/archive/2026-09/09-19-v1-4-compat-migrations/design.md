# Design: compatibility and schema foundation

Create small migrations rather than one release-sized migration: async core,
accounting, admin identity, then lifecycle/partition metadata. Later children may
extend their owning migration before merge, but applied migrations are immutable.
New nullable/additive columns keep old binaries readable during rollout. Tables
use text IDs consistent with the repository, UTC timestamps, JSONB only for
opaque normalized payloads, and explicit checks for states and amounts.

Golden fixtures remain owned by `internal/httpapi`; migration lifecycle remains
owned by `internal/store/pg/migration_test.go` and CI. Down scripts remove
dependents before parents and preserve pre-V1.4 data.
