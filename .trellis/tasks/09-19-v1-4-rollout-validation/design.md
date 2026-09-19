# Design: integrated validation and rollout

Extend the existing compose/smoke and offline provider contract setup with a
V1.4 scenario runner. Tests use deterministic IDs/clocks and injectable provider
latency/errors, while real PostgreSQL/Redis runs validate concurrency semantics.
Load profiles publish machine-readable thresholds and results, not just scripts.

Feature evaluation is centralized: global enablement AND model allowlist AND
tenant allowlist. Async and monetary enforcement evaluate independently. Flags
only narrow access; they never bypass auth, model policy, or existing quotas.
