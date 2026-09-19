# Design: async Responses

PostgreSQL owns job state. Creation transaction inserts the request digest,
idempotency mapping, job, and budget reservation reference. Workers claim with
`FOR UPDATE SKIP LOCKED` plus a compare-and-set lease token/expiry. Heartbeats
extend bounded leases; expired leases return running work to queued only when
retry policy permits. A process-local notifier reduces latency, while polling is
the correctness path and Redis is not required for job durability.

HTTP reuses Responses decoding and admission through extracted, typed helpers.
Workers invoke the existing gateway service with a detached bounded context and
normalized model request. Result/failure plus terminal transition is one DB
transaction. Cancel conditionally wins from queued/running and calls an in-memory
cancel registry when the lease is local; a remote worker observes cancellation
through heartbeat/commit failure. Results are size-capped and expire by policy.
