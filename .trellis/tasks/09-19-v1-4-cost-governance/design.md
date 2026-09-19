# Design: pricing and budgets

An `accounting.Gate` generalizes the current token reservation pattern. The
PostgreSQL ledger is authoritative and stores token categories, nullable cost,
currency, price version, state, and unique request/job key. Price resolution is
effective-at-request-time and snapshots the chosen version.

Redis Lua atomically checks/reserves subject and tenant day/month counters in one
operation; local mode mirrors semantics. Finalize uses a durable outbox/retry
record so database settlement cannot be lost after an HTTP response or worker
completion. Integer multiplication includes checked overflow and explicit unit
denominators. One budget currency is allowed per applicable policy.
