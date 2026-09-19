# Design: data lifecycle

Use a maintenance command/process separate from request serving. It creates
future monthly partitions, archives closed partitions or bounded primary-key
ranges to an `ArchiveSink`, writes a manifest atomically, then detaches/deletes
only after verification. The initial sink is filesystem/object-store-neutral
behind an interface; production configuration must choose a durable sink.

Online conversion of `llm_requests` uses a shadow partitioned table, dual-write
or bounded copy/catch-up, validation, and short controlled rename; rehearse on a
production-sized fixture. Exports use server-side cursors/keyset pagination and
typed projections that omit sensitive columns by construction.
