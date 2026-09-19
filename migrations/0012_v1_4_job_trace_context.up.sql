-- V1.4 observability: W3C trace context carried by background jobs. The
-- enqueue span's trace/span IDs are persisted as normalized lowercase hex
-- (validated before persistence by internal/tracing) so the worker can join
-- its spans to the caller's trace without propagating baggage or any request
-- content. Empty strings mean the job carries no trace context.

ALTER TABLE async_jobs ADD COLUMN trace_id text NOT NULL DEFAULT '';
ALTER TABLE async_jobs ADD COLUMN parent_span_id text NOT NULL DEFAULT '';
-- The sampled bit rides along so the worker's ParentBased sampler keeps the
-- execution spans inside the caller's sampled trace.
ALTER TABLE async_jobs ADD COLUMN trace_sampled boolean NOT NULL DEFAULT false;
