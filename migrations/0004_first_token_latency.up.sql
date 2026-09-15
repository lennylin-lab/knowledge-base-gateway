-- First-token latency for streamed requests: elapsed milliseconds from the
-- start of the request to the first streamed output event (text/args delta).
-- Nullable by contract: non-streaming requests record NULL (their full-latency
-- equivalent is latency_ms), and a stream that never produced output stays
-- NULL. Unknown is never fabricated as zero.
ALTER TABLE llm_requests
    ADD COLUMN first_token_millis INTEGER;
