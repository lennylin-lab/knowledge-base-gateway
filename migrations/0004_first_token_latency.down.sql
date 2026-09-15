-- Revert the first-token latency column.
ALTER TABLE llm_requests
    DROP COLUMN first_token_millis;
