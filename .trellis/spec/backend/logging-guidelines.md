# Logging Guidelines

The gateway requires structured, machine-readable logs. The concrete Go logger is not selected yet; it must support correlation fields and redaction before serialization.

Use DEBUG for development diagnostics, INFO for startup/shutdown and completed requests, WARN for rejected requests, limits, retries and degraded dependencies, and ERROR for unexpected failures and provider outages. Include timestamp, level, message, request_id/trace_id, operation, public model, status and latency where applicable.

Never log raw Authorization headers, API keys, provider secrets, internal URLs, full headers, prompt/completion bodies, or sensitive SQL values. Token usage may be logged only when supplied by the provider; missing usage is unknown, not zero.

