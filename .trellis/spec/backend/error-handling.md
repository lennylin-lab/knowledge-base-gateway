# Error Handling

Errors cross an HTTP/provider boundary, so classify them before returning them. Every request has a trace/request ID and errors are auditable without prompt content.

Return this stable envelope:

```json
{"error":{"type":"authentication_error","code":"invalid_api_key","message":"invalid API key","request_id":"req_..."}}
```

Use 401 for invalid/expired/revoked keys, 403 for forbidden models, 400/422 for invalid input, 429 for limits, 504 for upstream timeout, 503 for temporary upstream failure, and 500 for unknown internal failures. Do not pass through provider status codes or private error JSON unchanged.

Wrap errors with operation context while preserving cancellation. Retry only pre-output network errors, 429 and 5xx under one total deadline; never retry after SSE starts. Client cancellation stops upstream work. Log classification, request ID, model and latency, never authorization headers, raw bodies, secrets, SQL values or internal URLs.

