# Error Handling

Errors cross an HTTP/provider boundary, so classify them before returning them. Every request has a trace/request ID and errors are auditable without prompt content.

Return this stable envelope:

```json
{"error":{"type":"authentication_error","code":"invalid_api_key","message":"invalid API key","request_id":"req_..."}}
```

Use 401 for invalid/expired/revoked keys, 403 for forbidden models, 400/422 for invalid input, 429 for limits, 504 for upstream timeout, 503 for temporary upstream failure, and 500 for unknown internal failures. Do not pass through provider status codes or private error JSON unchanged.

Wrap errors with operation context while preserving cancellation. Known model that exists in the catalog but the subject lacks permission, and unknown models, collapse to the same non-leaky 403 `model_not_allowed` — never distinguish them in the response.

Distinguish rate-limit denial from limiter infrastructure failure. A genuine denial maps to 429 with `Retry-After`; an infrastructure failure (e.g. Redis unreachable) must surface as a sentinel like `limiter.ErrUnavailable` from the `limiter.Gate` interface and map to 503 `service_unavailable`/`limiter_unavailable` with no `Retry-After` and no rate-limit metric. Classify the sentinel before the 429 branch so the two can never collide. Never silently downgrade an outage to a client-facing 429.

Token-quota denials are 429 `rate_limit_error`/`code: "quota_exceeded"` with `Retry-After` pointing at the UTC boundary of the denied period; they reuse the same `limiter.ErrUnavailable` sentinel for outages, classified before all 429 branches. Denial messages leak no limits or remaining budget. Retry only pre-output network errors, 429 and 5xx under one total deadline; never retry after SSE starts. Client cancellation stops upstream work. Log classification, request ID, model and latency, never authorization headers, raw bodies, secrets, SQL values or internal URLs.

