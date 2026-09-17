# Gateway client contract (knowledge-base-server)

The Python service integrates with the gateway through this contract only: one
base URL, one internal API key, and the stable error envelope. Business code
never reads provider secrets or constructs provider URLs.

## Request

```
POST {GATEWAY_BASE_URL}/v1/chat/completions
Authorization: Bearer {GATEWAY_INTERNAL_API_KEY}
X-Request-ID: <uuid, generated per call>
X-Trace-ID: <optional trace root; defaults to the request ID>
Content-Type: application/json

{"model":"<public model>","messages":[...],"temperature":0.2,"max_tokens":1024,"stream":false}
```

- The public model name comes from the gateway catalog (e.g. `gateway-echo`).
  Clients cannot choose providers, base URLs, or upstream model names.
- `X-Request-ID` should be generated client-side and stored with logs so
  gateway audit rows, metrics, and traces can be correlated.
- Bodies are bounded (1 MiB, 64 messages, 32k chars/message by default).

## Responses

Non-streaming: OpenAI-compatible `chat.completion` JSON with `id`, `object`,
`created`, `model`, `choices`, and `usage` when the upstream reports it.

Streaming: `Content-Type: text/event-stream`, standard `data: {...}` chunks
(`chat.completion.chunk`), terminated by `data: [DONE]`. Chunks carry `usage`
when the upstream reports it for the stream (typically the final chunk), the
same `usage` object as non-streaming. If the stream fails
before any chunk was emitted, a final SSE event carries
`{"error":{"type":"...","request_id":"..."}}`.

## Errors

Stable envelope, never provider-raw:

```json
{"error":{"type":"authentication_error","code":"invalid_api_key","message":"invalid API key","request_id":"req_..."}}
```

| Status | Codes | Client action |
|---|---|---|
| 401 | `invalid_api_key`, `api_key_expired`, `api_key_revoked` | rotate key (admin API), fail fast |
| 403 | `model_not_allowed` | unknown and disallowed models are indistinguishable; check model name against configured list |
| 400 | `invalid_request`, `upstream_rejected_request` | fix payload |
| 429 | `rate_limit_exceeded`, `quota_exceeded` (with `Retry-After` when computable) | back off and retry; `quota_exceeded` clears at the next UTC day/month boundary |
| 503 | `upstream_unavailable`, `upstream_rate_limited`, `no_route_available`, `limiter_unavailable` | retry with backoff; `limiter_unavailable` has no `Retry-After` |
| 504 | `upstream_timeout` | retry allowed; gateway never retries past its own total deadline |

## Retry rules for the client

- The gateway already retries pre-output network/429/5xx/timeout failures and
  fails over primary -> backup before any output. Clients should only retry
  on 429/503/504 and network errors, honoring `Retry-After`.
- Never retry a streaming call after chunks were received.

## Minimal client sketch

```python
class GatewayClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    def chat(self, model: str, messages: list[dict], *, stream: bool = False,
             request_id: str | None = None, **kwargs):
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "X-Request-ID": request_id or str(uuid4()),
        }
        payload = {"model": model, "messages": messages, "stream": stream, **kwargs}
        resp = httpx.post(f"{self.base_url}/v1/chat/completions",
                          json=payload, headers=headers, timeout=90)
        if resp.status_code >= 400:
            detail = resp.json()["error"]           # stable envelope
            raise GatewayError(detail["code"], detail["message"],
                               request_id=detail["request_id"])
        if stream:
            return resp.iter_lines()                # SSE, ends with data: [DONE]
        return resp.json()
```

## Test fixture

`internal/e2e/e2e_test.go` in this repository exercises the exact contract a
client sees: failover on upstream 5xx, SSE termination, the error envelope
with echoed `X-Request-ID`, 401 for invalid keys, and `Retry-After` on 429.

## V1.3 additive notes (embeddings, default models, retrieval profiles)

The chat contract above is unchanged. Additive surfaces:

- `POST /v1/embeddings` — OpenAI-compatible embeddings: `{"model":...,
  "input": "text" | ["a","b"]}` returns `{"object":"list","data":[{"object":
  "embedding","index":0,"embedding":[...]}],"model":...,"usage":
  {"prompt_tokens":N,"total_tokens":N}}`. Usage is input-token only and
  settles into the same daily/monthly token pool as chat. Chat-only fields
  (`stream`, `tools`, `response_format`, ...) are rejected with 400.
- Requests may omit `model`: chat/responses backfill the subject's
  `default_model`, embeddings backfill `default_embedding_model`. Omitting
  `model` requires a configured default; otherwise the stable 400
  `invalid_request` applies.
- `GET /v1/models/{model}` gained `capabilities.embeddings`,
  `capabilities.embedding_dim` (the fixed vector width for the model — read
  it from discovery, keep `KB_EMBEDDING_DIM` only as an offline escape
  hatch), and `retrieval_profile` when the catalog row declares one. All are
  absent rather than null when undeclared.

Capability errors (`capability_not_supported`) name the failed capability key
and protocol in their message, e.g.
`model does not declare capability 'tools' (protocol chat)`. The full matrix
and key semantics: `docs/capabilities.md`.
