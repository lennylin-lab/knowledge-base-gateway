"""Plain HTTP client (httpx) for the gateway contract.

Mirrors docs/gateway-client-contract.md: one base URL, one internal key, the
stable error envelope, and client-generated request IDs.
"""
import json
import uuid

import httpx


class GatewayError(Exception):
    def __init__(self, code: str, message: str, request_id: str):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.request_id = request_id


class GatewayClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "X-Request-ID": str(uuid.uuid4()),
        }

    def _check(self, resp: httpx.Response) -> None:
        if resp.status_code >= 400:
            detail = resp.json()["error"]
            raise GatewayError(detail["code"], detail["message"],
                               detail["request_id"])

    def chat(self, model: str, messages, *, stream: bool = False, **kwargs):
        payload = {"model": model, "messages": messages, "stream": stream, **kwargs}
        with httpx.Client(timeout=90) as client:
            with client.stream("POST", f"{self.base_url}/v1/chat/completions",
                               json=payload, headers=self._headers()) as resp:
                if stream:
                    chunks = []
                    for line in resp.iter_lines():
                        if line.startswith("data: [DONE]"):
                            break
                        if line.startswith("data: "):
                            chunks.append(json.loads(line[len("data: "):]))
                    resp.raise_for_status()
                else:
                    resp.raise_for_status()
                    return resp.json()
        return chunks

    def respond(self, model: str, input_text: str, **kwargs):
        payload = {"model": model, "input": input_text, **kwargs}
        resp = httpx.post(f"{self.base_url}/v1/responses", json=payload,
                          headers=self._headers(), timeout=90)
        self._check(resp)
        return resp.json()

    def models(self) -> dict:
        resp = httpx.get(f"{self.base_url}/v1/models",
                         headers=self._headers(), timeout=30)
        self._check(resp)
        return resp.json()


if __name__ == "__main__":
    gw = GatewayClient("http://127.0.0.1:8080", "kb_dev_key_123")
    print("models:", [m["id"] for m in gw.models()["data"]])
    print("chat:", gw.chat("gateway-echo",
                           [{"role": "user", "content": "hello"}],
                           max_tokens=1024)["choices"][0]["message"]["content"])
    r = gw.respond("gateway-echo", "hello")
    print("responses:", r["status"])
    # Retry guidance: only retry 429/503/504 and network errors, honoring
    # Retry-After; never retry a streaming call after events arrived.
