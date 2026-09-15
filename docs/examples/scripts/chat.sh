#!/usr/bin/env bash
# Chat Completions examples. Shares the request samples with the automated
# smoke tests (internal/e2e/examples_test.go).
set -euo pipefail

GATEWAY="${GATEWAY:-http://127.0.0.1:8080}"
KEY="${KEY:-kb_dev_key_123}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"

echo "== non-streaming =="
curl -sS "$GATEWAY/v1/chat/completions" \
  -H "Authorization: Bearer $KEY" \
  -H "Content-Type: application/json" \
  -H "X-Request-ID: $(uuidgen 2>/dev/null || echo req-demo-1)" \
  -d @"$HERE/requests/chat_non_stream.json"
echo

echo "== streaming (SSE, ends with data: [DONE]) =="
curl -sS -N "$GATEWAY/v1/chat/completions" \
  -H "Authorization: Bearer $KEY" \
  -H "Content-Type: application/json" \
  -d @"$HERE/requests/chat_stream.json"
