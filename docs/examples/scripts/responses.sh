#!/usr/bin/env bash
# Responses API examples: non-streaming, SSE, tool calling, structured output.
set -euo pipefail

GATEWAY="${GATEWAY:-http://127.0.0.1:8080}"
KEY="${KEY:-kb_dev_key_123}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"

echo "== non-streaming =="
curl -sS "$GATEWAY/v1/responses" \
  -H "Authorization: Bearer $KEY" -H "Content-Type: application/json" \
  -d @"$HERE/requests/responses_non_stream.json"
echo

echo "== streaming (SSE events) =="
curl -sS -N "$GATEWAY/v1/responses" \
  -H "Authorization: Bearer $KEY" -H "Content-Type: application/json" \
  -d @"$HERE/requests/responses_stream.json"

echo "== tool calling: step 1, the model issues a tool call =="
curl -sS "$GATEWAY/v1/responses" \
  -H "Authorization: Bearer $KEY" -H "Content-Type: application/json" \
  -d @"$HERE/requests/responses_tools.json"
echo
echo "Execute the tool yourself, then send its output back:"
echo "  curl -sS ... -d @requests/responses_tool_result.json"

echo "== structured output (json_schema) =="
curl -sS "$GATEWAY/v1/responses" \
  -H "Authorization: Bearer $KEY" -H "Content-Type: application/json" \
  -d @"$HERE/requests/responses_structured.json"
echo
