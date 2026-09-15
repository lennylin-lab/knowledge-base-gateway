#!/usr/bin/env bash
# Model discovery examples.
set -euo pipefail
GATEWAY="${GATEWAY:-http://127.0.0.1:8080}"
KEY="${KEY:-kb_dev_key_123}"

curl -sS -H "Authorization: Bearer $KEY" "$GATEWAY/v1/models"
echo
curl -sS -H "Authorization: Bearer $KEY" "$GATEWAY/v1/models/gateway-echo"
echo
