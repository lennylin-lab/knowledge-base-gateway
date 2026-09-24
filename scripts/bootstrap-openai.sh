#!/usr/bin/env bash
# Import OpenAI chat + embedding catalog into the local dev gateway database.
#
# Seeds providers, model_catalog, model_routes, access_policies for:
#   - chat:  gpt-5.5  (provider openai-primary)
#   - embed: qwen3-embedding (provider openai-embed, upstream Qwen/Qwen3-Embedding-4B)
#
# Values mirror the kb-gateway dev DB snapshot. Override via .env or env:
#   GATEWAY_OPENAI_CHAT_BASE_URL, GATEWAY_OPENAI_EMBED_BASE_URL,
#   GATEWAY_OPENAI_CHAT_MODEL, GATEWAY_OPENAI_EMBED_MODEL, etc.
#
# Prerequisites:
#   - docker compose up -d (migrate completed; subject_default / gateway-echo seed exists)
#   - .env has OPENAI_API_KEY__OPENAI_PRIMARY and OPENAI_API_KEY__OPENAI_EMBED
#     (or a shared OPENAI_API_KEY fallback)
#   - Gateway container is up OR psql can reach PostgreSQL on the host port
#
# Usage (repo root):
#   ./scripts/bootstrap-openai.sh
#
# After import the script restarts gateway (when compose is in use), sets subject
# defaults via Admin API, mints a service key, and prints suggested client env lines.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(docker compose -f "${ROOT}/docker-compose.yml")
ENV_FILE="${ROOT}/.env"

# --- catalog defaults (from local dev gateway DB) ---
OPENAI_CHAT_PROVIDER="${GATEWAY_OPENAI_CHAT_PROVIDER:-openai-primary}"
OPENAI_CHAT_BASE_URL="${GATEWAY_OPENAI_CHAT_BASE_URL:-https://api.longxiadev.store/v1}"
OPENAI_CHAT_PUBLIC_MODEL="${GATEWAY_OPENAI_CHAT_MODEL:-gpt-5.5}"
OPENAI_CHAT_UPSTREAM_MODEL="${GATEWAY_OPENAI_CHAT_UPSTREAM:-gpt-5.5}"

OPENAI_EMBED_PROVIDER="${GATEWAY_OPENAI_EMBED_PROVIDER:-openai-embed}"
OPENAI_EMBED_BASE_URL="${GATEWAY_OPENAI_EMBED_BASE_URL:-https://router.tumuer.me/v1}"
OPENAI_EMBED_PUBLIC_MODEL="${GATEWAY_OPENAI_EMBED_MODEL:-qwen3-embedding}"
OPENAI_EMBED_UPSTREAM_MODEL="${GATEWAY_OPENAI_EMBED_UPSTREAM:-Qwen/Qwen3-Embedding-4B}"
EMBED_DIM="${GATEWAY_EMBEDDING_DIM:-1536}"

SUBJECT="${GATEWAY_SERVER_SUBJECT:-subject_default}"
GATEWAY_PUBLIC_URL="${GATEWAY_PUBLIC_URL:-http://127.0.0.1:${GATEWAY_HOST_PORT:-8091}}"
GATEWAY_ADMIN_URL="${GATEWAY_ADMIN_URL:-http://127.0.0.1:${GATEWAY_ADMIN_HOST_PORT:-8092}}"

CHAT_CAPABILITIES='{"chat": true, "tools": true, "usage": true, "stream": true, "vision": false, "json_mode": false, "max_tools": 16, "reasoning": false, "responses": true, "embeddings": true, "embedding_dim": 1536, "context_tokens": 128000, "max_output_tokens": 8192, "structured_output": true}'
EMBED_CAPABILITIES='{"chat": false, "stream": false, "embeddings": true, "embedding_dim": 1536}'

if [[ -f "${ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
fi

ADMIN_TOKEN="${GATEWAY_ADMIN_TOKEN:-smoke-admin-throwaway}"

compose_postgres_running() {
  "${COMPOSE[@]}" ps postgres --status running --quiet 2>/dev/null | grep -q .
}

compose_psql() {
  "${COMPOSE[@]}" exec -T postgres \
    psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER:-gateway}" -d "${POSTGRES_DB:-gateway}" "$@"
}

run_psql() {
  if compose_postgres_running; then
    compose_psql "$@"
    return
  fi

  if [[ -n "${GATEWAY_DATABASE_URL:-}" ]]; then
    if ! command -v psql >/dev/null 2>&1; then
      echo "error: GATEWAY_DATABASE_URL is set but psql is not installed on the host" >&2
      exit 1
    fi
    psql "${GATEWAY_DATABASE_URL}" -v ON_ERROR_STOP=1 "$@"
    return
  fi

  if command -v psql >/dev/null 2>&1; then
    PGPASSWORD="${POSTGRES_PASSWORD:-gateway-local-throwaway}" \
      psql -h "${POSTGRES_HOST:-127.0.0.1}" -p "${POSTGRES_HOST_PORT:-5433}" \
      -U "${POSTGRES_USER:-gateway}" -d "${POSTGRES_DB:-gateway}" \
      -v ON_ERROR_STOP=1 "$@"
    return
  fi

  echo "error: compose postgres is not running and host psql is unavailable" >&2
  echo "  start: docker compose up -d postgres" >&2
  echo "  or set GATEWAY_DATABASE_URL / install psql for direct access" >&2
  exit 1
}

sql_escape() {
  printf "%s" "$1" | sed "s/'/''/g"
}

require_upstream_keys() {
  local missing=()
  if [[ -z "${OPENAI_API_KEY__OPENAI_PRIMARY:-${OPENAI_API_KEY:-}}" ]]; then
    missing+=("OPENAI_API_KEY__OPENAI_PRIMARY or OPENAI_API_KEY")
  fi
  if [[ -z "${OPENAI_API_KEY__OPENAI_EMBED:-${OPENAI_API_KEY:-}}" ]]; then
    missing+=("OPENAI_API_KEY__OPENAI_EMBED or OPENAI_API_KEY")
  fi
  if ((${#missing[@]} > 0)); then
    echo "error: missing upstream credentials in .env:" >&2
    printf '  - %s\n' "${missing[@]}" >&2
    exit 1
  fi
}

import_catalog() {
  local chat_base embed_base
  chat_base="$(sql_escape "${OPENAI_CHAT_BASE_URL}")"
  embed_base="$(sql_escape "${OPENAI_EMBED_BASE_URL}")"

  run_psql <<SQL
BEGIN;

INSERT INTO providers (name, kind, base_url, enabled)
VALUES
  ('$(sql_escape "${OPENAI_CHAT_PROVIDER}")', 'openai', '${chat_base}', true),
  ('$(sql_escape "${OPENAI_EMBED_PROVIDER}")', 'openai', '${embed_base}', true)
ON CONFLICT (name) DO UPDATE
  SET base_url = EXCLUDED.base_url,
      enabled = true;

INSERT INTO model_catalog (public_name, provider, upstream_model, capabilities, enabled, config_version)
VALUES
  (
    '$(sql_escape "${OPENAI_CHAT_PUBLIC_MODEL}")',
    '$(sql_escape "${OPENAI_CHAT_PROVIDER}")',
    '$(sql_escape "${OPENAI_CHAT_UPSTREAM_MODEL}")',
    '${CHAT_CAPABILITIES}'::jsonb,
    true,
    1
  ),
  (
    '$(sql_escape "${OPENAI_EMBED_PUBLIC_MODEL}")',
    '$(sql_escape "${OPENAI_EMBED_PROVIDER}")',
    '$(sql_escape "${OPENAI_EMBED_UPSTREAM_MODEL}")',
    '${EMBED_CAPABILITIES}'::jsonb,
    true,
    1
  )
ON CONFLICT (public_name) DO UPDATE
  SET provider = EXCLUDED.provider,
      upstream_model = EXCLUDED.upstream_model,
      capabilities = EXCLUDED.capabilities,
      enabled = true,
      config_version = model_catalog.config_version + 1;

INSERT INTO model_routes (public_model, provider, upstream_model, priority, capabilities, timeout_ms, enabled, config_version)
VALUES
  (
    '$(sql_escape "${OPENAI_CHAT_PUBLIC_MODEL}")',
    '$(sql_escape "${OPENAI_CHAT_PROVIDER}")',
    '$(sql_escape "${OPENAI_CHAT_UPSTREAM_MODEL}")',
    10, '{}'::jsonb, 60000, true, 1
  ),
  (
    '$(sql_escape "${OPENAI_EMBED_PUBLIC_MODEL}")',
    '$(sql_escape "${OPENAI_EMBED_PROVIDER}")',
    '$(sql_escape "${OPENAI_EMBED_UPSTREAM_MODEL}")',
    10, '{}'::jsonb, 60000, true, 1
  )
ON CONFLICT (public_model, provider) DO UPDATE
  SET upstream_model = EXCLUDED.upstream_model,
      priority = EXCLUDED.priority,
      timeout_ms = EXCLUDED.timeout_ms,
      enabled = true,
      config_version = model_routes.config_version + 1;

INSERT INTO access_policies (
  subject_id, public_model, rate_per_minute, max_concurrent, daily_tokens,
  default_model, default_embedding_model
)
VALUES
  (
    '$(sql_escape "${SUBJECT}")',
    '$(sql_escape "${OPENAI_CHAT_PUBLIC_MODEL}")',
    120, 8, 1000000,
    '$(sql_escape "${OPENAI_CHAT_PUBLIC_MODEL}")',
    '$(sql_escape "${OPENAI_EMBED_PUBLIC_MODEL}")'
  ),
  (
    '$(sql_escape "${SUBJECT}")',
    '$(sql_escape "${OPENAI_EMBED_PUBLIC_MODEL}")',
    120, 8, 1000000,
    NULL,
    '$(sql_escape "${OPENAI_EMBED_PUBLIC_MODEL}")'
  )
ON CONFLICT (subject_id, public_model) DO UPDATE
  SET rate_per_minute = EXCLUDED.rate_per_minute,
      max_concurrent = EXCLUDED.max_concurrent,
      daily_tokens = EXCLUDED.daily_tokens,
      default_model = EXCLUDED.default_model,
      default_embedding_model = EXCLUDED.default_embedding_model;

UPDATE access_policies
SET default_model = '$(sql_escape "${OPENAI_CHAT_PUBLIC_MODEL}")',
    default_embedding_model = '$(sql_escape "${OPENAI_EMBED_PUBLIC_MODEL}")'
WHERE subject_id = '$(sql_escape "${SUBJECT}")'
  AND public_model = 'gateway-echo';

COMMIT;
SQL
}

restart_gateway_if_compose() {
  if ! "${COMPOSE[@]}" ps gateway --status running --quiet 2>/dev/null | grep -q .; then
    return 0
  fi
  echo "Restarting gateway to reload catalog..."
  "${COMPOSE[@]}" restart gateway
}

wait_ready() {
  local attempts=60
  for ((i = 1; i <= attempts; i++)); do
    if curl -fsS "${GATEWAY_PUBLIC_URL}/readyz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "Gateway /readyz did not return 200 in time." >&2
  exit 1
}

json_field() {
  python3 - "$1" "$2" <<'PY'
import json, sys
field, raw = sys.argv[1], sys.argv[2]
data = json.loads(raw)
value = data.get(field)
if value is None:
    sys.exit(1)
print(value)
PY
}

set_defaults_and_mint() {
  local auth=(-H "Authorization: Bearer ${ADMIN_TOKEN}" -H "Content-Type: application/json")
  local create_resp key_id api_key

  echo "Setting default chat model (${OPENAI_CHAT_PUBLIC_MODEL}) for ${SUBJECT}..."
  curl -fsS -X POST "${GATEWAY_ADMIN_URL}/admin/policies/${SUBJECT}/default-model" \
    "${auth[@]}" -d "{\"model\":\"${OPENAI_CHAT_PUBLIC_MODEL}\",\"kind\":\"chat\"}" >/dev/null

  echo "Setting default embedding model (${OPENAI_EMBED_PUBLIC_MODEL}) for ${SUBJECT}..."
  curl -fsS -X POST "${GATEWAY_ADMIN_URL}/admin/policies/${SUBJECT}/default-model" \
    "${auth[@]}" -d "{\"model\":\"${OPENAI_EMBED_PUBLIC_MODEL}\",\"kind\":\"embedding\"}" >/dev/null

  echo "Minting API key for ${SUBJECT}..."
  create_resp="$(curl -fsS -X POST "${GATEWAY_ADMIN_URL}/admin/keys" \
    "${auth[@]}" -d "{\"subject\":\"${SUBJECT}\",\"expires_in_hours\":8760}")"
  key_id="$(json_field key_id "${create_resp}")"
  api_key="$(json_field key "${create_resp}")"

  cat <<EOF

Done. Catalog imported from dev snapshot.

Providers:
  ${OPENAI_CHAT_PROVIDER} -> ${OPENAI_CHAT_BASE_URL}
  ${OPENAI_EMBED_PROVIDER} -> ${OPENAI_EMBED_BASE_URL}

Models:
  chat:  ${OPENAI_CHAT_PUBLIC_MODEL} (upstream ${OPENAI_CHAT_UPSTREAM_MODEL})
  embed: ${OPENAI_EMBED_PUBLIC_MODEL} (upstream ${OPENAI_EMBED_UPSTREAM_MODEL}, dim=${EMBED_DIM})

Suggested client env:

  KB_CHAT_API_KEY=${api_key}
  KB_EMBEDDING_API_KEY=${api_key}
  KB_CHAT_MODEL=${OPENAI_CHAT_PUBLIC_MODEL}
  KB_EMBEDDING_MODEL=${OPENAI_EMBED_PUBLIC_MODEL}
  KB_EMBEDDING_DIM=${EMBED_DIM}

Minted key id: ${key_id} (plaintext shown once above)
EOF
}

echo "Checking upstream credentials..."
require_upstream_keys

echo "Importing OpenAI chat + embedding catalog into gateway database..."
import_catalog

restart_gateway_if_compose

echo "Waiting for Gateway /readyz..."
wait_ready

echo "Verifying catalog..."
curl -fsS -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  "${GATEWAY_ADMIN_URL}/admin/models" \
  | python3 -c "import json,sys; m=json.load(sys.stdin); print('models:', ', '.join(sorted(x['public_name'] for x in m['models'])))"

set_defaults_and_mint
