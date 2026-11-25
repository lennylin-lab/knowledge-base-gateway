#!/usr/bin/env bash
# Bounded smoke test for the gateway container stack (docker-compose.yml)
# running against external PostgreSQL (and external Redis when
# GATEWAY_LIMITS_MODE=redis).
#
# Prerequisites:
#   - External PostgreSQL is reachable at GATEWAY_DATABASE_URL (set in env or .env)
#   - External Redis is reachable at GATEWAY_REDIS_ADDR when GATEWAY_LIMITS_MODE=redis
#     (default in docker-compose.yml)
#
# Proves, within bounded timeouts:
#   1. the gateway image builds and starts against external storage,
#   2. the one-shot migration job completed successfully and reached the latest
#      migration version (dirty=false),
#   3. /healthz answers 200 (liveness),
#   4. /readyz answers 200 (database, redis if enabled, catalog ready),
#   5. the chat path works end to end: a throwaway API key minted through the
#      admin API completes a non-streaming chat completion against the seeded
#      fake provider with an OpenAI-compatible envelope,
#   6. /v1/models and /v1/responses work against the seeded fake provider.
#
# On any failure the relevant service logs are printed to stderr and the
# script exits with a distinct non-zero status:
#
#   0  success
#   1  preflight error (docker/curl missing, missing GATEWAY_DATABASE_URL, bad arguments)
#   2  stack failed to build/start or migrate did not complete / schema mismatch
#   3  /healthz never returned 200
#   4  /readyz never returned 200
#  10  admin API: never became ready or the key could not be minted
#  11  chat completion: request failed or the envelope was not a
#      successful OpenAI-compatible completion
#  12  responses/models path: model discovery or the /v1/responses
#      protocol failed against the seeded fake provider
#
# Usage: scripts/smoke.sh [--skip-outage] [--down] [--timeout SECONDS]
#   (--skip-outage is accepted for compatibility; storage containers are external
#    and are not stopped or started by this script.)
#
# Logs printed on failure are structured gateway/migrate lines; none of them
# contain credentials — the DSN, the admin token, and minted API keys are
# never echoed.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/docker-compose.yml")
ENV_FILE="$ROOT/.env"

# Load .env for defaults when not already set in the caller's environment
# (existing environment variables take precedence, matching docker compose).
if [ -f "$ENV_FILE" ]; then
  while IFS= read -r line || [ -n "$line" ]; do
    line="${line%$'\r'}"
    case "$line" in
      ''|'#'*) continue ;;
      *=*)
        key="${line%%=*}"
        val="${line#*=}"
        key="${key#export }"
        # Strip matching outer single or double quotes if present.
        if [ "${#val}" -ge 2 ]; then
          case "$val" in
            \"*\") val="${val#\"}"; val="${val%\"}" ;;
            \'*\') val="${val#\'}"; val="${val%\'}" ;;
          esac
        fi
        if [ -z "${!key+x}" ]; then
          export "$key=$val"
        fi
        ;;
    esac
  done < "$ENV_FILE"
fi

SMOKE_URL="${GATEWAY_SMOKE_URL:-http://127.0.0.1:${GATEWAY_HOST_PORT:-8091}}"
ADMIN_URL="${GATEWAY_ADMIN_URL:-http://127.0.0.1:${GATEWAY_ADMIN_HOST_PORT:-8092}}"
ADMIN_TOKEN="${GATEWAY_ADMIN_TOKEN:-smoke-admin-throwaway}"
WAIT_SECONDS="${SMOKE_WAIT_SECONDS:-90}"
TEARDOWN=0

log() { printf '[smoke] %s\n' "$*"; }

usage() {
  # Print the header comment (from line 2 up to the first code line).
  awk 'NR==1 {next} /^#/ {sub(/^# ?/, ""); print; next} {exit}' "${BASH_SOURCE[0]}"
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --skip-outage) ;; # accepted for compatibility; storage is external
    --down) TEARDOWN=1 ;;
    --timeout) shift; WAIT_SECONDS="${1:-90}" ;;
    -h|--help) usage; exit 0 ;;
    *) printf '[smoke] unknown argument: %s\n' "$1" >&2; usage >&2; exit 1 ;;
  esac
  shift
done

# On any non-zero exit, dump service diagnostics before the shell unwinds.
rc=0
trap 'rc=$?; if [ "$rc" -ne 0 ]; then printf "\n[smoke] FAILED with exit status %s; service diagnostics follow\n" "$rc" >&2; dump_logs; fi' EXIT

# http_code URL -> prints the HTTP status code ("000" when unreachable).
http_code() {
  curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$1" 2>/dev/null || printf '000'
}

# wait_for URL WANT_CODE DEADLINE_EPOCH -> succeeds when the URL answers WANT.
wait_for() {
  local url="$1" want="$2" deadline="$3" code=""
  while [ "$(date +%s)" -lt "$deadline" ]; do
    code="$(http_code "$url")"
    if [ "$code" = "$want" ]; then
      log "ok: $url -> $code"
      return 0
    fi
    sleep 1
  done
  log "timeout: $url never returned $want within budget (last code: ${code:-none})"
  return 1
}

# shellcheck disable=SC2317  # reached via the EXIT trap above
dump_logs() {
  "${COMPOSE[@]}" ps -a >&2 2>/dev/null || true
  printf '\n[smoke] --- service logs (last 200 lines each) ---\n' >&2
  "${COMPOSE[@]}" logs --no-color --tail=200 migrate gateway >&2 2>/dev/null || true
}

# --- Chat-path phase ----------------------------------------------------------
# The compose gateway runs the admin API with the throwaway token from
# docker-compose.yml (dev stack only). The minted key and the token are used
# in request headers only and are never printed.

# admin_code -> HTTP status of an authenticated admin probe ("000" when the
# port is not accepting yet).
admin_code() {
  curl -s -o /dev/null -w '%{http_code}' --max-time 2 \
    -H "Authorization: Bearer ${ADMIN_TOKEN}" \
    "${ADMIN_URL}/admin/keys?subject=__smoke_probe__" 2>/dev/null || printf '000'
}

# json_string FIELD JSON -> value of a top-level string field (compact Go
# encoder output), empty when absent.
json_string() {
  printf '%s' "$2" | sed -n "s/.*\"$1\":\"\\([^\"]*\\)\".*/\\1/p"
}

chat_phase() {
  local deadline=""

  log "waiting for the admin API to answer an authenticated probe"
  deadline=$(( $(date +%s) + WAIT_SECONDS ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    [ "$(admin_code)" = "200" ] && break
    sleep 1
  done
  if [ "$(admin_code)" != "200" ]; then
    log "admin API never answered 200 (is GATEWAY_ADMIN_TOKEN set the same on both sides?)"
    return 10
  fi
  log "ok: admin API reachable"

  log "minting a throwaway API key for the seeded subject"
  local create_resp="" smoke_key_id=""
  create_resp="$(curl -s --max-time 5 -X POST \
    -H "Authorization: Bearer ${ADMIN_TOKEN}" -H 'Content-Type: application/json' \
    -d '{"subject":"subject_default","expires_in_hours":1}' \
    "${ADMIN_URL}/admin/keys" 2>/dev/null)" || create_resp=""
  SMOKE_KEY="$(json_string key "$create_resp")"
  SMOKE_KEY_ID="$(json_string key_id "$create_resp")"
  if [ -z "${SMOKE_KEY:-}" ] || [ -z "${SMOKE_KEY_ID:-}" ]; then
    # The response body holds the plaintext key, so it is never dumped.
    log "admin key minting failed (status: $(admin_code); response withheld: it contains the plaintext key)"
    return 10
  fi
  log "ok: key minted (id: ${SMOKE_KEY_ID})"

  log "requesting a non-streaming chat completion from the seeded fake provider"
  local body_file chat_status=""
  body_file="$(mktemp)"
  chat_status="$(curl -s --max-time 15 -o "$body_file" -w '%{http_code}' -X POST \
    -H "Authorization: Bearer ${SMOKE_KEY}" -H 'Content-Type: application/json' \
    -d '{"model":"gateway-echo","messages":[{"role":"user","content":"smoke-chat"}],"max_tokens":16}' \
    "${SMOKE_URL}/v1/chat/completions" 2>/dev/null)" || chat_status="000"
  if [ "$chat_status" != "200" ]; then
    log "chat completion failed (HTTP ${chat_status:-none}); response body follows"
    cat "$body_file" >&2 2>/dev/null || true
    rm -f "$body_file"
    return 11
  fi
  if ! grep -q '"object":"chat.completion"' "$body_file" \
    || ! grep -q '"choices":\[' "$body_file" \
    || ! grep -q '"finish_reason":"stop"' "$body_file" \
    || ! grep -q '"content":"echo: smoke-chat"' "$body_file"; then
    log "chat response is not a successful OpenAI-compatible completion; body follows"
    cat "$body_file" >&2 2>/dev/null || true
    rm -f "$body_file"
    return 11
  fi
  rm -f "$body_file"
  log "ok: chat completion returned the expected fake-provider envelope"
  return 0
}

# revoke_smoke_key KEY_ID -> best-effort cleanup of the throwaway key.
revoke_smoke_key() {
  local revoke_code=""
  revoke_code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 -X POST \
    -H "Authorization: Bearer ${ADMIN_TOKEN}" \
    "${ADMIN_URL}/admin/keys/$1/revoke" 2>/dev/null)" || revoke_code="000"
  if [ "$revoke_code" = "200" ]; then
    log "ok: throwaway key revoked"
  else
    log "warn: key revoke returned ${revoke_code:-none} (throwaway stack; not failing the smoke)"
  fi
}

# --- Responses/models phase -----------------------------------------------------
# Uses the key minted by chat_phase (passed as $1). Verifies caller-filtered
# discovery and the /v1/responses protocol against the seeded fake provider.

responses_phase() {
  local smoke_key="$1"
  local models_status="" body_file=""

  log "requesting model discovery"
  body_file="$(mktemp)"
  models_status="$(curl -s --max-time 10 -o "$body_file" -w '%{http_code}' \
    -H "Authorization: Bearer ${smoke_key}" \
    "${SMOKE_URL}/v1/models" 2>/dev/null)" || models_status="000"
  if [ "$models_status" != "200" ] \
    || ! grep -q '"gateway-echo"' "$body_file" \
    || grep -q '"fake' "$body_file"; then
    log "model discovery failed or leaked provider details (HTTP ${models_status:-none}); body follows"
    cat "$body_file" >&2 2>/dev/null || true
    rm -f "$body_file"
    return 12
  fi
  log "ok: /v1/models lists only public model metadata"

  log "requesting a non-streaming response from the seeded fake provider"
  local resp_status=""
  resp_status="$(curl -s --max-time 15 -o "$body_file" -w '%{http_code}' -X POST \
    -H "Authorization: Bearer ${smoke_key}" -H 'Content-Type: application/json' \
    -d '{"model":"gateway-echo","input":"smoke-responses"}' \
    "${SMOKE_URL}/v1/responses" 2>/dev/null)" || resp_status="000"
  if [ "$resp_status" != "200" ] \
    || ! grep -q '"object":"response"' "$body_file" \
    || ! grep -q '"status":"completed"' "$body_file" \
    || ! grep -q '"output_text"' "$body_file"; then
    log "responses call failed or returned an unexpected envelope (HTTP ${resp_status:-none}); body follows"
    cat "$body_file" >&2 2>/dev/null || true
    rm -f "$body_file"
    return 12
  fi
  rm -f "$body_file"
  log "ok: /v1/responses returned the expected stable envelope"
  return 0
}

# --- Preflight ---------------------------------------------------------------
for bin in docker curl; do
  command -v "$bin" >/dev/null 2>&1 || { log "preflight: '$bin' is required but not installed"; exit 1; }
done
docker compose version >/dev/null 2>&1 || { log "preflight: 'docker compose' is not available"; exit 1; }
if [ -z "${GATEWAY_DATABASE_URL:-}" ]; then
  log "preflight: GATEWAY_DATABASE_URL must be set (in env or .env) pointing to an external PostgreSQL database"
  exit 1
fi
if [ "${GATEWAY_LIMITS_MODE:-redis}" = "redis" ] && [ -z "${GATEWAY_REDIS_ADDR:-}" ]; then
  log "preflight: GATEWAY_REDIS_ADDR must be set when GATEWAY_LIMITS_MODE=redis (or set GATEWAY_LIMITS_MODE=local)"
  exit 1
fi

# --- Build and start ---------------------------------------------------------
log "building and starting the stack (gateway on $SMOKE_URL)"
"${COMPOSE[@]}" up -d --build || { log "docker compose up failed"; exit 2; }

# --- Migration completed ------------------------------------------------------
deadline=$(( $(date +%s) + WAIT_SECONDS ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  migrate_exit="$("${COMPOSE[@]}" ps -a --format '{{.Service}} {{.ExitCode}}' 2>/dev/null | awk '$1=="migrate"{print $2}')"
  [ "$migrate_exit" = "0" ] && break
  sleep 1
done
if [ "${migrate_exit:-}" != "0" ]; then
  log "migration job did not complete successfully (exit: ${migrate_exit:-unknown})"
  exit 2
fi
log "ok: migration job completed (exit 0)"

# --- Migration version assertion ------------------------------------------------
# The applied schema_migrations version must equal the highest migration file
# in the repository, so a migration silently failing to apply (or a stale
# image shipping an old migrations directory) fails the smoke instead of
# serving against a partially migrated schema. Mirrors the derivation in
# .github/workflows/ci.yml using the one-shot migrate service against the
# external PostgreSQL database.
expected_version=$((10#$(find "$ROOT/migrations" -maxdepth 1 -name '*.up.sql' \
  | sed -E 's|.*/([0-9]+)_.*|\1|' | sort -n | tail -1)))
version_out="$("${COMPOSE[@]}" run --rm --no-deps migrate -dir /app/migrations version 2>/dev/null | tr -d '\r' | tail -1)"
want_version="version ${expected_version} (dirty=false)"
if [ "$version_out" != "$want_version" ]; then
  log "schema version mismatch: got '${version_out:-empty}', want '${want_version}'"
  exit 2
fi
log "ok: schema at ${version_out}"

# --- Liveness, then readiness (each phase gets its own WAIT_SECONDS budget) ---
if ! wait_for "$SMOKE_URL/healthz" 200 "$(( $(date +%s) + WAIT_SECONDS ))"; then
  exit 3
fi
if ! wait_for "$SMOKE_URL/readyz" 200 "$(( $(date +%s) + WAIT_SECONDS ))"; then
  exit 4
fi

# --- Chat path: admin-minted key, non-streaming fake-provider completion ------
SMOKE_KEY="" SMOKE_KEY_ID=""
chat_phase_status=0
chat_phase || chat_phase_status=$?
if [ "$chat_phase_status" -ne 0 ]; then
  exit "$chat_phase_status"
fi

# --- Responses path: discovery plus the /v1/responses protocol ----------------
responses_phase "$SMOKE_KEY" || responses_status=$?
revoke_smoke_key "$SMOKE_KEY_ID"
if [ "${responses_status:-0}" -ne 0 ]; then
  exit "$responses_status"
fi

log "PASS: migration completed at the expected version, /healthz and /readyz verified, chat and responses paths verified"
if [ "$TEARDOWN" -eq 1 ]; then
  log "tearing down gateway stack (--down)"
  "${COMPOSE[@]}" down --remove-orphans >/dev/null 2>&1 || true
fi
exit 0
