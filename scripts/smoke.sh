#!/usr/bin/env bash
# Bounded smoke test for the gateway container stack (docker-compose.yml).
#
# Proves, within bounded timeouts:
#   1. the stack builds and starts with no manual database setup,
#   2. the one-shot migration job completed successfully,
#   3. /healthz answers 200 (liveness),
#   4. /readyz answers 200 only while dependencies are ready — stopping
#      PostgreSQL or Redis flips readiness to 503 while liveness stays 200,
#   5. restarting the dependency restores readiness.
#
# On any failure the relevant service logs are printed to stderr and the
# script exits with a distinct non-zero status:
#
#   0  success
#   1  preflight error (docker/curl missing, bad arguments)
#   2  stack failed to build/start or migrate did not complete
#   3  /healthz never returned 200
#   4  /readyz never returned 200
#   5  redis outage: /readyz did not fail while redis was down
#   6  postgres outage: /readyz did not fail while postgres was down
#   7  redis restart: /readyz did not return to 200
#   8  postgres restart: /readyz did not return to 200
#   9  liveness regression: /healthz stopped answering during an outage
#
# Usage: scripts/smoke.sh [--skip-outage] [--down] [--timeout SECONDS]
#
# Logs printed on failure are structured gateway/migrate lines and standard
# PostgreSQL/Redis service logs; none of them contain credentials — the DSN
# is never echoed and the gateway never logs secrets.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/docker-compose.yml")

SMOKE_URL="${GATEWAY_SMOKE_URL:-http://127.0.0.1:${GATEWAY_HOST_PORT:-8091}}"
WAIT_SECONDS="${SMOKE_WAIT_SECONDS:-90}"
OUTAGE_SECONDS="${SMOKE_OUTAGE_SECONDS:-30}"
SKIP_OUTAGE=0
TEARDOWN=0

log() { printf '[smoke] %s\n' "$*"; }

usage() {
  # Print the header comment (from line 2 up to the first code line).
  awk 'NR==1 {next} /^#/ {sub(/^# ?/, ""); print; next} {exit}' "${BASH_SOURCE[0]}"
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --skip-outage) SKIP_OUTAGE=1 ;;
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
  "${COMPOSE[@]}" logs --no-color --tail=200 postgres redis migrate gateway >&2 2>/dev/null || true
}

# outage_and_recovery DEP NO_FAIL_RC NO_RECOVER_RC LIVENESS_RC
outage_and_recovery() {
  local dep="$1" no_fail_rc="$2" no_recover_rc="$3" liveness_rc="$4"
  local deadline ready="" health=""

  log "stopping $dep to verify readiness fails while liveness holds"
  "${COMPOSE[@]}" stop "$dep" >/dev/null || { log "could not stop $dep"; return 2; }

  deadline=$(( $(date +%s) + OUTAGE_SECONDS ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    ready="$(http_code "$SMOKE_URL/readyz")"
    health="$(http_code "$SMOKE_URL/healthz")"
    if [ "$ready" = "503" ] && [ "$health" = "200" ]; then
      log "ok: /readyz -> 503 while /healthz stays 200 with $dep down"
      break
    fi
    if [ "$health" != "200" ]; then
      log "liveness regression: /healthz -> ${health:-none} during $dep outage"
      return "$liveness_rc"
    fi
    sleep 1
  done
  if [ "$ready" != "503" ]; then
    log "readiness did not fail while $dep was down (readyz=${ready:-none} healthz=${health:-none})"
    return "$no_fail_rc"
  fi

  log "restarting $dep and waiting for readiness to recover"
  "${COMPOSE[@]}" start "$dep" >/dev/null || { log "could not start $dep"; return 2; }
  if ! wait_for "$SMOKE_URL/readyz" 200 "$(( $(date +%s) + WAIT_SECONDS ))"; then
    log "readiness did not recover after $dep restart"
    return "$no_recover_rc"
  fi
  return 0
}

# --- Preflight ---------------------------------------------------------------
for bin in docker curl; do
  command -v "$bin" >/dev/null 2>&1 || { log "preflight: '$bin' is required but not installed"; exit 1; }
done
docker compose version >/dev/null 2>&1 || { log "preflight: 'docker compose' is not available"; exit 1; }

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

# --- Liveness, then readiness (each phase gets its own WAIT_SECONDS budget) ---
if ! wait_for "$SMOKE_URL/healthz" 200 "$(( $(date +%s) + WAIT_SECONDS ))"; then
  exit 3
fi
if ! wait_for "$SMOKE_URL/readyz" 200 "$(( $(date +%s) + WAIT_SECONDS ))"; then
  exit 4
fi

# --- Dependency outage and recovery -------------------------------------------
if [ "$SKIP_OUTAGE" -ne 1 ]; then
  outage_and_recovery redis 5 7 9    || exit $?
  outage_and_recovery postgres 6 8 9 || exit $?
else
  log "skipping dependency outage checks (--skip-outage)"
fi

log "PASS: migration completed, /healthz and /readyz verified, outage and recovery behavior confirmed"
if [ "$TEARDOWN" -eq 1 ]; then
  log "tearing down stack and volumes (--down)"
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
fi
exit 0
