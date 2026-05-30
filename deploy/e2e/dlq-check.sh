#!/usr/bin/env bash
#
# Real-broker E2E: prove that a dead-lettered message surfaces as a DeadLettered
# edge in the engine's topology snapshot.
#
# Run the stack first, then this script:
#   docker compose -f deploy/docker-compose.yml up --build      # engine + ui + rabbitmq
#   bash deploy/e2e/dlq-check.sh
#
# What it does (all over the RabbitMQ Management HTTP API — no AMQP client needed):
#   1. wait for RabbitMQ + the engine to be ready
#   2. declare a main queue that dead-letters to a DLX, plus the DLX and its DLQ
#   3. publish messages to the main queue with NO consumer, so they expire (TTL)
#      and get dead-lettered into the DLQ
#   4. wait past the TTL + one engine poll, then assert /snapshot shows a
#      DeadLettered edge  dlx -> orders.dlq
#
# Exit 0 on success; non-zero with a clear message otherwise.

set -euo pipefail

# ── Config (override via env) ───────────────────────────────────────────────
MGMT="${HOPSCOPE_MGMT_URL:-http://localhost:15672}"   # RabbitMQ Management API
ENGINE="${HOPSCOPE_ENGINE_URL:-http://localhost:8085}" # engine REST (host-mapped)
AUTH="${HOPSCOPE_RABBITMQ_AUTH:-hopscope:hopscope}"    # management creds
VHOST="%2F"                                            # url-encoded "/"
TTL_MS=1000                                            # x-message-ttl on the main queue
POLL_S=2                                               # engine HOPSCOPE_RABBITMQ_POLL_SECONDS
MSG_COUNT=5

MAIN_Q="orders.main"
DLX="dlx"
DLQ="orders.dlq"

# ── Tooling preflight ───────────────────────────────────────────────────────
command -v curl >/dev/null 2>&1 || { echo "FAIL: 'curl' is required." >&2; exit 2; }

# JSON assertion uses jq if present, else python3. (We never parse JSON by hand.)
if command -v jq >/dev/null 2>&1; then
  JSON_TOOL="jq"
elif command -v python3 >/dev/null 2>&1; then
  JSON_TOOL="python3"
else
  echo "FAIL: need 'jq' or 'python3' to read the snapshot (install one)." >&2
  exit 2
fi

curl_api() { curl -fsS -u "$AUTH" "$@"; }

wait_for() {
  # wait_for <description> <url> [curl-extra-args...]
  local desc="$1" url="$2"; shift 2
  local tries=0
  until curl -fsS -u "$AUTH" "$@" "$url" >/dev/null 2>&1; do
    tries=$((tries + 1))
    if [ "$tries" -ge 60 ]; then
      echo "FAIL: timed out waiting for $desc ($url)." >&2
      exit 1
    fi
    sleep 1
  done
  echo "  ✓ $desc ready"
}

echo "── 1/4  Waiting for RabbitMQ + engine ───────────────────────────────────"
wait_for "RabbitMQ Management API" "$MGMT/api/overview"
wait_for "engine /healthz"         "$ENGINE/healthz"

echo "── 2/4  Declaring DLX topology ──────────────────────────────────────────"
# Main queue: expires messages after TTL_MS and dead-letters them to the DLX.
curl_api -X PUT "$MGMT/api/queues/$VHOST/$MAIN_Q" \
  -H 'content-type: application/json' \
  -d "{\"durable\":true,\"arguments\":{\"x-dead-letter-exchange\":\"$DLX\",\"x-message-ttl\":$TTL_MS}}" \
  >/dev/null
echo "  ✓ queue $MAIN_Q (x-dead-letter-exchange=$DLX, x-message-ttl=$TTL_MS)"

# Dead-letter exchange (fanout) + the dead-letter queue, bound together.
curl_api -X PUT "$MGMT/api/exchanges/$VHOST/$DLX" \
  -H 'content-type: application/json' \
  -d '{"type":"fanout","durable":true}' >/dev/null
echo "  ✓ exchange $DLX (fanout)"

curl_api -X PUT "$MGMT/api/queues/$VHOST/$DLQ" \
  -H 'content-type: application/json' \
  -d '{"durable":true}' >/dev/null
echo "  ✓ queue $DLQ"

curl_api -X POST "$MGMT/api/bindings/$VHOST/e/$DLX/q/$DLQ" \
  -H 'content-type: application/json' -d '{"routing_key":""}' >/dev/null
echo "  ✓ binding $DLX -> $DLQ"

echo "── 3/4  Publishing $MSG_COUNT messages to $MAIN_Q (no consumer) ──────────"
for i in $(seq 1 "$MSG_COUNT"); do
  resp=$(curl_api -X POST "$MGMT/api/exchanges/$VHOST/amq.default/publish" \
    -H 'content-type: application/json' \
    -d "{\"properties\":{},\"routing_key\":\"$MAIN_Q\",\"payload\":\"dlq-e2e-$i\",\"payload_encoding\":\"string\"}")
  case "$resp" in
    *'"routed":true'*) ;;
    *) echo "FAIL: publish #$i was not routed: $resp" >&2; exit 1 ;;
  esac
done
echo "  ✓ published (each expires after ${TTL_MS}ms and dead-letters to $DLQ)"

# lastStatus == 2  ⇔  ExecutionStatus.DeadLettered (enums travel the wire as integers).
# Returns 0 if the snapshot on stdin has a DeadLettered  DLX -> DLQ  edge.
has_dlq_edge() {
  if [ "$JSON_TOOL" = "jq" ]; then
    jq -e --arg s "$DLX" --arg t "$DLQ" \
       '.edges[] | select(.sourceId==$s and .targetId==$t and .lastStatus==2)' >/dev/null 2>&1
  else
    # Values via env (E2E_SRC/E2E_TGT) so there's no shell-quoting inside the python.
    E2E_SRC="$DLX" E2E_TGT="$DLQ" python3 -c '
import os, sys, json
src, tgt = os.environ["E2E_SRC"], os.environ["E2E_TGT"]
d = json.load(sys.stdin)
sys.exit(0 if any(e.get("sourceId") == src and e.get("targetId") == tgt and e.get("lastStatus") == 2
                  for e in d.get("edges", [])) else 1)'
  fi
}

# Messages expire after TTL_MS and dead-letter; the engine then needs a poll to see
# the DLQ depth grow. Poll the snapshot until the DeadLettered edge appears (or give up).
DEADLINE=$(( (TTL_MS / 1000) + (POLL_S * 8) + 4 ))   # generous upper bound, in seconds
echo "── 4/4  Polling /snapshot up to ${DEADLINE}s for the DeadLettered edge ───"
snapshot=""
for _ in $(seq 1 "$DEADLINE"); do
  sleep 1
  snapshot="$(curl -fsS "$ENGINE/snapshot" || true)"
  if [ -n "$snapshot" ] && printf '%s' "$snapshot" | has_dlq_edge; then
    echo ""
    echo "PASS: snapshot shows a DeadLettered edge  $DLX -> $DLQ  (lastStatus=2)."
    exit 0
  fi
done

echo "" >&2
echo "FAIL: no DeadLettered edge $DLX -> $DLQ in /snapshot after ${DEADLINE}s." >&2
echo "Last snapshot edges were:" >&2
if [ "$JSON_TOOL" = "jq" ]; then
  printf '%s' "$snapshot" | jq '.edges' >&2 || printf '%s\n' "$snapshot" >&2
else
  printf '%s\n' "$snapshot" >&2
fi
exit 1
