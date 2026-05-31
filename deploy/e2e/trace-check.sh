#!/usr/bin/env bash
#
# Real-broker E2E (trace drill-down): prove that a dead-lettered message is not just a red
# edge on the map but a *queryable trace* — GET /traces surfaces it and GET /trace/{id}
# returns its causal chain ending in a DeadLettered hop that carries ErrorDetails. This is the
# "viewer → debugger" assertion: you can see WHY the message died, not just THAT it did.
#
# Run the stack first, then this script:
#   docker compose -f deploy/docker-compose.yml up --build      # engine + ui + rabbitmq
#   bash deploy/e2e/trace-check.sh
#
# What it does (all over the RabbitMQ Management HTTP API — no AMQP client needed):
#   1. wait for RabbitMQ + the engine to be ready
#   2. declare a main queue that dead-letters to a DLX, plus the DLX and its DLQ (same topology
#      as dlq-check.sh)
#   3. publish messages with NO consumer, so they expire (TTL) and dead-letter into the DLQ
#   4. poll GET /traces?status=deadlettered until a dead-lettered trace summary appears, and
#      take its traceId  (the real id embeds the vhost, e.g. "rmq-activity:/:orders.dlq:7")
#   5. GET /trace/{id} and assert the chain contains a hop with executionStatus==2
#      (DeadLettered), errorDetails.exceptionType=="DeadLettered", a non-empty message, and a
#      hop whose destination is the DLQ.
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

# JSON parsing uses jq if present, else python3. (We never parse JSON by hand.)
if command -v jq >/dev/null 2>&1; then
  JSON_TOOL="jq"
elif command -v python3 >/dev/null 2>&1; then
  JSON_TOOL="python3"
else
  echo "FAIL: need 'jq' or 'python3' to read the trace JSON (install one)." >&2
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

echo "── 1/5  Waiting for RabbitMQ + engine ───────────────────────────────────"
wait_for "RabbitMQ Management API" "$MGMT/api/overview"
wait_for "engine /healthz"         "$ENGINE/healthz"

echo "── 2/5  Declaring DLX topology ──────────────────────────────────────────"
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

echo "── 3/5  Publishing $MSG_COUNT messages to $MAIN_Q (no consumer) ──────────"
for i in $(seq 1 "$MSG_COUNT"); do
  resp=$(curl_api -X POST "$MGMT/api/exchanges/$VHOST/amq.default/publish" \
    -H 'content-type: application/json' \
    -d "{\"properties\":{},\"routing_key\":\"$MAIN_Q\",\"payload\":\"trace-e2e-$i\",\"payload_encoding\":\"string\"}")
  case "$resp" in
    *'"routed":true'*) ;;
    *) echo "FAIL: publish #$i was not routed: $resp" >&2; exit 1 ;;
  esac
done
echo "  ✓ published (each expires after ${TTL_MS}ms and dead-letters to $DLQ)"

# worstStatus / executionStatus == 2  ⇔  ExecutionStatus.DeadLettered (enums travel as integers).
# Prints the traceId of the first dead-lettered trace summary on stdin, or nothing.
first_deadlettered_trace_id() {
  if [ "$JSON_TOOL" = "jq" ]; then
    jq -r '[.[] | select(.worstStatus==2)][0].traceId // empty' 2>/dev/null
  else
    python3 -c '
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
for t in d:
    if t.get("worstStatus") == 2:
        print(t.get("traceId", "")); break'
  fi
}

# Returns 0 if the TraceView on stdin has a hop that is DeadLettered (executionStatus==2),
# carries ErrorDetails(exceptionType=="DeadLettered", non-empty message), AND a hop whose
# destination is the DLQ. Walks the causal tree recursively (roots → children).
trace_has_deadlettered_error() {
  if [ "$JSON_TOOL" = "jq" ]; then
    jq -e --arg dlq "$DLQ" '
      [.. | .envelope? // empty] as $envs
      | ($envs | any(
          .executionStatus == 2
          and .errorDetails != null
          and .errorDetails.exceptionType == "DeadLettered"
          and (.errorDetails.message | type == "string" and length > 0)))
        and ($envs | any(.destination == $dlq))' >/dev/null 2>&1
  else
    E2E_DLQ="$DLQ" python3 -c '
import os, sys, json
dlq = os.environ["E2E_DLQ"]
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
envs = []
def walk(node):
    if isinstance(node, dict):
        if "envelope" in node and isinstance(node["envelope"], dict):
            envs.append(node["envelope"])
        for v in node.values():
            walk(v)
    elif isinstance(node, list):
        for v in node:
            walk(v)
walk(d)
def is_dl_err(e):
    ed = e.get("errorDetails")
    return (e.get("executionStatus") == 2 and isinstance(ed, dict)
            and ed.get("exceptionType") == "DeadLettered"
            and isinstance(ed.get("message"), str) and len(ed.get("message")) > 0)
ok_err = any(is_dl_err(e) for e in envs)
ok_dlq = any(e.get("destination") == dlq for e in envs)
sys.exit(0 if (ok_err and ok_dlq) else 1)'
  fi
}

# Messages expire after TTL_MS and dead-letter; the engine then needs a poll to observe the DLQ
# depth grow and synthesize the DeadLettered activity envelope. Poll /traces for the summary.
DEADLINE=$(( (TTL_MS / 1000) + (POLL_S * 8) + 4 ))   # generous upper bound, in seconds
echo "── 4/5  Polling /traces?status=deadlettered up to ${DEADLINE}s ───────────"
trace_id=""
for _ in $(seq 1 "$DEADLINE"); do
  sleep 1
  traces="$(curl -fsS "$ENGINE/traces?status=deadlettered" || true)"
  [ -n "$traces" ] || continue
  trace_id="$(printf '%s' "$traces" | first_deadlettered_trace_id)"
  [ -n "$trace_id" ] && break
done

if [ -z "$trace_id" ]; then
  echo "" >&2
  echo "FAIL: no dead-lettered trace appeared in /traces?status=deadlettered after ${DEADLINE}s." >&2
  echo "Last /traces response was:" >&2
  printf '%s\n' "${traces:-<empty>}" >&2
  exit 1
fi
echo "  ✓ discovered dead-lettered trace: $trace_id"

echo "── 5/5  GET /trace/{id} — asserting causal chain + ErrorDetails ──────────"
# The traceId embeds the vhost "/" so the path has a slash; the engine's catch-all {*id} route
# captures it. curl sends it raw (colons are path-safe, the slash routes through).
trace="$(curl -fsS "$ENGINE/trace/$trace_id" || true)"

if [ -n "$trace" ] && printf '%s' "$trace" | trace_has_deadlettered_error; then
  echo ""
  echo "PASS: /trace/$trace_id returns a causal chain with a DeadLettered hop to $DLQ"
  echo "      carrying ErrorDetails(exceptionType=DeadLettered). The viewer is a debugger."
  exit 0
fi

echo "" >&2
echo "FAIL: /trace/$trace_id did not show a DeadLettered hop with ErrorDetails to $DLQ." >&2
echo "Trace response was:" >&2
if [ "$JSON_TOOL" = "jq" ]; then
  printf '%s' "$trace" | jq '.' >&2 || printf '%s\n' "${trace:-<empty>}" >&2
else
  printf '%s\n' "${trace:-<empty>}" >&2
fi
exit 1
