#!/usr/bin/env bash
#
# Real-broker E2E (the Phase-4 multi-broker seam proof): prove that real Redis keyevent
# traffic AND real RabbitMQ traffic co-render as nodes on the SAME engine topology snapshot.
#
# Run the stack first, then this script:
#   docker compose -f deploy/docker-compose.yml up --build   # engine + ui + rabbitmq + redis
#   bash deploy/e2e/redis-check.sh
#
# What it does (no language SDKs — redis-cli via docker exec + RabbitMQ Management HTTP API):
#   1. wait for the engine, Redis, and RabbitMQ to be ready
#   2. generate Redis keyevents (SET/DEL on colon-namespaced keys: user:*, order:*)
#   3. generate RabbitMQ topology+traffic (declare a queue, publish a message)
#   4. poll /snapshot until it contains BOTH a brokerType=="Redis" node AND a
#      brokerType=="RabbitMQ" node — i.e. both brokers on one canvas
#
# Exit 0 on success; non-zero with a clear message otherwise.

set -euo pipefail

# ── Config (override via env) ───────────────────────────────────────────────
ENGINE="${HOPSCOPE_ENGINE_URL:-http://localhost:8085}"   # engine REST (host-mapped)
MGMT="${HOPSCOPE_MGMT_URL:-http://localhost:15672}"      # RabbitMQ Management API
AUTH="${HOPSCOPE_RABBITMQ_AUTH:-hopscope:hopscope}"      # management creds
REDIS_CTR="${HOPSCOPE_REDIS_CONTAINER:-hopscope-redis}"  # for docker exec redis-cli
VHOST="%2F"                                              # url-encoded "/"
POLL_S=2                                                 # engine poll cadence
RMQ_Q="orders.redischeck"

# ── Tooling preflight ───────────────────────────────────────────────────────
command -v curl   >/dev/null 2>&1 || { echo "FAIL: 'curl' is required." >&2; exit 2; }
command -v docker >/dev/null 2>&1 || { echo "FAIL: 'docker' is required (redis-cli via exec)." >&2; exit 2; }

if command -v jq >/dev/null 2>&1; then
  JSON_TOOL="jq"
elif command -v python3 >/dev/null 2>&1; then
  JSON_TOOL="python3"
else
  echo "FAIL: need 'jq' or 'python3' to read the snapshot (install one)." >&2
  exit 2
fi

curl_api() { curl -fsS -u "$AUTH" "$@"; }
redis_cli() { docker exec "$REDIS_CTR" redis-cli "$@"; }

# ── POLLING IDIOM (mirrors dlq-check.sh) ────────────────────────────────────
wait_for() {
  # wait_for <description> <url> [curl-extra-args...]
  local desc="$1" url="$2"; shift 2
  local tries=0
  until curl -fsS "$@" "$url" >/dev/null 2>&1; do
    tries=$((tries + 1))
    if [ "$tries" -ge 60 ]; then
      echo "FAIL: timed out waiting for $desc ($url)." >&2
      exit 1
    fi
    sleep 1
  done
  echo "  ✓ $desc ready"
}

echo "── 1/4  Waiting for engine + Redis + RabbitMQ ───────────────────────────"
wait_for "engine /healthz"         "$ENGINE/healthz"
wait_for "RabbitMQ Management API" "$MGMT/api/overview" -u "$AUTH"
# Redis readiness: redis-cli ping via the container.
redis_tries=0
until [ "$(redis_cli ping 2>/dev/null || true)" = "PONG" ]; do
  redis_tries=$((redis_tries + 1))
  if [ "$redis_tries" -ge 60 ]; then
    echo "FAIL: timed out waiting for Redis (docker exec $REDIS_CTR redis-cli ping)." >&2
    exit 1
  fi
  sleep 1
done
echo "  ✓ Redis ready"

echo "── 2/4  Generating Redis keyevents (SET/DEL on user:* and order:*) ───────"
# notify-keyspace-events is set via compose (KEA); set it again defensively in case the
# stack was started without it. Harmless if already enabled.
redis_cli CONFIG SET notify-keyspace-events KEA >/dev/null 2>&1 || true
for i in $(seq 1 10); do
  redis_cli SET "user:$i"  "v$i" >/dev/null
  redis_cli SET "order:$i" "v$i" >/dev/null
  redis_cli DEL "user:$i"        >/dev/null
done
echo "  ✓ generated SET/DEL keyevents (→ Redis Topic nodes user:*, order:*)"

echo "── 3/4  Generating RabbitMQ traffic (declare queue + publish) ────────────"
curl_api -X PUT "$MGMT/api/queues/$VHOST/$RMQ_Q" \
  -H 'content-type: application/json' -d '{"durable":true}' >/dev/null
echo "  ✓ queue $RMQ_Q"
resp=$(curl_api -X POST "$MGMT/api/exchanges/$VHOST/amq.default/publish" \
  -H 'content-type: application/json' \
  -d "{\"properties\":{},\"routing_key\":\"$RMQ_Q\",\"payload\":\"redis-e2e\",\"payload_encoding\":\"string\"}")
case "$resp" in
  *'"routed":true'*) echo "  ✓ published to $RMQ_Q" ;;
  *) echo "FAIL: RabbitMQ publish was not routed: $resp" >&2; exit 1 ;;
esac

# Returns 0 if the snapshot on stdin has BOTH a Redis node AND a RabbitMQ node.
has_both_brokers() {
  if [ "$JSON_TOOL" = "jq" ]; then
    jq -e '([.nodes[].brokerType] | (any(. == "Redis")) and (any(. == "RabbitMQ")))' \
       >/dev/null 2>&1
  else
    python3 -c '
import sys, json
d = json.load(sys.stdin)
kinds = {n.get("brokerType") for n in d.get("nodes", [])}
sys.exit(0 if ("Redis" in kinds and "RabbitMQ" in kinds) else 1)'
  fi
}

# ── BOUNDED POLLING IDIOM ───────────────────────────────────────────────────
# Redis keyevents are pushed in real time; RabbitMQ needs an engine poll cycle. Generous bound.
DEADLINE=$(( (POLL_S * 10) + 6 ))
echo "── 4/4  Polling /snapshot up to ${DEADLINE}s for BOTH brokers ────────────"
snapshot=""
for _ in $(seq 1 "$DEADLINE"); do
  sleep 1
  snapshot="$(curl -fsS "$ENGINE/snapshot" || true)"
  if [ -n "$snapshot" ] && printf '%s' "$snapshot" | has_both_brokers; then
    echo ""
    echo "PASS: snapshot shows BOTH a Redis node AND a RabbitMQ node — multi-broker on one canvas."
    exit 0
  fi
done

echo "" >&2
echo "FAIL: snapshot did not contain both a Redis and a RabbitMQ node after ${DEADLINE}s." >&2
echo "Last snapshot nodes were:" >&2
if [ "$JSON_TOOL" = "jq" ]; then
  printf '%s' "$snapshot" | jq '[.nodes[] | {id, brokerType}]' >&2 || printf '%s\n' "$snapshot" >&2
else
  printf '%s\n' "$snapshot" >&2
fi
exit 1
