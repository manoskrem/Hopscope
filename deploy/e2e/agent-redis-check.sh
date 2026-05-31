#!/usr/bin/env bash
#
# The no-code headline (Phase-5 eBPF agent, Redis-RESP slice): prove that real Redis
# traffic renders on the engine canvas sourced ONLY from the agent's kernel capture —
# with the target NOT cooperating at all.
#
# This runs against the STANDALONE stack deploy/docker-compose.agent.yml, where:
#   * Redis runs with keyspace notifications OFF, and
#   * the engine has NO Redis provider (no HOPSCOPE_REDIS_URL).
# So a Redis node on /snapshot can ONLY come from the agent's eBPF tcp_sendmsg capture.
# Needs a real Linux kernel with BTF + privileged Docker (the GitHub ubuntu-latest runner).
#
#   docker compose -f deploy/docker-compose.agent.yml up -d --build --wait
#   bash deploy/e2e/agent-redis-check.sh
#
# Exit 0 on success; non-zero with a clear message otherwise.

set -euo pipefail

# ── Config (override via env) ───────────────────────────────────────────────
ENGINE="${HOPSCOPE_ENGINE_URL:-http://localhost:8085}"   # engine REST (host-mapped)
REDIS_CTR="${HOPSCOPE_REDIS_CONTAINER:-hopscope-redis}"   # for docker exec redis-cli
AGENT_CTR="${HOPSCOPE_AGENT_CONTAINER:-hopscope-agent}"   # for logs on failure
DEADLINE="${HOPSCOPE_SNAPSHOT_DEADLINE:-40}"             # seconds to wait for the hop

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

redis_cli() { docker exec "$REDIS_CTR" redis-cli "$@"; }

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

echo "── 1/3  Waiting for engine + Redis ──────────────────────────────────────"
wait_for "engine /healthz" "$ENGINE/healthz"
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

echo "── 2/3  Driving plain redis-cli traffic (SET/GET/DEL on user:* and order:*) ──"
# Deliberately NO 'CONFIG SET notify-keyspace-events' — keyevents stay OFF. The engine
# has no Redis provider, so nothing but the agent's kernel capture can observe this.
for i in $(seq 1 20); do
  redis_cli SET "user:$i"  "v$i" >/dev/null
  redis_cli GET "user:$i"        >/dev/null
  redis_cli SET "order:$i" "v$i" >/dev/null
  redis_cli DEL "user:$i"        >/dev/null
done
echo "  ✓ issued commands (the target did NOT cooperate — no keyevents, no provider)"

# Returns 0 if the snapshot on stdin has a brokerType=="Redis" node AND the "user:*"
# Topic node. Those ids are produced only by the agent's RESP→envelope mapping, and no
# Redis provider is configured, so they can ONLY have come from kernel capture.
has_agent_redis() {
  if [ "$JSON_TOOL" = "jq" ]; then
    jq -e '
      ([.nodes[].brokerType] | any(. == "Redis"))
      and ([.nodes[].id] | any(. == "user:*"))
    ' >/dev/null 2>&1
  else
    python3 -c '
import sys, json
d = json.load(sys.stdin)
nodes = d.get("nodes", [])
has_redis = any(n.get("brokerType") == "Redis" for n in nodes)
has_topic = any(n.get("id") == "user:*" for n in nodes)
sys.exit(0 if (has_redis and has_topic) else 1)'
  fi
}

echo "── 3/3  Polling /snapshot up to ${DEADLINE}s for the agent-sourced Redis hop ──"
snapshot=""
for _ in $(seq 1 "$DEADLINE"); do
  sleep 1
  snapshot="$(curl -fsS "$ENGINE/snapshot" || true)"
  if [ -n "$snapshot" ] && printf '%s' "$snapshot" | has_agent_redis; then
    echo ""
    echo "PASS: /snapshot shows a Redis node and the user:* Topic — rendered from kernel"
    echo "      capture alone (Redis provider OFF, keyevents OFF). The target did not cooperate."
    exit 0
  fi
done

echo "" >&2
echo "FAIL: no agent-sourced Redis hop on /snapshot after ${DEADLINE}s." >&2
echo "Last snapshot nodes were:" >&2
if [ "$JSON_TOOL" = "jq" ]; then
  printf '%s' "$snapshot" | jq '[.nodes[] | {id, brokerType}]' >&2 || printf '%s\n' "$snapshot" >&2
else
  printf '%s\n' "$snapshot" >&2
fi
echo "── agent logs ──" >&2
docker logs --tail 50 "$AGENT_CTR" >&2 2>&1 || true
exit 1
