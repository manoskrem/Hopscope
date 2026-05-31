#!/usr/bin/env bash
#
# Real-receiver E2E (Phase-5 agent gRPC ingestion seam): prove that envelopes streamed into the
# engine's hopscope.v1.Ingestion/Stream RPC (the SAME client-streaming RPC the Go eBPF agent uses)
# surface on the canvas as a hop — including the Failed path — with NO broker cooperation at all.
#
# This is the engine half of Phase 5: it exercises the receiver with grpcurl alone (no eBPF), so
# it runs anywhere the compose stack runs. PR 2 adds the real eBPF agent + the no-code proof.
#
# Run the stack first (agent receiver enabled via HOPSCOPE_AGENT_ENABLED in the compose engine):
#   docker compose -f deploy/docker-compose.yml up --build   # engine + ui + rabbitmq + redis
#   bash deploy/e2e/agent-check.sh
#
# We client-stream a deterministic two-envelope trace over real gRPC with grpcurl (using the
# shared contract proto): hop-1 (SUCCESS) then hop-2 (FAILED + ErrorDetails), both on the same
# source->destination, so the edge  agent-svc-a -> agent-topic-x  MUST render with lastStatus==3.
#
# Exit 0 on success; non-zero with a clear message otherwise.

set -euo pipefail

# Resolve the repo root from this script's location so the shared contract proto can be mounted.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ── Config (override via env) ───────────────────────────────────────────────
ENGINE="${HOPSCOPE_ENGINE_URL:-http://localhost:8085}"   # engine REST (host-mapped)
NET="${HOPSCOPE_COMPOSE_NET:-deploy_default}"            # compose bridge network
AGENT_TARGET="${HOPSCOPE_AGENT_TARGET:-engine:4318}"     # in-network gRPC endpoint
GRPCURL_IMG="${HOPSCOPE_GRPCURL_IMG:-fullstorydev/grpcurl:latest}"
PROTOS="${HOPSCOPE_AGENT_PROTOS:-$REPO_ROOT/contracts/proto}"   # the SHARED contract dir

SRC="agent-svc-a"
DST="agent-topic-x"

# ── Tooling preflight ───────────────────────────────────────────────────────
command -v curl   >/dev/null 2>&1 || { echo "FAIL: 'curl' is required." >&2; exit 2; }
command -v docker >/dev/null 2>&1 || { echo "FAIL: 'docker' is required (grpcurl via run)." >&2; exit 2; }
[ -f "$PROTOS/event.proto" ] || {
  echo "FAIL: contract proto not found at '$PROTOS/event.proto' (override via HOPSCOPE_AGENT_PROTOS)." >&2; exit 2; }

if command -v jq >/dev/null 2>&1; then
  JSON_TOOL="jq"
elif command -v python3 >/dev/null 2>&1; then
  JSON_TOOL="python3"
else
  echo "FAIL: need 'jq' or 'python3' to read the snapshot (install one)." >&2
  exit 2
fi

wait_for() {
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

echo "── 1/3  Waiting for engine ──────────────────────────────────────────────"
wait_for "engine /healthz" "$ENGINE/healthz"

echo "── 2/3  Client-streaming a two-hop trace (SUCCESS → FAILED) → $AGENT_TARGET ──"
# grpcurl reads multiple JSON values from stdin and, for a client-streaming RPC, sends each as one
# stream message then half-closes. Timestamps are RFC3339 (proto google.protobuf.Timestamp). The
# second hop carries executionStatus FAILED + errorDetails, so the edge MUST end at lastStatus==3.
AGENT_STREAM=$(cat <<JSON
{ "traceId": "agent-e2e-trace-1", "hopId": "agent-e2e-hop-1", "source": "$SRC", "destination": "$DST", "brokerType": "Redis", "executionStatus": "SUCCESS", "timestamp": "2026-05-31T00:00:00Z", "payloadMetadata": { "destinationKind": "Topic" } }
{ "traceId": "agent-e2e-trace-1", "hopId": "agent-e2e-hop-2", "parentHopId": "agent-e2e-hop-1", "source": "$SRC", "destination": "$DST", "brokerType": "Redis", "executionStatus": "FAILED", "timestamp": "2026-05-31T00:00:01Z", "errorDetails": { "exceptionType": "AgentE2eException", "message": "injected failure", "truncatedStackTrace": "at Agent.Send()" }, "payloadMetadata": { "destinationKind": "Topic" } }
JSON
)
if ! printf '%s' "$AGENT_STREAM" | docker run --rm -i --network "$NET" \
      -v "$PROTOS":/protos:ro "$GRPCURL_IMG" \
      -plaintext -import-path /protos -proto event.proto \
      -d @ "$AGENT_TARGET" \
      hopscope.v1.Ingestion/Stream >/tmp/agent-grpcurl.out 2>&1; then
  echo "FAIL: grpcurl could not stream to the agent receiver. Output:" >&2
  cat /tmp/agent-grpcurl.out >&2
  echo "Hint: ensure HOPSCOPE_AGENT_ENABLED=true on the engine and the compose network is '$NET'" >&2
  echo "      (override via HOPSCOPE_COMPOSE_NET / HOPSCOPE_AGENT_TARGET)." >&2
  exit 1
fi
echo "  ✓ streamed 2 envelopes; ack: $(tr -d '\n' </tmp/agent-grpcurl.out)"

# Returns 0 if the snapshot on stdin has the Failed edge  agent-svc-a -> agent-topic-x  (lastStatus==3).
# The source/target ids are unique to this check, so the edge can ONLY come from the streamed
# envelopes — no broker produced it. That is the receiver proof.
has_agent_failed_edge() {
  if [ "$JSON_TOOL" = "jq" ]; then
    jq -e --arg s "$SRC" --arg t "$DST" '
      [.edges[] | select(.sourceId == $s and .targetId == $t and .lastStatus == 3)] | length > 0
    ' >/dev/null 2>&1
  else
    SRC="$SRC" DST="$DST" python3 -c '
import sys, os, json
d = json.load(sys.stdin)
s, t = os.environ["SRC"], os.environ["DST"]
ok = any(e.get("sourceId") == s and e.get("targetId") == t and e.get("lastStatus") == 3
         for e in d.get("edges", []))
sys.exit(0 if ok else 1)'
  fi
}

DEADLINE=30
echo "── 3/3  Polling /snapshot up to ${DEADLINE}s for the agent Failed edge ──"
snapshot=""
for _ in $(seq 1 "$DEADLINE"); do
  sleep 1
  snapshot="$(curl -fsS "$ENGINE/snapshot" || true)"
  if [ -n "$snapshot" ] && printf '%s' "$snapshot" | has_agent_failed_edge; then
    echo ""
    echo "PASS: snapshot shows the agent-sourced Failed edge  $SRC -> $DST (lastStatus=3)"
    echo "      — streamed over the gRPC Ingestion seam with no broker cooperation."
    exit 0
  fi
done

echo "" >&2
echo "FAIL: did not observe the agent Failed edge  $SRC -> $DST  after ${DEADLINE}s." >&2
echo "Last snapshot was:" >&2
if [ "$JSON_TOOL" = "jq" ]; then
  printf '%s' "$snapshot" | jq '{nodes: [.nodes[] | {id, brokerType}], edges: [.edges[] | {id, lastStatus}]}' >&2 \
    || printf '%s\n' "$snapshot" >&2
else
  printf '%s\n' "$snapshot" >&2
fi
exit 1
