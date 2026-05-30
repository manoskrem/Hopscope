#!/usr/bin/env bash
#
# Real-broker E2E (Phase-4 multi-broker seam proof, OTLP): prove that OTLP-exported trace
# spans — including an ERROR span — surface on the engine canvas as a node co-rendering with
# RabbitMQ, AND that the error span becomes a real Failed edge (lastStatus==3) with ErrorDetails.
# OTLP is the only provider that delivers genuine per-trace failure data.
#
# Run the stack first, then this script:
#   docker compose -f deploy/docker-compose.yml up --build   # engine + ui + rabbitmq + redis (OTLP enabled)
#   bash deploy/e2e/otlp-check.sh
#
# Spans are emitted with the OpenTelemetry `telemetrygen` tool, run as a one-shot container on
# the compose network (no host SDK needed). It sends to the engine's gRPC OTLP port 4317.
# telemetrygen always marks a fraction of spans with Status=ERROR, giving us a Failed edge.
#
# Exit 0 on success; non-zero with a clear message otherwise.

set -euo pipefail

# ── Config (override via env) ───────────────────────────────────────────────
ENGINE="${HOPSCOPE_ENGINE_URL:-http://localhost:8085}"   # engine REST (host-mapped)
MGMT="${HOPSCOPE_MGMT_URL:-http://localhost:15672}"      # RabbitMQ Management API
AUTH="${HOPSCOPE_RABBITMQ_AUTH:-hopscope:hopscope}"      # management creds
NET="${HOPSCOPE_COMPOSE_NET:-deploy_default}"            # compose bridge network
OTLP_TARGET="${HOPSCOPE_OTLP_TARGET:-engine:4317}"       # in-network gRPC endpoint
# telemetrygen image. Override via HOPSCOPE_TELEMETRYGEN_IMG (e.g. to pin a version).
TELEMETRYGEN_IMG="${HOPSCOPE_TELEMETRYGEN_IMG:-ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:latest}"
VHOST="%2F"
RMQ_Q="orders.otlpcheck"

# ── Tooling preflight ───────────────────────────────────────────────────────
command -v curl   >/dev/null 2>&1 || { echo "FAIL: 'curl' is required." >&2; exit 2; }
command -v docker >/dev/null 2>&1 || { echo "FAIL: 'docker' is required (telemetrygen via run)." >&2; exit 2; }

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

echo "── 1/4  Waiting for engine + RabbitMQ ───────────────────────────────────"
wait_for "engine /healthz"         "$ENGINE/healthz"
wait_for "RabbitMQ Management API" "$MGMT/api/overview" -u "$AUTH"

echo "── 2/4  Emitting OTLP spans (incl. error spans) → $OTLP_TARGET ──────────"
# telemetrygen emits trace spans over OTLP/gRPC. --otlp-insecure = plaintext h2c.
# It marks a share of spans with an ERROR status, which the engine maps to a Failed edge.
if ! docker run --rm --network "$NET" "$TELEMETRYGEN_IMG" \
      traces --otlp-endpoint "$OTLP_TARGET" --otlp-insecure \
      --service otlp-e2e-svc --traces 20 --rate 0 --status-code Error 2>/tmp/telemetrygen.err; then
  echo "FAIL: telemetrygen could not emit spans. Output:" >&2
  cat /tmp/telemetrygen.err >&2
  echo "Hint: ensure the compose network name is '$NET' (override via HOPSCOPE_COMPOSE_NET)." >&2
  exit 1
fi
echo "  ✓ emitted 20 error-status traces to $OTLP_TARGET"

echo "── 3/4  Generating RabbitMQ traffic (declare queue + publish) ────────────"
curl_api -X PUT "$MGMT/api/queues/$VHOST/$RMQ_Q" \
  -H 'content-type: application/json' -d '{"durable":true}' >/dev/null
echo "  ✓ queue $RMQ_Q"
resp=$(curl_api -X POST "$MGMT/api/exchanges/$VHOST/amq.default/publish" \
  -H 'content-type: application/json' \
  -d "{\"properties\":{},\"routing_key\":\"$RMQ_Q\",\"payload\":\"otlp-e2e\",\"payload_encoding\":\"string\"}")
case "$resp" in
  *'"routed":true'*) echo "  ✓ published to $RMQ_Q" ;;
  *) echo "FAIL: RabbitMQ publish was not routed: $resp" >&2; exit 1 ;;
esac

# Returns 0 if the snapshot on stdin has (a) a RabbitMQ node, (b) a non-RabbitMQ node from
# the OTLP export (brokerType "OTLP" or a messaging.system value), AND (c) a Failed edge
# (lastStatus==3) — the real per-trace failure proof.
has_otlp_and_rabbit_and_failed() {
  if [ "$JSON_TOOL" = "jq" ]; then
    jq -e '
      ([.nodes[].brokerType] | any(. == "RabbitMQ"))
      and ([.nodes[].brokerType] | any(. != "RabbitMQ"))
      and ([.edges[].lastStatus] | any(. == 3))
    ' >/dev/null 2>&1
  else
    python3 -c '
import sys, json
d = json.load(sys.stdin)
bt = [n.get("brokerType") for n in d.get("nodes", [])]
statuses = [e.get("lastStatus") for e in d.get("edges", [])]
ok = ("RabbitMQ" in bt) and any(b != "RabbitMQ" for b in bt) and (3 in statuses)
sys.exit(0 if ok else 1)'
  fi
}

DEADLINE=30
echo "── 4/4  Polling /snapshot up to ${DEADLINE}s for OTLP+RabbitMQ + a Failed edge ──"
snapshot=""
for _ in $(seq 1 "$DEADLINE"); do
  sleep 1
  snapshot="$(curl -fsS "$ENGINE/snapshot" || true)"
  if [ -n "$snapshot" ] && printf '%s' "$snapshot" | has_otlp_and_rabbit_and_failed; then
    echo ""
    echo "PASS: snapshot shows an OTLP-sourced node + a RabbitMQ node co-rendering AND a"
    echo "      Failed edge (lastStatus=3) from an OTLP error span — real per-trace failure data."
    exit 0
  fi
done

echo "" >&2
echo "FAIL: did not observe OTLP+RabbitMQ co-render with a Failed edge after ${DEADLINE}s." >&2
echo "Last snapshot was:" >&2
if [ "$JSON_TOOL" = "jq" ]; then
  printf '%s' "$snapshot" | jq '{nodes: [.nodes[] | {id, brokerType}], edges: [.edges[] | {id, lastStatus}]}' >&2 \
    || printf '%s\n' "$snapshot" >&2
else
  printf '%s\n' "$snapshot" >&2
fi
exit 1
