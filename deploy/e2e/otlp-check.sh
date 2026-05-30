#!/usr/bin/env bash
#
# Real-broker E2E (Phase-4 multi-broker seam proof, OTLP): prove that OTLP-exported trace
# spans surface on the engine canvas as nodes co-rendering with RabbitMQ, AND that an ERROR
# span becomes a real Failed edge (lastStatus==3) — OTLP is the only provider that delivers
# genuine per-trace failure data.
#
# Run the stack first (OTLP enabled via HOPSCOPE_OTLP_ENABLED in the compose engine), then this:
#   docker compose -f deploy/docker-compose.yml up --build   # engine + ui + rabbitmq + redis
#   bash deploy/e2e/otlp-check.sh
#
# Why a hand-crafted export instead of telemetrygen: telemetrygen's spans render fine, but its
# --status-code Error is NOT emitted as span.status.code=ERROR by current builds, so it never
# produces a Failed edge (the engine maps a real ERROR span to Failed correctly — verified).
# To prove the failure path DETERMINISTICALLY we send a fixed two-span trace over real OTLP/gRPC
# with grpcurl (using the vendored protos): a CLIENT parent (status OK) and a SERVER child
# (status ERROR + an `exception` event). Both span-kinds are ones the engine keeps (INTERNAL /
# UNSPECIFIED spans are filtered out before ingestion), so the error hop is guaranteed to render.
#
# Exit 0 on success; non-zero with a clear message otherwise.

set -euo pipefail

# Resolve the repo root from this script's location so the vendored protos can be mounted.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ── Config (override via env) ───────────────────────────────────────────────
ENGINE="${HOPSCOPE_ENGINE_URL:-http://localhost:8085}"   # engine REST (host-mapped)
MGMT="${HOPSCOPE_MGMT_URL:-http://localhost:15672}"      # RabbitMQ Management API
AUTH="${HOPSCOPE_RABBITMQ_AUTH:-hopscope:hopscope}"      # management creds
NET="${HOPSCOPE_COMPOSE_NET:-deploy_default}"            # compose bridge network
OTLP_TARGET="${HOPSCOPE_OTLP_TARGET:-engine:4317}"       # in-network gRPC endpoint
GRPCURL_IMG="${HOPSCOPE_GRPCURL_IMG:-fullstorydev/grpcurl:latest}"
PROTOS="${HOPSCOPE_OTLP_PROTOS:-$REPO_ROOT/src/engine/Hopscope.Infrastructure/Providers/Otlp/protos}"
VHOST="%2F"
RMQ_Q="orders.otlpcheck"

# Fixed ids (base64 of 16/8 raw bytes) so the request is byte-stable and needs no JSON tooling
# to build. trace_id=01..10, parent span_id=01..08, child span_id=11..18 — all valid, non-zero.
TRACE_ID="AQIDBAUGBwgJCgsMDQ4PEA=="
PARENT_SPAN_ID="AQIDBAUGBwg="
CHILD_SPAN_ID="ERITFBUWFxg="

# ── Tooling preflight ───────────────────────────────────────────────────────
command -v curl   >/dev/null 2>&1 || { echo "FAIL: 'curl' is required." >&2; exit 2; }
command -v docker >/dev/null 2>&1 || { echo "FAIL: 'docker' is required (grpcurl via run)." >&2; exit 2; }
[ -f "$PROTOS/opentelemetry/proto/collector/trace/v1/trace_service.proto" ] || {
  echo "FAIL: OTLP protos not found under '$PROTOS' (override via HOPSCOPE_OTLP_PROTOS)." >&2; exit 2; }

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

echo "── 2/4  Emitting a crafted OTLP error trace (CLIENT ok → SERVER error) → $OTLP_TARGET ──"
# A deterministic two-span trace. The SERVER child carries status ERROR + an `exception` event,
# so it MUST surface as a Failed edge  otlp-e2e-svc -> payment-gw  with ErrorDetails.
OTLP_REQUEST=$(cat <<JSON
{
  "resourceSpans": [{
    "resource": { "attributes": [
      { "key": "service.name", "value": { "stringValue": "otlp-e2e-svc" } }
    ]},
    "scopeSpans": [{
      "spans": [
        {
          "traceId": "$TRACE_ID", "spanId": "$PARENT_SPAN_ID",
          "name": "checkout", "kind": "SPAN_KIND_CLIENT",
          "status": { "code": "STATUS_CODE_OK" },
          "attributes": [{ "key": "peer.service", "value": { "stringValue": "orders-api" } }]
        },
        {
          "traceId": "$TRACE_ID", "spanId": "$CHILD_SPAN_ID", "parentSpanId": "$PARENT_SPAN_ID",
          "name": "charge", "kind": "SPAN_KIND_SERVER",
          "status": { "code": "STATUS_CODE_ERROR", "message": "payment declined" },
          "attributes": [{ "key": "peer.service", "value": { "stringValue": "payment-gw" } }],
          "events": [{
            "name": "exception",
            "attributes": [
              { "key": "exception.type",    "value": { "stringValue": "PaymentDeclinedException" } },
              { "key": "exception.message", "value": { "stringValue": "card declined by gateway" } }
            ]
          }]
        }
      ]
    }]
  }]
}
JSON
)
if ! printf '%s' "$OTLP_REQUEST" | docker run --rm -i --network "$NET" \
      -v "$PROTOS":/protos:ro "$GRPCURL_IMG" \
      -plaintext -import-path /protos \
      -proto opentelemetry/proto/collector/trace/v1/trace_service.proto \
      -d @ "$OTLP_TARGET" \
      opentelemetry.proto.collector.trace.v1.TraceService/Export >/tmp/otlp-grpcurl.out 2>&1; then
  echo "FAIL: grpcurl could not export the OTLP trace. Output:" >&2
  cat /tmp/otlp-grpcurl.out >&2
  echo "Hint: ensure the compose network name is '$NET' (override via HOPSCOPE_COMPOSE_NET)." >&2
  exit 1
fi
echo "  ✓ exported a CLIENT→SERVER trace with a STATUS_CODE_ERROR child to $OTLP_TARGET"

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

# Returns 0 if the snapshot on stdin has (a) an OTLP node, (b) a RabbitMQ node, AND (c) the
# deterministic Failed edge  * -> payment-gw  (lastStatus==3) — the real per-trace failure proof.
has_otlp_and_rabbit_and_failed() {
  if [ "$JSON_TOOL" = "jq" ]; then
    jq -e '
      ([.nodes[].brokerType] | any(. == "OTLP"))
      and ([.nodes[].brokerType] | any(. == "RabbitMQ"))
      and ([.edges[] | select(.lastStatus == 3 and .targetId == "payment-gw")] | length > 0)
    ' >/dev/null 2>&1
  else
    python3 -c '
import sys, json
d = json.load(sys.stdin)
bt = [n.get("brokerType") for n in d.get("nodes", [])]
failed_pg = any(e.get("lastStatus") == 3 and e.get("targetId") == "payment-gw"
                for e in d.get("edges", []))
ok = ("OTLP" in bt) and ("RabbitMQ" in bt) and failed_pg
sys.exit(0 if ok else 1)'
  fi
}

DEADLINE=30
echo "── 4/4  Polling /snapshot up to ${DEADLINE}s for OTLP+RabbitMQ + the Failed edge ──"
snapshot=""
for _ in $(seq 1 "$DEADLINE"); do
  sleep 1
  snapshot="$(curl -fsS "$ENGINE/snapshot" || true)"
  if [ -n "$snapshot" ] && printf '%s' "$snapshot" | has_otlp_and_rabbit_and_failed; then
    echo ""
    echo "PASS: snapshot shows an OTLP node + a RabbitMQ node co-rendering AND the Failed edge"
    echo "      * -> payment-gw (lastStatus=3) from the OTLP error span — real per-trace failure data."
    exit 0
  fi
done

echo "" >&2
echo "FAIL: did not observe OTLP+RabbitMQ co-render with the payment-gw Failed edge after ${DEADLINE}s." >&2
echo "Last snapshot was:" >&2
if [ "$JSON_TOOL" = "jq" ]; then
  printf '%s' "$snapshot" | jq '{nodes: [.nodes[] | {id, brokerType}], edges: [.edges[] | {id, lastStatus}]}' >&2 \
    || printf '%s\n' "$snapshot" >&2
else
  printf '%s\n' "$snapshot" >&2
fi
exit 1
