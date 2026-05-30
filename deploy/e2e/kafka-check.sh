#!/usr/bin/env bash
#
# Real-broker E2E (Phase-4 multi-broker seam proof, Kafka opt-in variant): prove that real
# Kafka traffic AND real RabbitMQ traffic co-render as nodes on the SAME engine snapshot.
#
# Kafka ships as an opt-in build variant (Confluent.Kafka is not trim-clean), so this E2E
# runs the engine built from Dockerfile.kafka via the compose overlay. Run:
#   docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.kafka.yml up --build
#   bash deploy/e2e/kafka-check.sh
#
# What it does (no host SDKs — kafka CLI via docker exec + RabbitMQ Management HTTP API):
#   1. wait for the engine, Kafka, and RabbitMQ to be ready
#   2. create a Kafka topic and produce a few messages
#   3. generate RabbitMQ topology+traffic (declare a queue, publish a message)
#   4. poll /snapshot until it contains BOTH a brokerType=="Kafka" node AND a
#      brokerType=="RabbitMQ" node — i.e. both brokers on one canvas
#
# Exit 0 on success; non-zero with a clear message otherwise.

set -euo pipefail

# ── Config (override via env) ───────────────────────────────────────────────
ENGINE="${HOPSCOPE_ENGINE_URL:-http://localhost:8085}"   # engine REST (host-mapped)
MGMT="${HOPSCOPE_MGMT_URL:-http://localhost:15672}"      # RabbitMQ Management API
AUTH="${HOPSCOPE_RABBITMQ_AUTH:-hopscope:hopscope}"      # management creds
KAFKA_CTR="${HOPSCOPE_KAFKA_CONTAINER:-hopscope-kafka}"  # for docker exec kafka CLI
KAFKA_BIN="${HOPSCOPE_KAFKA_BIN:-/opt/kafka/bin}"        # CLI path inside apache/kafka image
VHOST="%2F"                                              # url-encoded "/"
TOPIC="hopscope.e2e.orders"
RMQ_Q="orders.kafkacheck"

# ── Tooling preflight ───────────────────────────────────────────────────────
command -v curl   >/dev/null 2>&1 || { echo "FAIL: 'curl' is required." >&2; exit 2; }
command -v docker >/dev/null 2>&1 || { echo "FAIL: 'docker' is required (kafka CLI via exec)." >&2; exit 2; }

if command -v jq >/dev/null 2>&1; then
  JSON_TOOL="jq"
elif command -v python3 >/dev/null 2>&1; then
  JSON_TOOL="python3"
else
  echo "FAIL: need 'jq' or 'python3' to read the snapshot (install one)." >&2
  exit 2
fi

curl_api() { curl -fsS -u "$AUTH" "$@"; }

# ── POLLING IDIOM (mirrors redis-check.sh) ──────────────────────────────────
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

echo "── 1/4  Waiting for engine + Kafka + RabbitMQ ───────────────────────────"
wait_for "engine /healthz"         "$ENGINE/healthz"
wait_for "RabbitMQ Management API" "$MGMT/api/overview" -u "$AUTH"
# Kafka readiness: list topics via the in-container CLI.
kafka_tries=0
until docker exec "$KAFKA_CTR" "$KAFKA_BIN/kafka-topics.sh" \
        --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
  kafka_tries=$((kafka_tries + 1))
  if [ "$kafka_tries" -ge 60 ]; then
    echo "FAIL: timed out waiting for Kafka ($KAFKA_CTR kafka-topics --list)." >&2
    exit 1
  fi
  sleep 1
done
echo "  ✓ Kafka ready"

echo "── 2/4  Creating Kafka topic + producing messages ───────────────────────"
docker exec "$KAFKA_CTR" "$KAFKA_BIN/kafka-topics.sh" \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic "$TOPIC" --partitions 1 --replication-factor 1 >/dev/null
echo "  ✓ topic $TOPIC"
# Produce a few records (newline-delimited) into the topic.
printf 'm1\nm2\nm3\nm4\nm5\n' | docker exec -i "$KAFKA_CTR" \
  "$KAFKA_BIN/kafka-console-producer.sh" \
  --bootstrap-server localhost:9092 --topic "$TOPIC" >/dev/null 2>&1
echo "  ✓ produced 5 messages to $TOPIC"

echo "── 3/4  Generating RabbitMQ traffic (declare queue + publish) ────────────"
curl_api -X PUT "$MGMT/api/queues/$VHOST/$RMQ_Q" \
  -H 'content-type: application/json' -d '{"durable":true}' >/dev/null
echo "  ✓ queue $RMQ_Q"
resp=$(curl_api -X POST "$MGMT/api/exchanges/$VHOST/amq.default/publish" \
  -H 'content-type: application/json' \
  -d "{\"properties\":{},\"routing_key\":\"$RMQ_Q\",\"payload\":\"kafka-e2e\",\"payload_encoding\":\"string\"}")
case "$resp" in
  *'"routed":true'*) echo "  ✓ published to $RMQ_Q" ;;
  *) echo "FAIL: RabbitMQ publish was not routed: $resp" >&2; exit 1 ;;
esac

# Returns 0 if the snapshot on stdin has BOTH a Kafka node AND a RabbitMQ node.
has_both_brokers() {
  if [ "$JSON_TOOL" = "jq" ]; then
    jq -e '([.nodes[].brokerType] | (any(. == "Kafka")) and (any(. == "RabbitMQ")))' \
       >/dev/null 2>&1
  else
    python3 -c '
import sys, json
d = json.load(sys.stdin)
kinds = {n.get("brokerType") for n in d.get("nodes", [])}
sys.exit(0 if ("Kafka" in kinds and "RabbitMQ" in kinds) else 1)'
  fi
}

# ── BOUNDED POLLING IDIOM ───────────────────────────────────────────────────
# The engine discovers the topic via metadata then consumes; RabbitMQ needs an engine poll.
DEADLINE=40
echo "── 4/4  Polling /snapshot up to ${DEADLINE}s for BOTH brokers ────────────"
snapshot=""
for _ in $(seq 1 "$DEADLINE"); do
  sleep 1
  snapshot="$(curl -fsS "$ENGINE/snapshot" || true)"
  if [ -n "$snapshot" ] && printf '%s' "$snapshot" | has_both_brokers; then
    echo ""
    echo "PASS: snapshot shows BOTH a Kafka node AND a RabbitMQ node — multi-broker on one canvas."
    exit 0
  fi
done

echo "" >&2
echo "FAIL: snapshot did not contain both a Kafka and a RabbitMQ node after ${DEADLINE}s." >&2
echo "Last snapshot nodes were:" >&2
if [ "$JSON_TOOL" = "jq" ]; then
  printf '%s' "$snapshot" | jq '[.nodes[] | {id, brokerType}]' >&2 || printf '%s\n' "$snapshot" >&2
else
  printf '%s\n' "$snapshot" >&2
fi
exit 1
