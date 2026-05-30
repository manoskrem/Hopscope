using Hopscope.Domain.Events;

namespace Hopscope.Infrastructure.Providers.Kafka;

/// <summary>
/// Pure, static mapping logic: Kafka consumed messages and topic metadata →
/// <see cref="EventEnvelope"/>.
///
/// All Confluent.Kafka types are kept OUT of this file — callers pass primitives
/// (topic, partition, offset, header dict) so the mapper is unit-testable without
/// a live broker and without a reference to Confluent.Kafka in the test project.
///
/// AOT safety: no reflection, no LINQ Expressions, no Enum.Parse, no dynamic
/// codegen. All parsing is hand-written span/string operations.
///
/// HopId uniqueness:
///   ConsumedMessageToEnvelope — $"kafka:{topic}:{partition}:{offset}".
///   The Kafka (topic, partition, offset) triple is globally unique per cluster.
///   This is the natural idempotency key: the aggregator dedupes on it so
///   re-delivered observations are collapsed, not duplicated.
///
/// TraceId stability:
///   When no W3C traceparent or X-Trace-Id header is present the TraceId is
///   fixed to $"kafka:{topic}" — stable per topic rather than per message.
///   High-volume traffic on the same topic therefore shares one trace, keeping
///   the aggregator's trace-LRU cardinality bounded (same lesson as Redis).
///
/// DLQ convention:
///   Topic names ending with ".dlq" or ".DLT" (ordinal-ignore-case) are treated
///   as dead-letter topics and produce DeadLettered envelopes.
///
/// Source identity:
///   Producer identity is read from message headers with precedence:
///     "service.name" > "X-Source-Service" > fallback "kafka-producer".
///   The fallback is a stable synthetic — it does NOT mint a node per message,
///   keeping canvas cardinality bounded.
///
/// PayloadMetadata (contract: metadata only — never message bodies/values):
///   destinationKind, sourceKind, partition, offset, and any trace headers used.
///   Message keys and values are never included.
/// </summary>
internal static class KafkaMapper
{
    // ── DLQ-suffix detection ──────────────────────────────────────────────────

    private static readonly string[] DlqSuffixes = [".dlq", ".dlt"];

    internal static bool IsDlqTopic(string topic)
    {
        foreach (var suffix in DlqSuffixes)
        {
            if (topic.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }

    // ── Consumed message → envelope ──────────────────────────────────────────

    /// <summary>
    /// Builds an <see cref="EventEnvelope"/> for a single consumed Kafka message.
    ///
    /// Callers pass primitives only — no Confluent.Kafka types cross this boundary.
    /// </summary>
    /// <param name="topic">Kafka topic name.</param>
    /// <param name="partition">Kafka partition number.</param>
    /// <param name="offset">Kafka offset (unique within partition).</param>
    /// <param name="headers">Message headers (key→value, UTF-8 decoded). Never null.</param>
    /// <param name="counter">Monotonic counter from the ingestor (not used for HopId here,
    /// but kept for API symmetry with RedisMapper and future use).</param>
    /// <param name="ts">Observation timestamp (UTC).</param>
    internal static EventEnvelope ConsumedMessageToEnvelope(
        string topic,
        int    partition,
        long   offset,
        IReadOnlyDictionary<string, string> headers,
        long   counter,
        DateTimeOffset ts)
    {
        // HopId: naturally unique per cluster message — the ideal dedupe key.
        var hopId = $"kafka:{topic}:{partition}:{offset}";

        // Correlation: W3C traceparent > X-Trace-Id/X-Parent-Hop-Id > stable fallback.
        var (traceId, parentHopId) = ExtractTraceCorrelation(topic, headers);

        // Source: producer identity from headers, bounded cardinality fallback.
        var source = ExtractSource(headers);

        // Execution status and error details.
        ExecutionStatus status;
        ErrorDetails?   errorDetails;

        if (IsDlqTopic(topic))
        {
            status       = ExecutionStatus.DeadLettered;
            errorDetails = new ErrorDetails(
                ExceptionType:       "DeadLettered",
                Message:             $"message dead-lettered to {topic}",
                TruncatedStackTrace: null);
        }
        else
        {
            status       = ExecutionStatus.Success;
            errorDetails = null;
        }

        // PayloadMetadata: routing/trace metadata only — never bodies or message keys.
        var metadata = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["destinationKind"] = "Topic",
            ["sourceKind"]      = "Service",
            ["partition"]       = partition.ToString(),
            ["offset"]          = offset.ToString(),
        };

        // Include trace headers that were used (for topology-aware UIs), but not values.
        if (headers.TryGetValue("traceparent", out var tp) && !string.IsNullOrEmpty(tp))
            metadata["traceparent"] = tp;
        else if (headers.TryGetValue("X-Trace-Id", out var xti) && !string.IsNullOrEmpty(xti))
            metadata["X-Trace-Id"] = xti;

        return new EventEnvelope
        {
            TraceId         = traceId,
            HopId           = hopId,
            ParentHopId     = parentHopId,
            Source          = source,
            Destination     = topic,
            BrokerType      = "Kafka",
            Timestamp       = ts,
            ExecutionStatus = status,
            ErrorDetails    = errorDetails,
            PayloadMetadata = metadata,
        };
    }

    // ── Topic metadata → topology envelope ───────────────────────────────────

    /// <summary>
    /// Builds a TOPOLOGY envelope for a discovered Kafka topic so the canvas renders
    /// topic nodes even before any messages are consumed on them.
    ///
    /// HopId is STABLE per topic (no counter, no timestamp) → the aggregator dedupes
    /// repeated metadata polls and renders the node exactly once.
    /// </summary>
    /// <param name="bootstrapServers">Bootstrap server string (used to derive a cluster label).</param>
    /// <param name="topic">Kafka topic name.</param>
    /// <param name="ts">Observation timestamp (UTC).</param>
    internal static EventEnvelope TopicMetadataToEnvelope(
        string bootstrapServers,
        string topic,
        DateTimeOffset ts)
    {
        var clusterLabel = DeriveClusterLabel(bootstrapServers);

        return new EventEnvelope
        {
            TraceId         = $"kafka-meta:{topic}",
            HopId           = $"kafka-topic:{topic}",    // stable — deduped across polls
            ParentHopId     = null,
            Source          = clusterLabel,
            Destination     = topic,
            BrokerType      = "Kafka",
            Timestamp       = ts,
            ExecutionStatus = ExecutionStatus.Success,
            ErrorDetails    = null,
            PayloadMetadata = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["destinationKind"] = "Topic",
                ["sourceKind"]      = "Service",
            },
        };
    }

    // ── Header parsing helpers (pure, deterministic, AOT-safe) ───────────────

    /// <summary>
    /// Parses a W3C traceparent header value.
    /// Expected format: 00-{32hex}-{16hex}-{2hex}
    /// Returns (traceId, parentSpanId), or (null, null) if malformed.
    /// No Enum.Parse, no reflection — pure span operations.
    /// </summary>
    internal static (string? TraceId, string? ParentSpanId) ParseTraceParent(string traceparent)
    {
        // version(2)-dash(1)-traceId(32)-dash(1)-parentSpanId(16)-dash(1)-flags(2) = 55 chars
        if (string.IsNullOrEmpty(traceparent) || traceparent.Length < 55)
            return (null, null);

        var span = traceparent.AsSpan();

        // Version must be "00"
        if (span[0] != '0' || span[1] != '0')
            return (null, null);

        if (span[2] != '-')
            return (null, null);

        // TraceId: chars [3..34] (32 hex chars)
        var traceIdSpan = span.Slice(3, 32);
        if (!IsHex(traceIdSpan))
            return (null, null);

        if (span[35] != '-')
            return (null, null);

        // ParentSpanId: chars [36..51] (16 hex chars)
        var parentSpanSpan = span.Slice(36, 16);
        if (!IsHex(parentSpanSpan))
            return (null, null);

        if (span[52] != '-')
            return (null, null);

        // Flags: chars [53..54]
        if (!IsHex(span.Slice(53, 2)))
            return (null, null);

        var traceId     = traceIdSpan.ToString();
        var parentSpanId = parentSpanSpan.ToString();

        // All-zero trace-id is invalid per spec
        if (traceId == "00000000000000000000000000000000")
            return (null, null);

        return (traceId, string.IsNullOrEmpty(parentSpanId) ? null : parentSpanId);
    }

    /// <summary>
    /// Extracts trace correlation from the header dict.
    /// Precedence: W3C traceparent > X-Trace-Id/X-Parent-Hop-Id > stable per-topic fallback.
    /// Returns (traceId, parentHopId).
    /// </summary>
    internal static (string TraceId, string? ParentHopId) ExtractTraceCorrelation(
        string topic,
        IReadOnlyDictionary<string, string> headers)
    {
        // 1. W3C traceparent
        if (headers.TryGetValue("traceparent", out var tp) && !string.IsNullOrEmpty(tp))
        {
            var (tid, psid) = ParseTraceParent(tp);
            if (tid is not null)
                return (tid, string.IsNullOrEmpty(psid) ? null : psid);
        }

        // 2. Custom X-Trace-Id / X-Parent-Hop-Id
        if (headers.TryGetValue("X-Trace-Id", out var xti) && !string.IsNullOrEmpty(xti))
        {
            headers.TryGetValue("X-Parent-Hop-Id", out var xphi);
            return (xti, string.IsNullOrEmpty(xphi) ? null : xphi);
        }

        // 3. Stable per-topic fallback — keeps trace cardinality bounded.
        return ($"kafka:{topic}", null);
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    private static string ExtractSource(IReadOnlyDictionary<string, string> headers)
    {
        if (headers.TryGetValue("service.name", out var sn) && !string.IsNullOrEmpty(sn))
            return sn;

        if (headers.TryGetValue("X-Source-Service", out var xss) && !string.IsNullOrEmpty(xss))
            return xss;

        // Stable synthetic fallback — one node per canvas, bounded cardinality.
        return "kafka-producer";
    }

    /// <summary>
    /// Derives a stable cluster label from the bootstrap servers string.
    /// Takes just the first host (before any comma or port suffix after the colon when there
    /// are multiple entries) so the label is short and stable across config reshuffles.
    /// e.g. "kafka:9092,kafka2:9092" → "kafka-cluster"
    ///      "broker.example.com:9092" → "kafka-cluster"
    /// We always return "kafka-cluster" to keep cardinality to one node per cluster.
    /// </summary>
    private static string DeriveClusterLabel(string bootstrapServers)
    {
        // Fixed label: one node per cluster regardless of how many brokers are listed.
        // This keeps the canvas uncluttered and cardinality bounded.
        _ = bootstrapServers; // documented: used only if we want host-derived labels later
        return "kafka-cluster";
    }

    private static bool IsHex(ReadOnlySpan<char> span)
    {
        foreach (var c in span)
        {
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')))
                return false;
        }
        return true;
    }
}
