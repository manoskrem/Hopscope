using Hopscope.Domain.Events;

namespace Hopscope.Infrastructure.Providers.Redis;

/// <summary>
/// Pure, static mapping logic: Redis keyevent notifications → <see cref="EventEnvelope"/>.
///
/// Extracted so unit tests exercise mapping without a live socket.
/// All methods are AOT-safe: no reflection, no LINQ Expressions, no dynamic codegen.
///
/// Key-prefix grouping rationale:
///   Redis keys like "user:123" carry high cardinality and PII (the id segment).
///   We truncate to the first <paramref name="depth"/> colon-separated segments and
///   append ":*" — "user:123" at depth 1 → "user:*" — bounding both cardinality
///   and data exposure. Keys with no colon separator fall back to the sentinel "keys:*"
///   so the canvas always has a destination node rather than a raw key value.
///
/// Keyevent-only rationale (vs keyspace):
///   Redis publishes two notification flavours: keyspace (__keyspace@db__:key, payload=event)
///   and keyevent (__keyevent@db__:event, payload=key). We subscribe only to keyevent because:
///   1. The channel carries db + event type (the routing metadata we care about).
///   2. The payload carries the key (which we prefix-truncate for privacy).
///   3. Subscribing to both would double-count every operation.
/// </summary>
internal static class RedisMapper
{
    // ── Channel parsing ──────────────────────────────────────────────────────

    /// <summary>
    /// Parses a keyevent channel name: <c>__keyevent@&lt;db&gt;__:&lt;event&gt;</c>.
    /// Returns (db, eventType), or <see langword="null"/> if the channel does not match.
    /// </summary>
    internal static (int Db, string EventType)? ParseKeyEventChannel(string channel)
    {
        // Expected format: __keyevent@<db>__:<event>
        const string prefix = "__keyevent@";
        const string middle = "__:";

        if (!channel.StartsWith(prefix, StringComparison.Ordinal))
            return null;

        var afterPrefix = channel.AsSpan(prefix.Length);
        var middleIdx   = afterPrefix.IndexOf(middle, StringComparison.Ordinal);
        if (middleIdx < 0) return null;

        var dbSpan = afterPrefix[..middleIdx];
        if (!int.TryParse(dbSpan, out var db)) return null;

        var eventType = afterPrefix[(middleIdx + middle.Length)..].ToString();
        if (string.IsNullOrEmpty(eventType)) return null;

        return (db, eventType);
    }

    // ── Key-prefix truncation ────────────────────────────────────────────────

    /// <summary>
    /// Truncates <paramref name="key"/> to the first <paramref name="depth"/>
    /// colon-separated segments and appends ":*".
    ///
    /// Examples (depth=1): "user:123" → "user:*", "order:456:item" → "order:*".
    /// Examples (depth=2): "a:b:c"    → "a:b:*".
    /// Keys with no ":" fall back to "keys:*" (bounds cardinality, no id exposed).
    /// Empty key → "keys:*".
    /// </summary>
    internal static string KeyPrefix(string key, int depth)
    {
        if (string.IsNullOrEmpty(key)) return "keys:*";
        if (depth < 1) depth = 1;

        var span    = key.AsSpan();
        var segment = 0;
        var pos     = 0;

        while (pos < span.Length)
        {
            var colonIdx = span[pos..].IndexOf(':');
            if (colonIdx < 0) break;          // no more colons

            segment++;
            pos += colonIdx + 1;              // move past the colon

            if (segment >= depth)
                // Return the portion up-to-and-including the colon, plus "*".
                return string.Concat(span[..pos], "*");
        }

        // Fewer colons than depth (incl. zero colons) → sentinel.
        return "keys:*";
    }

    // ── Envelope construction ────────────────────────────────────────────────

    /// <summary>
    /// Builds an <see cref="EventEnvelope"/> for a single Redis keyevent observation.
    ///
    /// Source = "redis-db&lt;db&gt;" (the Redis instance+db as the originating service).
    /// Destination = KeyPrefix(key, keyDepth) (the topic/key family the event targets).
    /// HopId is fresh per observation (counter ensures uniqueness).
    /// PayloadMetadata contains routing metadata only — no full key value (privacy guard).
    /// </summary>
    internal static EventEnvelope KeyEventToEnvelope(
        int             db,
        string          eventType,
        string          key,
        int             keyDepth,
        long            counter,
        DateTimeOffset  timestamp)
    {
        var prefix      = KeyPrefix(key, keyDepth);
        var source      = $"redis-db{db}";
        var destination = prefix;
        // HopId is fresh per observation (counter) so the source→dest edge accumulates Count.
        var hopId       = $"redis:{db}:{prefix}:{counter}";
        // TraceId is STABLE per db+key-family (no counter): all events on a key family share
        // one trace, so high-frequency keyevents do not flood the aggregator's trace LRU with
        // singleton traces (which would evict real multi-hop traces and pin RAM at the cap).
        var traceId     = $"redis-activity:{db}:{prefix}";

        var metadata = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["destinationKind"] = "Topic",
            ["sourceKind"]      = "Service",
            ["redisEvent"]      = eventType,
            ["db"]              = db.ToString(),
            ["keyPrefix"]       = prefix,
        };

        return new EventEnvelope
        {
            TraceId         = traceId,
            HopId           = hopId,
            ParentHopId     = null,
            Source          = source,
            Destination     = destination,
            BrokerType      = "Redis",
            Timestamp       = timestamp,
            ExecutionStatus = ExecutionStatus.Success,
            ErrorDetails    = null,
            PayloadMetadata = metadata,
        };
    }

    /// <summary>
    /// Combines channel parsing and envelope construction for a RESP pmessage frame.
    /// Returns <see langword="null"/> if <paramref name="channel"/> is not a keyevent channel.
    /// </summary>
    internal static EventEnvelope? PmessageToEnvelope(
        string         channel,
        string         keyPayload,
        int            keyDepth,
        long           counter,
        DateTimeOffset ts)
    {
        var parsed = ParseKeyEventChannel(channel);
        if (parsed is null) return null;

        var (db, eventType) = parsed.Value;
        return KeyEventToEnvelope(db, eventType, keyPayload, keyDepth, counter, ts);
    }
}
