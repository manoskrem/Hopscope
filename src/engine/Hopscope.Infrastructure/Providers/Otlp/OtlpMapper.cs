using Hopscope.Domain.Events;

namespace Hopscope.Infrastructure.Providers.Otlp;

/// <summary>
/// Pure, static mapping from raw OTLP span primitives to <see cref="EventEnvelope"/>.
///
/// All protobuf-generated types are kept OUT of this file's public/internal signature.
/// Callers pass primitives (byte arrays, strings, dictionaries) so this class is
/// unit-testable without constructing protobuf objects and without a reference to
/// Grpc.AspNetCore in the test project.
///
/// AOT safety:
///   - No reflection, no Enum.Parse, no LINQ Expressions, no dynamic codegen.
///   - Hex encoding is hand-written (no Convert.ToHexString — AOT-fine on .NET 10,
///     but kept hand-written to make the logic explicit and testable).
///   - No protobuf Descriptor/reflection/JsonFormatter APIs — only the generated
///     parse/serialize path is used (in OtlpTraceService, not here).
///
/// PayloadMetadata allowlist (contract: metadata only, never bodies):
///   spanName, statusCode, messaging.system, messaging.operation,
///   rpc.system, destinationKind, sourceKind.
///   Count is capped at 8 entries — no wholesale attribute copy.
///
/// Stack trace truncation hard cap: 2048 characters.
/// </summary>
internal static class OtlpMapper
{
    internal const int StackTraceCap = 2048;

    // W3C / OTLP fixed id widths: trace_id is 16 bytes, span_id is 8 bytes.
    private const int TraceIdBytes = 16;
    private const int SpanIdBytes  = 8;

    // ── Span-kind filter ──────────────────────────────────────────────────────

    /// <summary>
    /// Whether a span of the given OTLP <c>SpanKind</c> represents a real hop/edge worth
    /// rendering on the canvas. The caller drops everything else BEFORE building an envelope
    /// so non-hop spans never reach the bounded bridge or the trace-retention window.
    ///
    /// trace.proto SpanKind values:
    ///   0 UNSPECIFIED, 1 INTERNAL, 2 SERVER, 3 CLIENT, 4 PRODUCER, 5 CONSUMER.
    ///
    /// Kept = SERVER/CLIENT (service-call edges) + PRODUCER/CONSUMER (messaging hops) — the
    /// four application-boundary kinds, each of which describes traffic crossing between two
    /// parties. Dropped = INTERNAL (in-process work — no hop) and UNSPECIFIED (no declared
    /// role; the spec says it MAY be treated as INTERNAL). Without this filter, INTERNAL spans
    /// — typically the bulk of a trace — flood the canvas with non-edge nodes and burn the
    /// &lt;35 MB RAM budget.
    /// </summary>
    internal static bool IsHopSpanKind(int spanKind) => spanKind is 2 or 3 or 4 or 5;

    // ── Public entry point ────────────────────────────────────────────────────

    /// <summary>
    /// Maps a single OTLP span (passed as primitives) to an <see cref="EventEnvelope"/>,
    /// or returns <see langword="null"/> if the span carries a structurally-invalid identity
    /// (the caller drops it rather than enqueue a malformed envelope).
    ///
    /// trace.proto declares a trace_id that is not exactly 16 bytes (or all-zero) and a
    /// span_id that is not exactly 8 bytes (or all-zero) as INVALID. We must NOT emit such a
    /// span: an empty/short HopId would collapse every malformed span onto one dedupe key
    /// (idempotency drops all but the first) and an empty TraceId breaks ParentHopId
    /// correlation. So a bad id ⇒ reject the span.
    /// </summary>
    /// <param name="traceId">Raw trace-id from the protobuf Span (must be 16 bytes, non-zero).</param>
    /// <param name="spanId">Raw span-id (must be 8 bytes, non-zero).</param>
    /// <param name="parentSpanId">Raw 8-byte parent span-id (may be empty / all-zero == root;
    ///   a wrong-length parent is treated as absent rather than rejecting the whole span).</param>
    /// <param name="spanName">Span name (operation name).</param>
    /// <param name="resourceServiceName">service.name from the parent ResourceSpans Resource (may be null).</param>
    /// <param name="attributes">Flat string→string view of the span's KeyValue attributes (allowlisted by caller).</param>
    /// <param name="statusCode">OTLP Status.StatusCode integer (0=Unset, 1=Ok, 2=Error).</param>
    /// <param name="exceptionEvent">
    ///   Optional exception span-event extracted by the caller:
    ///   (exception.type, exception.message, exception.stacktrace). Null if no exception event.
    /// </param>
    /// <param name="ts">Span start timestamp (UTC).</param>
    internal static EventEnvelope? SpanToEnvelope(
        byte[]                                  traceId,
        byte[]                                  spanId,
        byte[]                                  parentSpanId,
        string                                  spanName,
        string?                                 resourceServiceName,
        IReadOnlyDictionary<string, string>     attributes,
        int                                     statusCode,
        (string type, string message, string stack)? exceptionEvent,
        DateTimeOffset                          ts)
    {
        // ── Identity validation (reject malformed spans) ──────────────────────
        // trace_id MUST be 16 non-zero bytes; span_id MUST be 8 non-zero bytes.
        if (traceId.Length != TraceIdBytes || IsAllZero(traceId) ||
            spanId.Length  != SpanIdBytes  || IsAllZero(spanId))
        {
            return null;
        }

        // ── Identity ──────────────────────────────────────────────────────────
        var traceIdHex  = ToHexLower(traceId);   // 32 lower-hex chars (validated above)
        var hopIdHex    = ToHexLower(spanId);     // 16 lower-hex chars (validated above)
        // A parent is present only when it is exactly 8 non-zero bytes; anything else == root.
        var parentHopId = (parentSpanId.Length == SpanIdBytes && !IsAllZero(parentSpanId))
            ? ToHexLower(parentSpanId)
            : null;

        // ── Source (service identity) ─────────────────────────────────────────
        var source = resourceServiceName
            ?? GetAttr(attributes, "service.name")
            ?? GetAttr(attributes, "peer.service")
            ?? "otlp-service";

        // ── Destination (what is being called / target) ───────────────────────
        var destination = GetAttr(attributes, "messaging.destination.name")
            ?? GetAttr(attributes, "messaging.destination")
            ?? GetAttr(attributes, "server.address")
            ?? GetAttr(attributes, "peer.service")
            ?? spanName;

        // ── Topology kinds ────────────────────────────────────────────────────
        var hasMessaging    = attributes.ContainsKey("messaging.destination.name")
                           || attributes.ContainsKey("messaging.destination")
                           || attributes.ContainsKey("messaging.system");
        var destinationKind = hasMessaging ? "Topic" : "Service";

        // ── BrokerType ───────────────────────────────────────────────────────
        var brokerType = GetAttr(attributes, "messaging.system") ?? "OTLP";

        // ── ExecutionStatus + ErrorDetails ───────────────────────────────────
        // OTLP STATUS_CODE_ERROR = 2 → Failed; anything else → Success.
        // Retrying / DeadLettered have no natural OTLP equivalent.
        ExecutionStatus executionStatus;
        ErrorDetails?   errorDetails;

        if (statusCode == 2) // STATUS_CODE_ERROR
        {
            executionStatus = ExecutionStatus.Failed;

            if (exceptionEvent.HasValue)
            {
                var (exType, exMsg, exStack) = exceptionEvent.Value;
                var truncatedStack = exStack.Length > StackTraceCap
                    ? exStack[..StackTraceCap]
                    : (exStack.Length > 0 ? exStack : null);

                errorDetails = new ErrorDetails(
                    ExceptionType:       string.IsNullOrEmpty(exType)  ? "SpanError" : exType,
                    Message:             string.IsNullOrEmpty(exMsg)   ? spanName    : exMsg,
                    TruncatedStackTrace: truncatedStack);
            }
            else
            {
                // Error status but no exception event — synthesize from status.
                errorDetails = new ErrorDetails(
                    ExceptionType:       "SpanError",
                    Message:             spanName,
                    TruncatedStackTrace: null);
            }
        }
        else
        {
            executionStatus = ExecutionStatus.Success;
            errorDetails    = null;
        }

        // ── PayloadMetadata (allowlisted — never bodies) ──────────────────────
        var metadata = new Dictionary<string, string>(8, StringComparer.Ordinal)
        {
            ["spanName"]        = spanName,
            ["statusCode"]      = statusCode.ToString(),
            ["destinationKind"] = destinationKind,
            ["sourceKind"]      = "Service",
        };

        // Small allowlist of useful routing/topology attributes.
        TryCopyAttr(attributes, "messaging.system",    metadata, "messaging.system");
        TryCopyAttr(attributes, "messaging.operation", metadata, "messaging.operation");
        TryCopyAttr(attributes, "rpc.system",          metadata, "rpc.system");
        TryCopyAttr(attributes, "rpc.service",         metadata, "rpc.service");

        return new EventEnvelope
        {
            TraceId         = traceIdHex,
            HopId           = hopIdHex,
            ParentHopId     = parentHopId,
            Source          = source,
            Destination     = destination,
            BrokerType      = brokerType,
            Timestamp       = ts,
            ExecutionStatus = executionStatus,
            ErrorDetails    = errorDetails,
            PayloadMetadata = metadata,
        };
    }

    // ── Hex helpers (hand-written, no reflection) ─────────────────────────────

    /// <summary>
    /// Converts a byte array to a lower-hex string.
    /// 16-byte input → 32-char string; 8-byte input → 16-char string.
    /// No reflection, no format strings — pure char arithmetic (AOT-safe).
    /// </summary>
    internal static string ToHexLower(byte[] bytes)
    {
        if (bytes.Length == 0) return string.Empty;

        var chars = new char[bytes.Length * 2];
        for (var i = 0; i < bytes.Length; i++)
        {
            var b = bytes[i];
            chars[i * 2]     = ToHexChar(b >> 4);
            chars[i * 2 + 1] = ToHexChar(b & 0xF);
        }
        return new string(chars);
    }

    private static char ToHexChar(int nibble)
        => nibble < 10 ? (char)('0' + nibble) : (char)('a' + nibble - 10);

    /// <summary>
    /// Returns true iff every byte in <paramref name="bytes"/> is zero.
    /// Used to detect the "no parent" sentinel in OTLP span parent_span_id.
    /// </summary>
    internal static bool IsAllZero(byte[] bytes)
    {
        foreach (var b in bytes)
        {
            if (b != 0) return false;
        }
        return true;
    }

    // ── Attribute helpers ─────────────────────────────────────────────────────

    private static string? GetAttr(IReadOnlyDictionary<string, string> attrs, string key)
        => attrs.TryGetValue(key, out var v) && !string.IsNullOrEmpty(v) ? v : null;

    private static void TryCopyAttr(
        IReadOnlyDictionary<string, string> src,
        string                              srcKey,
        Dictionary<string, string>          dst,
        string                              dstKey)
    {
        if (src.TryGetValue(srcKey, out var v) && !string.IsNullOrEmpty(v))
            dst[dstKey] = v;
    }
}
