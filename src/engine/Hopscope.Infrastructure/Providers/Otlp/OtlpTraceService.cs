using Grpc.Core;
using Hopscope.Domain.Events;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Proto.Collector.Trace.V1;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Trace.V1;

namespace Hopscope.Infrastructure.Providers.Otlp;

/// <summary>
/// gRPC service implementation for the OTLP TraceService/Export RPC.
/// Kestrel hosts this on the dedicated HTTP/2 port (default 4317).
///
/// Responsibilities:
///   - Iterate ResourceSpans → ScopeSpans → Spans.
///   - Extract resource-level service.name once per ResourceSpans.
///   - Extract span attributes into a flat string dict (allowlisted, not wholesale).
///   - Find the first 'exception' span event (exception.type/message/stacktrace).
///   - Call OtlpMapper.SpanToEnvelope (pure, protobuf-free, unit-testable).
///   - Write the resulting EventEnvelope to OtlpChannelBridge (non-blocking).
///   - Return ExportTraceServiceResponse (no partial_success unless we want to signal errors).
///
/// Error handling: a bad span must never 500 the gRPC Export call — map defensively,
/// skip unparseable bytes, log at Warning. The bridge uses DropOldest under back-pressure,
/// so WriteAsync never blocks.
///
/// AOT safety:
///   - Derives from the Grpc.Tools-generated TraceServiceBase (source-gen, AOT-safe).
///   - Uses ONLY generated parse/serialize paths — NO protobuf Descriptor/reflection/
///     JsonFormatter APIs, which are trim-unfriendly.
///   - No IConfiguration.Get&lt;T&gt;()/Bind() — no config needed here.
/// </summary>
public sealed class OtlpTraceService : TraceService.TraceServiceBase
{
    private readonly OtlpChannelBridge _bridge;
    private readonly ILogger<OtlpTraceService> _log;

    // Last-observed bridge drop count, so we log only when NEW drops occur (not every call).
    private long _lastDroppedSeen;

    public OtlpTraceService(OtlpChannelBridge bridge, ILogger<OtlpTraceService> log)
    {
        _bridge = bridge;
        _log    = log;
    }

    /// <inheritdoc/>
    public override async Task<ExportTraceServiceResponse> Export(
        ExportTraceServiceRequest request,
        ServerCallContext context)
    {
        var ct = context.CancellationToken;

        foreach (var resourceSpans in request.ResourceSpans)
        {
            // Extract resource-level service.name once — it propagates to all spans.
            var resourceServiceName = ExtractServiceName(resourceSpans.Resource?.Attributes);

            foreach (var scopeSpans in resourceSpans.ScopeSpans)
            {
                foreach (var span in scopeSpans.Spans)
                {
                    // Span-kind filter: only SERVER/CLIENT/PRODUCER/CONSUMER are real hops.
                    // Drop INTERNAL/UNSPECIFIED BEFORE mapping so they never reach the bounded
                    // bridge or the trace-retention window — INTERNAL spans are usually the bulk
                    // of a trace and would flood the canvas with non-edge nodes. Silent by design
                    // (high volume); the invalid-id drop below stays a Warning.
                    if (!OtlpMapper.IsHopSpanKind((int)span.Kind))
                        continue;

                    try
                    {
                        var envelope = MapSpan(span, resourceServiceName);
                        if (envelope is null)
                        {
                            // Structurally-invalid identity (bad trace_id/span_id) — drop it
                            // rather than enqueue an envelope with an empty HopId/TraceId.
                            _log.LogWarning(
                                "OTLP: dropping span '{SpanName}' with invalid trace/span id.",
                                span.Name);
                            continue;
                        }
                        await _bridge.WriteAsync(envelope, ct).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        return new ExportTraceServiceResponse();
                    }
                    catch (Exception ex)
                    {
                        // Log the span name (not a body — the name is safe routing metadata).
                        _log.LogWarning(ex,
                            "OTLP: failed to map span '{SpanName}'; skipping.", span.Name);
                    }
                }
            }
        }

        // Surface back-pressure loss: if the bridge dropped envelopes since our last Export,
        // log it once with the delta. A dropped span may be an error span, so the loss must
        // be visible rather than silent.
        var dropped = _bridge.DroppedCount;
        if (dropped > _lastDroppedSeen)
        {
            _log.LogWarning(
                "OTLP: bridge dropped {Delta} span(s) under back-pressure ({Total} total) — " +
                "ingest is falling behind; some hops will be missing from the canvas.",
                dropped - _lastDroppedSeen, dropped);
            _lastDroppedSeen = dropped;
        }

        return new ExportTraceServiceResponse();
    }

    // ── Private span mapping ──────────────────────────────────────────────────

    private static EventEnvelope? MapSpan(Span span, string? resourceServiceName)
    {
        // Flatten span attributes into a string dict (allowlist — no wholesale copy).
        var attributes = FlattenAttributes(span.Attributes);

        // Find the first 'exception' event, if any.
        (string type, string message, string stack)? exceptionEvent = null;
        foreach (var ev in span.Events)
        {
            if (!string.Equals(ev.Name, "exception", StringComparison.OrdinalIgnoreCase))
                continue;

            var evAttrs = FlattenAttributes(ev.Attributes);
            evAttrs.TryGetValue("exception.type",       out var exType);
            evAttrs.TryGetValue("exception.message",    out var exMsg);
            evAttrs.TryGetValue("exception.stacktrace", out var exStack);

            exceptionEvent = (exType ?? string.Empty, exMsg ?? string.Empty, exStack ?? string.Empty);
            break; // first exception event only
        }

        // Convert the start time (nanoseconds since Unix epoch) to DateTimeOffset UTC,
        // preserving sub-millisecond precision (1 tick = 100 ns). An unset/zero start time
        // would map to 1970 — an instantly-evictable edge — so fall back to interception
        // time (UtcNow) in that case. Both APIs are AOT-safe.
        var ts = span.StartTimeUnixNano == 0
            ? DateTimeOffset.UtcNow
            : DateTimeOffset.UnixEpoch.AddTicks((long)(span.StartTimeUnixNano / 100UL));

        return OtlpMapper.SpanToEnvelope(
            traceId:             span.TraceId.ToByteArray(),
            spanId:              span.SpanId.ToByteArray(),
            parentSpanId:        span.ParentSpanId.IsEmpty ? [] : span.ParentSpanId.ToByteArray(),
            spanName:            span.Name,
            resourceServiceName: resourceServiceName,
            attributes:          attributes,
            statusCode:          (int)span.Status.Code,
            exceptionEvent:      exceptionEvent,
            ts:                  ts);
    }

    /// <summary>
    /// Flattens a KeyValue list into a plain string dict using only string_value entries.
    /// Other AnyValue types (int, bool, etc.) are skipped — we only want routing metadata.
    /// No LINQ, no reflection — plain foreach.
    /// </summary>
    private static Dictionary<string, string> FlattenAttributes(
        Google.Protobuf.Collections.RepeatedField<KeyValue> kvs)
    {
        var dict = new Dictionary<string, string>(kvs.Count, StringComparer.Ordinal);
        foreach (var kv in kvs)
        {
            if (kv.Value?.ValueCase == AnyValue.ValueOneofCase.StringValue)
                dict[kv.Key] = kv.Value.StringValue;
        }
        return dict;
    }

    /// <summary>
    /// Extracts service.name from a resource's attribute list.
    /// Returns null if absent — OtlpMapper falls back to span-level attributes.
    /// </summary>
    private static string? ExtractServiceName(
        Google.Protobuf.Collections.RepeatedField<KeyValue>? attrs)
    {
        if (attrs is null) return null;
        foreach (var kv in attrs)
        {
            if (string.Equals(kv.Key, "service.name", StringComparison.Ordinal)
                && kv.Value?.ValueCase == AnyValue.ValueOneofCase.StringValue
                && !string.IsNullOrEmpty(kv.Value.StringValue))
            {
                return kv.Value.StringValue;
            }
        }
        return null;
    }
}
