using Hopscope.Domain.Events;
using ProtoEnvelope = Hopscope.Contracts.V1.EventEnvelope;
using ProtoStatus = Hopscope.Contracts.V1.ExecutionStatus;
using ProtoErrorDetails = Hopscope.Contracts.V1.ErrorDetails;

namespace Hopscope.Infrastructure.Providers.Agent;

/// <summary>
/// Pure, static mapping from the generated protobuf <c>Hopscope.Contracts.V1.EventEnvelope</c>
/// (received on the gRPC stream from a remote agent) to the domain
/// <see cref="EventEnvelope"/>. The aggregator can't tell an agent-sourced envelope from an
/// in-proc provider's — both arrive as the canonical domain shape.
///
/// proto3 nullability seam (proto3 has no null):
///   - empty <c>parent_hop_id</c> ⇒ null (root).
///   - unset <c>error_details</c> message (null property) ⇒ null.
///   - unset / all-zero <c>timestamp</c> ⇒ interception time (UtcNow).
///
/// Reject-don't-emit discipline (mirrors OtlpMapper): a structurally-invalid envelope returns
/// null so the caller drops it rather than enqueue a malformed hop. An empty <c>hop_id</c> would
/// collapse every malformed envelope onto one dedupe key (the aggregator drops all but the first);
/// an empty <c>trace_id</c> breaks ParentHopId correlation; an empty <c>source</c>/<c>destination</c>
/// would render as an empty-id node (the projector keys nodes by those). So all four are required.
///
/// AOT safety: no reflection, no Enum.Parse, no LINQ Expressions, no JsonParser/JsonFormatter/
/// Descriptor APIs — only the generated property accessors, a hand-written enum switch, and a
/// plain foreach over the MapField. <c>Timestamp.ToDateTimeOffset()</c> is pure arithmetic.
///
/// PayloadMetadata is copied as-is (headers / routing keys / type-names) — never bodies. The agent
/// is contractually responsible for sending metadata only and a pre-truncated stack; the
/// <see cref="StackTraceCap"/> here is defence-in-depth against a misbehaving agent blowing the
/// trace-retention RAM budget.
/// </summary>
internal static class AgentMapper
{
    /// <summary>Hard cap on a received stack trace — defensive RAM guard (matches OTLP).</summary>
    internal const int StackTraceCap = 2048;

    internal static EventEnvelope? Map(ProtoEnvelope proto)
    {
        // ── Identity validation (reject malformed) ──────────────────────────────
        if (string.IsNullOrWhiteSpace(proto.TraceId) || string.IsNullOrWhiteSpace(proto.HopId))
            return null;
        if (string.IsNullOrWhiteSpace(proto.Source) || string.IsNullOrWhiteSpace(proto.Destination))
            return null;

        // ── parent_hop_id: empty string == null (proto3 has no null) ────────────
        var parentHopId = string.IsNullOrEmpty(proto.ParentHopId) ? null : proto.ParentHopId;

        // ── Timestamp: unset (null) or all-zero ⇒ interception time ─────────────
        // A message-typed field is null when never set on the wire.
        var ts = proto.Timestamp is null ||
                 (proto.Timestamp.Seconds == 0 && proto.Timestamp.Nanos == 0)
            ? DateTimeOffset.UtcNow
            : proto.Timestamp.ToDateTimeOffset();

        // ── ErrorDetails: unset message == null ─────────────────────────────────
        var errorDetails = proto.ErrorDetails is null ? null : MapError(proto.ErrorDetails);

        // ── payload_metadata: MapField<string,string> → Dictionary (no LINQ) ────
        var metadata = new Dictionary<string, string>(proto.PayloadMetadata.Count, StringComparer.Ordinal);
        foreach (var kv in proto.PayloadMetadata)
            metadata[kv.Key] = kv.Value;

        return new EventEnvelope
        {
            TraceId         = proto.TraceId,
            HopId           = proto.HopId,
            ParentHopId     = parentHopId,
            Source          = proto.Source,
            Destination     = proto.Destination,
            BrokerType      = string.IsNullOrEmpty(proto.BrokerType) ? "Agent" : proto.BrokerType,
            PayloadMetadata = metadata,
            Timestamp       = ts,
            ExecutionStatus = MapStatus(proto.ExecutionStatus),
            ErrorDetails    = errorDetails,
        };
    }

    // Hand-written switch (clearer than a (int) cast and defends against future enum drift).
    private static ExecutionStatus MapStatus(ProtoStatus s) => s switch
    {
        ProtoStatus.Success      => ExecutionStatus.Success,
        ProtoStatus.Retrying     => ExecutionStatus.Retrying,
        ProtoStatus.DeadLettered => ExecutionStatus.DeadLettered,
        ProtoStatus.Failed       => ExecutionStatus.Failed,
        _                        => ExecutionStatus.Success, // unknown wire value ⇒ safe default
    };

    private static ErrorDetails MapError(ProtoErrorDetails e)
    {
        var stack = e.TruncatedStackTrace;
        var truncated = string.IsNullOrEmpty(stack)
            ? null
            : (stack.Length > StackTraceCap ? stack[..StackTraceCap] : stack);

        return new ErrorDetails(
            ExceptionType:       e.ExceptionType ?? string.Empty,
            Message:             e.Message ?? string.Empty,
            TruncatedStackTrace: truncated);
    }
}
