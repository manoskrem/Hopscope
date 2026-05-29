namespace Hopscope.Domain.Events;

/// <summary>
/// The normalized envelope EVERY ingestor MUST emit before touching the engine.
/// In-process providers and the remote agent's gRPC receiver produce this identical
/// shape — the aggregator can't tell them apart. Congruent with
/// <c>contracts/proto/event.proto</c> (<c>hopscope.v1.EventEnvelope</c>).
/// </summary>
public sealed record EventEnvelope
{
    /// <summary>Global correlation id tying all hops of one logical flow together.</summary>
    public required string TraceId { get; init; }

    /// <summary>Unique per message instance — the idempotency / dedupe key.</summary>
    public required string HopId { get; init; }

    /// <summary>Upstream hop that caused this one. Null == root (proto: empty string).</summary>
    public string? ParentHopId { get; init; }

    /// <summary>Originating component.</summary>
    public required string Source { get; init; }

    /// <summary>Exchange / topic / queue this hop targets.</summary>
    public required string Destination { get; init; }

    /// <summary>"RabbitMQ" | "Kafka" | "Redis" | ...</summary>
    public required string BrokerType { get; init; }

    /// <summary>
    /// Headers / routing keys / type-names ONLY — NEVER message bodies (RAM guard).
    /// </summary>
    public IReadOnlyDictionary<string, string> PayloadMetadata { get; init; }
        = new Dictionary<string, string>();

    /// <summary>UTC at the moment of interception.</summary>
    public required DateTimeOffset Timestamp { get; init; }

    public required ExecutionStatus ExecutionStatus { get; init; }

    /// <summary>Non-null iff <see cref="ExecutionStatus"/> is a failure (proto: unset == null).</summary>
    public ErrorDetails? ErrorDetails { get; init; }
}
