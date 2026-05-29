using Hopscope.Domain.Events;

namespace Hopscope.Infrastructure.Providers.RabbitMq;

/// <summary>
/// Pure, static mapping logic: RabbitMQ Management DTOs → <see cref="EventEnvelope"/>.
///
/// Extracted into its own class so unit tests can exercise the mapping without
/// standing up an HTTP client or a live broker. All methods are AOT-safe:
/// no reflection, no LINQ Expressions, no dynamic codegen.
/// </summary>
internal static class RabbitMqMapper
{
    /// <summary>
    /// Label used for the default (nameless) exchange in RabbitMQ.
    /// The Management API returns an empty string for <c>source</c> on default-exchange bindings.
    /// </summary>
    internal const string DefaultExchangeLabel = "(default)";

    // -----------------------------------------------------------------------
    // Topology mapping
    // -----------------------------------------------------------------------

    /// <summary>
    /// Maps a single <see cref="RmqBinding"/> to a topology <see cref="EventEnvelope"/>.
    ///
    /// The HopId is <em>stable</em> across polls — same binding always produces the same
    /// HopId so the aggregator dedupes it and the topology edge is rendered exactly once.
    /// </summary>
    internal static EventEnvelope BindingToEnvelope(RmqBinding binding, DateTimeOffset timestamp)
    {
        var source      = NormalizeExchangeName(binding.Source);
        var destination = binding.Destination;
        var vhost       = binding.Vhost;
        var routingKey  = binding.RoutingKey;
        var destKind    = ResolveDestinationKind(binding.DestinationType);

        // Stable HopId — same binding → same id every poll → aggregator dedupes → edge renders once.
        var hopId   = BuildStableBindingHopId(vhost, source, destination, routingKey);
        var traceId = $"rmq-topology:{hopId}";

        var metadata = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["routingKey"]       = routingKey,
            ["sourceKind"]       = "Exchange",
            ["destinationKind"]  = destKind,
            ["vhost"]            = vhost,
        };

        return new EventEnvelope
        {
            TraceId         = traceId,
            HopId           = hopId,
            ParentHopId     = null,
            Source          = source,
            Destination     = destination,
            BrokerType      = "RabbitMQ",
            Timestamp       = timestamp,
            ExecutionStatus = ExecutionStatus.Success,
            ErrorDetails    = null,
            PayloadMetadata = metadata,
        };
    }

    /// <summary>
    /// Builds a stable, deterministic HopId for a binding.
    /// Format: <c>binding:{vhost}:{source}-&gt;{destination}:{routingKey}</c>
    /// </summary>
    internal static string BuildStableBindingHopId(
        string vhost, string source, string destination, string routingKey) =>
        $"binding:{vhost}:{source}->{destination}:{routingKey}";

    // -----------------------------------------------------------------------
    // Live-activity mapping
    // -----------------------------------------------------------------------

    /// <summary>
    /// Computes the activity delta for a queue between two polls and, when the
    /// deliver/publish count has increased, returns a fresh-HopId
    /// <see cref="EventEnvelope"/> so the edge count grows on the canvas.
    ///
    /// <paramref name="counter"/> is a monotonically-incrementing value supplied
    /// by the ingestor to make the HopId unique per observation.
    /// </summary>
    /// <param name="queue">The current queue stats snapshot.</param>
    /// <param name="previousStats">Stats from the previous poll (null = first poll).</param>
    /// <param name="inboundBindingSource">The exchange that feeds this queue (may be null if unknown).</param>
    /// <param name="counter">Monotonic poll counter for fresh-HopId uniqueness.</param>
    /// <param name="timestamp">UTC timestamp for the envelope.</param>
    /// <returns>An activity envelope, or <c>null</c> if counts have not increased.</returns>
    internal static EventEnvelope? QueueActivityToEnvelope(
        RmqQueue queue,
        RmqMessageStats? previousStats,
        string? inboundBindingSource,
        long counter,
        DateTimeOffset timestamp)
    {
        var current = queue.MessageStats;
        if (current is null) return null;

        var prevDeliverGet = previousStats?.DeliverGet ?? 0L;
        var prevPublish    = previousStats?.Publish    ?? 0L;

        // Activity detected when either metric increases.
        var deliverDelta = current.DeliverGet - prevDeliverGet;
        var publishDelta = current.Publish    - prevPublish;

        if (deliverDelta <= 0 && publishDelta <= 0) return null;

        var source      = NormalizeExchangeName(inboundBindingSource ?? string.Empty);
        var destination = queue.Name;
        var vhost       = queue.Vhost;

        // Fresh HopId: includes the counter so each activity observation is distinct.
        var hopId   = $"activity:{vhost}:{destination}:{counter}";
        var traceId = $"rmq-activity:{vhost}:{destination}:{counter}";

        var metadata = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["sourceKind"]       = "Exchange",
            ["destinationKind"]  = "Queue",
            ["vhost"]            = vhost,
            ["deliverDelta"]     = deliverDelta.ToString(),
            ["publishDelta"]     = publishDelta.ToString(),
        };

        return new EventEnvelope
        {
            TraceId         = traceId,
            HopId           = hopId,
            ParentHopId     = null,
            Source          = source,
            Destination     = destination,
            BrokerType      = "RabbitMQ",
            Timestamp       = timestamp,
            ExecutionStatus = ExecutionStatus.Success,
            ErrorDetails    = null,
            PayloadMetadata = metadata,
        };
    }

    // -----------------------------------------------------------------------
    // Helpers (internal for test access)
    // -----------------------------------------------------------------------

    /// <summary>
    /// Replaces an empty exchange name (RabbitMQ's default exchange) with the
    /// human-readable label <c>(default)</c>.
    /// </summary>
    internal static string NormalizeExchangeName(string name) =>
        string.IsNullOrEmpty(name) ? DefaultExchangeLabel : name;

    /// <summary>
    /// Maps the <c>destination_type</c> field from the Management API to the
    /// <c>destinationKind</c> metadata value the engine projector understands.
    /// </summary>
    private static string ResolveDestinationKind(string destinationType) =>
        destinationType switch
        {
            "queue"    => "Queue",
            "exchange" => "Exchange",
            _          => "Queue",   // safe default
        };
}
