using System.Text.Json;
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
    /// Computes the activity delta for a queue between two polls and, when there is
    /// fresh activity, returns a fresh-HopId <see cref="EventEnvelope"/> so the edge
    /// count grows on the canvas.
    ///
    /// The <see cref="EventEnvelope.ExecutionStatus"/> is derived honestly from the
    /// Management-API signals (precedence DeadLettered &gt; Retrying &gt; Success):
    /// <list type="bullet">
    ///   <item><b>DeadLettered</b> — the queue is a dead-letter queue
    ///         (<paramref name="isDeadLetterQueue"/>) and messages just arrived in it.
    ///         The reliable arrival signal is the queue <b>depth</b> growing
    ///         (<c>messages</c>): dead-lettered arrivals do <em>not</em> populate
    ///         <c>message_stats.publish</c> (only direct channel publishes do, and for a
    ///         DLQ <c>message_stats</c> is typically <c>null</c>). A <c>publish</c> delta
    ///         is kept as a secondary trigger for brokers that do report it. Carries
    ///         <see cref="ErrorDetails"/> (no stack trace — aggregate stats have none).</item>
    ///   <item><b>Retrying</b> — the <c>redeliver</c> count grew (a consumer nacked/requeued
    ///         and the broker re-attempted delivery).</item>
    ///   <item><b>Success</b> — plain throughput, no failure signal.</item>
    /// </list>
    /// <c>Failed</c> is intentionally never synthesized here: aggregate signals cannot
    /// distinguish a true failure from a dead-letter/redeliver — that is left to the
    /// per-message / eBPF path (Phase 5).
    ///
    /// <paramref name="counter"/> is a monotonically-incrementing value supplied
    /// by the ingestor to make the HopId unique per observation.
    /// </summary>
    /// <param name="queue">The current queue snapshot (depth + optional stats).</param>
    /// <param name="previousStats">Stats from the previous poll (null = first poll / no stats).</param>
    /// <param name="previousMessages">Queue depth at the previous poll (0 = first poll).</param>
    /// <param name="inboundBindingSource">The exchange that feeds this queue (may be null if unknown).</param>
    /// <param name="isDeadLetterQueue">True if this queue is bound to a dead-letter exchange.</param>
    /// <param name="counter">Monotonic poll counter for fresh-HopId uniqueness.</param>
    /// <param name="timestamp">UTC timestamp for the envelope.</param>
    /// <returns>An activity envelope, or <c>null</c> if nothing changed.</returns>
    internal static EventEnvelope? QueueActivityToEnvelope(
        RmqQueue queue,
        RmqMessageStats? previousStats,
        long previousMessages,
        string? inboundBindingSource,
        bool isDeadLetterQueue,
        long counter,
        DateTimeOffset timestamp)
    {
        var current = queue.MessageStats;   // may be null for dead-letter / idle queues

        // Throughput deltas come from message_stats (0 when absent).
        var deliverDelta   = (current?.DeliverGet ?? 0L) - (previousStats?.DeliverGet ?? 0L);
        var publishDelta   = (current?.Publish    ?? 0L) - (previousStats?.Publish    ?? 0L);
        var redeliverDelta = (current?.Redeliver  ?? 0L) - (previousStats?.Redeliver  ?? 0L);

        // Dead-letter arrivals show up as a depth increase, not a message_stats counter.
        var messagesDelta  = queue.Messages - previousMessages;

        var destination = queue.Name;
        var vhost       = queue.Vhost;

        // Honest status (precedence: DeadLettered > Retrying > Success). Each branch is
        // also the activity gate — if none fires, there is nothing to report.
        ExecutionStatus status;
        ErrorDetails?   error;
        if (isDeadLetterQueue && (messagesDelta > 0 || publishDelta > 0))
        {
            status = ExecutionStatus.DeadLettered;
            error  = new ErrorDetails(
                ExceptionType:       "DeadLettered",
                Message:             $"message dead-lettered to {destination}",
                TruncatedStackTrace: null);
        }
        else if (redeliverDelta > 0)
        {
            status = ExecutionStatus.Retrying;
            error  = null;
        }
        else if (deliverDelta > 0 || publishDelta > 0)
        {
            status = ExecutionStatus.Success;
            error  = null;
        }
        else
        {
            return null;   // no activity this poll
        }

        var source = NormalizeExchangeName(inboundBindingSource ?? string.Empty);

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
            ["redeliverDelta"]   = redeliverDelta.ToString(),
            ["messagesDelta"]    = messagesDelta.ToString(),
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
            ExecutionStatus = status,
            ErrorDetails    = error,
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

    // -----------------------------------------------------------------------
    // Dead-letter classification (internal for test access)
    // -----------------------------------------------------------------------

    /// <summary>
    /// The argument key a queue declares to route expired/rejected messages to a
    /// dead-letter exchange.
    /// </summary>
    internal const string DeadLetterExchangeArg = "x-dead-letter-exchange";

    /// <summary>
    /// Reads a queue's <c>x-dead-letter-exchange</c> argument, or <c>null</c> when the
    /// queue declares none. Robust to a missing/empty/non-object <c>arguments</c> blob and
    /// to non-string argument values (e.g. <c>x-message-ttl</c>).
    /// </summary>
    internal static string? TryGetDeadLetterExchange(RmqQueue queue)
    {
        var args = queue.Arguments;
        if (args.ValueKind != JsonValueKind.Object) return null;
        if (args.TryGetProperty(DeadLetterExchangeArg, out var value)
            && value.ValueKind == JsonValueKind.String)
        {
            var dlx = value.GetString();
            return string.IsNullOrEmpty(dlx) ? null : dlx;
        }
        return null;
    }

    /// <summary>
    /// Identifies the dead-letter queues for this poll: a queue is a DLQ when it is
    /// bound (in <c>/api/bindings</c>) to an exchange named as some queue's
    /// <c>x-dead-letter-exchange</c> (read from <c>/api/queues</c>).
    /// </summary>
    /// <returns>
    /// A map of <em>DLQ name → its dead-letter exchange name</em>. The DLX name (not just a
    /// bool) lets the ingestor force the activity edge's source to the DLX, so the edge is
    /// <c>DLX → DLQ</c> even if the DLQ carries other bindings.
    /// </returns>
    internal static Dictionary<string, string> IdentifyDeadLetterQueues(
        List<RmqQueue> queues,
        List<RmqBinding>? bindings)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        if (bindings is null || bindings.Count == 0) return result;

        // 1. Collect the set of DLX exchange names declared across all queues.
        var dlxNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (var q in queues)
        {
            var dlx = TryGetDeadLetterExchange(q);
            if (dlx is not null) dlxNames.Add(dlx);
        }
        if (dlxNames.Count == 0) return result;

        // 2. A queue bound to one of those DLX exchanges is a dead-letter queue.
        foreach (var b in bindings)
        {
            if (b.DestinationType == "queue"
                && dlxNames.Contains(b.Source)
                && !result.ContainsKey(b.Destination))
            {
                result[b.Destination] = b.Source;
            }
        }

        return result;
    }
}
