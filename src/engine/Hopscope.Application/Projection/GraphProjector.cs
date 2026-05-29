using Hopscope.Application.Abstractions;
using Hopscope.Domain.Events;
using Hopscope.Domain.Topology;

namespace Hopscope.Application.Projection;

/// <summary>
/// Pure, stateless implementation of <see cref="IGraphProjector"/>.
///
/// Design contract:
///   - ZERO mutable state — every call is side-effect-free.
///   - The aggregator owns edge counts and the monotonic sequence and passes
///     them in; the projector only shapes the delta.
///   - No System.Linq.Expressions; no reflection. Node/edge construction is
///     plain dictionary lookups and switch expressions — safe on Native AOT.
/// </summary>
public sealed class GraphProjector : IGraphProjector
{
    /// <inheritdoc />
    public GraphDelta Project(
        EventEnvelope evt,
        AggregationResult result,
        long edgeCount,
        long sequence)
    {
        var sourceNode = new GraphNode(
            Id:         evt.Source,
            Kind:       NodeKind.Service,
            Label:      evt.Source,
            BrokerType: evt.BrokerType);

        var destKind   = ResolveDestinationKind(evt);
        var destNode   = new GraphNode(
            Id:         evt.Destination,
            Kind:       destKind,
            Label:      evt.Destination,
            BrokerType: evt.BrokerType);

        var edge = new GraphEdge(
            Id:         $"{evt.Source}->{evt.Destination}",
            SourceId:   evt.Source,
            TargetId:   evt.Destination,
            LastStatus: evt.ExecutionStatus,
            Count:      edgeCount);

        return new GraphDelta(
            UpsertNodes: new[] { sourceNode, destNode },
            UpsertEdges: new[] { edge },
            Sequence:    sequence);
    }

    // -----------------------------------------------------------------------
    // Destination-kind resolution
    // Priority: explicit PayloadMetadata["destinationKind"] > BrokerType inference.
    // An unrecognised metadata value falls through to broker inference rather
    // than throwing — keeps the hot path branch-free.
    // -----------------------------------------------------------------------
    private static NodeKind ResolveDestinationKind(EventEnvelope evt)
    {
        if (evt.PayloadMetadata.TryGetValue("destinationKind", out var raw))
        {
            // Hand-written switch — no Enum.Parse reflection under AOT.
            switch (raw)
            {
                case "Service":  return NodeKind.Service;
                case "Exchange": return NodeKind.Exchange;
                case "Topic":    return NodeKind.Topic;
                case "Queue":    return NodeKind.Queue;
                // Unknown value: fall through to broker inference below.
            }
        }

        return InferFromBrokerType(evt.BrokerType);
    }

    /// <summary>
    /// Broker-type → destination node kind inference.
    /// RabbitMQ → Exchange, Kafka → Topic, Redis → Topic, anything else → Queue.
    /// </summary>
    private static NodeKind InferFromBrokerType(string brokerType) =>
        brokerType switch
        {
            "RabbitMQ" => NodeKind.Exchange,
            "Kafka"    => NodeKind.Topic,
            "Redis"    => NodeKind.Topic,
            _          => NodeKind.Queue
        };
}
