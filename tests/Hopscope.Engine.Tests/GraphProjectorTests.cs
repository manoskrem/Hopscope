using Hopscope.Application.Abstractions;
using Hopscope.Application.Projection;
using Hopscope.Domain.Events;
using Hopscope.Domain.Topology;

namespace Hopscope.Engine.Tests;

/// <summary>
/// GraphProjector is pure (no mutable state), so every test constructs
/// a fresh instance and asserts the returned GraphDelta in isolation.
/// </summary>
public sealed class GraphProjectorTests
{
    private static readonly IGraphProjector Projector = new GraphProjector();

    // -----------------------------------------------------------------------
    // Helper: minimal valid envelope
    // -----------------------------------------------------------------------
    private static EventEnvelope MakeEnvelope(
        string source = "svc-a",
        string destination = "orders",
        string brokerType = "RabbitMQ",
        ExecutionStatus status = ExecutionStatus.Success,
        Dictionary<string, string>? metadata = null,
        string? traceId = null,
        string? hopId = null) =>
        new()
        {
            TraceId      = traceId      ?? "trace-1",
            HopId        = hopId        ?? "hop-1",
            Source       = source,
            Destination  = destination,
            BrokerType   = brokerType,
            Timestamp    = DateTimeOffset.UtcNow,
            ExecutionStatus = status,
            PayloadMetadata = metadata ?? new Dictionary<string, string>()
        };

    private static AggregationResult NewResult(string traceId = "trace-1") =>
        new(IsNew: true, IsDuplicate: false, TraceId: traceId);

    // -----------------------------------------------------------------------
    // 1. Source node is always Service
    // -----------------------------------------------------------------------
    [Fact]
    public void Project_SourceNode_IsServiceKind()
    {
        var evt = MakeEnvelope(source: "payment-svc");
        var delta = Projector.Project(evt, NewResult(), edgeCount: 1, sequence: 1);

        var src = delta.UpsertNodes.Single(n => n.Id == "payment-svc");
        Assert.Equal(NodeKind.Service, src.Kind);
        Assert.Equal("payment-svc", src.Label);
        Assert.Equal("RabbitMQ", src.BrokerType);
    }

    // -----------------------------------------------------------------------
    // 2. Destination kind: explicit metadata wins
    // -----------------------------------------------------------------------
    [Theory]
    [InlineData("Service",  NodeKind.Service)]
    [InlineData("Exchange", NodeKind.Exchange)]
    [InlineData("Topic",    NodeKind.Topic)]
    [InlineData("Queue",    NodeKind.Queue)]
    public void Project_DestinationKind_MetadataWins(string metaValue, NodeKind expected)
    {
        var evt = MakeEnvelope(
            destination: "dest",
            metadata: new Dictionary<string, string> { ["destinationKind"] = metaValue });

        var delta = Projector.Project(evt, NewResult(), edgeCount: 1, sequence: 1);

        var dest = delta.UpsertNodes.Single(n => n.Id == "dest");
        Assert.Equal(expected, dest.Kind);
    }

    // -----------------------------------------------------------------------
    // 3. Destination kind: broker-type inference when no metadata key
    // -----------------------------------------------------------------------
    [Theory]
    [InlineData("RabbitMQ", NodeKind.Exchange)]
    [InlineData("Kafka",    NodeKind.Topic)]
    [InlineData("Redis",    NodeKind.Topic)]
    [InlineData("SQS",      NodeKind.Queue)]   // anything else → Queue
    [InlineData("Azure",    NodeKind.Queue)]
    public void Project_DestinationKind_BrokerInference(string brokerType, NodeKind expected)
    {
        var evt = MakeEnvelope(destination: "dest", brokerType: brokerType);
        var delta = Projector.Project(evt, NewResult(), edgeCount: 1, sequence: 1);

        var dest = delta.UpsertNodes.Single(n => n.Id == "dest");
        Assert.Equal(expected, dest.Kind);
    }

    // -----------------------------------------------------------------------
    // 4. Edge shape: Id, SourceId, TargetId, LastStatus, Count
    // -----------------------------------------------------------------------
    [Fact]
    public void Project_Edge_HasCorrectFieldsAndCount()
    {
        var evt   = MakeEnvelope(source: "svc-a", destination: "q-orders", status: ExecutionStatus.DeadLettered);
        var delta = Projector.Project(evt, NewResult(), edgeCount: 7, sequence: 42);

        Assert.Single(delta.UpsertEdges);
        var edge = delta.UpsertEdges[0];
        Assert.Equal("svc-a->q-orders",        edge.Id);
        Assert.Equal("svc-a",                  edge.SourceId);
        Assert.Equal("q-orders",               edge.TargetId);
        Assert.Equal(ExecutionStatus.DeadLettered, edge.LastStatus);
        Assert.Equal(7L,                       edge.Count);
    }

    // -----------------------------------------------------------------------
    // 5. Sequence is stamped on the delta from the caller-supplied value
    // -----------------------------------------------------------------------
    [Fact]
    public void Project_Sequence_IsPassedThrough()
    {
        var evt   = MakeEnvelope();
        var delta = Projector.Project(evt, NewResult(), edgeCount: 1, sequence: 999);

        Assert.Equal(999L, delta.Sequence);
    }

    // -----------------------------------------------------------------------
    // 6. Delta always contains exactly 2 upsert nodes and 1 edge
    // -----------------------------------------------------------------------
    [Fact]
    public void Project_AlwaysProducesTwoNodesAndOneEdge()
    {
        var evt   = MakeEnvelope(source: "a", destination: "b");
        var delta = Projector.Project(evt, NewResult(), edgeCount: 1, sequence: 1);

        Assert.Equal(2, delta.UpsertNodes.Count);
        Assert.Single(delta.UpsertEdges);
    }

    // -----------------------------------------------------------------------
    // 7. Unknown/invalid destinationKind metadata falls back to broker inference
    // -----------------------------------------------------------------------
    [Fact]
    public void Project_UnknownDestinationKindMetadata_FallsBackToBrokerInference()
    {
        var evt = MakeEnvelope(
            brokerType: "Kafka",
            metadata: new Dictionary<string, string> { ["destinationKind"] = "Nonsense" });

        var delta = Projector.Project(evt, NewResult(), edgeCount: 1, sequence: 1);

        var dest = delta.UpsertNodes.Single(n => n.Id == evt.Destination);
        Assert.Equal(NodeKind.Topic, dest.Kind); // Kafka fallback
    }
}
