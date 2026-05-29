using Hopscope.Domain.Events;

namespace Hopscope.Domain.Topology;

/// <summary>
/// A directed message-hop edge between two nodes. <paramref name="Count"/> aggregates
/// how many hops traversed it; <paramref name="LastStatus"/> colors it on the canvas.
/// </summary>
public sealed record GraphEdge(string Id, string SourceId, string TargetId,
                               ExecutionStatus LastStatus, long Count);
