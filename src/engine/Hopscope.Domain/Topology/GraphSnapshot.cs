namespace Hopscope.Domain.Topology;

/// <summary>
/// Full topology handed to a late-joining client before it starts consuming deltas.
/// <paramref name="Sequence"/> marks the delta cursor the snapshot is current as of.
/// </summary>
public sealed record GraphSnapshot(IReadOnlyList<GraphNode> Nodes,
                                   IReadOnlyList<GraphEdge> Edges, long Sequence);
