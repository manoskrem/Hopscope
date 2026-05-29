namespace Hopscope.Domain.Topology;

/// <summary>
/// Incremental topology update pushed to the UI. <paramref name="Sequence"/> is monotonic
/// so clients can detect gaps and request a fresh snapshot.
/// </summary>
public sealed record GraphDelta(IReadOnlyList<GraphNode> UpsertNodes,
                                IReadOnlyList<GraphEdge> UpsertEdges, long Sequence);
