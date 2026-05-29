namespace Hopscope.Domain.Tracing;

/// <summary>
/// Drill-down view of one trace: its causal hop tree(s) reconstructed from
/// <c>ParentHopId</c> links. <see cref="Roots"/> is a list (not a single root) because a
/// hop whose parent has been window-evicted, or which arrived before its parent, surfaces
/// as an additional root rather than being dropped.
/// </summary>
public sealed record TraceView(string TraceId, IReadOnlyList<HopNode> Roots, int HopCount);
