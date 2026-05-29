using Hopscope.Domain.Events;

namespace Hopscope.Domain.Tracing;

/// <summary>
/// One node in a trace's causal tree: the hop's envelope plus the child hops it caused
/// (linked via <see cref="EventEnvelope.ParentHopId"/>).
/// </summary>
public sealed record HopNode(EventEnvelope Envelope, IReadOnlyList<HopNode> Children);
