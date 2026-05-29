using Hopscope.Domain.Events;
using Hopscope.Domain.Topology;

namespace Hopscope.Application.Abstractions;

/// <summary>
/// Turns an aggregator state change into UI-shaped node/edge upserts.
/// The projector is PURE — it holds no mutable state. The aggregator owns
/// counts and the monotonic sequence counter and passes them in so the
/// projector remains trivially testable.
/// </summary>
public interface IGraphProjector
{
    /// <summary>
    /// Build a <see cref="GraphDelta"/> for the given envelope.
    /// </summary>
    /// <param name="evt">The envelope just ingested.</param>
    /// <param name="result">Outcome metadata from the aggregator.</param>
    /// <param name="edgeCount">Current (post-increment) traversal count for the source→dest edge.</param>
    /// <param name="sequence">Monotonic sequence number stamped on the delta.</param>
    GraphDelta Project(EventEnvelope evt, AggregationResult result, long edgeCount, long sequence);
}
