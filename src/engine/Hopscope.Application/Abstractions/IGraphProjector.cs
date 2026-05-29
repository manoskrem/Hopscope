using Hopscope.Domain.Events;
using Hopscope.Domain.Topology;

namespace Hopscope.Application.Abstractions;

/// <summary>Turns an aggregator state change into UI-shaped node/edge upserts.</summary>
public interface IGraphProjector
{
    GraphDelta Project(EventEnvelope evt, AggregationResult result);
}
