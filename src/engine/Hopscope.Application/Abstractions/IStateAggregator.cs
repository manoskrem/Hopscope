using Hopscope.Domain.Events;
using Hopscope.Domain.Topology;
using Hopscope.Domain.Tracing;

namespace Hopscope.Application.Abstractions;

/// <summary>
/// In-memory correlation core. Single-writer; fed by one bounded
/// <c>Channel&lt;EventEnvelope&gt;</c>. Owns all mutable state — no locks.
/// </summary>
public interface IStateAggregator
{
    /// <summary>Idempotent on <see cref="EventEnvelope.HopId"/>; duplicates are no-ops.</summary>
    ValueTask<GraphDelta?> IngestAsync(EventEnvelope evt, CancellationToken ct);

    /// <summary>Current full topology, for a late-joining UI client.</summary>
    GraphSnapshot Snapshot();

    /// <summary>Drill-down: the full causal tree of one trace, or null if unknown/evicted.</summary>
    TraceView? GetTrace(string traceId);
}
