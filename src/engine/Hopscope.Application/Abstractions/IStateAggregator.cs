using Hopscope.Domain.Events;
using Hopscope.Domain.Topology;
using Hopscope.Domain.Tracing;

namespace Hopscope.Application.Abstractions;

/// <summary>
/// In-memory correlation core. Single-writer; fed by one bounded
/// <c>Channel&lt;EventEnvelope&gt;</c>. Owns all mutable state — no locks
/// on the topology path; trace-store reads use <c>_traceGate</c>.
/// </summary>
public interface IStateAggregator
{
    /// <summary>Idempotent on <see cref="EventEnvelope.HopId"/>; duplicates are no-ops.</summary>
    ValueTask<GraphDelta?> IngestAsync(EventEnvelope evt, CancellationToken ct);

    /// <summary>Current full topology, for a late-joining UI client.</summary>
    GraphSnapshot Snapshot();

    /// <summary>Drill-down: the full causal tree of one trace, or null if unknown/evicted.</summary>
    TraceView? GetTrace(string traceId);

    /// <summary>
    /// Bounded, newest-first trace summaries for the debugger list.
    /// <paramref name="status"/>: "failed" | "deadlettered" | "error" | null/""/"all" (no filter).
    /// <paramref name="source"/> / <paramref name="target"/>: restrict to traces that have a hop
    /// on that edge; null or empty means "any".
    /// <paramref name="limit"/> is caller-capped (1–1000).
    /// Thread-safe: held under _traceGate for the duration of the scan.
    /// </summary>
    IReadOnlyList<TraceSummary> GetTraces(string? status, string? source, string? target, int limit);
}
