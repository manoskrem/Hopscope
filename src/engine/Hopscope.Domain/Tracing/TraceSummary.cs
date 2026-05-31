using Hopscope.Domain.Events;

namespace Hopscope.Domain.Tracing;

/// <summary>
/// Lightweight summary of one trace, returned by the /traces list endpoint.
/// Immutable record — safe to hand out across the lock boundary.
/// </summary>
public sealed record TraceSummary(
    string TraceId,
    int HopCount,
    ExecutionStatus WorstStatus,
    bool HasError,
    DateTimeOffset LastTimestamp);
