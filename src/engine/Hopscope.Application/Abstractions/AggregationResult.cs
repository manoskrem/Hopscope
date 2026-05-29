namespace Hopscope.Application.Abstractions;

/// <summary>Outcome of ingesting one envelope into the aggregator.</summary>
public readonly record struct AggregationResult(bool IsNew, bool IsDuplicate, string TraceId);
