namespace Hopscope.Application.Abstractions;

/// <summary>
/// Runtime configuration describing one configured tap. Providers are compile-time
/// registered; this carries the per-source connection details they need.
/// </summary>
public sealed record IngestionSource(string BrokerType, string ConnectionString,
                                     IReadOnlyDictionary<string, string> Options);
