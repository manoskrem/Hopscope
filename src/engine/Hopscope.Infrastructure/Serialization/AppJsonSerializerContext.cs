using System.Text.Json.Serialization;
using Hopscope.Domain.Events;
using Hopscope.Domain.Topology;
using Hopscope.Domain.Tracing;

namespace Hopscope.Infrastructure.Serialization;

/// <summary>
/// Source-generated JSON metadata for every type that crosses the WebSocket / REST
/// boundary. AOT has no reflection fallback — a wire type missing here fails at runtime,
/// not compile time, so add new wire DTOs in the same edit that introduces them.
/// Nested types (<see cref="GraphNode"/>, <see cref="GraphEdge"/>,
/// <see cref="ErrorDetails"/>, <see cref="HopNode"/>, <see cref="EventEnvelope"/>) are
/// generated transitively via their containers.
/// <see cref="PushFrame"/> is the WebSocket wire wrapper (snapshot OR delta per frame).
/// <see cref="TraceView"/> and <see cref="TraceSummary"/> are the trace drill-down types.
/// Public (not internal per the §4d sketch) so the Host assembly can reference
/// <c>AppJsonSerializerContext.Default</c> when wiring <c>ConfigureHttpJsonOptions</c>.
/// </summary>
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(PushFrame))]
[JsonSerializable(typeof(GraphDelta))]
[JsonSerializable(typeof(GraphSnapshot))]
[JsonSerializable(typeof(EventEnvelope))]
[JsonSerializable(typeof(TraceView))]
[JsonSerializable(typeof(TraceSummary))]
[JsonSerializable(typeof(IReadOnlyList<TraceSummary>))]
public sealed partial class AppJsonSerializerContext : JsonSerializerContext;
