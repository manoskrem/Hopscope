using System.Text.Json.Serialization;
using Hopscope.Domain.Events;
using Hopscope.Domain.Topology;

namespace Hopscope.Infrastructure.Serialization;

/// <summary>
/// Source-generated JSON metadata for every type that crosses the WebSocket / REST
/// boundary. AOT has no reflection fallback — a wire type missing here fails at runtime,
/// not compile time, so add new wire DTOs in the same edit that introduces them.
/// Nested types (<see cref="GraphNode"/>, <see cref="GraphEdge"/>,
/// <see cref="ErrorDetails"/>) are generated transitively via their containers.
/// Public (not internal per the §4d sketch) so the Host assembly can reference
/// <c>AppJsonSerializerContext.Default</c> when wiring <c>ConfigureHttpJsonOptions</c>.
/// </summary>
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(GraphDelta))]
[JsonSerializable(typeof(GraphSnapshot))]
[JsonSerializable(typeof(EventEnvelope))]
public sealed partial class AppJsonSerializerContext : JsonSerializerContext;
