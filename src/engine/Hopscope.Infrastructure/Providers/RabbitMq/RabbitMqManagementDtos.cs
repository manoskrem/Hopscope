using System.Text.Json.Serialization;

namespace Hopscope.Infrastructure.Providers.RabbitMq;

// ---------------------------------------------------------------------------
// Management API response DTOs
// Only the fields we actually consume are mapped; extras are silently ignored
// by STJ source-gen (no reflection needed for missing properties).
// All snake_case names are declared explicitly via [JsonPropertyName] so the
// context can be configured with camelCase off.
// ---------------------------------------------------------------------------

/// <summary>
/// Represents one binding from the RabbitMQ Management API <c>/api/bindings</c>.
/// </summary>
internal sealed record RmqBinding
{
    [JsonPropertyName("source")]
    public string Source { get; init; } = string.Empty;

    [JsonPropertyName("destination")]
    public string Destination { get; init; } = string.Empty;

    /// <summary>"queue" or "exchange".</summary>
    [JsonPropertyName("destination_type")]
    public string DestinationType { get; init; } = string.Empty;

    [JsonPropertyName("routing_key")]
    public string RoutingKey { get; init; } = string.Empty;

    [JsonPropertyName("vhost")]
    public string Vhost { get; init; } = "/";
}

/// <summary>
/// Counters embedded in a queue's <c>message_stats</c> sub-object.
/// </summary>
internal sealed record RmqMessageStats
{
    /// <summary>Total messages published into this queue.</summary>
    [JsonPropertyName("publish")]
    public long Publish { get; init; }

    /// <summary>Total messages delivered out of this queue (acks + nacks).</summary>
    [JsonPropertyName("deliver_get")]
    public long DeliverGet { get; init; }
}

/// <summary>
/// Subset of a queue object from <c>/api/queues</c>.
/// </summary>
internal sealed record RmqQueue
{
    [JsonPropertyName("name")]
    public string Name { get; init; } = string.Empty;

    [JsonPropertyName("vhost")]
    public string Vhost { get; init; } = "/";

    /// <summary>May be absent when the queue has never received a message.</summary>
    [JsonPropertyName("message_stats")]
    public RmqMessageStats? MessageStats { get; init; }
}

/// <summary>
/// Provider-local source-generated JSON context for the Management-API DTOs.
/// Deliberately separate from <c>AppJsonSerializerContext</c> (which is for
/// UI wire types only). No camelCase policy — field names are explicitly set.
/// </summary>
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.Unspecified,
    GenerationMode = JsonSourceGenerationMode.Metadata)]
[JsonSerializable(typeof(List<RmqBinding>))]
[JsonSerializable(typeof(List<RmqQueue>))]
[JsonSerializable(typeof(RmqBinding))]
[JsonSerializable(typeof(RmqQueue))]
[JsonSerializable(typeof(RmqMessageStats))]
internal sealed partial class RabbitMqJsonContext : System.Text.Json.Serialization.JsonSerializerContext;
