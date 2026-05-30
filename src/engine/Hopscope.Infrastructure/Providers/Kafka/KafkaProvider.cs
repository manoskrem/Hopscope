using Hopscope.Application.Abstractions;
using Microsoft.Extensions.Logging;

namespace Hopscope.Infrastructure.Providers.Kafka;

/// <summary>
/// <see cref="IBrokerProvider"/> for Kafka.
/// Compile-time DI-registered in <c>Hopscope.Host/Program.cs</c> — no reflection,
/// no runtime scanning. AOT-safe.
/// </summary>
public sealed class KafkaProvider : IBrokerProvider
{
    private readonly ILoggerFactory _loggerFactory;

    public KafkaProvider(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
    }

    /// <inheritdoc/>
    public string BrokerType => "Kafka";

    /// <inheritdoc/>
    /// <remarks>
    /// Case-insensitive ordinal comparison so "kafka", "KAFKA", and
    /// "Kafka" all match.
    /// </remarks>
    public bool CanHandle(IngestionSource source) =>
        string.Equals(source.BrokerType, "Kafka", StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc/>
    public IEventIngestor CreateIngestor(IngestionSource source)
    {
        var logger = _loggerFactory.CreateLogger<KafkaIngestor>();
        return new KafkaIngestor(source, logger);
    }
}
