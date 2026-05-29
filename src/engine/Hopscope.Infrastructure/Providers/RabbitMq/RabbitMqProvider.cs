using Hopscope.Application.Abstractions;
using Microsoft.Extensions.Logging;

namespace Hopscope.Infrastructure.Providers.RabbitMq;

/// <summary>
/// <see cref="IBrokerProvider"/> for RabbitMQ.
/// Compile-time DI-registered in <c>Hopscope.Host/Program.cs</c> — no reflection,
/// no runtime scanning. AOT-safe.
/// </summary>
public sealed class RabbitMqProvider : IBrokerProvider
{
    private readonly ILoggerFactory _loggerFactory;

    public RabbitMqProvider(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
    }

    /// <inheritdoc/>
    public string BrokerType => "RabbitMQ";

    /// <inheritdoc/>
    /// <remarks>
    /// Case-insensitive ordinal comparison so "rabbitmq", "RABBITMQ", and
    /// "RabbitMQ" all match.
    /// </remarks>
    public bool CanHandle(IngestionSource source) =>
        string.Equals(source.BrokerType, "RabbitMQ", StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc/>
    public IEventIngestor CreateIngestor(IngestionSource source)
    {
        var logger = _loggerFactory.CreateLogger<RabbitMqIngestor>();
        return new RabbitMqIngestor(source, logger);
    }
}
