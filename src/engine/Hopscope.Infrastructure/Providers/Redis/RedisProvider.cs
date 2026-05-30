using Hopscope.Application.Abstractions;
using Microsoft.Extensions.Logging;

namespace Hopscope.Infrastructure.Providers.Redis;

/// <summary>
/// <see cref="IBrokerProvider"/> for Redis.
/// Compile-time DI-registered in <c>Hopscope.Host/Program.cs</c> — no reflection,
/// no runtime scanning. AOT-safe.
/// </summary>
public sealed class RedisProvider : IBrokerProvider
{
    private readonly ILoggerFactory _loggerFactory;

    public RedisProvider(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
    }

    /// <inheritdoc/>
    public string BrokerType => "Redis";

    /// <inheritdoc/>
    /// <remarks>
    /// Case-insensitive ordinal comparison so "redis", "REDIS", and
    /// "Redis" all match.
    /// </remarks>
    public bool CanHandle(IngestionSource source) =>
        string.Equals(source.BrokerType, "Redis", StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc/>
    public IEventIngestor CreateIngestor(IngestionSource source)
    {
        var logger = _loggerFactory.CreateLogger<RedisIngestor>();
        return new RedisIngestor(source, logger);
    }
}
