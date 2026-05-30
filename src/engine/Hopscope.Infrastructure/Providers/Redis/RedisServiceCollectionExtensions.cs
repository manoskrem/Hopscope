using Hopscope.Application.Abstractions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Hopscope.Infrastructure.Providers.Redis;

/// <summary>
/// Host wiring for the Redis provider — the Phase-4 seam in one line:
/// adding a broker = its <c>Providers/&lt;Broker&gt;/</c> folder + a single
/// <c>builder.Services.AddRedisIngestion(config)</c> call in <c>Program.cs</c>.
///
/// Config is read MANUALLY (NO reflective <c>IConfiguration.Get&lt;T&gt;()</c>/<c>Bind()</c> —
/// they are not AOT-safe). Supported keys (env var or appsettings):
/// <list type="bullet">
///   <item><c>HOPSCOPE_REDIS_URL</c> / <c>Hopscope:Redis:Url</c></item>
///   <item><c>HOPSCOPE_REDIS_KEY_DEPTH</c> / <c>Hopscope:Redis:KeyDepth</c></item>
///   <item><c>HOPSCOPE_REDIS_DB</c> / <c>Hopscope:Redis:Db</c></item>
/// </list>
///
/// When no URL is configured this registers nothing — so the FakeIngestor fallback
/// (<see cref="Fake.FakeIngestionServiceCollectionExtensions"/>) can take over when
/// neither RabbitMQ nor Redis are configured. Real data only — never fake + real.
/// </summary>
public static class RedisServiceCollectionExtensions
{
    public static IServiceCollection AddRedisIngestion(
        this IServiceCollection services, IConfiguration config)
    {
        var url = config["HOPSCOPE_REDIS_URL"] ?? config["Hopscope:Redis:Url"];
        if (string.IsNullOrWhiteSpace(url))
            return services;

        var keyDepth = config["HOPSCOPE_REDIS_KEY_DEPTH"] ?? config["Hopscope:Redis:KeyDepth"];
        var db       = config["HOPSCOPE_REDIS_DB"]        ?? config["Hopscope:Redis:Db"];

        var options = new Dictionary<string, string>(StringComparer.Ordinal);
        if (!string.IsNullOrWhiteSpace(keyDepth)) options["keyDepth"] = keyDepth;
        if (!string.IsNullOrWhiteSpace(db))       options["db"]       = db;

        var source = new IngestionSource(
            BrokerType:       "Redis",
            ConnectionString: url,
            Options:          options);

        // The provider registration is the canonical seam line; the ingestor factory
        // resolves the matching provider via CanHandle (deterministic, reflection-free)
        // so multiple brokers coexist without ordering assumptions.
        services.AddSingleton<IBrokerProvider, RedisProvider>();
        services.AddSingleton<IEventIngestor>(sp => CreateIngestor(sp, source));

        return services;
    }

    private static IEventIngestor CreateIngestor(IServiceProvider sp, IngestionSource source)
    {
        foreach (var provider in sp.GetServices<IBrokerProvider>())
        {
            if (provider.CanHandle(source))
                return provider.CreateIngestor(source);
        }

        // Unreachable in practice: the provider above is always registered alongside
        // this factory. Constructing directly keeps the method total.
        return new RedisProvider(sp.GetRequiredService<ILoggerFactory>()).CreateIngestor(source);
    }
}
