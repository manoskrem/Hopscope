using Hopscope.Application.Abstractions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Hopscope.Infrastructure.Providers.Kafka;

/// <summary>
/// Host wiring for the Kafka provider — the Phase-4 seam in one line:
/// adding a broker = its <c>Providers/&lt;Broker&gt;/</c> folder + a single
/// <c>builder.Services.AddKafkaIngestion(config)</c> call in <c>Program.cs</c>.
///
/// Config is read MANUALLY (NO reflective <c>IConfiguration.Get&lt;T&gt;()</c>/<c>Bind()</c> —
/// they are not AOT-safe). Supported keys (env var or appsettings):
/// <list type="bullet">
///   <item><c>HOPSCOPE_KAFKA_URL</c> / <c>Hopscope:Kafka:Url</c> — bootstrap servers</item>
///   <item><c>HOPSCOPE_KAFKA_GROUP</c> / <c>Hopscope:Kafka:Group</c> — consumer group</item>
///   <item><c>HOPSCOPE_KAFKA_TOPICS</c> / <c>Hopscope:Kafka:Topics</c> — comma-separated allowlist</item>
/// </list>
///
/// When no URL is configured this registers nothing — so the FakeIngestor fallback
/// (<see cref="Fake.FakeIngestionServiceCollectionExtensions"/>) can take over when
/// no broker is configured. Real data only — never fake + real.
/// </summary>
public static class KafkaServiceCollectionExtensions
{
    public static IServiceCollection AddKafkaIngestion(
        this IServiceCollection services, IConfiguration config)
    {
        var url = config["HOPSCOPE_KAFKA_URL"] ?? config["Hopscope:Kafka:Url"];
        if (string.IsNullOrWhiteSpace(url))
            return services;

        var group  = config["HOPSCOPE_KAFKA_GROUP"]  ?? config["Hopscope:Kafka:Group"];
        var topics = config["HOPSCOPE_KAFKA_TOPICS"] ?? config["Hopscope:Kafka:Topics"];

        var options = new Dictionary<string, string>(StringComparer.Ordinal);
        if (!string.IsNullOrWhiteSpace(group))  options["group"]  = group;
        if (!string.IsNullOrWhiteSpace(topics)) options["topics"] = topics;

        var source = new IngestionSource(
            BrokerType:       "Kafka",
            ConnectionString: url,
            Options:          options);

        // The provider registration is the canonical seam line; the ingestor factory
        // resolves the matching provider via CanHandle (deterministic, reflection-free)
        // so multiple brokers coexist without ordering assumptions.
        services.AddSingleton<IBrokerProvider, KafkaProvider>();
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

        // Unreachable in practice: KafkaProvider is always registered alongside this factory.
        return new KafkaProvider(sp.GetRequiredService<ILoggerFactory>()).CreateIngestor(source);
    }
}
