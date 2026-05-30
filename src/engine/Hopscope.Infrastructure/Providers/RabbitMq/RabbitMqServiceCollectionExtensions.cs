using Hopscope.Application.Abstractions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Hopscope.Infrastructure.Providers.RabbitMq;

/// <summary>
/// Host wiring for the RabbitMQ provider — the Phase-4 seam in one line:
/// adding a broker = its <c>Providers/&lt;Broker&gt;/</c> folder + a single
/// <c>builder.Services.AddRabbitMqIngestion(config)</c> call in <c>Program.cs</c>.
///
/// Config is read MANUALLY (NO reflective <c>IConfiguration.Get&lt;T&gt;()</c>/<c>Bind()</c> —
/// they are not AOT-safe). Supported keys (env var or appsettings):
/// <list type="bullet">
///   <item><c>HOPSCOPE_RABBITMQ_URL</c> / <c>Hopscope:RabbitMq:Url</c></item>
///   <item><c>HOPSCOPE_RABBITMQ_VHOST</c> / <c>Hopscope:RabbitMq:Vhost</c></item>
///   <item><c>HOPSCOPE_RABBITMQ_POLL_SECONDS</c> / <c>Hopscope:RabbitMq:PollSeconds</c></item>
/// </list>
///
/// When no URL is configured this registers nothing — so the FakeIngestor fallback
/// (<see cref="Fake.FakeIngestionServiceCollectionExtensions"/>) can take over. Real
/// data only — never both RabbitMQ and Fake.
/// </summary>
public static class RabbitMqServiceCollectionExtensions
{
    public static IServiceCollection AddRabbitMqIngestion(
        this IServiceCollection services, IConfiguration config)
    {
        var url = config["HOPSCOPE_RABBITMQ_URL"] ?? config["Hopscope:RabbitMq:Url"];
        if (string.IsNullOrWhiteSpace(url))
            return services;

        var vhost = config["HOPSCOPE_RABBITMQ_VHOST"] ?? config["Hopscope:RabbitMq:Vhost"];
        var poll  = config["HOPSCOPE_RABBITMQ_POLL_SECONDS"] ?? config["Hopscope:RabbitMq:PollSeconds"];

        var options = new Dictionary<string, string>(StringComparer.Ordinal);
        if (!string.IsNullOrWhiteSpace(vhost)) options["vhost"]       = vhost;
        if (!string.IsNullOrWhiteSpace(poll))  options["pollSeconds"] = poll;

        var source = new IngestionSource(
            BrokerType:       "RabbitMQ",
            ConnectionString: url,
            Options:          options);

        // The provider registration is the canonical seam line; the ingestor factory
        // resolves the matching provider via CanHandle (deterministic, reflection-free)
        // so multiple brokers coexist without ordering assumptions.
        services.AddSingleton<IBrokerProvider, RabbitMqProvider>();
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
        return new RabbitMqProvider(sp.GetRequiredService<ILoggerFactory>()).CreateIngestor(source);
    }
}
