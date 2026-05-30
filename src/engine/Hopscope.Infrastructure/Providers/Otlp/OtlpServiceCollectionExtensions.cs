using Hopscope.Application.Abstractions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Hopscope.Infrastructure.Providers.Otlp;

/// <summary>
/// Host wiring for the OTLP provider — the Phase-4 seam in one line:
/// adding a broker = its <c>Providers/&lt;Broker&gt;/</c> folder + a single
/// <c>builder.Services.AddOtlpIngestion(config)</c> call in <c>Program.cs</c>.
///
/// Config is read MANUALLY (NO reflective <c>IConfiguration.Get&lt;T&gt;()</c>/<c>Bind()</c> —
/// not AOT-safe). Supported keys (env var or appsettings):
/// <list type="bullet">
///   <item><c>HOPSCOPE_OTLP_ENABLED</c> — "true" or "1" enables OTLP unconditionally.</item>
///   <item><c>HOPSCOPE_OTLP_PORT</c> — gRPC listen port (default 4317). Presence also enables OTLP.</item>
/// </list>
///
/// When disabled: registers nothing — the FakeIngestor fallback takes over when no brokers
/// are configured. Real data only — never fake + real.
///
/// NOTE: The gRPC service registration (<c>AddGrpc</c>), Kestrel HTTP/2 listener, and
/// <c>MapGrpcService&lt;OtlpTraceService&gt;</c> are wired in <c>Program.cs</c> (not here)
/// because they require the <c>WebApplicationBuilder</c> / <c>WebApplication</c>, which
/// this extension does not have access to. The <c>otlpPort</c> return value lets
/// <c>Program.cs</c> configure Kestrel with the same resolved port.
/// </summary>
public static class OtlpServiceCollectionExtensions
{
    /// <summary>
    /// Registers the OTLP bridge, provider, and ingestor when OTLP is enabled.
    /// Returns (enabled, port) so <c>Program.cs</c> can gate Kestrel + MapGrpcService.
    /// </summary>
    public static (bool Enabled, int Port) AddOtlpIngestion(
        this IServiceCollection services, IConfiguration config)
    {
        var enabledStr = config["HOPSCOPE_OTLP_ENABLED"];
        var portStr    = config["HOPSCOPE_OTLP_PORT"];

        // Enabled if HOPSCOPE_OTLP_ENABLED=true/1 OR HOPSCOPE_OTLP_PORT is set.
        var enabled =
            string.Equals(enabledStr, "true", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(enabledStr, "1",    StringComparison.Ordinal)            ||
            !string.IsNullOrWhiteSpace(portStr);

        if (!enabled)
            return (false, 4317);

        var port = 4317;
        if (!string.IsNullOrWhiteSpace(portStr) && int.TryParse(portStr, out var parsedPort) && parsedPort > 0)
            port = parsedPort;

        // Singleton bridge — shared between OtlpTraceService (writer) and OtlpIngestor (reader).
        services.AddSingleton<OtlpChannelBridge>();

        // Provider registered so CanHandle resolution works consistently with Redis/Kafka.
        services.AddSingleton<IBrokerProvider, OtlpProvider>();

        // Ingestor: drains the bridge into the engine's aggregation channel.
        services.AddSingleton<IEventIngestor>(sp =>
        {
            var bridge = sp.GetRequiredService<OtlpChannelBridge>();
            return new OtlpIngestor(bridge);
        });

        return (true, port);
    }
}
