using Hopscope.Application.Abstractions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Hopscope.Infrastructure.Providers.Agent;

/// <summary>
/// Host wiring for the remote-agent gRPC receiver — the Phase-4 seam in one line:
/// adding it = its <c>Providers/Agent/</c> folder + a single
/// <c>builder.Services.AddAgentIngestion(config)</c> call in <c>Program.cs</c>.
///
/// This is the engine half of Phase 5: the Go eBPF agent opens the client-streaming
/// <c>hopscope.v1.Ingestion/Stream</c> RPC and the engine receives normalized envelopes through
/// the SAME <see cref="IEventIngestor"/> seam every in-proc provider uses.
///
/// Config is read MANUALLY (NO reflective <c>IConfiguration.Get&lt;T&gt;()</c>/<c>Bind()</c> —
/// not AOT-safe). Supported keys (env var or appsettings):
/// <list type="bullet">
///   <item><c>HOPSCOPE_AGENT_ENABLED</c> — "true" or "1" enables the receiver unconditionally.</item>
///   <item><c>HOPSCOPE_AGENT_PORT</c> — gRPC listen port (default 4318, distinct from OTLP's
///     4317). Presence also enables the receiver.</item>
/// </list>
///
/// When disabled: registers nothing — the FakeIngestor fallback takes over when no brokers are
/// configured. Real data only — never fake + real.
///
/// NOTE: <c>AddGrpc</c>, the Kestrel HTTP/2 listener, and
/// <c>MapGrpcService&lt;AgentIngestionService&gt;</c> are wired in <c>Program.cs</c> (not here)
/// because they require the <c>WebApplicationBuilder</c> / <c>WebApplication</c>. The returned
/// port lets <c>Program.cs</c> bind Kestrel with the same resolved port. The service type itself
/// needs no DI registration — <c>MapGrpcService&lt;T&gt;</c> activates it via ActivatorUtilities
/// (its ctor args — the bridge + logger — are resolved from DI).
/// </summary>
public static class AgentServiceCollectionExtensions
{
    /// <summary>Default agent gRPC port — distinct from OTLP's 4317 so both can run at once.</summary>
    public const int DefaultPort = 4318;

    /// <summary>
    /// Registers the agent bridge, provider, and ingestor when the receiver is enabled.
    /// Returns (enabled, port) so <c>Program.cs</c> can gate Kestrel + MapGrpcService.
    /// </summary>
    public static (bool Enabled, int Port) AddAgentIngestion(
        this IServiceCollection services, IConfiguration config)
    {
        var enabledStr = config["HOPSCOPE_AGENT_ENABLED"];
        var portStr    = config["HOPSCOPE_AGENT_PORT"];

        // Enabled if HOPSCOPE_AGENT_ENABLED=true/1 OR HOPSCOPE_AGENT_PORT is set.
        var enabled =
            string.Equals(enabledStr, "true", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(enabledStr, "1",    StringComparison.Ordinal)            ||
            !string.IsNullOrWhiteSpace(portStr);

        if (!enabled)
            return (false, DefaultPort);

        var port = DefaultPort;
        if (!string.IsNullOrWhiteSpace(portStr) && int.TryParse(portStr, out var parsedPort) && parsedPort > 0)
            port = parsedPort;

        // Singleton bridge — shared between AgentIngestionService (writer) and AgentIngestor (reader).
        services.AddSingleton<AgentChannelBridge>();

        // Provider registered so CanHandle resolution works consistently with the other providers.
        services.AddSingleton<IBrokerProvider, AgentProvider>();

        // Ingestor: drains the bridge into the engine's aggregation channel.
        services.AddSingleton<IEventIngestor>(sp =>
        {
            var bridge = sp.GetRequiredService<AgentChannelBridge>();
            return new AgentIngestor(bridge);
        });

        return (true, port);
    }
}
