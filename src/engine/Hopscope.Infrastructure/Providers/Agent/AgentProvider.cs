using Hopscope.Application.Abstractions;

namespace Hopscope.Infrastructure.Providers.Agent;

/// <summary>
/// <see cref="IBrokerProvider"/> for the remote-agent gRPC receiver.
/// Compile-time DI-registered in <c>Hopscope.Host/Program.cs</c> — no reflection,
/// no runtime scanning. AOT-safe. Registered for parity with the other providers so
/// CanHandle resolution works consistently.
///
/// The ingestor created here does NOT open a socket — it drains the shared
/// <see cref="AgentChannelBridge"/> that <see cref="AgentIngestionService"/> (hosted by
/// Kestrel on the agent gRPC port) writes into.
/// </summary>
public sealed class AgentProvider : IBrokerProvider
{
    private readonly AgentChannelBridge _bridge;

    public AgentProvider(AgentChannelBridge bridge)
    {
        _bridge = bridge;
    }

    /// <inheritdoc/>
    public string BrokerType => "Agent";

    /// <inheritdoc/>
    /// <remarks>Accepts "Agent"/"agent" — ordinal ignore-case.</remarks>
    public bool CanHandle(IngestionSource source) =>
        string.Equals(source.BrokerType, "Agent", StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc/>
    public IEventIngestor CreateIngestor(IngestionSource source)
        => new AgentIngestor(_bridge);
}
