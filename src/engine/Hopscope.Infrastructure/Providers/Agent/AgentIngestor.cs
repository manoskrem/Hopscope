using System.Runtime.CompilerServices;
using Hopscope.Application.Abstractions;
using Hopscope.Domain.Events;

namespace Hopscope.Infrastructure.Providers.Agent;

/// <summary>
/// <see cref="IEventIngestor"/> for the remote-agent gRPC receiver.
///
/// The ingestor does NOT open sockets or bind ports — Kestrel hosts the gRPC
/// <see cref="AgentIngestionService"/> and feeds the shared <see cref="AgentChannelBridge"/>.
/// This class only drains that bridge, yielding <see cref="EventEnvelope"/>s to the engine's
/// aggregation <c>Channel</c> — exactly like the OTLP ingestor, so the engine can't tell an
/// agent-sourced envelope from an in-proc provider's.
///
/// AOT safety:
///   - <c>IAsyncEnumerable</c> + <c>[EnumeratorCancellation]</c> — fully AOT-compatible.
///   - No try/catch wrapping a yield (C# CS1626; not needed since ReadAllAsync only throws on
///     cancellation, which exits the iterator naturally).
///   - No reflection, no configuration binding.
/// </summary>
public sealed class AgentIngestor : IEventIngestor
{
    private readonly AgentChannelBridge _bridge;

    public AgentIngestor(AgentChannelBridge bridge)
    {
        _bridge = bridge;
    }

    /// <inheritdoc/>
    public string Name => "Agent";

    /// <inheritdoc/>
    public async IAsyncEnumerable<EventEnvelope> StreamAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var envelope in _bridge.ReadAllAsync(ct).ConfigureAwait(false))
        {
            yield return envelope;
        }
    }
}
