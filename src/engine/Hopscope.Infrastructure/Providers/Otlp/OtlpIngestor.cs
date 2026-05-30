using System.Runtime.CompilerServices;
using Hopscope.Application.Abstractions;
using Hopscope.Domain.Events;

namespace Hopscope.Infrastructure.Providers.Otlp;

/// <summary>
/// <see cref="IEventIngestor"/> for the OTLP provider.
///
/// The ingestor does NOT open sockets or bind ports — Kestrel hosts the gRPC
/// <see cref="OtlpTraceService"/> and feeds the shared <see cref="OtlpChannelBridge"/>.
/// This class only drains that bridge, yielding <see cref="EventEnvelope"/>s to the
/// engine's aggregation <c>Channel</c>.
///
/// AOT safety:
///   - <c>IAsyncEnumerable</c> + <c>[EnumeratorCancellation]</c> — fully AOT-compatible.
///   - No try/catch wrapping a yield (C# language restriction; not needed here since
///     ReadAllAsync only throws on cancellation, which exits the iterator naturally).
///   - No reflection, no configuration binding.
/// </summary>
public sealed class OtlpIngestor : IEventIngestor
{
    private readonly OtlpChannelBridge _bridge;

    public OtlpIngestor(OtlpChannelBridge bridge)
    {
        _bridge = bridge;
    }

    /// <inheritdoc/>
    public string Name => "OTLP";

    /// <inheritdoc/>
    /// <remarks>
    /// Drains <see cref="OtlpChannelBridge.ReadAllAsync"/> until <paramref name="ct"/>
    /// is cancelled. The bridge's <c>ReadAllAsync</c> exits cleanly on cancellation
    /// without throwing, so no try/catch is needed around the yield.
    /// </remarks>
    public async IAsyncEnumerable<EventEnvelope> StreamAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var envelope in _bridge.ReadAllAsync(ct).ConfigureAwait(false))
        {
            yield return envelope;
        }
    }
}
