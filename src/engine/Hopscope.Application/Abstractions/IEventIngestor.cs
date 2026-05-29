using Hopscope.Domain.Events;

namespace Hopscope.Application.Abstractions;

/// <summary>
/// A running source of normalized envelopes. In-process broker providers AND the remote
/// agent's gRPC receiver implement this identically — the engine can't tell them apart.
/// </summary>
public interface IEventIngestor
{
    string Name { get; }

    /// <summary>Streams normalized envelopes until <paramref name="ct"/> is cancelled.</summary>
    IAsyncEnumerable<EventEnvelope> StreamAsync(CancellationToken ct);
}
