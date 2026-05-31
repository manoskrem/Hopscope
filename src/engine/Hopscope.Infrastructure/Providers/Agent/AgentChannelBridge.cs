using System.Threading.Channels;
using Hopscope.Domain.Events;

namespace Hopscope.Infrastructure.Providers.Agent;

/// <summary>
/// Thread-safe bridge between the gRPC <see cref="AgentIngestionService"/> (multiple concurrent
/// writers — one per open client-stream from a remote agent) and the single-reader
/// <see cref="AgentIngestor"/> that drains envelopes into the engine's aggregation <c>Channel</c>.
///
/// Identical design to the OTLP bridge — the engine cannot tell an agent-sourced envelope from an
/// in-proc provider's:
///   - SingleReader=true  — only AgentIngestor drains, so the reader lock is elided.
///   - SingleWriter=false — concurrent Stream calls on Kestrel's gRPC worker threads write.
///   - BoundedChannelFullMode.DropOldest — under back-pressure the oldest envelope is silently
///     dropped rather than blocking the Stream RPC (keeps ingest responsive).
///   - Capacity 2048 — covers agent bursts; the trace-retention window downstream is the RAM knob.
///
/// AOT safety: <see cref="Channel{T}"/> is fully AOT-compatible — no reflection, no codegen.
/// </summary>
public sealed class AgentChannelBridge
{
    private readonly Channel<EventEnvelope> _channel;

    // Count of envelopes dropped under back-pressure (DropOldest). Surfaced via DroppedCount so the
    // gRPC service can log when loss occurs — a silently-dropped envelope could be THE error hop,
    // so the loss must be observable rather than invisible.
    private long _droppedCount;

    public AgentChannelBridge()
    {
        _channel = Channel.CreateBounded<EventEnvelope>(
            new BoundedChannelOptions(2048)
            {
                SingleReader                  = true,
                SingleWriter                  = false,
                FullMode                      = BoundedChannelFullMode.DropOldest,
                AllowSynchronousContinuations = false,
            },
            // itemDropped callback fires when DropOldest evicts an envelope — count it.
            _ => Interlocked.Increment(ref _droppedCount));
    }

    /// <summary>Total envelopes dropped under back-pressure since startup (monotonic).</summary>
    public long DroppedCount => Interlocked.Read(ref _droppedCount);

    /// <summary>
    /// Writes an envelope from the gRPC service into the bridge.
    /// Returns immediately (DropOldest) — never blocks the Stream RPC.
    /// </summary>
    public ValueTask WriteAsync(EventEnvelope envelope, CancellationToken ct)
        => _channel.Writer.WriteAsync(envelope, ct);

    /// <summary>
    /// Returns an async-enumerable that the ingestor drains indefinitely until
    /// <paramref name="ct"/> is cancelled (which signals the writer to complete).
    /// </summary>
    public IAsyncEnumerable<EventEnvelope> ReadAllAsync(CancellationToken ct)
        => _channel.Reader.ReadAllAsync(ct);

    /// <summary>
    /// Signals the channel is complete — called on engine shutdown so the ingestor
    /// exits its ReadAllAsync loop cleanly without relying solely on cancellation.
    /// </summary>
    internal void Complete() => _channel.Writer.TryComplete();
}
