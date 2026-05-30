using System.Threading.Channels;
using Hopscope.Domain.Events;

namespace Hopscope.Infrastructure.Providers.Otlp;

/// <summary>
/// Thread-safe bridge between the gRPC <see cref="OtlpTraceService"/> (multiple concurrent
/// writers — one per incoming Export call) and the single-reader <see cref="OtlpIngestor"/>
/// that pulls envelopes into the engine's aggregation <c>Channel</c>.
///
/// Design:
///   - SingleReader=true  — only OtlpIngestor drains, so the reader lock is elided.
///   - SingleWriter=false — concurrent Export calls from Kestrel's gRPC worker threads write.
///   - BoundedChannelFullMode.DropOldest — under back-pressure the oldest span is silently
///     dropped rather than blocking the Export RPC (keeps the gRPC response fast).
///   - Capacity 1024 — covers bursts from a local telemetrygen run; tune via OTLP options
///     if needed in a future config pass.
///
/// AOT safety: <see cref="Channel{T}"/> is fully AOT-compatible — no reflection, no codegen.
/// </summary>
public sealed class OtlpChannelBridge
{
    private readonly Channel<EventEnvelope> _channel;

    // Count of envelopes dropped under back-pressure (DropOldest). Surfaced via
    // DroppedCount so the gRPC service can log when loss occurs — a silently-dropped span
    // could be THE error span, so the loss must be observable rather than invisible.
    private long _droppedCount;

    public OtlpChannelBridge()
    {
        _channel = Channel.CreateBounded<EventEnvelope>(
            new BoundedChannelOptions(1024)
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
    /// Returns immediately (DropOldest) — never blocks the Export RPC.
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
