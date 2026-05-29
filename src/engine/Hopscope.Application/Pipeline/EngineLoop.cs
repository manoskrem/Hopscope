using System.Threading.Channels;
using Hopscope.Application.Abstractions;
using Hopscope.Domain.Events;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Hopscope.Application.Pipeline;

/// <summary>
/// The single-writer pipeline loop.
///
/// Architecture invariants:
///   - ONE bounded <see cref="Channel{T}"/> is the only path into
///     <see cref="IStateAggregator"/>. All <see cref="IEventIngestor"/> pump tasks
///     write to the channel; exactly one consumer task reads it. This gives the
///     aggregator its single-writer guarantee without locks.
///   - Back-pressure: <see cref="BoundedChannelFullMode.Wait"/> blocks producers
///     rather than dropping envelopes.
///   - Idempotency is enforced by the aggregator (HopId dedup); the loop itself
///     has no dedup logic.
///   - <see cref="IStateAggregator.Snapshot"/> is exposed for the /ws and /snapshot
///     endpoints via the injected <see cref="IStateAggregator"/> singleton — callers
///     read it directly; no extra plumbing needed here.
/// </summary>
public sealed class EngineLoop : BackgroundService
{
    private const int ChannelCapacity = 1_024;

    private readonly IEnumerable<IEventIngestor> _ingestors;
    private readonly IStateAggregator            _aggregator;
    private readonly IPushChannel                _pushChannel;
    private readonly ILogger<EngineLoop>         _logger;

    private readonly Channel<EventEnvelope> _channel =
        Channel.CreateBounded<EventEnvelope>(
            new BoundedChannelOptions(ChannelCapacity)
            {
                FullMode     = BoundedChannelFullMode.Wait,
                SingleReader = true,   // one consumer → aggregator single-writer guarantee
                SingleWriter = false,  // many ingestor pumps write concurrently
            });

    public EngineLoop(
        IEnumerable<IEventIngestor> ingestors,
        IStateAggregator aggregator,
        IPushChannel pushChannel,
        ILogger<EngineLoop> logger)
    {
        _ingestors   = ingestors;
        _aggregator  = aggregator;
        _pushChannel = pushChannel;
        _logger      = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Start one pump task per ingestor (producers).
        var pumps = new List<Task>();
        foreach (var ingestor in _ingestors)
        {
            var captured = ingestor; // capture for closure
            pumps.Add(Task.Run(() => PumpAsync(captured, stoppingToken), stoppingToken));
        }

        // Complete the channel writer ONLY when EVERY pump has finished — never when
        // just the first does. Otherwise a finite or faulting ingestor would close the
        // channel out from under the still-running ones, dropping their envelopes.
        var drainWriter = CompleteWriterWhenAllPumpsDone(pumps);

        // Single consumer — owns all aggregator state mutations. Exits when the writer
        // is completed (all pumps done) or on cancellation.
        await ConsumeAsync(stoppingToken).ConfigureAwait(false);
        await drainWriter.ConfigureAwait(false);
    }

    private async Task CompleteWriterWhenAllPumpsDone(List<Task> pumps)
    {
        try
        {
            await Task.WhenAll(pumps).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown.
        }
        finally
        {
            _channel.Writer.TryComplete();
        }
    }

    // -----------------------------------------------------------------------
    // Producer: one per IEventIngestor
    // -----------------------------------------------------------------------
    private async Task PumpAsync(IEventIngestor ingestor, CancellationToken ct)
    {
        try
        {
            await foreach (var envelope in ingestor.StreamAsync(ct).ConfigureAwait(false))
            {
                await _channel.Writer.WriteAsync(envelope, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown.
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Ingestor '{Name}' faulted.", ingestor.Name);
        }
        // NOTE: do NOT complete the writer here — that is done once in
        // CompleteWriterWhenAllPumpsDone, after every pump has finished.
    }

    // -----------------------------------------------------------------------
    // Consumer: single reader, owns the aggregator
    // -----------------------------------------------------------------------
    private async Task ConsumeAsync(CancellationToken ct)
    {
        try
        {
            await foreach (var envelope in _channel.Reader.ReadAllAsync(ct).ConfigureAwait(false))
            {
                try
                {
                    var delta = await _aggregator.IngestAsync(envelope, ct).ConfigureAwait(false);
                    if (delta is not null)
                        await _pushChannel.BroadcastAsync(delta, ct).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogError(ex,
                        "Failed to process envelope HopId='{HopId}'.", envelope.HopId);
                    // Continue: one bad envelope must not kill the pipeline.
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown.
        }
    }
}
