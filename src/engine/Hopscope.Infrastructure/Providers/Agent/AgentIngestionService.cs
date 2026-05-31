using Grpc.Core;
using Hopscope.Domain.Events;
using Microsoft.Extensions.Logging;
using ProtoEnvelope = Hopscope.Contracts.V1.EventEnvelope;
using Ingestion = Hopscope.Contracts.V1.Ingestion;
using IngestAck = Hopscope.Contracts.V1.IngestAck;

namespace Hopscope.Infrastructure.Providers.Agent;

/// <summary>
/// gRPC service for the Hopscope <c>Ingestion/Stream</c> RPC — the engine half of the no-code
/// Phase-5 agent seam. The remote Go eBPF agent opens a CLIENT-STREAM and pushes normalized
/// envelopes; Kestrel hosts this service on the dedicated HTTP/2 agent port (default 4318).
///
/// For each envelope on the stream: map (pure <see cref="AgentMapper"/>), drop it if the mapper
/// rejects it (invalid identity) or throws (defensive), else write it to the bridge
/// (non-blocking, DropOldest). When the client half-closes, return <c>IngestAck{Accepted=N}</c>
/// where N counts only envelopes actually enqueued (so the agent can detect partial loss).
/// <c>Deduped</c> stays 0 — deduplication is the aggregator's job (by HopId), not the receiver's.
///
/// A single malformed envelope must NEVER fault the whole stream — map defensively per item and
/// log at Warning. AOT safety: derives from the Grpc.Tools-generated <c>Ingestion.IngestionBase</c>
/// (source-gen) and uses ONLY the generated parse/serialize paths — no Descriptor/reflection/
/// JsonParser. The service type needs no DI registration; <c>MapGrpcService&lt;T&gt;</c> activates
/// it via ActivatorUtilities (ctor args resolved from DI).
/// </summary>
public sealed class AgentIngestionService : Ingestion.IngestionBase
{
    private readonly AgentChannelBridge _bridge;
    private readonly ILogger<AgentIngestionService> _log;

    // Tracks the bridge drop count we last logged, so we surface only NEW back-pressure loss.
    private long _lastDroppedSeen;

    public AgentIngestionService(AgentChannelBridge bridge, ILogger<AgentIngestionService> log)
    {
        _bridge = bridge;
        _log    = log;
    }

    /// <inheritdoc/>
    public override async Task<IngestAck> Stream(
        IAsyncStreamReader<ProtoEnvelope> requestStream, ServerCallContext context)
    {
        var ct = context.CancellationToken;
        ulong accepted = 0;

        try
        {
            await foreach (var proto in requestStream.ReadAllAsync(ct).ConfigureAwait(false))
            {
                EventEnvelope? envelope;
                try
                {
                    envelope = AgentMapper.Map(proto);
                }
                catch (Exception ex)
                {
                    // HopId is safe routing metadata, not a body.
                    _log.LogWarning(ex,
                        "Agent: failed to map envelope HopId='{HopId}'; skipping.", proto.HopId);
                    continue;
                }

                if (envelope is null)
                {
                    _log.LogWarning(
                        "Agent: dropping envelope with invalid identity (TraceId='{TraceId}', HopId='{HopId}').",
                        proto.TraceId, proto.HopId);
                    continue;
                }

                await _bridge.WriteAsync(envelope, ct).ConfigureAwait(false);
                accepted++;
            }
        }
        catch (OperationCanceledException)
        {
            // Client disconnected / shutdown — return what we accepted so far.
        }

        // Surface back-pressure loss: log once per delta when DropOldest evicts envelopes.
        var dropped = _bridge.DroppedCount;
        if (dropped > _lastDroppedSeen)
        {
            _log.LogWarning(
                "Agent: bridge dropped {Delta} envelope(s) under back-pressure ({Total} total) — "
                + "ingest is falling behind; some hops will be missing from the canvas.",
                dropped - _lastDroppedSeen, dropped);
            _lastDroppedSeen = dropped;
        }

        return new IngestAck { Accepted = accepted }; // Deduped stays 0 (aggregator dedupes by HopId)
    }
}
