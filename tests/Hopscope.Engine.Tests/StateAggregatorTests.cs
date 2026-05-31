using Hopscope.Application.Aggregation;
using Hopscope.Application.Projection;
using Hopscope.Domain.Events;
using Hopscope.Domain.Topology;

namespace Hopscope.Engine.Tests;

/// <summary>
/// StateAggregator tests. Each test builds its own aggregator so state never
/// leaks between cases.
/// </summary>
public sealed class StateAggregatorTests
{
    // -----------------------------------------------------------------------
    // Factory helpers
    // -----------------------------------------------------------------------
    private static StateAggregator MakeAggregator(int maxTraces = 10_000) =>
        new(new GraphProjector(), maxTraces);

    private static EventEnvelope MakeEnvelope(
        string hopId,
        string traceId        = "trace-1",
        string? parentHopId   = null,
        string source         = "svc-a",
        string destination    = "q-orders",
        string brokerType     = "RabbitMQ",
        ExecutionStatus status = ExecutionStatus.Success,
        Dictionary<string, string>? metadata = null,
        ErrorDetails? errorDetails = null,
        DateTimeOffset? timestamp  = null) =>
        new()
        {
            HopId           = hopId,
            TraceId         = traceId,
            ParentHopId     = parentHopId,
            Source          = source,
            Destination     = destination,
            BrokerType      = brokerType,
            Timestamp       = timestamp ?? DateTimeOffset.UtcNow,
            ExecutionStatus = status,
            ErrorDetails    = errorDetails,
            PayloadMetadata = metadata ?? new Dictionary<string, string>()
        };

    // -----------------------------------------------------------------------
    // 1. Duplicate HopId → second IngestAsync returns null; snapshot unchanged
    // -----------------------------------------------------------------------
    [Fact]
    public async Task DuplicateHopId_ReturnsNullAndSnapshotUnchanged()
    {
        var agg = MakeAggregator();
        var evt = MakeEnvelope("hop-1");

        var first  = await agg.IngestAsync(evt, CancellationToken.None);
        var before = agg.Snapshot();

        var second = await agg.IngestAsync(evt, CancellationToken.None);
        var after  = agg.Snapshot();

        Assert.NotNull(first);
        Assert.Null(second);
        Assert.Equal(before.Nodes.Count, after.Nodes.Count);
        Assert.Equal(before.Edges.Count, after.Edges.Count);
        Assert.Equal(before.Sequence,    after.Sequence);
    }

    // -----------------------------------------------------------------------
    // 2. New hop → delta upserts two nodes + one edge; Sequence increments
    // -----------------------------------------------------------------------
    [Fact]
    public async Task NewHop_DeltaContainsTwoNodesOneEdgeAndIncrementsSequence()
    {
        var agg = MakeAggregator();
        var evt = MakeEnvelope("hop-1", source: "svc-a", destination: "q-orders");

        var delta = await agg.IngestAsync(evt, CancellationToken.None);

        Assert.NotNull(delta);
        Assert.Equal(2, delta!.UpsertNodes.Count);
        Assert.Single(delta.UpsertEdges);
        Assert.Equal(1L, delta.Sequence);

        // Second new hop bumps sequence
        var evt2   = MakeEnvelope("hop-2", source: "svc-a", destination: "q-orders");
        var delta2 = await agg.IngestAsync(evt2, CancellationToken.None);
        Assert.NotNull(delta2);
        Assert.Equal(2L, delta2!.Sequence);
    }

    // -----------------------------------------------------------------------
    // 3. Edge aggregation: Count grows; LastStatus reflects most recent hop
    // -----------------------------------------------------------------------
    [Fact]
    public async Task EdgeAggregation_CountGrowsAndLastStatusUpdates()
    {
        var agg = MakeAggregator();

        await agg.IngestAsync(MakeEnvelope("h1", status: ExecutionStatus.Success),    CancellationToken.None);
        await agg.IngestAsync(MakeEnvelope("h2", status: ExecutionStatus.Retrying),   CancellationToken.None);
        var delta = await agg.IngestAsync(MakeEnvelope("h3", status: ExecutionStatus.Failed), CancellationToken.None);

        Assert.NotNull(delta);
        var edge = delta!.UpsertEdges.Single();
        Assert.Equal(3L,                    edge.Count);
        Assert.Equal(ExecutionStatus.Failed, edge.LastStatus);

        // Snapshot must also reflect the final state
        var snap = agg.Snapshot();
        var snapEdge = snap.Edges.Single(e => e.Id == "svc-a->q-orders");
        Assert.Equal(3L,                    snapEdge.Count);
        Assert.Equal(ExecutionStatus.Failed, snapEdge.LastStatus);
    }

    // -----------------------------------------------------------------------
    // 4a. Correlation: parent arrives before child → correct causal tree
    // -----------------------------------------------------------------------
    [Fact]
    public async Task Correlation_ParentBeforeChild_CorrectTree()
    {
        var agg = MakeAggregator();

        await agg.IngestAsync(MakeEnvelope("parent", traceId: "t1", parentHopId: null),   CancellationToken.None);
        await agg.IngestAsync(MakeEnvelope("child",  traceId: "t1", parentHopId: "parent"), CancellationToken.None);

        var view = agg.GetTrace("t1");
        Assert.NotNull(view);
        Assert.Equal("t1", view!.TraceId);
        Assert.Equal(2,    view.HopCount);
        Assert.Single(view.Roots);
        Assert.Equal("parent", view.Roots[0].Envelope.HopId);
        Assert.Single(view.Roots[0].Children);
        Assert.Equal("child", view.Roots[0].Children[0].Envelope.HopId);
    }

    // -----------------------------------------------------------------------
    // 4b. Correlation: child arrives before parent → still correct tree
    // -----------------------------------------------------------------------
    [Fact]
    public async Task Correlation_ChildBeforeParent_CorrectTree()
    {
        var agg = MakeAggregator();

        // Child arrives first
        await agg.IngestAsync(MakeEnvelope("child",  traceId: "t1", parentHopId: "parent"), CancellationToken.None);
        // Parent arrives second
        await agg.IngestAsync(MakeEnvelope("parent", traceId: "t1", parentHopId: null),   CancellationToken.None);

        var view = agg.GetTrace("t1");
        Assert.NotNull(view);
        Assert.Equal(2, view!.HopCount);
        Assert.Single(view.Roots);
        Assert.Equal("parent", view.Roots[0].Envelope.HopId);
        Assert.Single(view.Roots[0].Children);
        Assert.Equal("child", view.Roots[0].Children[0].Envelope.HopId);
    }

    // -----------------------------------------------------------------------
    // 5. Orphan hop (parent never arrives) surfaces as root
    // -----------------------------------------------------------------------
    [Fact]
    public async Task Orphan_HopWithUnknownParent_SurfacesAsRoot()
    {
        var agg = MakeAggregator();

        await agg.IngestAsync(
            MakeEnvelope("orphan", traceId: "t1", parentHopId: "ghost-parent"),
            CancellationToken.None);

        var view = agg.GetTrace("t1");
        Assert.NotNull(view);
        Assert.Equal(1,        view!.HopCount);
        Assert.Single(view.Roots);
        Assert.Equal("orphan", view.Roots[0].Envelope.HopId);
        Assert.Empty(view.Roots[0].Children);
    }

    // -----------------------------------------------------------------------
    // 6. Eviction: oldest trace is dropped when maxTraces exceeded
    // -----------------------------------------------------------------------
    [Fact]
    public async Task Eviction_OldestTraceEvictedWhenMaxTracesExceeded()
    {
        const int max = 3;
        var agg = MakeAggregator(maxTraces: max);

        // Fill exactly max traces
        for (int i = 0; i < max; i++)
        {
            await agg.IngestAsync(
                MakeEnvelope($"hop-{i}", traceId: $"trace-{i}"),
                CancellationToken.None);
        }

        // All three should be present
        for (int i = 0; i < max; i++)
            Assert.NotNull(agg.GetTrace($"trace-{i}"));

        // Adding one more trace should evict the oldest (trace-0)
        await agg.IngestAsync(
            MakeEnvelope("hop-new", traceId: "trace-new"),
            CancellationToken.None);

        Assert.Null(agg.GetTrace("trace-0"));          // evicted
        Assert.NotNull(agg.GetTrace("trace-1"));       // still present
        Assert.NotNull(agg.GetTrace("trace-2"));       // still present
        Assert.NotNull(agg.GetTrace("trace-new"));     // just added
    }

    // -----------------------------------------------------------------------
    // 7. destinationKind metadata honored (Service node for destination)
    // -----------------------------------------------------------------------
    [Fact]
    public async Task DestinationKindMetadata_Honored()
    {
        var agg = MakeAggregator();
        var evt = MakeEnvelope(
            "hop-1",
            destination: "downstream-svc",
            metadata: new Dictionary<string, string> { ["destinationKind"] = "Service" });

        var delta = await agg.IngestAsync(evt, CancellationToken.None);

        Assert.NotNull(delta);
        var dest = delta!.UpsertNodes.Single(n => n.Id == "downstream-svc");
        Assert.Equal(NodeKind.Service, dest.Kind);
    }

    // -----------------------------------------------------------------------
    // 8. Broker inference fallback: Kafka → Topic
    // -----------------------------------------------------------------------
    [Fact]
    public async Task BrokerInference_Kafka_DestinationIsTopic()
    {
        var agg = MakeAggregator();
        var evt = MakeEnvelope("hop-1", brokerType: "Kafka", destination: "events-topic");

        var delta = await agg.IngestAsync(evt, CancellationToken.None);

        Assert.NotNull(delta);
        var dest = delta!.UpsertNodes.Single(n => n.Id == "events-topic");
        Assert.Equal(NodeKind.Topic, dest.Kind);
    }

    // -----------------------------------------------------------------------
    // 9. Snapshot reflects all accumulated nodes and edges
    // -----------------------------------------------------------------------
    [Fact]
    public async Task Snapshot_ReflectsAllNodesAndEdges()
    {
        var agg = MakeAggregator();

        await agg.IngestAsync(MakeEnvelope("h1", source: "a", destination: "b"), CancellationToken.None);
        await agg.IngestAsync(MakeEnvelope("h2", source: "b", destination: "c"), CancellationToken.None);

        var snap = agg.Snapshot();

        // Nodes: a, b, c  (b is both source and destination)
        Assert.Equal(3, snap.Nodes.Count);
        // Edges: a→b and b→c
        Assert.Equal(2, snap.Edges.Count);
        Assert.Equal(2L, snap.Sequence);
    }

    // -----------------------------------------------------------------------
    // 10. GetTrace returns null for unknown/never-seen trace
    // -----------------------------------------------------------------------
    [Fact]
    public void GetTrace_UnknownTraceId_ReturnsNull()
    {
        var agg = MakeAggregator();
        Assert.Null(agg.GetTrace("no-such-trace"));
    }

    // -----------------------------------------------------------------------
    // 11. Cycle guard: a hop that points to itself does not hang GetTrace
    // -----------------------------------------------------------------------
    [Fact]
    public async Task CycleGuard_SelfReferentialHop_DoesNotHang()
    {
        var agg = MakeAggregator();

        // hop-1 claims its own HopId as its parent — a cycle of length 1
        await agg.IngestAsync(
            MakeEnvelope("hop-1", traceId: "t-cycle", parentHopId: "hop-1"),
            CancellationToken.None);

        // Must return (not infinite-loop) and treat the hop as a root
        var view = agg.GetTrace("t-cycle");
        Assert.NotNull(view);
        Assert.Equal(1, view!.HopCount);
        Assert.Single(view.Roots);
    }

    // -----------------------------------------------------------------------
    // 12. Multi-hop eviction: two hops in same trace evicted together
    // -----------------------------------------------------------------------
    [Fact]
    public async Task Eviction_MultiHopTrace_EvictedAsUnit()
    {
        const int max = 2;
        var agg = MakeAggregator(maxTraces: max);

        // Trace "t0" gets two hops
        await agg.IngestAsync(MakeEnvelope("h0a", traceId: "t0"), CancellationToken.None);
        await agg.IngestAsync(MakeEnvelope("h0b", traceId: "t0", parentHopId: "h0a"), CancellationToken.None);
        // Trace "t1" gets one hop
        await agg.IngestAsync(MakeEnvelope("h1",  traceId: "t1"), CancellationToken.None);

        // Now add a third distinct trace → t0 evicted
        await agg.IngestAsync(MakeEnvelope("h2", traceId: "t2"), CancellationToken.None);

        Assert.Null(agg.GetTrace("t0"));       // evicted
        Assert.NotNull(agg.GetTrace("t1"));
        Assert.NotNull(agg.GetTrace("t2"));
    }

    // -----------------------------------------------------------------------
    // 14. sourceKind metadata honored: Exchange source node has Kind=Exchange
    // -----------------------------------------------------------------------
    [Fact]
    public async Task SourceKindMetadata_Exchange_YieldsExchangeNode()
    {
        var agg = MakeAggregator();
        var evt = MakeEnvelope(
            "hop-1",
            source: "orders.exchange",
            metadata: new Dictionary<string, string> { ["sourceKind"] = "Exchange" });

        var delta = await agg.IngestAsync(evt, CancellationToken.None);

        Assert.NotNull(delta);
        var src = delta!.UpsertNodes.Single(n => n.Id == "orders.exchange");
        Assert.Equal(NodeKind.Exchange, src.Kind);
    }

    // -----------------------------------------------------------------------
    // 15. No sourceKind metadata → source node defaults to Service (FakeIngestor
    //     regression guard)
    // -----------------------------------------------------------------------
    [Fact]
    public async Task NoSourceKindMetadata_SourceDefaultsToService()
    {
        var agg = MakeAggregator();
        // Deliberately no sourceKind key — simulates FakeIngestor envelopes.
        var evt = MakeEnvelope("hop-1", source: "fake-svc");

        var delta = await agg.IngestAsync(evt, CancellationToken.None);

        Assert.NotNull(delta);
        var src = delta!.UpsertNodes.Single(n => n.Id == "fake-svc");
        Assert.Equal(NodeKind.Service, src.Kind);
    }

    // -----------------------------------------------------------------------
    // 13. Windowed idempotency: re-ingesting an evicted HopId is treated as NEW
    //     (dedupe set is purged together with the evicted trace)
    // -----------------------------------------------------------------------
    [Fact]
    public async Task Eviction_EvictedHopId_ReingestionTreatedAsNew()
    {
        const int max = 2;
        var agg = MakeAggregator(maxTraces: max);

        // Fill two traces; "h0" lives in t0.
        await agg.IngestAsync(MakeEnvelope("h0", traceId: "t0"), CancellationToken.None);
        await agg.IngestAsync(MakeEnvelope("h1", traceId: "t1"), CancellationToken.None);

        // Adding a third trace forces eviction of t0, which purges "h0" from _seenHopIds.
        await agg.IngestAsync(MakeEnvelope("h2", traceId: "t2"), CancellationToken.None);

        Assert.Null(agg.GetTrace("t0")); // confirm t0 is gone

        // Re-ingest the same HopId "h0" (now in a new trace context).
        // Because its trace was evicted, the dedupe set no longer contains "h0",
        // so this must be processed as a brand-new hop (non-null delta).
        var delta = await agg.IngestAsync(
            MakeEnvelope("h0", traceId: "t0-revisit"),
            CancellationToken.None);

        Assert.NotNull(delta); // windowed idempotency: evicted HopId accepted as new
    }

    // -----------------------------------------------------------------------
    // 16. GetTraces: 3-hop chain ending in DeadLettered with ErrorDetails
    //     → single summary with WorstStatus=DeadLettered, HasError=true, HopCount=3
    // -----------------------------------------------------------------------
    [Fact]
    public async Task GetTraces_DeadLetteredChain_SummaryWorstStatusAndHasError()
    {
        var agg = MakeAggregator();
        var ct  = CancellationToken.None;

        // Hop 1: Success, no error
        await agg.IngestAsync(MakeEnvelope("dl-h1", traceId: "dl-trace",
            status: ExecutionStatus.Success), ct);

        // Hop 2: Retrying, no error
        await agg.IngestAsync(MakeEnvelope("dl-h2", traceId: "dl-trace",
            parentHopId: "dl-h1",
            status: ExecutionStatus.Retrying), ct);

        // Hop 3: DeadLettered with ErrorDetails
        var err = new ErrorDetails("PaymentGatewayException", "Gateway timeout", null);
        await agg.IngestAsync(MakeEnvelope("dl-h3", traceId: "dl-trace",
            parentHopId: "dl-h2",
            status: ExecutionStatus.DeadLettered,
            errorDetails: err), ct);

        var summaries = agg.GetTraces(null, null, null, 1000);

        var s = Assert.Single(summaries);
        Assert.Equal("dl-trace",               s.TraceId);
        Assert.Equal(3,                        s.HopCount);
        Assert.Equal(ExecutionStatus.DeadLettered, s.WorstStatus);
        Assert.True(s.HasError);
    }

    // -----------------------------------------------------------------------
    // 17. GetTraces: status filter isolates Failed / DeadLettered / "error" / null
    // -----------------------------------------------------------------------
    [Fact]
    public async Task GetTraces_StatusFilter_FiltersByWorst()
    {
        var agg = MakeAggregator();
        var ct  = CancellationToken.None;

        // All-Success trace
        await agg.IngestAsync(MakeEnvelope("sf-h1", traceId: "sf-ok",
            status: ExecutionStatus.Success), ct);

        // Failed trace
        await agg.IngestAsync(MakeEnvelope("sf-h2", traceId: "sf-failed",
            status: ExecutionStatus.Failed), ct);

        // DeadLettered trace
        await agg.IngestAsync(MakeEnvelope("sf-h3", traceId: "sf-dl",
            status: ExecutionStatus.DeadLettered), ct);

        // "failed" → only the Failed trace
        var failedOnly = agg.GetTraces("failed", null, null, 1000);
        Assert.Single(failedOnly);
        Assert.Equal("sf-failed", failedOnly[0].TraceId);

        // "deadlettered" → only the DeadLettered trace
        var dlOnly = agg.GetTraces("deadlettered", null, null, 1000);
        Assert.Single(dlOnly);
        Assert.Equal("sf-dl", dlOnly[0].TraceId);

        // "error" → both Failed and DeadLettered
        var errorBoth = agg.GetTraces("error", null, null, 1000);
        Assert.Equal(2, errorBoth.Count);
        Assert.Contains(errorBoth, s => s.TraceId == "sf-failed");
        Assert.Contains(errorBoth, s => s.TraceId == "sf-dl");

        // null → all three
        var all = agg.GetTraces(null, null, null, 1000);
        Assert.Equal(3, all.Count);
    }

    // -----------------------------------------------------------------------
    // 18. GetTraces: source+target filter restricts to traces with a hop on that edge
    // -----------------------------------------------------------------------
    [Fact]
    public async Task GetTraces_SourceTargetFilter_RestrictsToEdge()
    {
        var agg = MakeAggregator();
        var ct  = CancellationToken.None;

        // Trace with svc-x → q-target hop
        await agg.IngestAsync(MakeEnvelope("stf-h1", traceId: "stf-match",
            source: "svc-x", destination: "q-target"), ct);

        // Trace with svc-x → q-other (same source, different dest)
        await agg.IngestAsync(MakeEnvelope("stf-h2", traceId: "stf-no-match",
            source: "svc-x", destination: "q-other"), ct);

        // Filter: source=svc-x AND target=q-target → only stf-match
        var results = agg.GetTraces(null, "svc-x", "q-target", 1000);
        Assert.Single(results);
        Assert.Equal("stf-match", results[0].TraceId);

        // Filter: source=svc-x only → both
        var srcOnly = agg.GetTraces(null, "svc-x", null, 1000);
        Assert.Equal(2, srcOnly.Count);

        // Filter: target=q-target only → only stf-match
        var tgtOnly = agg.GetTraces(null, null, "q-target", 1000);
        Assert.Single(tgtOnly);
        Assert.Equal("stf-match", tgtOnly[0].TraceId);
    }

    // -----------------------------------------------------------------------
    // 19. GetTraces: limit caps count and results are newest-first by LastTimestamp
    // -----------------------------------------------------------------------
    [Fact]
    public async Task GetTraces_Limit_CapsAndNewestFirst()
    {
        var agg  = MakeAggregator();
        var ct   = CancellationToken.None;
        var base_ = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Ingest 5 traces with strictly increasing timestamps
        for (int i = 0; i < 5; i++)
        {
            await agg.IngestAsync(MakeEnvelope($"lim-h{i}", traceId: $"lim-t{i}",
                timestamp: base_.AddMinutes(i)), ct);
        }

        // limit=3 → 3 results, newest first (lim-t4, lim-t3, lim-t2)
        var results = agg.GetTraces(null, null, null, 3);
        Assert.Equal(3, results.Count);
        Assert.Equal("lim-t4", results[0].TraceId);
        Assert.Equal("lim-t3", results[1].TraceId);
        Assert.Equal("lim-t2", results[2].TraceId);

        // Results are strictly descending by LastTimestamp
        for (int i = 0; i < results.Count - 1; i++)
            Assert.True(results[i].LastTimestamp > results[i + 1].LastTimestamp);
    }

    // -----------------------------------------------------------------------
    // 20. GetTrace returns null for an ID that was never ingested
    // -----------------------------------------------------------------------
    [Fact]
    public void GetTrace_UnknownId_ReturnsNull()
    {
        var agg = MakeAggregator();
        Assert.Null(agg.GetTrace("never-seen-id"));
    }

    // -----------------------------------------------------------------------
    // 21. GetTraces concurrent with ingest (including eviction) does not throw
    // -----------------------------------------------------------------------
    [Fact]
    public async Task GetTraces_ConcurrentIngest_DoesNotThrow()
    {
        // Small maxTraces to force frequent eviction under the lock
        var agg = MakeAggregator(maxTraces: 5);
        var ct  = CancellationToken.None;

        using var cts = new CancellationTokenSource();

        // Writer task: ingest many envelopes across many traces as fast as possible
        var writer = Task.Run(async () =>
        {
            for (int i = 0; i < 500 && !cts.Token.IsCancellationRequested; i++)
            {
                await agg.IngestAsync(
                    MakeEnvelope($"conc-h{i}", traceId: $"conc-t{i % 20}"),
                    CancellationToken.None);
            }
        });

        // Reader task: hammer GetTraces and GetTrace while the writer runs
        var reader = Task.Run(() =>
        {
            var ex = (Exception?)null;
            for (int i = 0; i < 500 && ex is null; i++)
            {
                try
                {
                    agg.GetTraces(null, null, null, 100);
                    agg.GetTrace($"conc-t{i % 20}");
                }
                catch (Exception e)
                {
                    ex = e;
                }
            }
            return ex;
        });

        await writer;
        cts.Cancel();
        var readerException = await reader;

        Assert.Null(readerException);
    }
}
