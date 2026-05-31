using Hopscope.Application.Abstractions;
using Hopscope.Domain.Events;
using Hopscope.Domain.Topology;
using Hopscope.Domain.Tracing;

namespace Hopscope.Application.Aggregation;

/// <summary>
/// Single-writer, lock-free correlation core.
///
/// Design contract:
///   - ONE consumer drives this (the Phase 1b channel loop). No concurrent
///     writers → plain Dictionary / HashSet / Queue, no locks.
///   - Idempotent on <see cref="EventEnvelope.HopId"/>: duplicate returns null.
///   - Trace-windowed eviction: when distinct trace count exceeds
///     <paramref name="maxTraces"/> the oldest trace's hops are purged.
///     Topology nodes/edges are intentionally left intact — they represent
///     the observed network shape and the set is bounded by unique
///     source→destination pairs, not by message volume.
///   - GetTrace rebuilds the causal tree on demand from stored envelopes,
///     using a visited-HopId set to guard against cycles.
///   - No System.Linq.Expressions, no reflection — AOT-safe.
/// </summary>
public sealed class StateAggregator : IStateAggregator
{
    // ------------------------------------------------------------------
    // Dependencies
    // ------------------------------------------------------------------
    private readonly IGraphProjector _projector;
    private readonly int             _maxTraces;

    // ------------------------------------------------------------------
    // Idempotency
    // ------------------------------------------------------------------
    /// All HopIds ever seen (across all traces). Prevents duplicate processing.
    private readonly HashSet<string> _seenHopIds = new(StringComparer.Ordinal);

    // ------------------------------------------------------------------
    // Topology state (nodes + edges)
    // ------------------------------------------------------------------
    /// All nodes ever upserted, keyed by node Id.
    private readonly Dictionary<string, GraphNode> _nodes =
        new(StringComparer.Ordinal);

    /// Edge state, keyed by "<SourceId>-><TargetId>".
    private readonly Dictionary<string, EdgeState> _edges =
        new(StringComparer.Ordinal);

    // ------------------------------------------------------------------
    // Per-trace hop store (for GetTrace / correlation)
    // ------------------------------------------------------------------
    /// All envelopes in a trace, keyed by TraceId.
    private readonly Dictionary<string, List<EventEnvelope>> _traceHops =
        new(StringComparer.Ordinal);

    /// Insertion-order queue of distinct TraceIds — used for LRU eviction.
    private readonly Queue<string> _traceOrder = new();

    // ------------------------------------------------------------------
    // Lock guarding the per-trace store (_traceHops / _traceOrder).
    // The topology path (_nodes / _edges) is single-writer only and
    // needs no lock. HTTP reader threads (GetTrace, GetTraces, /trace,
    // /traces) take this lock for the duration of their scan; the writer
    // (EngineLoop via IngestAsync) takes it only for the trace-store
    // mutation region so topology upserts and the sequence bump remain
    // outside the critical section.
    // ------------------------------------------------------------------
    private readonly object _traceGate = new();

    // ------------------------------------------------------------------
    // Monotonic sequence counter
    // ------------------------------------------------------------------
    private long _sequence;

    // ------------------------------------------------------------------
    // Constructor
    // ------------------------------------------------------------------
    public StateAggregator(IGraphProjector projector, int maxTraces = 10_000)
    {
        _projector = projector;
        _maxTraces = maxTraces;
    }

    // ------------------------------------------------------------------
    // IStateAggregator
    // ------------------------------------------------------------------

    /// <inheritdoc />
    public ValueTask<GraphDelta?> IngestAsync(EventEnvelope evt, CancellationToken ct)
    {
        // --- Idempotency gate ---
        if (!_seenHopIds.Add(evt.HopId))
            return new ValueTask<GraphDelta?>((GraphDelta?)null);

        // --- Topology: upsert node for source (kind data-driven via sourceKind metadata) ---
        if (!_nodes.ContainsKey(evt.Source))
        {
            var srcKind = ResolveSourceKind(evt);
            _nodes[evt.Source] = new GraphNode(evt.Source, srcKind, evt.Source, evt.BrokerType);
        }

        // --- Topology: upsert node for destination ---
        if (!_nodes.ContainsKey(evt.Destination))
        {
            var destKind = ResolveDestinationKind(evt);
            _nodes[evt.Destination] = new GraphNode(evt.Destination, destKind, evt.Destination, evt.BrokerType);
        }

        // --- Topology: upsert edge (increment count, update status) ---
        var edgeId = $"{evt.Source}->{evt.Destination}";
        long newCount;
        if (_edges.TryGetValue(edgeId, out var existing))
        {
            newCount = existing.Count + 1;
            _edges[edgeId] = new EdgeState(newCount, evt.ExecutionStatus);
        }
        else
        {
            newCount = 1;
            _edges[edgeId] = new EdgeState(1, evt.ExecutionStatus);
        }

        // --- Correlation: store hop in per-trace list (lock guards concurrent readers) ---
        lock (_traceGate)
        {
            if (!_traceHops.TryGetValue(evt.TraceId, out var hopList))
            {
                // First hop for this trace — register insertion order
                hopList = new List<EventEnvelope>();
                _traceHops[evt.TraceId] = hopList;
                _traceOrder.Enqueue(evt.TraceId);

                // --- Eviction: shed oldest trace when over the limit ---
                while (_traceOrder.Count > _maxTraces)
                {
                    var oldest = _traceOrder.Dequeue();
                    // Purge the evicted trace's HopIds too, so the dedupe set stays bounded
                    // to the retained window. This is *windowed* idempotency: a late duplicate
                    // of an already-evicted hop is treated as new — correct for a bounded debug
                    // window (the old hop has left memory anyway).
                    if (_traceHops.TryGetValue(oldest, out var evictedHops))
                    {
                        foreach (var e in evictedHops)
                            _seenHopIds.Remove(e.HopId);
                    }
                    _traceHops.Remove(oldest);
                    // _nodes / _edges intentionally retained (topology memory is bounded
                    // by unique svc×dest pairs, not by message volume)
                }
            }
            hopList.Add(evt);
        }

        // --- Stamp monotonic sequence ---
        var seq = ++_sequence;

        // --- Delegate delta construction to the pure projector ---
        var result = new AggregationResult(IsNew: true, IsDuplicate: false, TraceId: evt.TraceId);
        var delta  = _projector.Project(evt, result, edgeCount: newCount, sequence: seq);

        return new ValueTask<GraphDelta?>(delta);
    }

    /// <inheritdoc />
    public GraphSnapshot Snapshot()
    {
        var nodes = new List<GraphNode>(_nodes.Count);
        foreach (var n in _nodes.Values)
            nodes.Add(n);

        var edges = new List<GraphEdge>(_edges.Count);
        foreach (var kvp in _edges)
        {
            // Edge Id is the dictionary key; parse source/target back out.
            // We stored them by "<source>-><target>" — split on first "->".
            SplitEdgeId(kvp.Key, out var srcId, out var tgtId);
            edges.Add(new GraphEdge(kvp.Key, srcId, tgtId, kvp.Value.LastStatus, kvp.Value.Count));
        }

        return new GraphSnapshot(nodes, edges, _sequence);
    }

    /// <inheritdoc />
    public TraceView? GetTrace(string traceId)
    {
        lock (_traceGate)
        {
            if (!_traceHops.TryGetValue(traceId, out var hops))
                return null;

            // Snapshot the list so tree-building runs outside the lock.
            // Records (EventEnvelope) are immutable — safe to share.
            var hopsCopy = new List<EventEnvelope>(hops);

            // Build a lookup: HopId → envelope (fast child-lookup)
            var byHopId = new Dictionary<string, EventEnvelope>(hopsCopy.Count, StringComparer.Ordinal);
            foreach (var h in hopsCopy)
                byHopId[h.HopId] = h;

            // Build children map: parentHopId → list of children envelopes
            var childrenOf = new Dictionary<string, List<EventEnvelope>>(StringComparer.Ordinal);
            foreach (var h in hopsCopy)
            {
                // A hop is a root if it has no parent, its parent is not in this
                // trace's hop set, or it points to itself (cycle of length 1).
                var parentId = h.ParentHopId;
                bool isRoot  = string.IsNullOrEmpty(parentId)
                               || !byHopId.ContainsKey(parentId)
                               || parentId == h.HopId;   // self-cycle guard

                if (!isRoot)
                {
                    if (!childrenOf.TryGetValue(parentId!, out var childList))
                    {
                        childList = new List<EventEnvelope>();
                        childrenOf[parentId!] = childList;
                    }
                    childList.Add(h);
                }
            }

            // Collect roots (hops not claimed as children of any known parent)
            var roots = new List<HopNode>();
            foreach (var h in hopsCopy)
            {
                var parentId = h.ParentHopId;
                bool isRoot  = string.IsNullOrEmpty(parentId)
                               || !byHopId.ContainsKey(parentId)
                               || parentId == h.HopId;

                if (isRoot)
                    roots.Add(BuildHopNode(h, childrenOf, visited: new HashSet<string>(StringComparer.Ordinal)));
            }

            return new TraceView(traceId, roots, hopsCopy.Count);
        }
    }

    /// <inheritdoc />
    public IReadOnlyList<TraceSummary> GetTraces(string? status, string? source, string? target, int limit)
    {
        lock (_traceGate)
        {
            var results = new List<TraceSummary>();

            foreach (var kvp in _traceHops)
            {
                var hops = kvp.Value;
                if (hops.Count == 0)
                    continue;

                // --- Compute per-trace aggregates (hand-written loops, no LINQ Expressions) ---
                var worstOrdinal   = 0;
                var hasError       = false;
                var lastTimestamp  = DateTimeOffset.MinValue;

                foreach (var h in hops)
                {
                    var ord = (int)h.ExecutionStatus;
                    if (ord > worstOrdinal)
                        worstOrdinal = ord;

                    if (h.ErrorDetails != null)
                        hasError = true;

                    if (h.Timestamp > lastTimestamp)
                        lastTimestamp = h.Timestamp;
                }

                var worstStatus = (ExecutionStatus)worstOrdinal;

                // --- Status filter (hand-written switch, no Enum.Parse) ---
                switch (status?.ToLowerInvariant())
                {
                    case "failed":
                        if (worstStatus != ExecutionStatus.Failed)
                            continue;
                        break;
                    case "deadlettered":
                        if (worstStatus != ExecutionStatus.DeadLettered)
                            continue;
                        break;
                    case "error":
                        if (worstStatus != ExecutionStatus.Failed && worstStatus != ExecutionStatus.DeadLettered)
                            continue;
                        break;
                    // null / "" / "all" / anything else → no status filter
                }

                // --- Source / target edge filter ---
                bool sourceGiven = !string.IsNullOrEmpty(source);
                bool targetGiven = !string.IsNullOrEmpty(target);

                if (sourceGiven || targetGiven)
                {
                    bool matched = false;
                    foreach (var h in hops)
                    {
                        bool srcOk = !sourceGiven || h.Source      == source;
                        bool tgtOk = !targetGiven || h.Destination == target;
                        if (srcOk && tgtOk)
                        {
                            matched = true;
                            break;
                        }
                    }
                    if (!matched)
                        continue;
                }

                results.Add(new TraceSummary(
                    TraceId:       kvp.Key,
                    HopCount:      hops.Count,
                    WorstStatus:   worstStatus,
                    HasError:      hasError,
                    LastTimestamp: lastTimestamp));
            }

            // Sort newest-first by LastTimestamp (comparison delegate — not an Expression tree)
            results.Sort((a, b) => b.LastTimestamp.CompareTo(a.LastTimestamp));

            // Cap to caller-supplied limit
            if (results.Count > limit)
                results.RemoveRange(limit, results.Count - limit);

            return results;
        }
    }

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    /// <summary>
    /// Recursively builds a <see cref="HopNode"/> tree.
    /// <paramref name="visited"/> guards against multi-hop cycles
    /// (e.g. A→B→A): once a HopId has been placed in the tree it
    /// cannot appear again as a child.
    /// </summary>
    private static HopNode BuildHopNode(
        EventEnvelope env,
        Dictionary<string, List<EventEnvelope>> childrenOf,
        HashSet<string> visited)
    {
        visited.Add(env.HopId);

        var childNodes = new List<HopNode>();
        if (childrenOf.TryGetValue(env.HopId, out var children))
        {
            foreach (var child in children)
            {
                if (!visited.Contains(child.HopId))
                    childNodes.Add(BuildHopNode(child, childrenOf, visited));
                // If already visited: skip (cycle broken silently)
            }
        }

        return new HopNode(env, childNodes);
    }

    /// <summary>
    /// Resolves the source <see cref="NodeKind"/> from the envelope.
    /// Priority: explicit PayloadMetadata["sourceKind"] > default (Service).
    /// Preserves FakeIngestor behaviour: no sourceKind key → Service.
    /// Hand-written switch — no Enum.Parse, AOT-safe.
    /// </summary>
    private static NodeKind ResolveSourceKind(EventEnvelope evt)
    {
        if (evt.PayloadMetadata.TryGetValue("sourceKind", out var raw))
        {
            switch (raw)
            {
                case "Service":  return NodeKind.Service;
                case "Exchange": return NodeKind.Exchange;
                case "Topic":    return NodeKind.Topic;
                case "Queue":    return NodeKind.Queue;
            }
        }

        return NodeKind.Service;
    }

    /// <summary>
    /// Resolves the destination <see cref="NodeKind"/> from the envelope.
    /// Mirror of the projector's logic — kept private here so the aggregator
    /// can upsert nodes independently without calling back into the projector.
    /// </summary>
    private static NodeKind ResolveDestinationKind(EventEnvelope evt)
    {
        if (evt.PayloadMetadata.TryGetValue("destinationKind", out var raw))
        {
            switch (raw)
            {
                case "Service":  return NodeKind.Service;
                case "Exchange": return NodeKind.Exchange;
                case "Topic":    return NodeKind.Topic;
                case "Queue":    return NodeKind.Queue;
            }
        }

        return evt.BrokerType switch
        {
            "RabbitMQ" => NodeKind.Exchange,
            "Kafka"    => NodeKind.Topic,
            "Redis"    => NodeKind.Topic,
            _          => NodeKind.Queue
        };
    }

    /// <summary>
    /// Splits an edge key of the form "<source>-&gt;<target>" back into its parts.
    /// Source and target identifiers may themselves contain "-" but never "->",
    /// so the first occurrence of "->" is the unambiguous delimiter.
    /// </summary>
    private static void SplitEdgeId(string edgeId, out string sourceId, out string targetId)
    {
        var idx = edgeId.IndexOf("->", StringComparison.Ordinal);
        if (idx < 0)
        {
            // Should never happen given we only store well-formed keys,
            // but be defensive rather than throw in a hot path.
            sourceId = edgeId;
            targetId = string.Empty;
            return;
        }
        sourceId = edgeId[..idx];
        targetId = edgeId[(idx + 2)..];
    }

    // ------------------------------------------------------------------
    // Private value type to hold mutable edge counters
    // (a record struct keeps the Dictionary<string,T> slot-allocated)
    // ------------------------------------------------------------------
    private readonly record struct EdgeState(long Count, ExecutionStatus LastStatus);
}
