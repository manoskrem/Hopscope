using Grpc.Core;
using Hopscope.Infrastructure.Providers.Agent;
using Microsoft.Extensions.Logging.Abstractions;
using ProtoEnvelope = Hopscope.Contracts.V1.EventEnvelope;
using ProtoStatus = Hopscope.Contracts.V1.ExecutionStatus;
using DomainEnvelope = Hopscope.Domain.Events.EventEnvelope;

namespace Hopscope.Engine.Tests;

/// <summary>
/// Tests for <see cref="AgentIngestionService"/> — the client-streaming gRPC service that reads
/// proto envelopes off the request stream, maps them, and writes valid ones into the bridge.
/// This is the genuinely-new shape in Phase 5 (client-streaming vs OTLP's unary Export), so the
/// skip-invalid + accepted-count logic gets a focused unit test. Uses a real (in-memory) bridge,
/// a fake request stream, and a minimal fake ServerCallContext.
/// </summary>
public sealed class AgentServiceTests
{
    [Fact]
    public async Task Stream_ValidEnvelopes_WritesToBridge_ReturnsAcceptedCount()
    {
        var bridge  = new AgentChannelBridge();
        var service = new AgentIngestionService(bridge, NullLogger<AgentIngestionService>.Instance);

        var ack = await service.Stream(
            Reader(Envelope("h1"), Envelope("h2")), FakeContext());

        Assert.Equal(2UL, ack.Accepted);

        var drained = await DrainAsync(bridge, 2);
        Assert.Equal(2, drained.Count);
        Assert.Contains(drained, e => e.HopId == "h1");
        Assert.Contains(drained, e => e.HopId == "h2");
    }

    [Fact]
    public async Task Stream_SkipsInvalidEnvelope_NotCounted()
    {
        var bridge  = new AgentChannelBridge();
        var service = new AgentIngestionService(bridge, NullLogger<AgentIngestionService>.Instance);

        var bad = Envelope("h1");
        bad.HopId = ""; // mapper rejects → skipped, not counted

        var ack = await service.Stream(Reader(bad, Envelope("h2")), FakeContext());

        Assert.Equal(1UL, ack.Accepted);

        var drained = await DrainAsync(bridge, 1);
        Assert.Single(drained);
        Assert.Equal("h2", drained[0].HopId);
    }

    [Fact]
    public async Task Stream_EmptyStream_ReturnsZero()
    {
        var bridge  = new AgentChannelBridge();
        var service = new AgentIngestionService(bridge, NullLogger<AgentIngestionService>.Instance);

        var ack = await service.Stream(Reader(), FakeContext());

        Assert.Equal(0UL, ack.Accepted);
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    private static ProtoEnvelope Envelope(string hopId) => new()
    {
        TraceId         = "trace-1",
        HopId           = hopId,
        Source          = "svc-a",
        Destination     = "queue-x",
        BrokerType      = "RabbitMQ",
        ExecutionStatus = ProtoStatus.Success,
    };

    private static FakeAsyncStreamReader<ProtoEnvelope> Reader(params ProtoEnvelope[] items)
        => new(items);

    private static ServerCallContext FakeContext(CancellationToken ct = default)
        => new FakeServerCallContext(ct);

    private static async Task<List<DomainEnvelope>> DrainAsync(AgentChannelBridge bridge, int max)
    {
        var result = new List<DomainEnvelope>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        try
        {
            await foreach (var e in bridge.ReadAllAsync(cts.Token))
            {
                result.Add(e);
                if (result.Count >= max) break;
            }
        }
        catch (OperationCanceledException) { }
        return result;
    }

    private sealed class FakeAsyncStreamReader<T> : IAsyncStreamReader<T>
    {
        private readonly IEnumerator<T> _items;
        public FakeAsyncStreamReader(IEnumerable<T> items) => _items = items.GetEnumerator();
        public T Current => _items.Current;
        public Task<bool> MoveNext(CancellationToken cancellationToken)
            => Task.FromResult(_items.MoveNext());
    }

    private sealed class FakeServerCallContext : ServerCallContext
    {
        private readonly CancellationToken _ct;
        public FakeServerCallContext(CancellationToken ct) => _ct = ct;

        protected override CancellationToken CancellationTokenCore => _ct;
        protected override string MethodCore => "test";
        protected override string HostCore => "test";
        protected override string PeerCore => "test";
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Metadata RequestHeadersCore => new();
        protected override Metadata ResponseTrailersCore => new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore =>
            new(string.Empty, new Dictionary<string, List<AuthProperty>>());

        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders)
            => Task.CompletedTask;
        protected override ContextPropagationToken CreatePropagationTokenCore(
            ContextPropagationOptions? options) => null!;
    }
}
