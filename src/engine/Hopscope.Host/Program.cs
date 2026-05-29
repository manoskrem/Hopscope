using System.Net.WebSockets;
using Hopscope.Application.Abstractions;
using Hopscope.Application.Aggregation;
using Hopscope.Application.Pipeline;
using Hopscope.Application.Projection;
using Hopscope.Infrastructure.Providers.Fake;
using Hopscope.Infrastructure.Serialization;
using Hopscope.Push;

var builder = WebApplication.CreateSlimBuilder(args);

// ── Serialization ──────────────────────────────────────────────────────────
// Source-gen context covers all wire types: PushFrame, GraphSnapshot,
// GraphDelta, EventEnvelope (and their nested records).
builder.Services.ConfigureHttpJsonOptions(o =>
    o.SerializerOptions.TypeInfoResolverChain.Insert(0, AppJsonSerializerContext.Default));

// ── Engine singletons (compile-time DI — AOT rule §3) ─────────────────────
builder.Services.AddSingleton<IGraphProjector, GraphProjector>();
builder.Services.AddSingleton<IStateAggregator, StateAggregator>();
builder.Services.AddSingleton<IPushChannel, WebSocketPushChannel>();

// Fake ingestor — registered as both IEventIngestor and the concrete type.
// IEnumerable<IEventIngestor> in EngineLoop receives all registrations.
builder.Services.AddSingleton<IEventIngestor, FakeIngestor>();

// ── Background pipeline ───────────────────────────────────────────────────
builder.Services.AddHostedService<EngineLoop>();

// ── Logging ───────────────────────────────────────────────────────────────
builder.Logging.AddConsole();

var app = builder.Build();

// ── WebSocket middleware ───────────────────────────────────────────────────
app.UseWebSockets();

// ── Endpoints ─────────────────────────────────────────────────────────────

// Health probe
app.MapGet("/healthz", () => "OK");

// Full topology snapshot (for REST polling / debugging)
app.MapGet("/snapshot", (IStateAggregator aggregator) =>
    Results.Ok(aggregator.Snapshot()));

// Live push: WebSocket upgrade → snapshot-on-connect, then ordered GraphDelta stream
app.MapGet("/ws", async (HttpContext ctx,
                         IStateAggregator aggregator,
                         IPushChannel push,
                         CancellationToken ct) =>
{
    if (!ctx.WebSockets.IsWebSocketRequest)
    {
        ctx.Response.StatusCode = 400;
        return;
    }

    var socket       = await ctx.WebSockets.AcceptWebSocketAsync();
    var connectionId = Guid.NewGuid().ToString("N");
    var snapshot     = aggregator.Snapshot();

    // Cast is safe: WebSocketPushChannel is the sole registered IPushChannel.
    var hub = (WebSocketPushChannel)push;
    await hub.HandleClientAsync(connectionId, socket, snapshot, ct);
});

app.Run();
