using Hopscope.Application.Abstractions;
using Hopscope.Application.Aggregation;
using Hopscope.Application.Pipeline;
using Hopscope.Application.Projection;
using Hopscope.Infrastructure.Providers.Fake;
using Hopscope.Infrastructure.Providers.RabbitMq;
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

// ── Ingestion wiring (the Phase-4 seam) ────────────────────────────────────
// Each broker is ONE line + its own Providers/<Broker>/ folder. Each extension
// reads its OWN config MANUALLY (no reflective IConfiguration.Get<T>()/Bind() —
// not AOT-safe) and registers an IEventIngestor only when that broker is
// configured. Multiple configured brokers co-render on one canvas (EngineLoop
// pumps every registered IEventIngestor). With NONE configured the FakeIngestor
// drives synthetic traffic so Phase-1 smoke-testing still works (real data wins).
var config = builder.Configuration;
builder.Services.AddRabbitMqIngestion(config);
builder.Services.AddFakeIngestionIfNoneRegistered();

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

    // Cast is safe: WebSocketPushChannel is the sole registered IPushChannel.
    // Pass the snapshot factory (not a pre-captured snapshot) so the hub captures it
    // AFTER registering the client — closing the connect race where a delta emitted
    // between capture and registration would reach no one.
    var hub = (WebSocketPushChannel)push;
    await hub.HandleClientAsync(connectionId, socket, aggregator.Snapshot, ct);
});

app.Run();
