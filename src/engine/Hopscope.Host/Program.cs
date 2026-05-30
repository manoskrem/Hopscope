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

// ── Broker providers (compile-time registered — AOT rule §3) ──────────────
builder.Services.AddSingleton<IBrokerProvider, RabbitMqProvider>();

// ── Ingestor wiring ────────────────────────────────────────────────────────
// Read config MANUALLY — IConfiguration.Get<T>()/Bind() use reflection and
// are NOT AOT-safe. Supported config keys (env var or appsettings):
//   HOPSCOPE_RABBITMQ_URL  (env)  or  Hopscope:RabbitMq:Url  (appsettings)
//   HOPSCOPE_RABBITMQ_VHOST                Hopscope:RabbitMq:Vhost
//   HOPSCOPE_RABBITMQ_POLL_SECONDS         Hopscope:RabbitMq:PollSeconds
//
// When a RabbitMQ URL is present the RabbitMqProvider creates the ingestor and
// FakeIngestor is NOT registered (real data only). With no URL configured the
// FakeIngestor is the fallback so Phase-1 smoke-testing still works.

var config    = builder.Configuration;
var rmqUrl    = config["HOPSCOPE_RABBITMQ_URL"] ?? config["Hopscope:RabbitMq:Url"];
var rmqVhost  = config["HOPSCOPE_RABBITMQ_VHOST"] ?? config["Hopscope:RabbitMq:Vhost"];
var rmqPoll   = config["HOPSCOPE_RABBITMQ_POLL_SECONDS"] ?? config["Hopscope:RabbitMq:PollSeconds"];

if (!string.IsNullOrWhiteSpace(rmqUrl))
{
    // Build the IngestionSource from manually-read config values.
    var options = new Dictionary<string, string>(StringComparer.Ordinal);
    if (!string.IsNullOrWhiteSpace(rmqVhost))
        options["vhost"] = rmqVhost;
    if (!string.IsNullOrWhiteSpace(rmqPoll))
        options["pollSeconds"] = rmqPoll;

    var rmqSource = new IngestionSource(
        BrokerType:       "RabbitMQ",
        ConnectionString: rmqUrl,
        Options:          options);

    // Register a factory delegate so DI resolves IBrokerProvider (and its
    // ILoggerFactory dep) before constructing the ingestor. Compile-time,
    // no reflection.
    builder.Services.AddSingleton<IEventIngestor>(sp =>
    {
        var provider = sp.GetRequiredService<IBrokerProvider>();
        return provider.CreateIngestor(rmqSource);
    });
}
else
{
    // No real broker configured — fall back to the synthetic FakeIngestor.
    builder.Services.AddSingleton<IEventIngestor, FakeIngestor>();
}

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
