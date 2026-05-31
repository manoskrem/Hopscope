using Hopscope.Application.Abstractions;
using Hopscope.Application.Aggregation;
using Hopscope.Application.Pipeline;
using Hopscope.Application.Projection;
using Hopscope.Infrastructure.Providers.Agent;
using Hopscope.Infrastructure.Providers.Fake;
using Hopscope.Infrastructure.Providers.Otlp;
using Hopscope.Infrastructure.Providers.RabbitMq;
using Hopscope.Infrastructure.Providers.Redis;
#if KAFKA
using Hopscope.Infrastructure.Providers.Kafka;
#endif
using Hopscope.Infrastructure.Serialization;
using Hopscope.Push;
using Microsoft.AspNetCore.Server.Kestrel.Core;

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
builder.Services.AddRedisIngestion(config);
#if KAFKA
// Kafka is an opt-in build variant (Confluent.Kafka is not trim-clean). This line is
// compiled in only when built with -p:EnableKafka=true (Dockerfile.kafka).
builder.Services.AddKafkaIngestion(config);
#endif

// ── gRPC receivers: OTLP (4317) and the remote Agent (4318) ──────────────────────
// Both are INDEPENDENT, default-OFF gates (HOPSCOPE_OTLP_ENABLED / HOPSCOPE_AGENT_ENABLED).
// Grpc.AspNetCore + Google.Protobuf are AOT-safe (generated parse/serialize paths only).
// When both are disabled: registers nothing; the Fake fallback still works.
var (otlpEnabled,  otlpPort)  = builder.Services.AddOtlpIngestion(config);
var (agentEnabled, agentPort) = builder.Services.AddAgentIngestion(config);

if (otlpEnabled && agentEnabled && otlpPort == agentPort)
    throw new InvalidOperationException(
        $"HOPSCOPE_OTLP_PORT and HOPSCOPE_AGENT_PORT must differ (both = {otlpPort}).");

if (otlpEnabled || agentEnabled)
{
    // AddGrpc registers the gRPC framework middleware ONCE — even if BOTH receivers are on.
    builder.Services.AddGrpc();

    // ONE Kestrel config. IMPORTANT: calling ConfigureKestrel/ListenAnyIP makes Kestrel IGNORE
    // ASPNETCORE_URLS entirely, AND these actions are ADDITIVE (a second ConfigureKestrel would
    // double-bind the HTTP port → AddressInUseException). So we re-establish the HTTP/1.1 listener
    // HERE (or /ws + /snapshot vanish) and add each enabled gRPC port as h2c in the SAME block.
    // Verify with lsof/curl, never by trusting this comment (PR #9 lesson).
    var httpPort = ResolveHttpPort(config["ASPNETCORE_URLS"], defaultPort: 8080);
    builder.WebHost.ConfigureKestrel(o =>
    {
        o.ListenAnyIP(httpPort);   // HTTP/1.1 — WebSocket + REST (was ASPNETCORE_URLS)
        if (otlpEnabled)
            o.ListenAnyIP(otlpPort,  lo => lo.Protocols = HttpProtocols.Http2);   // h2c gRPC (OTLP)
        if (agentEnabled)
            o.ListenAnyIP(agentPort, lo => lo.Protocols = HttpProtocols.Http2);   // h2c gRPC (Agent)
    });
}

// Parses the first port from an ASPNETCORE_URLS value (e.g. "http://+:8080" → 8080).
// Returns the default if the value is absent/unparseable. Manual parse — no reflection.
static int ResolveHttpPort(string? urls, int defaultPort)
{
    if (string.IsNullOrWhiteSpace(urls))
        return defaultPort;

    // Take the first ';'-separated url, then the substring after the last ':'.
    var first = urls.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)[0];
    var colon = first.LastIndexOf(':');
    if (colon < 0 || colon == first.Length - 1)
        return defaultPort;

    var portSpan = first.AsSpan(colon + 1);
    // Strip any trailing path (e.g. ":8080/").
    var slash = portSpan.IndexOf('/');
    if (slash >= 0) portSpan = portSpan[..slash];

    return int.TryParse(portSpan, out var p) && p > 0 ? p : defaultPort;
}

builder.Services.AddFakeIngestionIfNoneRegistered();

// ── Background pipeline ───────────────────────────────────────────────────
builder.Services.AddHostedService<EngineLoop>();

// ── Logging ───────────────────────────────────────────────────────────────
builder.Logging.AddConsole();

var app = builder.Build();

// ── Map gRPC service endpoints (same gates as above) ─────────────────────
// MapGrpcService requires WebApplication (not available at builder time), so it lives here.
// Each service is reachable ONLY on its HTTP/2 gRPC port; the 8080 WS/REST endpoints are
// unaffected — UseWebSockets() and MapGet() below continue to bind there. MapGrpcService<T>
// activates the service via ActivatorUtilities (its ctor args resolve from DI) — no explicit
// service registration is needed.
if (otlpEnabled)
{
    app.MapGrpcService<OtlpTraceService>();
}
if (agentEnabled)
{
    app.MapGrpcService<AgentIngestionService>();
}

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
