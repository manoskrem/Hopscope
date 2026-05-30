using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using Hopscope.Application.Abstractions;
using Hopscope.Domain.Events;
using Microsoft.Extensions.Logging;

namespace Hopscope.Infrastructure.Providers.Redis;

/// <summary>
/// <see cref="IEventIngestor"/> that subscribes to Redis keyevent notifications over a raw
/// TCP socket using the RESP protocol, and normalizes them to <see cref="EventEnvelope"/>.
///
/// AOT safety:
///   - Raw <see cref="TcpClient"/> / <see cref="NetworkStream"/> — fully AOT-compatible.
///   - RESP parsing via <see cref="RespReader"/> — no reflection, no JSON.
///   - No NuGet deps beyond the BCL (StackExchange.Redis is excluded: reflection-heavy,
///     would defeat AOT + the &lt;60 MB footprint gate).
///   - No IConfiguration.Get&lt;T&gt;() / Bind() — config keys read manually from
///     <see cref="IngestionSource.Options"/>.
///
/// Resilience: <see cref="StreamAsync"/> NEVER throws out. A socket/IO/parse fault ends the
/// current read session; the loop backs off (1s→10s, exponential) and reconnects. Cancellation
/// exits cleanly. C# forbids <c>yield</c> inside a try-with-catch, so the fallible work lives in
/// non-iterator helpers (<see cref="ConnectAsync"/>, <see cref="ReadOneAsync"/>) that capture
/// faults into return values rather than throwing across a <c>yield</c>; the iterator's only
/// <c>using</c> lowers to try/<i>finally</i> (no catch), where <c>yield</c> is legal.
///
/// Connection-string format: redis://[:password@]host:port[/db]
///   e.g.  redis://localhost:6379   redis://:secret@redis:6379/2
///
/// Options (via IngestionSource.Options):
///   keyDepth  int, default 1 — colon-separated key segments kept when building the
///             destination prefix (privacy + cardinality bound).
///   db        int, default from URL — selects the logical Redis database.
///
/// Subscribe strategy: PSUBSCRIBE __keyevent@*__:*  — keyevent channels carry db + event-type,
/// the pmessage payload carries the key. We subscribe ONLY to keyevent (not keyspace) so each
/// operation is observed exactly once.
/// </summary>
public sealed class RedisIngestor : IEventIngestor
{
    public string Name => "Redis";

    private readonly string  _host;
    private readonly int     _port;
    private readonly string? _password;
    private readonly int     _db;
    private readonly int     _keyDepth;
    private readonly ILogger _log;

    // Monotonic counter for fresh-HopId uniqueness (mirrors RabbitMqIngestor._activityCounter).
    private long _activityCounter;

    // Reconnect backoff: starts at 1 s, caps at 10 s.
    private static readonly TimeSpan BackoffMin = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan BackoffMax = TimeSpan.FromSeconds(10);

    public RedisIngestor(IngestionSource source, ILogger<RedisIngestor> logger)
    {
        _log = logger;

        // ── Parse connection string ─────────────────────────────────────────
        Uri uri;
        try
        {
            uri = new Uri(source.ConnectionString, UriKind.Absolute);
        }
        catch (UriFormatException)
        {
            uri = new Uri("redis://localhost:6379", UriKind.Absolute);
        }

        _host = string.IsNullOrEmpty(uri.Host) ? "localhost" : uri.Host;
        _port = uri.Port > 0 ? uri.Port : 6379;

        // Extract password from UserInfo. Accepted forms: ":pass" or "user:pass".
        // We always use the password segment (last component after last ':').
        var userInfo = uri.UserInfo;
        if (!string.IsNullOrEmpty(userInfo))
        {
            var colonIdx = userInfo.LastIndexOf(':');
            var pass     = colonIdx >= 0 ? userInfo[(colonIdx + 1)..] : userInfo;
            _password    = string.IsNullOrEmpty(pass) ? null : Uri.UnescapeDataString(pass);
        }

        // DB from URL path (/0, /1, …), overridable via Options["db"].
        var pathDb = 0;
        if (!string.IsNullOrEmpty(uri.AbsolutePath) && uri.AbsolutePath.Length > 1)
            int.TryParse(uri.AbsolutePath.TrimStart('/'), out pathDb);
        _db = pathDb;

        // ── Options ─────────────────────────────────────────────────────────
        if (source.Options.TryGetValue("db", out var dbOpt) && int.TryParse(dbOpt, out var dbVal))
            _db = dbVal;

        _keyDepth = 1;
        if (source.Options.TryGetValue("keyDepth", out var kd) && int.TryParse(kd, out var kdVal) && kdVal > 0)
            _keyDepth = kdVal;
    }

    // ── IEventIngestor ──────────────────────────────────────────────────────

    public async IAsyncEnumerable<EventEnvelope> StreamAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        var backoff = BackoffMin;

        while (!ct.IsCancellationRequested)
        {
            // One connect+subscribe+read session. The enumerable completes on fault or
            // cancellation; it NEVER throws. The yield here sits outside any try/catch.
            await foreach (var env in ReadSessionAsync(ct).ConfigureAwait(false))
            {
                backoff = BackoffMin;  // healthy traffic → reset backoff
                yield return env;
            }

            if (ct.IsCancellationRequested)
                yield break;

            // Bounded backoff before reconnecting; exit cleanly if cancelled mid-wait.
            if (!await DelayQuietAsync(backoff, ct).ConfigureAwait(false))
                yield break;

            backoff = backoff * 2 > BackoffMax ? BackoffMax : backoff * 2;
        }
    }

    /// <summary>
    /// One connection lifetime: connect + subscribe, then drain pmessage frames until the
    /// socket faults or the token cancels. The <c>using</c> lowers to try/finally (no catch),
    /// so the <c>yield</c>s here are legal; all throwing work is delegated to helpers that
    /// return results instead of throwing across the yield.
    /// </summary>
    private async IAsyncEnumerable<EventEnvelope> ReadSessionAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        using var conn = await ConnectAsync(ct).ConfigureAwait(false);
        if (conn is null)
            yield break;  // connect/subscribe failed or cancelled → caller backs off + retries

        while (!ct.IsCancellationRequested)
        {
            var (ended, envelope) = await ReadOneAsync(conn.Stream, ct).ConfigureAwait(false);
            if (ended)
                yield break;  // socket fault or cancellation → caller reconnects
            if (envelope is not null)
                yield return envelope;
        }
    }

    /// <summary>
    /// Opens the socket and issues AUTH (if configured), a best-effort
    /// <c>CONFIG SET notify-keyspace-events KEA</c>, and the PSUBSCRIBE (draining its
    /// confirmation). Returns null on any failure (logged) or cancellation — never throws.
    /// </summary>
    private async Task<RedisConnection?> ConnectAsync(CancellationToken ct)
    {
        TcpClient? tcp = null;
        try
        {
            tcp = new TcpClient { NoDelay = true };
            await tcp.ConnectAsync(_host, _port, ct).ConfigureAwait(false);
            var stream = tcp.GetStream();

            if (_password is not null)
            {
                await SendCommandAsync(stream, ct, "AUTH", _password).ConfigureAwait(false);
                var authReply = await RespReader.ReadValueAsync(stream, ct).ConfigureAwait(false);
                var authOk = authReply is not null
                             && authReply.StartsWith("OK", StringComparison.OrdinalIgnoreCase);
                if (!authOk)
                {
                    // Surface the failure and reconnect rather than subscribing on an
                    // unauthenticated socket (which would just fault and loop silently).
                    _log.LogWarning("Redis AUTH failed (reply: {Reply}); will retry.", authReply);
                    tcp.Dispose();
                    return null;
                }
            }

            // Best-effort enable keyspace notifications; tolerate an error reply (a managed
            // Redis may forbid CONFIG SET — the operator can enable KEA out of band).
            await SendCommandAsync(stream, ct, "CONFIG", "SET", "notify-keyspace-events", "KEA")
                .ConfigureAwait(false);
            await RespReader.ReadValueAsync(stream, ct).ConfigureAwait(false);

            await SendCommandAsync(stream, ct, "PSUBSCRIBE", "__keyevent@*__:*").ConfigureAwait(false);
            // Drain the psubscribe confirmation frame to keep the stream in sync.
            await RespReader.ReadPmessageAsync(stream, ct).ConfigureAwait(false);

            _log.LogInformation("Redis keyevent subscription active on {Host}:{Port}/db{Db}.",
                _host, _port, _db);

            return new RedisConnection(tcp, stream);
        }
        catch (OperationCanceledException)
        {
            tcp?.Dispose();
            return null;
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "Redis connect failed on {Host}:{Port}; will retry after backoff.",
                _host, _port);
            tcp?.Dispose();
            return null;
        }
    }

    /// <summary>
    /// Reads one pmessage frame and maps it. Returns <c>Ended=true</c> on any IO/parse fault
    /// or cancellation (caller reconnects); otherwise <c>Envelope</c> is the mapped event, or
    /// null for a non-pmessage control frame. Never throws.
    /// </summary>
    private async Task<(bool Ended, EventEnvelope? Envelope)> ReadOneAsync(
        NetworkStream stream, CancellationToken ct)
    {
        try
        {
            var frame = await RespReader.ReadPmessageAsync(stream, ct).ConfigureAwait(false);
            if (frame is null)
                return (false, null);

            var (_, channel, payload) = frame.Value;
            var counter = Interlocked.Increment(ref _activityCounter);
            var envelope = RedisMapper.PmessageToEnvelope(
                channel, payload, _keyDepth, counter, DateTimeOffset.UtcNow);
            return (false, envelope);
        }
        catch (OperationCanceledException)
        {
            return (true, null);
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "Redis read failed on {Host}:{Port}; will reconnect.", _host, _port);
            return (true, null);
        }
    }

    /// <summary>Awaits a delay, swallowing cancellation. Returns false iff cancelled.</summary>
    private static async Task<bool> DelayQuietAsync(TimeSpan delay, CancellationToken ct)
    {
        try
        {
            await Task.Delay(delay, ct).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    // ── RESP command sender ──────────────────────────────────────────────────

    /// <summary>
    /// Sends a RESP array command to <paramref name="stream"/>.
    /// Format: *{argc}\r\n$len\r\narg\r\n...  — AOT-safe (no reflection).
    /// </summary>
    private static async ValueTask SendCommandAsync(
        Stream stream, CancellationToken ct, params string[] args)
    {
        var sb = new StringBuilder();
        sb.Append('*').Append(args.Length).Append("\r\n");
        foreach (var arg in args)
        {
            var bytes = Encoding.UTF8.GetByteCount(arg);
            sb.Append('$').Append(bytes).Append("\r\n").Append(arg).Append("\r\n");
        }

        var buf = Encoding.UTF8.GetBytes(sb.ToString());
        await stream.WriteAsync(buf.AsMemory(), ct).ConfigureAwait(false);
        await stream.FlushAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Owns the socket + stream for one connection lifetime.</summary>
    private sealed class RedisConnection(TcpClient tcp, NetworkStream stream) : IDisposable
    {
        private readonly TcpClient _tcp = tcp;
        public NetworkStream Stream { get; } = stream;

        public void Dispose()
        {
            Stream.Dispose();
            _tcp.Dispose();
        }
    }
}
