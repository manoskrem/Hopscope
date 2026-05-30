using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Hopscope.Application.Abstractions;
using Hopscope.Domain.Events;
using Microsoft.Extensions.Logging;

namespace Hopscope.Infrastructure.Providers.RabbitMq;

/// <summary>
/// <see cref="IEventIngestor"/> that polls the RabbitMQ Management HTTP API.
///
/// AOT safety:
///   - HTTP via <see cref="HttpClient"/> — fully AOT-compatible.
///   - Deserialization via <see cref="RabbitMqJsonContext"/> source-gen overloads only.
///   - No IConfiguration.Get&lt;T&gt;() / Bind() — config keys read manually.
///   - No LINQ Expressions in the hot path.
///   - Unreachable broker is logged and retried; StreamAsync never throws.
/// </summary>
public sealed class RabbitMqIngestor : IEventIngestor
{
    public string Name => "RabbitMQ";

    private readonly HttpClient       _http;
    private readonly TimeSpan         _pollInterval;
    private readonly string           _vhost;
    private readonly ILogger          _log;

    // Queue name → last-seen MessageStats for throughput delta detection.
    private readonly Dictionary<string, RmqMessageStats> _prevStats =
        new(StringComparer.Ordinal);

    // Queue name → last-seen depth (messages). The reliable dead-letter signal:
    // arrivals raise depth but do not populate message_stats.publish.
    private readonly Dictionary<string, long> _prevMessages =
        new(StringComparer.Ordinal);

    // Monotonic counter for fresh-HopId activity envelopes.
    private long _activityCounter;

    /// <summary>
    /// Constructs an ingestor from an <see cref="IngestionSource"/>.
    /// <list type="bullet">
    ///   <item><c>ConnectionString</c> — Management base URL including credentials,
    ///         e.g. <c>http://guest:guest@localhost:15672</c>.</item>
    ///   <item><c>Options["pollSeconds"]</c> — poll interval in seconds (default 2).</item>
    ///   <item><c>Options["vhost"]</c> — virtual host to scope queries (default "/").</item>
    /// </list>
    /// </summary>
    public RabbitMqIngestor(IngestionSource source, ILogger<RabbitMqIngestor> logger)
    {
        _log = logger;

        // ── Poll interval ───────────────────────────────────────────────────
        _pollInterval = source.Options.TryGetValue("pollSeconds", out var ps)
                        && int.TryParse(ps, out var secs) && secs > 0
            ? TimeSpan.FromSeconds(secs)
            : TimeSpan.FromSeconds(2);

        // ── Virtual host ────────────────────────────────────────────────────
        _vhost = source.Options.TryGetValue("vhost", out var vh) && !string.IsNullOrEmpty(vh)
            ? vh
            : "/";

        // ── HttpClient with Basic-auth extracted from the connection string ─
        _http = BuildHttpClient(source.ConnectionString);
    }

    // ── IEventIngestor ──────────────────────────────────────────────────────

    public async IAsyncEnumerable<EventEnvelope> StreamAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            // Yield topology + activity envelopes for this poll tick.
            await foreach (var envelope in PollOnceAsync(ct).ConfigureAwait(false))
            {
                if (ct.IsCancellationRequested) yield break;
                yield return envelope;
            }

            // Wait for the next poll, or exit cleanly on cancellation.
            try
            {
                await Task.Delay(_pollInterval, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                yield break;
            }
        }
    }

    // ── Private polling logic ───────────────────────────────────────────────

    private async IAsyncEnumerable<EventEnvelope> PollOnceAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        var encodedVhost = Uri.EscapeDataString(_vhost);
        var now          = DateTimeOffset.UtcNow;

        List<RmqBinding>? bindings = null;
        List<RmqQueue>?   queues   = null;

        // ── Fetch bindings ──────────────────────────────────────────────────
        try
        {
            var bindingsUrl = $"/api/bindings/{encodedVhost}";
            using var resp  = await _http.GetAsync(bindingsUrl, ct).ConfigureAwait(false);
            resp.EnsureSuccessStatusCode();

            var stream = await resp.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
            bindings = await JsonSerializer
                .DeserializeAsync(stream, RabbitMqJsonContext.Default.ListRmqBinding, ct)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _log.LogWarning(ex,
                "RabbitMQ Management API unreachable (bindings). Retrying in {Interval}.",
                _pollInterval);
        }

        // ── Fetch queues ────────────────────────────────────────────────────
        try
        {
            var queuesUrl = $"/api/queues/{encodedVhost}";
            using var resp = await _http.GetAsync(queuesUrl, ct).ConfigureAwait(false);
            resp.EnsureSuccessStatusCode();

            var stream = await resp.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
            queues = await JsonSerializer
                .DeserializeAsync(stream, RabbitMqJsonContext.Default.ListRmqQueue, ct)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _log.LogWarning(ex,
                "RabbitMQ Management API unreachable (queues). Retrying in {Interval}.",
                _pollInterval);
        }

        // ── Topology envelopes ──────────────────────────────────────────────
        if (bindings is not null)
        {
            foreach (var binding in bindings)
            {
                if (ct.IsCancellationRequested) yield break;
                yield return RabbitMqMapper.BindingToEnvelope(binding, now);
            }
        }

        // ── Activity envelopes ──────────────────────────────────────────────
        if (queues is not null)
        {
            // Build a lookup: queue name → the first exchange that binds to it.
            // Used to populate Source on activity envelopes.
            var queueToSource = BuildQueueSourceMap(bindings);

            // Classify dead-letter queues for this poll: DLQ name → its DLX exchange.
            var dlqMap = RabbitMqMapper.IdentifyDeadLetterQueues(queues, bindings);

            foreach (var queue in queues)
            {
                if (ct.IsCancellationRequested) yield break;

                _prevStats.TryGetValue(queue.Name, out var prev);
                _prevMessages.TryGetValue(queue.Name, out var prevMessages);

                // A DLQ's inbound edge is, by definition, its dead-letter exchange —
                // prefer that over the first arbitrary binding so the edge is DLX → DLQ.
                var isDlq = dlqMap.TryGetValue(queue.Name, out var dlxName);
                string? inboundSource;
                if (isDlq)
                    inboundSource = dlxName;
                else
                    queueToSource.TryGetValue(queue.Name, out inboundSource);

                var activityEnvelope = RabbitMqMapper.QueueActivityToEnvelope(
                    queue,
                    prev,
                    prevMessages,
                    inboundSource,
                    isDlq,
                    ++_activityCounter,
                    now);

                // Update the running baselines. Stats only when present (a DLQ's
                // message_stats stays null); depth always (it's the dead-letter signal).
                if (queue.MessageStats is not null)
                    _prevStats[queue.Name] = queue.MessageStats;
                _prevMessages[queue.Name] = queue.Messages;

                if (activityEnvelope is not null)
                    yield return activityEnvelope;
            }
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Builds queue-name → first-inbound-exchange-name map from the binding list.
    /// Used to set Source on activity envelopes.
    /// </summary>
    private static Dictionary<string, string> BuildQueueSourceMap(List<RmqBinding>? bindings)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        if (bindings is null) return map;

        foreach (var b in bindings)
        {
            if (b.DestinationType == "queue" && !map.ContainsKey(b.Destination))
                map[b.Destination] = RabbitMqMapper.NormalizeExchangeName(b.Source);
        }

        return map;
    }

    /// <summary>
    /// Constructs a long-lived <see cref="HttpClient"/> with Basic auth extracted
    /// from the URL's userinfo component.
    ///
    /// AOT-safe: no reflection, no config binding, plain string parsing.
    /// </summary>
    private static HttpClient BuildHttpClient(string connectionString)
    {
        // Parse the connection string as a URI to extract host + credentials.
        // Expected form: http[s]://user:pass@host:port
        Uri uri;
        try
        {
            uri = new Uri(connectionString, UriKind.Absolute);
        }
        catch (UriFormatException)
        {
            // Fallback: treat as base URL with no credentials.
            uri = new Uri("http://localhost:15672", UriKind.Absolute);
        }

        // Reconstruct a base URL without userinfo so we don't double-encode it.
        var baseUrl = $"{uri.Scheme}://{uri.Host}:{uri.Port}";
        var client  = new HttpClient { BaseAddress = new Uri(baseUrl) };

        // Extract Basic auth from the userinfo segment (user:pass).
        var userInfo = uri.UserInfo;   // "user:pass" or "" if absent
        if (!string.IsNullOrEmpty(userInfo))
        {
            var encoded = Convert.ToBase64String(Encoding.UTF8.GetBytes(userInfo));
            client.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Basic", encoded);
        }

        return client;
    }
}
