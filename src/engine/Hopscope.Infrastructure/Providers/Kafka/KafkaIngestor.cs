using System.Runtime.CompilerServices;
using System.Text;
using Confluent.Kafka;
using Hopscope.Application.Abstractions;
using Hopscope.Domain.Events;
using Microsoft.Extensions.Logging;

namespace Hopscope.Infrastructure.Providers.Kafka;

/// <summary>
/// <see cref="IEventIngestor"/> that taps Kafka topics via Confluent.Kafka and normalizes
/// messages to <see cref="EventEnvelope"/>.
///
/// AOT safety:
///   - Confluent.Kafka P/Invokes native librdkafka — P/Invoke is AOT-compatible.
///   - No reflection, no LINQ Expressions, no dynamic codegen in our code.
///   - Config keys read manually from <see cref="IngestionSource.Options"/> —
///     NO IConfiguration.Get&lt;T&gt;()/Bind() (not AOT-safe).
///   - No new JsonSerializerContext: Kafka uses binary protocol, not JSON.
///
/// Resilience: <see cref="StreamAsync"/> NEVER throws out. All fallible work lives in
/// helper methods that return results rather than throwing across a yield boundary
/// (C# forbids yield inside try-with-catch: CS1626). The outer loop backs off
/// (1 s → 10 s exponential) and reconnects on fault. OperationCanceledException exits cleanly.
///
/// Iterator structure (mirrors RedisIngestor):
///   StreamAsync          — outer retry loop; yield is OUTSIDE any try/catch.
///   ReadSessionAsync     — one consumer lifetime; using lowers to try/finally (no catch),
///                          so yield is legal. Delegates all throwing work to helpers.
///   ConsumeOneAsync      — fallible: returns (ended, envelope?). Never throws across yield.
///   DiscoverAndSubscribe — fallible: builds consumer + subscribes. Returns null on failure.
///
/// Options (via IngestionSource.Options):
///   bootstrapServers — comma-separated broker list (falls back to ConnectionString).
///   group            — consumer group id (default "hopscope-tap").
///   topics           — comma-separated allowlist; empty = all non-internal discovered topics.
///
/// Topology tap:
///   On connect an IAdminClient metadata call discovers all topics; internal topics
///   (names starting with "__") are filtered out. If an allowlist is configured only
///   those topics are watched. Metadata is refreshed every MetadataRefreshEvery polls
///   to pick up newly created topics.
/// </summary>
public sealed class KafkaIngestor : IEventIngestor
{
    public string Name => "Kafka";

    private readonly string  _bootstrapServers;
    private readonly string  _group;
    private readonly string[] _topicAllowlist;   // empty = all non-internal
    private readonly ILogger  _log;

    // Monotonic counter (mirrors RabbitMqIngestor / RedisIngestor).
    private long _activityCounter;

    // How many Consume() polls between metadata refreshes (pick up new topics).
    private const int MetadataRefreshEvery = 200;

    // Reconnect backoff: 1 s → 10 s.
    private static readonly TimeSpan BackoffMin = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan BackoffMax = TimeSpan.FromSeconds(10);

    // Metadata fetch timeout.
    private static readonly TimeSpan MetadataTimeout = TimeSpan.FromSeconds(10);

    // How long to wait in each Consume() call before checking cancellation.
    private static readonly TimeSpan ConsumeTimeout = TimeSpan.FromMilliseconds(200);

    public KafkaIngestor(IngestionSource source, ILogger<KafkaIngestor> logger)
    {
        _log = logger;

        // bootstrapServers: Options["bootstrapServers"] ?? ConnectionString.
        if (!source.Options.TryGetValue("bootstrapServers", out var bs) || string.IsNullOrWhiteSpace(bs))
            bs = source.ConnectionString;
        _bootstrapServers = string.IsNullOrWhiteSpace(bs) ? "localhost:9092" : bs;

        // Consumer group.
        if (!source.Options.TryGetValue("group", out var grp) || string.IsNullOrWhiteSpace(grp))
            grp = "hopscope-tap";
        _group = grp;

        // Topic allowlist (empty = all non-internal).
        if (source.Options.TryGetValue("topics", out var topicsCsv) && !string.IsNullOrWhiteSpace(topicsCsv))
        {
            _topicAllowlist = topicsCsv.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }
        else
        {
            _topicAllowlist = [];
        }
    }

    // ── IEventIngestor ──────────────────────────────────────────────────────

    public async IAsyncEnumerable<EventEnvelope> StreamAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        var backoff = BackoffMin;

        while (!ct.IsCancellationRequested)
        {
            // One connect+subscribe+consume session. yield is OUTSIDE any try/catch here.
            await foreach (var env in ReadSessionAsync(ct).ConfigureAwait(false))
            {
                backoff = BackoffMin;   // healthy traffic → reset backoff
                yield return env;
            }

            if (ct.IsCancellationRequested)
                yield break;

            if (!await DelayQuietAsync(backoff, ct).ConfigureAwait(false))
                yield break;

            backoff = backoff * 2 > BackoffMax ? BackoffMax : backoff * 2;
        }
    }

    /// <summary>
    /// One consumer lifetime: discover topics, subscribe, drain messages, emit topology.
    /// The <c>using</c> lowers to try/finally (no catch) so yield is legal here.
    /// All throwing work is delegated to helpers that return results.
    /// </summary>
    private async IAsyncEnumerable<EventEnvelope> ReadSessionAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        // DiscoverAndSubscribe returns null on any failure — we just yield break and retry.
        var session = await Task.Run(() => DiscoverAndSubscribe(ct), ct).ConfigureAwait(false);
        if (session is null)
            yield break;

        // Emit topology envelopes for the initially discovered topics.
        var now = DateTimeOffset.UtcNow;
        foreach (var topic in session.Topics)
        {
            if (ct.IsCancellationRequested) yield break;
            yield return KafkaMapper.TopicMetadataToEnvelope(_bootstrapServers, topic, now);
        }

        // using lowers to try/finally — yield inside is legal (no catch clause).
        using (session)
        {
            var pollCount = 0;

            while (!ct.IsCancellationRequested)
            {
                // Periodic metadata refresh to pick up newly created topics.
                if (pollCount > 0 && pollCount % MetadataRefreshEvery == 0)
                {
                    var (refreshEnded, newTopics) = await Task.Run(
                        () => RefreshTopics(session), ct).ConfigureAwait(false);

                    if (refreshEnded)
                        yield break;

                    if (newTopics is not null)
                    {
                        var refreshNow = DateTimeOffset.UtcNow;
                        foreach (var t in newTopics)
                        {
                            if (ct.IsCancellationRequested) yield break;
                            yield return KafkaMapper.TopicMetadataToEnvelope(
                                _bootstrapServers, t, refreshNow);
                        }
                    }
                }

                // Consume one message (runs synchronous Confluent.Kafka call off-thread).
                var counter = Interlocked.Increment(ref _activityCounter);
                var (ended, envelope) = await Task.Run(
                    () => ConsumeOne(session.Consumer, counter, ct), ct).ConfigureAwait(false);

                if (ended)
                    yield break;

                if (envelope is not null)
                    yield return envelope;

                pollCount++;
            }
        }
    }

    // ── Helpers (all return results, never throw across a yield) ─────────────

    /// <summary>
    /// Discovers topics via admin client metadata, builds and subscribes a consumer.
    /// Returns null on any failure (logged). Called via Task.Run — synchronous Confluent.Kafka.
    /// </summary>
    private KafkaSession? DiscoverAndSubscribe(CancellationToken ct)
    {
        IAdminClient? admin = null;
        IConsumer<byte[], byte[]>? consumer = null;
        try
        {
            if (ct.IsCancellationRequested) return null;

            var adminConfig = new AdminClientConfig { BootstrapServers = _bootstrapServers };
            admin = new AdminClientBuilder(adminConfig).Build();

            var meta    = admin.GetMetadata(MetadataTimeout);
            var topics  = FilterTopics(meta);

            if (topics.Count == 0)
            {
                _log.LogInformation(
                    "Kafka: no topics to subscribe to on {Bootstrap} (allowlist: [{Allowlist}]). " +
                    "Will retry after backoff.",
                    _bootstrapServers,
                    _topicAllowlist.Length > 0 ? string.Join(",", _topicAllowlist) : "(all)");
                admin.Dispose();
                return null;
            }

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers       = _bootstrapServers,
                GroupId                = _group,
                AutoOffsetReset        = AutoOffsetReset.Latest,
                EnableAutoCommit       = true,
                // Don't block partition assignment indefinitely.
                SessionTimeoutMs       = 30_000,
                HeartbeatIntervalMs    = 3_000,
            };
            consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build();
            consumer.Subscribe(topics);

            _log.LogInformation(
                "Kafka: subscribed to {Count} topic(s) on {Bootstrap} (group={Group}).",
                topics.Count, _bootstrapServers, _group);

            return new KafkaSession(admin, consumer, topics);
        }
        catch (OperationCanceledException)
        {
            admin?.Dispose();
            consumer?.Close();
            consumer?.Dispose();
            return null;
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex,
                "Kafka: failed to connect/subscribe on {Bootstrap}; will retry after backoff.",
                _bootstrapServers);
            admin?.Dispose();
            consumer?.Close();
            consumer?.Dispose();
            return null;
        }
    }

    /// <summary>
    /// Refreshes the topic list via the admin client already in the session.
    /// Returns (ended=true, null) on fault; (false, newTopics) otherwise where
    /// newTopics contains only topics not previously seen (may be empty list).
    /// Called via Task.Run — synchronous Confluent.Kafka.
    /// </summary>
    private (bool Ended, List<string>? NewTopics) RefreshTopics(KafkaSession session)
    {
        try
        {
            var meta      = session.Admin.GetMetadata(MetadataTimeout);
            var allTopics = FilterTopics(meta);
            var newTopics = new List<string>();

            foreach (var t in allTopics)
            {
                if (session.Topics.Add(t))
                    newTopics.Add(t);
            }

            if (newTopics.Count > 0)
            {
                // Re-subscribe with the expanded list.
                session.Consumer.Subscribe(session.Topics);
                _log.LogInformation(
                    "Kafka: discovered {Count} new topic(s): {Topics}",
                    newTopics.Count, string.Join(", ", newTopics));
            }

            return (false, newTopics);
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "Kafka: metadata refresh failed; will reconnect.");
            return (true, null);
        }
    }

    /// <summary>
    /// Calls consumer.Consume(timeout) once and maps the result to an envelope.
    /// Returns (ended=true, null) on KafkaException or OperationCanceledException;
    /// (false, null) on timeout (no message yet); (false, envelope) on a message.
    /// Called via Task.Run — synchronous Confluent.Kafka.
    /// </summary>
    private (bool Ended, EventEnvelope? Envelope) ConsumeOne(
        IConsumer<byte[], byte[]> consumer,
        long counter,
        CancellationToken ct)
    {
        try
        {
            if (ct.IsCancellationRequested) return (true, null);

            var result = consumer.Consume(ConsumeTimeout);
            if (result is null || result.IsPartitionEOF)
                return (false, null);

            // Extract headers into a plain string dict — keeps Confluent.Kafka types
            // entirely out of the mapper (AOT-safe, unit-testable without a broker).
            var headers = ExtractHeaders(result.Message.Headers);
            var ts      = result.Message.Timestamp.UtcDateTime == DateTime.MinValue
                ? DateTimeOffset.UtcNow
                : new DateTimeOffset(result.Message.Timestamp.UtcDateTime, TimeSpan.Zero);

            var envelope = KafkaMapper.ConsumedMessageToEnvelope(
                topic:     result.Topic,
                partition: result.Partition.Value,
                offset:    result.Offset.Value,
                headers:   headers,
                counter:   counter,
                ts:        ts);

            return (false, envelope);
        }
        catch (OperationCanceledException)
        {
            return (true, null);
        }
        catch (ConsumeException ex)
        {
            _log.LogWarning(ex, "Kafka: consume error (code={Code}); will reconnect.",
                ex.Error.Code);
            return (true, null);
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "Kafka: unexpected error during consume; will reconnect.");
            return (true, null);
        }
    }

    // ── Utility ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns the filtered topic list from Kafka metadata.
    /// Skips internal topics (names starting with "__") and applies the allowlist if set.
    /// </summary>
    private List<string> FilterTopics(Metadata meta)
    {
        var result = new List<string>(meta.Topics.Count);
        foreach (var tmeta in meta.Topics)
        {
            // Skip internal topics (e.g. __consumer_offsets, __transaction_state).
            if (tmeta.Topic.StartsWith("__", StringComparison.Ordinal))
                continue;

            // Apply allowlist if configured.
            if (_topicAllowlist.Length > 0 && !ArrayContains(_topicAllowlist, tmeta.Topic))
                continue;

            result.Add(tmeta.Topic);
        }
        return result;
    }

    private static bool ArrayContains(string[] arr, string value)
    {
        foreach (var s in arr)
        {
            if (string.Equals(s, value, StringComparison.Ordinal))
                return true;
        }
        return false;
    }

    /// <summary>
    /// Decodes Kafka message headers into a plain string→string dictionary.
    /// UTF-8 decode each value; skip headers with null values.
    /// Keeps all Confluent.Kafka header types inside the ingestor, out of the mapper.
    /// </summary>
    private static IReadOnlyDictionary<string, string> ExtractHeaders(Headers? headers)
    {
        if (headers is null || headers.Count == 0)
            return new Dictionary<string, string>(StringComparer.Ordinal);

        var dict = new Dictionary<string, string>(headers.Count, StringComparer.Ordinal);
        foreach (var hdr in headers)
        {
            if (hdr.GetValueBytes() is { } bytes)
            {
                // Last-write-wins for duplicate header keys (Kafka allows duplicates).
                dict[hdr.Key] = Encoding.UTF8.GetString(bytes);
            }
        }
        return dict;
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

    // ── Session ownership ─────────────────────────────────────────────────────

    /// <summary>
    /// Owns an admin client + consumer for one connection lifetime.
    /// Topics is the mutable set used for subscription and new-topic detection.
    /// </summary>
    private sealed class KafkaSession(
        IAdminClient admin,
        IConsumer<byte[], byte[]> consumer,
        List<string> topics) : IDisposable
    {
        public IAdminClient              Admin    { get; } = admin;
        public IConsumer<byte[], byte[]> Consumer { get; } = consumer;

        // Mutable: grows on metadata refresh to track newly discovered topics.
        public HashSet<string> Topics { get; } = new(topics, StringComparer.Ordinal);

        public void Dispose()
        {
            try { Consumer.Close(); }    catch { /* best-effort */ }
            try { Consumer.Dispose(); }  catch { /* best-effort */ }
            try { Admin.Dispose(); }     catch { /* best-effort */ }
        }
    }
}
