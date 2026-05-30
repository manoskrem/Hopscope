using System.Text;
using Hopscope.Application.Abstractions;
using Hopscope.Domain.Events;
using Hopscope.Infrastructure.Providers.Redis;
using Microsoft.Extensions.Logging.Abstractions;

namespace Hopscope.Engine.Tests;

/// <summary>
/// Unit tests for <see cref="RedisMapper"/>, <see cref="RespReader"/>, and
/// <see cref="RedisProvider"/>. No live broker required — all tests exercise
/// pure mapping and parsing logic.
/// </summary>
public sealed class RedisMapperTests
{
    private static readonly DateTimeOffset FixedNow =
        new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    // =========================================================================
    // ParseKeyEventChannel
    // =========================================================================

    [Theory]
    [InlineData("__keyevent@0__:set",    0,  "set")]
    [InlineData("__keyevent@3__:del",    3,  "del")]
    [InlineData("__keyevent@15__:expire",15, "expire")]
    public void ParseKeyEventChannel_Valid_ReturnsParsedDbAndEvent(
        string channel, int expectedDb, string expectedEvent)
    {
        var result = RedisMapper.ParseKeyEventChannel(channel);

        Assert.NotNull(result);
        Assert.Equal(expectedDb,    result!.Value.Db);
        Assert.Equal(expectedEvent, result.Value.EventType);
    }

    [Theory]
    [InlineData("__keyspace@0__:mykey")]   // keyspace, not keyevent
    [InlineData("__keyevent@notanint__:set")]
    [InlineData("randomstring")]
    [InlineData("")]
    [InlineData("__keyevent@0__:")]        // empty event type
    public void ParseKeyEventChannel_Invalid_ReturnsNull(string channel)
    {
        var result = RedisMapper.ParseKeyEventChannel(channel);
        Assert.Null(result);
    }

    // =========================================================================
    // KeyPrefix grouping
    // =========================================================================

    [Theory]
    [InlineData("user:123",    1, "user:*")]
    [InlineData("order:456",   1, "order:*")]
    [InlineData("a:b:c",       2, "a:b:*")]
    [InlineData("a:b:c",       1, "a:*")]
    [InlineData("session:abc:token", 2, "session:abc:*")]
    public void KeyPrefix_ColonKey_TruncatesToDepth(string key, int depth, string expected)
    {
        Assert.Equal(expected, RedisMapper.KeyPrefix(key, depth));
    }

    [Theory]
    [InlineData("plainkey")]   // no colon
    [InlineData("")]           // empty
    public void KeyPrefix_NoColon_ReturnsSentinel(string key)
    {
        Assert.Equal("keys:*", RedisMapper.KeyPrefix(key, 1));
    }

    [Fact]
    public void KeyPrefix_DepthExceedsSegments_ReturnsSentinel()
    {
        // "user:123" has one colon → only depth=1 produces a real prefix.
        // Asking for depth=3 on a key with one colon → sentinel.
        Assert.Equal("keys:*", RedisMapper.KeyPrefix("user:123", 3));
    }

    [Fact]
    public void KeyPrefix_DepthRespected_DifferentDepthsDifferentPrefixes()
    {
        var d1 = RedisMapper.KeyPrefix("a:b:c", 1);
        var d2 = RedisMapper.KeyPrefix("a:b:c", 2);

        Assert.Equal("a:*",   d1);
        Assert.Equal("a:b:*", d2);
        Assert.NotEqual(d1, d2);
    }

    // =========================================================================
    // KeyEventToEnvelope — field correctness
    // =========================================================================

    [Fact]
    public void KeyEventToEnvelope_FieldsCorrect()
    {
        var env = RedisMapper.KeyEventToEnvelope(
            db: 0, eventType: "set", key: "user:123",
            keyDepth: 1, counter: 1, timestamp: FixedNow);

        Assert.Equal("redis-db0",              env.Source);
        Assert.Equal("user:*",                 env.Destination);
        Assert.Equal("Redis",                  env.BrokerType);
        Assert.Equal(ExecutionStatus.Success,  env.ExecutionStatus);
        Assert.Null(env.ParentHopId);
        Assert.Null(env.ErrorDetails);
        Assert.Equal(FixedNow,                 env.Timestamp);
    }

    [Fact]
    public void KeyEventToEnvelope_MetadataContainsExpectedKeys()
    {
        var env = RedisMapper.KeyEventToEnvelope(
            db: 2, eventType: "del", key: "order:789",
            keyDepth: 1, counter: 5, timestamp: FixedNow);

        Assert.True(env.PayloadMetadata.TryGetValue("destinationKind", out var dk));
        Assert.Equal("Topic", dk);

        Assert.True(env.PayloadMetadata.TryGetValue("sourceKind", out var sk));
        Assert.Equal("Service", sk);

        Assert.True(env.PayloadMetadata.TryGetValue("redisEvent", out var re));
        Assert.Equal("del", re);

        Assert.True(env.PayloadMetadata.TryGetValue("db", out var dbVal));
        Assert.Equal("2", dbVal);

        Assert.True(env.PayloadMetadata.TryGetValue("keyPrefix", out var kp));
        Assert.Equal("order:*", kp);
    }

    // =========================================================================
    // Fresh HopId — deterministic format, counter-driven uniqueness
    // =========================================================================

    [Fact]
    public void KeyEventToEnvelope_DifferentCounter_DifferentHopId()
    {
        var env1 = RedisMapper.KeyEventToEnvelope(0, "set", "user:1", 1, counter: 10, FixedNow);
        var env2 = RedisMapper.KeyEventToEnvelope(0, "set", "user:1", 1, counter: 11, FixedNow);

        Assert.NotEqual(env1.HopId, env2.HopId);
    }

    [Fact]
    public void KeyEventToEnvelope_HopId_DeterministicFormat()
    {
        var env = RedisMapper.KeyEventToEnvelope(
            db: 0, eventType: "set", key: "user:42",
            keyDepth: 1, counter: 7, timestamp: FixedNow);

        // Format: redis:<db>:<prefix>:<counter>
        Assert.Equal("redis:0:user:*:7", env.HopId);
    }

    [Fact]
    public void KeyEventToEnvelope_TraceId_StablePerKeyFamily_DistinctHopIds()
    {
        // Same db+prefix, different counters → SHARED TraceId (stable, no counter) but
        // DISTINCT HopIds (counter-driven). This keeps trace cardinality bounded by the
        // number of key families so high-volume Redis traffic can't churn the trace LRU.
        var env1 = RedisMapper.KeyEventToEnvelope(0, "set", "user:1", 1, counter: 100, FixedNow);
        var env2 = RedisMapper.KeyEventToEnvelope(0, "del", "user:2", 1, counter: 101, FixedNow);

        Assert.Equal("redis-activity:0:user:*", env1.TraceId);
        Assert.Equal(env1.TraceId, env2.TraceId);   // shared trace
        Assert.NotEqual(env1.HopId, env2.HopId);     // distinct hops
    }

    [Fact]
    public void KeyEventToEnvelope_TraceId_DiffersPerPrefixAndDb()
    {
        var userDb0    = RedisMapper.KeyEventToEnvelope(0, "set", "user:1",    1, 1, FixedNow);
        var orderDb0   = RedisMapper.KeyEventToEnvelope(0, "set", "order:1",   1, 2, FixedNow);
        var userDb1    = RedisMapper.KeyEventToEnvelope(1, "set", "user:1",    1, 3, FixedNow);

        Assert.NotEqual(userDb0.TraceId, orderDb0.TraceId);  // different key family
        Assert.NotEqual(userDb0.TraceId, userDb1.TraceId);   // different db
    }

    // =========================================================================
    // Privacy — no full key value in PayloadMetadata
    // =========================================================================

    [Fact]
    public void KeyEventToEnvelope_NoFullKeyInMetadata()
    {
        const string fullKey = "user:12345";
        var env = RedisMapper.KeyEventToEnvelope(
            db: 0, eventType: "set", key: fullKey,
            keyDepth: 1, counter: 1, timestamp: FixedNow);

        foreach (var (k, v) in env.PayloadMetadata)
        {
            Assert.False(
                v.Contains(fullKey, StringComparison.Ordinal),
                $"PayloadMetadata[{k}] = '{v}' contains the full key '{fullKey}'.");
        }
    }

    [Fact]
    public void KeyEventToEnvelope_DestinationIsPrefix_NotFullKey()
    {
        var env = RedisMapper.KeyEventToEnvelope(
            db: 0, eventType: "set", key: "session:abc123",
            keyDepth: 1, counter: 1, timestamp: FixedNow);

        Assert.Equal("session:*", env.Destination);
        Assert.DoesNotContain("abc123", env.Destination, StringComparison.Ordinal);
    }

    // =========================================================================
    // PmessageToEnvelope — integration of channel parse + envelope build
    // =========================================================================

    [Fact]
    public void PmessageToEnvelope_ValidKeyeventChannel_ReturnsEnvelope()
    {
        var env = RedisMapper.PmessageToEnvelope(
            channel: "__keyevent@0__:set",
            keyPayload: "user:999",
            keyDepth: 1,
            counter: 3,
            ts: FixedNow);

        Assert.NotNull(env);
        Assert.Equal("redis-db0", env!.Source);
        Assert.Equal("user:*",    env.Destination);
        Assert.Equal("Redis",     env.BrokerType);
    }

    [Fact]
    public void PmessageToEnvelope_NonKeyeventChannel_ReturnsNull()
    {
        var env = RedisMapper.PmessageToEnvelope(
            channel: "__keyspace@0__:user:123",
            keyPayload: "set",
            keyDepth: 1,
            counter: 1,
            ts: FixedNow);

        Assert.Null(env);
    }

    // =========================================================================
    // RESP reader — frame parsing
    // =========================================================================

    private static MemoryStream RespStream(string resp) =>
        new(Encoding.UTF8.GetBytes(resp));

    [Fact]
    public async Task RespReader_Pmessage_ParsesPatternChannelPayload()
    {
        // A well-formed RESP *4 pmessage frame (keyevent notification shape).
        const string frame =
            "*4\r\n" +
            "$8\r\npmessage\r\n" +
            "$16\r\n__keyevent@*__:*\r\n" +
            "$18\r\n__keyevent@0__:set\r\n" +
            "$8\r\nuser:123\r\n";

        using var stream = RespStream(frame);
        var result = await RespReader.ReadPmessageAsync(stream, CancellationToken.None);

        Assert.NotNull(result);
        Assert.Equal("__keyevent@*__:*",  result!.Value.Pattern);
        Assert.Equal("__keyevent@0__:set", result.Value.Channel);
        Assert.Equal("user:123",          result.Value.Payload);
    }

    [Fact]
    public async Task RespReader_BulkNull_ReturnsNull()
    {
        using var stream = RespStream("$-1\r\n");
        var result = await RespReader.ReadBulkStringValueAsync(stream, CancellationToken.None);
        Assert.Null(result);
    }

    [Fact]
    public async Task RespReader_BulkString_LengthBased_HandlesBinaryPayload()
    {
        // Payload contains a non-text byte (0x00) — proves LENGTH-based read, not line-split.
        var payloadBytes = new byte[] { (byte)'h', (byte)'i', 0x00, (byte)'!' };
        var header = Encoding.UTF8.GetBytes($"${payloadBytes.Length}\r\n");
        var crlf   = Encoding.UTF8.GetBytes("\r\n");

        using var ms = new MemoryStream();
        ms.Write(header);
        ms.Write(payloadBytes);
        ms.Write(crlf);
        ms.Position = 0;

        var result = await RespReader.ReadBulkStringValueAsync(ms, CancellationToken.None);

        // Result length must equal payloadBytes.Length (the null byte is included).
        Assert.NotNull(result);
        Assert.Equal(payloadBytes.Length, Encoding.UTF8.GetByteCount(result!));
    }

    [Fact]
    public async Task RespReader_IntegerFrame_ReadAsValue()
    {
        using var stream = RespStream(":42\r\n");
        var result = await RespReader.ReadValueAsync(stream, CancellationToken.None);
        Assert.Equal("42", result);
    }

    [Fact]
    public async Task RespReader_SimpleString_ReadAsValue()
    {
        using var stream = RespStream("+OK\r\n");
        var result = await RespReader.ReadValueAsync(stream, CancellationToken.None);
        Assert.Equal("OK", result);
    }

    [Fact]
    public async Task RespReader_ErrorFrame_ReadAsValue()
    {
        using var stream = RespStream("-ERR unknown command\r\n");
        var result = await RespReader.ReadValueAsync(stream, CancellationToken.None);
        Assert.Equal("ERR unknown command", result);
    }

    [Fact]
    public async Task RespReader_NonPmessageArray_ReturnsNull()
    {
        // A "subscribe" confirmation frame — not a pmessage.
        const string frame =
            "*3\r\n" +
            "$9\r\nsubscribe\r\n" +
            "$16\r\n__keyevent@*__:*\r\n" +
            ":1\r\n";

        using var stream = RespStream(frame);
        var result = await RespReader.ReadPmessageAsync(stream, CancellationToken.None);
        Assert.Null(result);
    }

    [Fact]
    public async Task RespReader_PsubscribeConfirmationThenPmessage_DrainsAndParsesNext()
    {
        // Regression: the PSUBSCRIBE confirmation is a *3 frame. If ReadPmessageAsync
        // returns null WITHOUT consuming its 3 elements, the leftover bytes desync the
        // stream and the next read throws on the wrong prefix — bouncing the connection
        // and delivering zero envelopes. The first call must fully drain the *3 so the
        // following *4 pmessage parses cleanly.
        const string frames =
            "*3\r\n" +
            "$10\r\npsubscribe\r\n" +
            "$16\r\n__keyevent@*__:*\r\n" +
            ":1\r\n" +
            "*4\r\n" +
            "$8\r\npmessage\r\n" +
            "$16\r\n__keyevent@*__:*\r\n" +
            "$18\r\n__keyevent@0__:set\r\n" +
            "$8\r\nuser:123\r\n";

        using var stream = RespStream(frames);

        var confirmation = await RespReader.ReadPmessageAsync(stream, CancellationToken.None);
        Assert.Null(confirmation);   // *3 confirmation drained, not mis-parsed

        var pmessage = await RespReader.ReadPmessageAsync(stream, CancellationToken.None);
        Assert.NotNull(pmessage);
        Assert.Equal("__keyevent@0__:set", pmessage!.Value.Channel);
        Assert.Equal("user:123",           pmessage.Value.Payload);
    }

    // =========================================================================
    // RedisProvider.CanHandle — case-insensitivity
    // =========================================================================

    [Theory]
    [InlineData("Redis",    true)]
    [InlineData("redis",    true)]
    [InlineData("REDIS",    true)]
    [InlineData("Kafka",    false)]
    [InlineData("RabbitMQ", false)]
    [InlineData("",         false)]
    public void Provider_CanHandle_CaseInsensitive(string brokerType, bool expected)
    {
        var provider = new RedisProvider(NullLoggerFactory.Instance);
        var source   = new IngestionSource(brokerType, "redis://localhost:6379",
                                           new Dictionary<string, string>());

        Assert.Equal(expected, provider.CanHandle(source));
    }

    [Fact]
    public void Provider_CreateIngestor_ReturnsIngestorWithNameRedis()
    {
        var provider = new RedisProvider(NullLoggerFactory.Instance);
        var source   = new IngestionSource(
            "Redis",
            "redis://localhost:6379",
            new Dictionary<string, string> { ["keyDepth"] = "2" });

        var ingestor = provider.CreateIngestor(source);

        Assert.NotNull(ingestor);
        Assert.Equal("Redis", ingestor.Name);
    }
}
