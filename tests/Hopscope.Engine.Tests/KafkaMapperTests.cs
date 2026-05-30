using Hopscope.Domain.Events;
using Hopscope.Infrastructure.Providers.Kafka;
#if KAFKA
using Hopscope.Application.Abstractions;          // IngestionSource — provider tests only
using Microsoft.Extensions.Logging.Abstractions;  // NullLoggerFactory — provider tests only
#endif

namespace Hopscope.Engine.Tests;

/// <summary>
/// Unit tests for <see cref="KafkaMapper"/> and <see cref="KafkaProvider"/>.
/// No live broker required — all tests exercise pure mapping and parsing logic.
/// </summary>
public sealed class KafkaMapperTests
{
    private static readonly DateTimeOffset FixedNow =
        new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
        new Dictionary<string, string>(StringComparer.Ordinal);

    // =========================================================================
    // ConsumedMessageToEnvelope — core field correctness
    // =========================================================================

    [Fact]
    public void ConsumedMessageToEnvelope_HopId_Format()
    {
        var env = KafkaMapper.ConsumedMessageToEnvelope(
            topic: "orders", partition: 2, offset: 99,
            headers: EmptyHeaders, counter: 1, ts: FixedNow);

        Assert.Equal("kafka:orders:2:99", env.HopId);
    }

    [Fact]
    public void ConsumedMessageToEnvelope_Destination_IsTopicName()
    {
        var env = KafkaMapper.ConsumedMessageToEnvelope(
            topic: "payments", partition: 0, offset: 0,
            headers: EmptyHeaders, counter: 1, ts: FixedNow);

        Assert.Equal("payments", env.Destination);
    }

    [Fact]
    public void ConsumedMessageToEnvelope_BrokerType_IsKafka()
    {
        var env = KafkaMapper.ConsumedMessageToEnvelope(
            topic: "events", partition: 0, offset: 0,
            headers: EmptyHeaders, counter: 1, ts: FixedNow);

        Assert.Equal("Kafka", env.BrokerType);
    }

    [Fact]
    public void ConsumedMessageToEnvelope_MetadataContains_DestinationKindTopic()
    {
        var env = KafkaMapper.ConsumedMessageToEnvelope(
            topic: "orders", partition: 1, offset: 5,
            headers: EmptyHeaders, counter: 1, ts: FixedNow);

        Assert.True(env.PayloadMetadata.TryGetValue("destinationKind", out var dk));
        Assert.Equal("Topic", dk);
    }

    [Fact]
    public void ConsumedMessageToEnvelope_MetadataContains_PartitionAndOffset()
    {
        var env = KafkaMapper.ConsumedMessageToEnvelope(
            topic: "orders", partition: 3, offset: 42,
            headers: EmptyHeaders, counter: 1, ts: FixedNow);

        Assert.True(env.PayloadMetadata.TryGetValue("partition", out var p));
        Assert.Equal("3", p);
        Assert.True(env.PayloadMetadata.TryGetValue("offset", out var o));
        Assert.Equal("42", o);
    }

    [Fact]
    public void ConsumedMessageToEnvelope_DifferentOffsets_DifferentHopIds()
    {
        var env1 = KafkaMapper.ConsumedMessageToEnvelope(
            "orders", 0, 100, EmptyHeaders, 1, FixedNow);
        var env2 = KafkaMapper.ConsumedMessageToEnvelope(
            "orders", 0, 101, EmptyHeaders, 2, FixedNow);

        Assert.NotEqual(env1.HopId, env2.HopId);
    }

    [Fact]
    public void ConsumedMessageToEnvelope_DifferentPartitions_DifferentHopIds()
    {
        var env1 = KafkaMapper.ConsumedMessageToEnvelope(
            "orders", 0, 100, EmptyHeaders, 1, FixedNow);
        var env2 = KafkaMapper.ConsumedMessageToEnvelope(
            "orders", 1, 100, EmptyHeaders, 2, FixedNow);

        Assert.NotEqual(env1.HopId, env2.HopId);
    }

    [Fact]
    public void ConsumedMessageToEnvelope_Timestamp_Preserved()
    {
        var env = KafkaMapper.ConsumedMessageToEnvelope(
            "t", 0, 0, EmptyHeaders, 1, FixedNow);
        Assert.Equal(FixedNow, env.Timestamp);
    }

    // =========================================================================
    // traceparent parsing
    // =========================================================================

    [Fact]
    public void ParseTraceParent_ValidHeader_ExtractsTraceIdAndParentSpanId()
    {
        const string tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

        var (traceId, parentSpanId) = KafkaMapper.ParseTraceParent(tp);

        Assert.Equal("4bf92f3577b34da6a3ce929d0e0e4736", traceId);
        Assert.Equal("00f067aa0ba902b7", parentSpanId);
    }

    [Theory]
    [InlineData("")]                                              // empty
    [InlineData("not-a-traceparent")]                            // wrong format
    [InlineData("00-ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ-00f067aa0ba902b7-01")] // non-hex traceId
    [InlineData("00-4bf92f3577b34da6a3ce929d0e0e4736-ZZZZZZZZZZZZZZZZ-01")] // non-hex spanId
    [InlineData("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")] // version != 00
    [InlineData("00-00000000000000000000000000000000-00f067aa0ba902b7-01")] // all-zero traceId invalid
    public void ParseTraceParent_Malformed_ReturnsNullNull(string tp)
    {
        var (traceId, parentSpanId) = KafkaMapper.ParseTraceParent(tp);
        Assert.Null(traceId);
        Assert.Null(parentSpanId);
    }

    // =========================================================================
    // Header precedence for trace correlation
    // =========================================================================

    [Fact]
    public void TraceCorrelation_TraceparentPresent_UsedOverXTraceId()
    {
        var headers = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["traceparent"] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            ["X-Trace-Id"]  = "should-not-be-used",
        };

        var env = KafkaMapper.ConsumedMessageToEnvelope(
            "orders", 0, 0, headers, 1, FixedNow);

        Assert.Equal("4bf92f3577b34da6a3ce929d0e0e4736", env.TraceId);
        Assert.Equal("00f067aa0ba902b7", env.ParentHopId);
    }

    [Fact]
    public void TraceCorrelation_OnlyXTraceId_UsedWhenNoTraceparent()
    {
        var headers = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["X-Trace-Id"]      = "my-custom-trace-id",
            ["X-Parent-Hop-Id"] = "my-parent-hop",
        };

        var env = KafkaMapper.ConsumedMessageToEnvelope(
            "orders", 0, 0, headers, 1, FixedNow);

        Assert.Equal("my-custom-trace-id", env.TraceId);
        Assert.Equal("my-parent-hop",      env.ParentHopId);
    }

    [Fact]
    public void TraceCorrelation_NoHeaders_TraceIdIsStablePerTopic_ParentHopIdNull()
    {
        var env = KafkaMapper.ConsumedMessageToEnvelope(
            "orders", 0, 0, EmptyHeaders, 1, FixedNow);

        Assert.Equal("kafka:orders", env.TraceId);
        Assert.Null(env.ParentHopId);
    }

    [Fact]
    public void TraceCorrelation_NoHeaders_TraceId_StableAcrossMessages()
    {
        // High-volume traffic on the same topic must share one TraceId so the
        // aggregator trace-LRU cardinality stays bounded.
        var env1 = KafkaMapper.ConsumedMessageToEnvelope("orders", 0, 0,  EmptyHeaders, 1, FixedNow);
        var env2 = KafkaMapper.ConsumedMessageToEnvelope("orders", 0, 1,  EmptyHeaders, 2, FixedNow);
        var env3 = KafkaMapper.ConsumedMessageToEnvelope("orders", 1, 99, EmptyHeaders, 3, FixedNow);

        Assert.Equal(env1.TraceId, env2.TraceId);
        Assert.Equal(env1.TraceId, env3.TraceId);
        // But HopIds must remain distinct.
        Assert.NotEqual(env1.HopId, env2.HopId);
        Assert.NotEqual(env1.HopId, env3.HopId);
    }

    [Fact]
    public void TraceCorrelation_MalformedTraceparent_FallsBackToStable()
    {
        var headers = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["traceparent"] = "not-valid",
        };

        var env = KafkaMapper.ConsumedMessageToEnvelope(
            "orders", 0, 0, headers, 1, FixedNow);

        // Malformed traceparent → falls through to stable fallback.
        Assert.Equal("kafka:orders", env.TraceId);
        Assert.Null(env.ParentHopId);
    }

    // =========================================================================
    // DLQ suffix detection
    // =========================================================================

    [Theory]
    [InlineData("orders.dlq",  ExecutionStatus.DeadLettered)]
    [InlineData("orders.DLQ",  ExecutionStatus.DeadLettered)]
    [InlineData("orders.DLT",  ExecutionStatus.DeadLettered)]
    [InlineData("orders.dlt",  ExecutionStatus.DeadLettered)]
    [InlineData("orders",      ExecutionStatus.Success)]
    [InlineData("orders.dead", ExecutionStatus.Success)]   // not a recognised suffix
    [InlineData("dlq.orders",  ExecutionStatus.Success)]   // suffix only, not prefix
    public void DlqSuffix_CorrectStatusAndErrorDetails(string topic, ExecutionStatus expected)
    {
        var env = KafkaMapper.ConsumedMessageToEnvelope(
            topic, 0, 0, EmptyHeaders, 1, FixedNow);

        Assert.Equal(expected, env.ExecutionStatus);

        if (expected == ExecutionStatus.DeadLettered)
        {
            Assert.NotNull(env.ErrorDetails);
            Assert.Equal("DeadLettered", env.ErrorDetails!.ExceptionType);
            Assert.Contains(topic, env.ErrorDetails.Message, StringComparison.Ordinal);
            Assert.Null(env.ErrorDetails.TruncatedStackTrace);
        }
        else
        {
            Assert.Null(env.ErrorDetails);
        }
    }

    // =========================================================================
    // Privacy — no message key or value in PayloadMetadata
    // =========================================================================

    [Fact]
    public void ConsumedMessageToEnvelope_PayloadMetadata_NeverContainsMessageKeyOrValue()
    {
        // The mapper does not even accept a key/value — this test asserts that the
        // allowed metadata keys are restricted to the routing/trace set and that
        // no caller-supplied "body-like" header values leak through.
        const string sensitiveValue = "super-secret-payload-body";
        var headers = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["X-Trace-Id"] = "trace-abc",
            // Simulate a header that should NOT appear as metadata value.
            ["X-Sensitive"] = sensitiveValue,
        };

        var env = KafkaMapper.ConsumedMessageToEnvelope(
            "orders", 0, 0, headers, 1, FixedNow);

        // Sensitive header value must not appear anywhere in PayloadMetadata.
        foreach (var (k, v) in env.PayloadMetadata)
        {
            Assert.False(
                v.Contains(sensitiveValue, StringComparison.Ordinal),
                $"PayloadMetadata[{k}] = '{v}' contains sensitive value.");
        }

        // Allowed metadata keys are a known set.
        var allowedKeys = new HashSet<string>(StringComparer.Ordinal)
        {
            "destinationKind", "sourceKind", "partition", "offset",
            "traceparent", "X-Trace-Id",
        };
        foreach (var k in env.PayloadMetadata.Keys)
        {
            Assert.True(allowedKeys.Contains(k),
                $"Unexpected PayloadMetadata key '{k}' — possible body/key leak.");
        }
    }

    // =========================================================================
    // TopicMetadataToEnvelope
    // =========================================================================

    [Fact]
    public void TopicMetadataToEnvelope_StableHopId_SameAcrossCalls()
    {
        var env1 = KafkaMapper.TopicMetadataToEnvelope("kafka:9092", "orders", FixedNow);
        var env2 = KafkaMapper.TopicMetadataToEnvelope("kafka:9092", "orders",
                                                        FixedNow.AddSeconds(30));

        Assert.Equal(env1.HopId, env2.HopId);
        Assert.Equal("kafka-topic:orders", env1.HopId);
    }

    [Fact]
    public void TopicMetadataToEnvelope_Source_IsKafkaCluster()
    {
        var env = KafkaMapper.TopicMetadataToEnvelope("broker.example.com:9092", "events", FixedNow);
        Assert.Equal("kafka-cluster", env.Source);
    }

    [Fact]
    public void TopicMetadataToEnvelope_Destination_IsTopic()
    {
        var env = KafkaMapper.TopicMetadataToEnvelope("kafka:9092", "payments", FixedNow);
        Assert.Equal("payments", env.Destination);
    }

    [Fact]
    public void TopicMetadataToEnvelope_DestinationKind_IsTopic()
    {
        var env = KafkaMapper.TopicMetadataToEnvelope("kafka:9092", "orders", FixedNow);
        Assert.True(env.PayloadMetadata.TryGetValue("destinationKind", out var dk));
        Assert.Equal("Topic", dk);
    }

    [Fact]
    public void TopicMetadataToEnvelope_DifferentTopics_DifferentHopIds()
    {
        var env1 = KafkaMapper.TopicMetadataToEnvelope("kafka:9092", "orders",   FixedNow);
        var env2 = KafkaMapper.TopicMetadataToEnvelope("kafka:9092", "payments", FixedNow);
        Assert.NotEqual(env1.HopId, env2.HopId);
    }

    [Fact]
    public void TopicMetadataToEnvelope_ExecutionStatus_IsSuccess()
    {
        var env = KafkaMapper.TopicMetadataToEnvelope("kafka:9092", "orders", FixedNow);
        Assert.Equal(ExecutionStatus.Success, env.ExecutionStatus);
        Assert.Null(env.ErrorDetails);
    }

    // =========================================================================
    // KafkaProvider.CanHandle — case-insensitivity
    //
    // KafkaProvider depends on Confluent.Kafka (via KafkaIngestor), so it only exists in
    // the Kafka opt-in build. These tests compile/run under -p:EnableKafka=true; the pure
    // KafkaMapper tests above always run. (Run: dotnet test -p:EnableKafka=true)
    // =========================================================================
#if KAFKA
    [Theory]
    [InlineData("Kafka",    true)]
    [InlineData("kafka",    true)]
    [InlineData("KAFKA",    true)]
    [InlineData("Redis",    false)]
    [InlineData("RabbitMQ", false)]
    [InlineData("",         false)]
    public void Provider_CanHandle_CaseInsensitive(string brokerType, bool expected)
    {
        var provider = new KafkaProvider(NullLoggerFactory.Instance);
        var source   = new IngestionSource(brokerType, "kafka:9092",
                                           new Dictionary<string, string>());

        Assert.Equal(expected, provider.CanHandle(source));
    }

    [Fact]
    public void Provider_CreateIngestor_ReturnsIngestorWithNameKafka()
    {
        var provider = new KafkaProvider(NullLoggerFactory.Instance);
        var source   = new IngestionSource(
            "Kafka",
            "kafka:9092",
            new Dictionary<string, string> { ["group"] = "test-group" });

        var ingestor = provider.CreateIngestor(source);

        Assert.NotNull(ingestor);
        Assert.Equal("Kafka", ingestor.Name);
    }
#endif

    // =========================================================================
    // ExtractTraceCorrelation — direct helper tests
    // =========================================================================

    [Fact]
    public void ExtractTraceCorrelation_ValidTraceparent_ReturnsParsedIds()
    {
        var headers = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["traceparent"] = "00-aabbccddeeff00112233445566778899-1122334455667788-00",
        };

        var (traceId, parentHopId) = KafkaMapper.ExtractTraceCorrelation("mytopic", headers);

        Assert.Equal("aabbccddeeff00112233445566778899", traceId);
        Assert.Equal("1122334455667788", parentHopId);
    }

    [Fact]
    public void ExtractTraceCorrelation_NoHeaders_ReturnStablePerTopicTrace()
    {
        var (traceId, parentHopId) = KafkaMapper.ExtractTraceCorrelation("mytopic", EmptyHeaders);

        Assert.Equal("kafka:mytopic", traceId);
        Assert.Null(parentHopId);
    }
}
