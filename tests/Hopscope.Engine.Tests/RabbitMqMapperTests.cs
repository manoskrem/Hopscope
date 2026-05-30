using System.Text.Json;
using Hopscope.Application.Abstractions;
using Hopscope.Domain.Events;
using Hopscope.Infrastructure.Providers.RabbitMq;

namespace Hopscope.Engine.Tests;

/// <summary>
/// Unit tests for <see cref="RabbitMqMapper"/> and <see cref="RabbitMqProvider"/>.
/// No live broker required — all tests exercise pure mapping logic.
/// </summary>
public sealed class RabbitMqMapperTests
{
    private static readonly DateTimeOffset FixedNow =
        new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    // =========================================================================
    // BindingToEnvelope — topology mapping
    // =========================================================================

    [Fact]
    public void BindingToEnvelope_NormalBinding_CorrectSourceAndDestination()
    {
        var binding = new RmqBinding
        {
            Source          = "orders.exchange",
            Destination     = "orders.queue",
            DestinationType = "queue",
            RoutingKey      = "order.created",
            Vhost           = "/",
        };

        var envelope = RabbitMqMapper.BindingToEnvelope(binding, FixedNow);

        Assert.Equal("orders.exchange", envelope.Source);
        Assert.Equal("orders.queue",    envelope.Destination);
        Assert.Equal("RabbitMQ",        envelope.BrokerType);
        Assert.Equal(ExecutionStatus.Success, envelope.ExecutionStatus);
        Assert.Null(envelope.ErrorDetails);
    }

    [Fact]
    public void BindingToEnvelope_EmptySource_UsesDefaultExchangeLabel()
    {
        var binding = new RmqBinding
        {
            Source          = "",           // default exchange
            Destination     = "my.queue",
            DestinationType = "queue",
            RoutingKey      = "my.queue",
            Vhost           = "/",
        };

        var envelope = RabbitMqMapper.BindingToEnvelope(binding, FixedNow);

        Assert.Equal(RabbitMqMapper.DefaultExchangeLabel, envelope.Source);
    }

    [Fact]
    public void BindingToEnvelope_QueueDestination_DestinationKindIsQueue()
    {
        var binding = new RmqBinding
        {
            Source          = "ex",
            Destination     = "q",
            DestinationType = "queue",
            RoutingKey      = "rk",
            Vhost           = "/",
        };

        var envelope = RabbitMqMapper.BindingToEnvelope(binding, FixedNow);

        Assert.True(envelope.PayloadMetadata.TryGetValue("destinationKind", out var kind));
        Assert.Equal("Queue", kind);
    }

    [Fact]
    public void BindingToEnvelope_ExchangeDestination_DestinationKindIsExchange()
    {
        var binding = new RmqBinding
        {
            Source          = "fanout.ex",
            Destination     = "other.ex",
            DestinationType = "exchange",
            RoutingKey      = "",
            Vhost           = "/",
        };

        var envelope = RabbitMqMapper.BindingToEnvelope(binding, FixedNow);

        Assert.True(envelope.PayloadMetadata.TryGetValue("destinationKind", out var kind));
        Assert.Equal("Exchange", kind);
    }

    [Fact]
    public void BindingToEnvelope_RoutingKeyPresentInMetadata()
    {
        var binding = new RmqBinding
        {
            Source          = "ex",
            Destination     = "q",
            DestinationType = "queue",
            RoutingKey      = "order.#",
            Vhost           = "/",
        };

        var envelope = RabbitMqMapper.BindingToEnvelope(binding, FixedNow);

        Assert.True(envelope.PayloadMetadata.TryGetValue("routingKey", out var rk));
        Assert.Equal("order.#", rk);
    }

    [Fact]
    public void BindingToEnvelope_VhostPresentInMetadata()
    {
        var binding = new RmqBinding
        {
            Source          = "ex",
            Destination     = "q",
            DestinationType = "queue",
            RoutingKey      = "rk",
            Vhost           = "prod",
        };

        var envelope = RabbitMqMapper.BindingToEnvelope(binding, FixedNow);

        Assert.True(envelope.PayloadMetadata.TryGetValue("vhost", out var vh));
        Assert.Equal("prod", vh);
    }

    [Fact]
    public void BindingToEnvelope_StableHopId_SameBindingProducesSameHopId()
    {
        var binding = new RmqBinding
        {
            Source          = "ex",
            Destination     = "q",
            DestinationType = "queue",
            RoutingKey      = "rk",
            Vhost           = "/",
        };

        var env1 = RabbitMqMapper.BindingToEnvelope(binding, FixedNow);
        var env2 = RabbitMqMapper.BindingToEnvelope(binding, FixedNow.AddSeconds(30));

        Assert.Equal(env1.HopId, env2.HopId);
    }

    [Fact]
    public void BindingToEnvelope_DifferentRoutingKey_DifferentHopId()
    {
        var b1 = new RmqBinding { Source = "ex", Destination = "q", DestinationType = "queue", RoutingKey = "rk1", Vhost = "/" };
        var b2 = new RmqBinding { Source = "ex", Destination = "q", DestinationType = "queue", RoutingKey = "rk2", Vhost = "/" };

        var env1 = RabbitMqMapper.BindingToEnvelope(b1, FixedNow);
        var env2 = RabbitMqMapper.BindingToEnvelope(b2, FixedNow);

        Assert.NotEqual(env1.HopId, env2.HopId);
    }

    [Fact]
    public void BindingToEnvelope_DifferentVhost_DifferentHopId()
    {
        var b1 = new RmqBinding { Source = "ex", Destination = "q", DestinationType = "queue", RoutingKey = "rk", Vhost = "/a" };
        var b2 = new RmqBinding { Source = "ex", Destination = "q", DestinationType = "queue", RoutingKey = "rk", Vhost = "/b" };

        var env1 = RabbitMqMapper.BindingToEnvelope(b1, FixedNow);
        var env2 = RabbitMqMapper.BindingToEnvelope(b2, FixedNow);

        Assert.NotEqual(env1.HopId, env2.HopId);
    }

    // =========================================================================
    // QueueActivityToEnvelope — activity delta mapping
    // =========================================================================

    [Fact]
    public void QueueActivity_NoStats_ReturnsNull()
    {
        var queue = new RmqQueue { Name = "q", Vhost = "/", MessageStats = null };

        var result = RabbitMqMapper.QueueActivityToEnvelope(queue, null, 0, "ex", false, 1, FixedNow);

        Assert.Null(result);
    }

    [Fact]
    public void QueueActivity_FirstPoll_CountsIncreased_ReturnsEnvelope()
    {
        // First poll: prev is null, current has non-zero counters → activity detected.
        var queue = new RmqQueue
        {
            Name         = "orders.queue",
            Vhost        = "/",
            MessageStats = new RmqMessageStats { Publish = 10, DeliverGet = 5 },
        };

        var result = RabbitMqMapper.QueueActivityToEnvelope(queue, null, 0, "orders.exchange", false, 1, FixedNow);

        Assert.NotNull(result);
        Assert.Equal("orders.exchange", result!.Source);
        Assert.Equal("orders.queue",    result.Destination);
        Assert.Equal("RabbitMQ",        result.BrokerType);
        Assert.Equal(ExecutionStatus.Success, result.ExecutionStatus);
    }

    [Fact]
    public void QueueActivity_CountsUnchanged_ReturnsNull()
    {
        var stats = new RmqMessageStats { Publish = 10, DeliverGet = 5 };
        var queue = new RmqQueue { Name = "q", Vhost = "/", MessageStats = stats };

        // Same stats as prev → no delta.
        var result = RabbitMqMapper.QueueActivityToEnvelope(queue, stats, 0, "ex", false, 2, FixedNow);

        Assert.Null(result);
    }

    [Fact]
    public void QueueActivity_CountsIncreased_ReturnsFreshHopId()
    {
        var prev  = new RmqMessageStats { Publish = 5, DeliverGet = 3 };
        var queue = new RmqQueue
        {
            Name         = "q",
            Vhost        = "/",
            MessageStats = new RmqMessageStats { Publish = 10, DeliverGet = 7 },
        };

        var env1 = RabbitMqMapper.QueueActivityToEnvelope(queue, prev, 0, "ex", false, 10, FixedNow);
        var env2 = RabbitMqMapper.QueueActivityToEnvelope(queue, prev, 0, "ex", false, 11, FixedNow);

        Assert.NotNull(env1);
        Assert.NotNull(env2);
        // Different counter → different HopId (fresh per observation).
        Assert.NotEqual(env1!.HopId, env2!.HopId);
    }

    [Fact]
    public void QueueActivity_NoInboundSource_UsesDefaultExchangeLabel()
    {
        var queue = new RmqQueue
        {
            Name         = "q",
            Vhost        = "/",
            MessageStats = new RmqMessageStats { Publish = 1, DeliverGet = 0 },
        };

        var result = RabbitMqMapper.QueueActivityToEnvelope(queue, null, 0, null, false, 1, FixedNow);

        Assert.NotNull(result);
        Assert.Equal(RabbitMqMapper.DefaultExchangeLabel, result!.Source);
    }

    [Fact]
    public void QueueActivity_ActivityEnvelope_DestinationKindIsQueue()
    {
        var queue = new RmqQueue
        {
            Name         = "q",
            Vhost        = "/",
            MessageStats = new RmqMessageStats { Publish = 3, DeliverGet = 1 },
        };

        var result = RabbitMqMapper.QueueActivityToEnvelope(queue, null, 0, "ex", false, 1, FixedNow);

        Assert.NotNull(result);
        Assert.True(result!.PayloadMetadata.TryGetValue("destinationKind", out var kind));
        Assert.Equal("Queue", kind);
    }

    // =========================================================================
    // Dead-letter detection — TryGetDeadLetterExchange / IdentifyDeadLetterQueues
    // =========================================================================

    /// <summary>Self-contained JsonElement for a queue's <c>arguments</c> blob.</summary>
    private static System.Text.Json.JsonElement Args(string json) =>
        JsonSerializer.Deserialize<System.Text.Json.JsonElement>(json);

    [Fact]
    public void TryGetDeadLetterExchange_WithArg_ReturnsExchangeName()
    {
        var queue = new RmqQueue
        {
            Name      = "orders.main",
            Vhost     = "/",
            // arguments mix value types: a string DLX next to a numeric TTL.
            Arguments = Args("""{"x-dead-letter-exchange":"dlx","x-message-ttl":1000}"""),
        };

        Assert.Equal("dlx", RabbitMqMapper.TryGetDeadLetterExchange(queue));
    }

    [Fact]
    public void TryGetDeadLetterExchange_NoArguments_ReturnsNull()
    {
        // default(JsonElement) — ValueKind.Undefined — must not throw.
        var queue = new RmqQueue { Name = "q", Vhost = "/" };

        Assert.Null(RabbitMqMapper.TryGetDeadLetterExchange(queue));
    }

    [Fact]
    public void IdentifyDeadLetterQueues_QueueBoundToDlx_IsClassifiedWithItsExchange()
    {
        var queues = new List<RmqQueue>
        {
            new()
            {
                Name      = "orders.main",
                Vhost     = "/",
                Arguments = Args("""{"x-dead-letter-exchange":"dlx","x-message-ttl":1000}"""),
            },
            new() { Name = "orders.dlq", Vhost = "/" },   // no DLX arg of its own
        };
        var bindings = new List<RmqBinding>
        {
            new() { Source = "orders.ex", Destination = "orders.main", DestinationType = "queue", Vhost = "/" },
            new() { Source = "dlx",       Destination = "orders.dlq",  DestinationType = "queue", Vhost = "/" },
        };

        var dlqMap = RabbitMqMapper.IdentifyDeadLetterQueues(queues, bindings);

        // orders.dlq is bound to "dlx" (a declared DLX) → it is a DLQ, mapped to "dlx".
        Assert.True(dlqMap.TryGetValue("orders.dlq", out var dlx));
        Assert.Equal("dlx", dlx);
        // The main queue is fed by a normal exchange → NOT a DLQ.
        Assert.False(dlqMap.ContainsKey("orders.main"));
    }

    [Fact]
    public void IdentifyDeadLetterQueues_NoDeadLetterArgs_ReturnsEmpty()
    {
        var queues = new List<RmqQueue>
        {
            new() { Name = "a", Vhost = "/" },
            new() { Name = "b", Vhost = "/" },
        };
        var bindings = new List<RmqBinding>
        {
            new() { Source = "ex", Destination = "a", DestinationType = "queue", Vhost = "/" },
        };

        Assert.Empty(RabbitMqMapper.IdentifyDeadLetterQueues(queues, bindings));
    }

    // =========================================================================
    // QueueActivityToEnvelope — honest status mapping
    // =========================================================================

    [Fact]
    public void QueueActivity_DeadLetterQueue_DepthIncrease_YieldsDeadLetteredWithErrorDetails()
    {
        // Dead-lettered arrivals do NOT populate message_stats (it stays null on a DLQ);
        // the reliable signal is the queue depth growing.
        var queue = new RmqQueue
        {
            Name         = "orders.dlq",
            Vhost        = "/",
            Messages     = 3,       // 3 messages dead-lettered in since the last poll
            MessageStats = null,    // realistic for a DLQ
        };

        var env = RabbitMqMapper.QueueActivityToEnvelope(
            queue, previousStats: null, previousMessages: 0,
            inboundBindingSource: "dlx", isDeadLetterQueue: true, counter: 1, timestamp: FixedNow);

        Assert.NotNull(env);
        Assert.Equal(ExecutionStatus.DeadLettered, env!.ExecutionStatus);
        Assert.Equal("dlx",        env.Source);        // edge is DLX → DLQ
        Assert.Equal("orders.dlq", env.Destination);
        Assert.NotNull(env.ErrorDetails);
        Assert.Equal("DeadLettered", env.ErrorDetails!.ExceptionType);
        Assert.Contains("orders.dlq", env.ErrorDetails.Message);
        Assert.Null(env.ErrorDetails.TruncatedStackTrace);   // aggregate stats carry no trace
    }

    [Fact]
    public void QueueActivity_DeadLetterQueue_PublishDelta_AlsoYieldsDeadLettered()
    {
        // Secondary trigger: a broker that DOES report message_stats.publish on the DLQ.
        var prev  = new RmqMessageStats { Publish = 0 };
        var queue = new RmqQueue
        {
            Name         = "orders.dlq",
            Vhost        = "/",
            Messages     = 0,
            MessageStats = new RmqMessageStats { Publish = 4 },
        };

        var env = RabbitMqMapper.QueueActivityToEnvelope(
            queue, prev, previousMessages: 0,
            inboundBindingSource: "dlx", isDeadLetterQueue: true, counter: 1, timestamp: FixedNow);

        Assert.NotNull(env);
        Assert.Equal(ExecutionStatus.DeadLettered, env!.ExecutionStatus);
    }

    [Fact]
    public void QueueActivity_RedeliverDelta_YieldsRetrying()
    {
        var prev  = new RmqMessageStats { Publish = 10, DeliverGet = 10, Redeliver = 0 };
        var queue = new RmqQueue
        {
            Name         = "orders.queue",
            Vhost        = "/",
            // A consumer nacked/requeued → redeliver grew.
            MessageStats = new RmqMessageStats { Publish = 10, DeliverGet = 12, Redeliver = 2 },
        };

        var env = RabbitMqMapper.QueueActivityToEnvelope(
            queue, prev, previousMessages: 0,
            inboundBindingSource: "orders.ex", isDeadLetterQueue: false, counter: 1, timestamp: FixedNow);

        Assert.NotNull(env);
        Assert.Equal(ExecutionStatus.Retrying, env!.ExecutionStatus);
        Assert.Null(env.ErrorDetails);
    }

    [Fact]
    public void QueueActivity_NormalQueue_PlainThroughput_YieldsSuccess()
    {
        var prev  = new RmqMessageStats { Publish = 5, DeliverGet = 5, Redeliver = 0 };
        var queue = new RmqQueue
        {
            Name         = "orders.queue",
            Vhost        = "/",
            MessageStats = new RmqMessageStats { Publish = 8, DeliverGet = 8, Redeliver = 0 },
        };

        var env = RabbitMqMapper.QueueActivityToEnvelope(
            queue, prev, previousMessages: 0,
            inboundBindingSource: "orders.ex", isDeadLetterQueue: false, counter: 1, timestamp: FixedNow);

        Assert.NotNull(env);
        Assert.Equal(ExecutionStatus.Success, env!.ExecutionStatus);
        Assert.Null(env.ErrorDetails);
    }

    [Fact]
    public void QueueActivity_DeadLetterQueue_NoDepthIncrease_DoesNotDeadLetter()
    {
        // A DLQ that is being drained (depth fell) with only a delivery recorded is not a
        // fresh dead-letter event → must not be reported as DeadLettered.
        var prev  = new RmqMessageStats { DeliverGet = 0 };
        var queue = new RmqQueue
        {
            Name         = "orders.dlq",
            Vhost        = "/",
            Messages     = 2,                                       // fell from 3 (drained)
            MessageStats = new RmqMessageStats { DeliverGet = 1 },  // a delivery happened
        };

        var env = RabbitMqMapper.QueueActivityToEnvelope(
            queue, prev, previousMessages: 3,
            inboundBindingSource: "dlx", isDeadLetterQueue: true, counter: 1, timestamp: FixedNow);

        Assert.NotNull(env);
        Assert.Equal(ExecutionStatus.Success, env!.ExecutionStatus);
        Assert.Null(env.ErrorDetails);
    }

    // =========================================================================
    // BuildStableBindingHopId — determinism check
    // =========================================================================

    [Fact]
    public void BuildStableBindingHopId_DeterministicFormat()
    {
        var hopId = RabbitMqMapper.BuildStableBindingHopId("/", "ex", "q", "rk");
        Assert.Equal("binding:/:ex->q:rk", hopId);
    }

    // =========================================================================
    // NormalizeExchangeName
    // =========================================================================

    [Theory]
    [InlineData("",    RabbitMqMapper.DefaultExchangeLabel)]
    [InlineData("ex",  "ex")]
    [InlineData("amq.direct", "amq.direct")]
    public void NormalizeExchangeName_EmptyBecomesDefaultLabel(string input, string expected)
    {
        Assert.Equal(expected, RabbitMqMapper.NormalizeExchangeName(input));
    }

    // =========================================================================
    // RabbitMqProvider.CanHandle
    // =========================================================================

    [Theory]
    [InlineData("RabbitMQ",  true)]
    [InlineData("rabbitmq",  true)]
    [InlineData("RABBITMQ",  true)]
    [InlineData("Kafka",     false)]
    [InlineData("Redis",     false)]
    [InlineData("",          false)]
    public void Provider_CanHandle_CaseInsensitive(string brokerType, bool expected)
    {
        var provider = new RabbitMqProvider(Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance);
        var source   = new IngestionSource(brokerType, "http://localhost:15672", new Dictionary<string, string>());

        Assert.Equal(expected, provider.CanHandle(source));
    }

    [Fact]
    public void Provider_CreateIngestor_ReturnsRabbitMqIngestor()
    {
        var provider = new RabbitMqProvider(Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance);
        var source   = new IngestionSource(
            "RabbitMQ",
            "http://guest:guest@localhost:15672",
            new Dictionary<string, string> { ["pollSeconds"] = "5", ["vhost"] = "/" });

        var ingestor = provider.CreateIngestor(source);

        Assert.NotNull(ingestor);
        Assert.Equal("RabbitMQ", ingestor.Name);
    }
}
