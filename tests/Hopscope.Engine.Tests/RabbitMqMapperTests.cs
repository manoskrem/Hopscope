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

        var result = RabbitMqMapper.QueueActivityToEnvelope(queue, null, "ex", 1, FixedNow);

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

        var result = RabbitMqMapper.QueueActivityToEnvelope(queue, null, "orders.exchange", 1, FixedNow);

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
        var result = RabbitMqMapper.QueueActivityToEnvelope(queue, stats, "ex", 2, FixedNow);

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

        var env1 = RabbitMqMapper.QueueActivityToEnvelope(queue, prev, "ex", 10, FixedNow);
        var env2 = RabbitMqMapper.QueueActivityToEnvelope(queue, prev, "ex", 11, FixedNow);

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

        var result = RabbitMqMapper.QueueActivityToEnvelope(queue, null, null, 1, FixedNow);

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

        var result = RabbitMqMapper.QueueActivityToEnvelope(queue, null, "ex", 1, FixedNow);

        Assert.NotNull(result);
        Assert.True(result!.PayloadMetadata.TryGetValue("destinationKind", out var kind));
        Assert.Equal("Queue", kind);
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
