using Google.Protobuf.WellKnownTypes;
using Hopscope.Domain.Events;
using Hopscope.Infrastructure.Providers.Agent;
using ProtoEnvelope = Hopscope.Contracts.V1.EventEnvelope;
using ProtoStatus = Hopscope.Contracts.V1.ExecutionStatus;
using ProtoErrorDetails = Hopscope.Contracts.V1.ErrorDetails;

namespace Hopscope.Engine.Tests;

/// <summary>
/// Tests for <see cref="AgentMapper"/> — the pure proto-EventEnvelope → domain-EventEnvelope mapper.
/// Constructs generated protobuf envelopes and asserts the null/empty/reject seam plus
/// timestamp/metadata/status/error mapping.
/// </summary>
public sealed class AgentMapperTests
{
    private static ProtoEnvelope Valid() => new()
    {
        TraceId         = "t1",
        HopId           = "h1",
        Source          = "svc-a",
        Destination     = "queue-x",
        BrokerType      = "RabbitMQ",
        ExecutionStatus = ProtoStatus.Success,
        Timestamp       = Timestamp.FromDateTimeOffset(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)),
    };

    [Fact]
    public void Map_ValidEnvelope_MapsCoreFields()
    {
        var env = AgentMapper.Map(Valid());

        Assert.NotNull(env);
        Assert.Equal("t1",       env!.TraceId);
        Assert.Equal("h1",       env.HopId);
        Assert.Equal("svc-a",    env.Source);
        Assert.Equal("queue-x",  env.Destination);
        Assert.Equal("RabbitMQ", env.BrokerType);
        Assert.Equal(ExecutionStatus.Success, env.ExecutionStatus);
    }

    [Theory]
    [InlineData("",  "h1")]
    [InlineData(" ", "h1")]
    [InlineData("t1", "")]
    [InlineData("t1", " ")]
    public void Map_EmptyOrWhitespaceTraceOrHop_ReturnsNull(string trace, string hop)
    {
        var proto = Valid();
        proto.TraceId = trace;
        proto.HopId   = hop;

        Assert.Null(AgentMapper.Map(proto));
    }

    [Theory]
    [InlineData("",  "queue-x")]
    [InlineData(" ", "queue-x")]
    [InlineData("svc-a", "")]
    [InlineData("svc-a", " ")]
    public void Map_EmptyOrWhitespaceSourceOrDestination_ReturnsNull(string source, string destination)
    {
        var proto = Valid();
        proto.Source      = source;
        proto.Destination = destination;

        Assert.Null(AgentMapper.Map(proto));
    }

    [Fact]
    public void Map_EmptyParentHopId_ParentNull()
    {
        // proto3 string defaults to "" — never set ParentHopId.
        var env = AgentMapper.Map(Valid());

        Assert.NotNull(env);
        Assert.Null(env!.ParentHopId);
    }

    [Fact]
    public void Map_ParentHopId_PassesThrough()
    {
        var proto = Valid();
        proto.ParentHopId = "parent-1";

        var env = AgentMapper.Map(proto);

        Assert.NotNull(env);
        Assert.Equal("parent-1", env!.ParentHopId);
    }

    [Fact]
    public void Map_NoErrorDetails_ErrorDetailsNull()
    {
        // ErrorDetails is a message field — null when never set.
        var env = AgentMapper.Map(Valid());

        Assert.NotNull(env);
        Assert.Null(env!.ErrorDetails);
    }

    [Fact]
    public void Map_WithErrorDetails_Mapped()
    {
        var proto = Valid();
        proto.ExecutionStatus = ProtoStatus.Failed;
        proto.ErrorDetails = new ProtoErrorDetails
        {
            ExceptionType       = "TimeoutException",
            Message             = "downstream timed out",
            TruncatedStackTrace = "at Foo.Bar()",
        };

        var env = AgentMapper.Map(proto);

        Assert.NotNull(env);
        Assert.Equal(ExecutionStatus.Failed, env!.ExecutionStatus);
        Assert.NotNull(env.ErrorDetails);
        Assert.Equal("TimeoutException",     env.ErrorDetails!.ExceptionType);
        Assert.Equal("downstream timed out", env.ErrorDetails.Message);
        Assert.Equal("at Foo.Bar()",         env.ErrorDetails.TruncatedStackTrace);
    }

    [Fact]
    public void Map_ErrorDetailsEmptyStack_TruncatedStackTraceNull()
    {
        var proto = Valid();
        proto.ErrorDetails = new ProtoErrorDetails
        {
            ExceptionType = "Boom",
            Message       = "msg",
            // TruncatedStackTrace left as "" (proto3 default).
        };

        var env = AgentMapper.Map(proto);

        Assert.NotNull(env);
        Assert.NotNull(env!.ErrorDetails);
        Assert.Null(env.ErrorDetails!.TruncatedStackTrace);
    }

    [Fact]
    public void Map_UnsetTimestamp_FallsBackToUtcNow()
    {
        var proto = Valid();
        proto.Timestamp = null; // never set on the wire

        var before = DateTimeOffset.UtcNow;
        var env    = AgentMapper.Map(proto);
        var after  = DateTimeOffset.UtcNow;

        Assert.NotNull(env);
        Assert.InRange(env!.Timestamp, before.AddSeconds(-1), after.AddSeconds(1));
    }

    [Fact]
    public void Map_ZeroTimestamp_FallsBackToUtcNow()
    {
        var proto = Valid();
        proto.Timestamp = new Timestamp { Seconds = 0, Nanos = 0 };

        var before = DateTimeOffset.UtcNow;
        var env    = AgentMapper.Map(proto);
        var after  = DateTimeOffset.UtcNow;

        Assert.NotNull(env);
        Assert.InRange(env!.Timestamp, before.AddSeconds(-1), after.AddSeconds(1));
    }

    [Fact]
    public void Map_SetTimestamp_RoundTrips()
    {
        var when  = new DateTimeOffset(2026, 5, 31, 12, 0, 0, TimeSpan.Zero);
        var proto = Valid();
        proto.Timestamp = Timestamp.FromDateTimeOffset(when);

        var env = AgentMapper.Map(proto);

        Assert.NotNull(env);
        Assert.Equal(when, env!.Timestamp);
    }

    [Fact]
    public void Map_PayloadMetadata_CopiedFaithfully()
    {
        var proto = Valid();
        proto.PayloadMetadata.Add("routingKey", "orders.created");
        proto.PayloadMetadata.Add("contentType", "application/json");

        var env = AgentMapper.Map(proto);

        Assert.NotNull(env);
        Assert.Equal(2, env!.PayloadMetadata.Count);
        Assert.Equal("orders.created",   env.PayloadMetadata["routingKey"]);
        Assert.Equal("application/json", env.PayloadMetadata["contentType"]);
    }

    [Theory]
    [InlineData(ProtoStatus.Success,      ExecutionStatus.Success)]
    [InlineData(ProtoStatus.Retrying,     ExecutionStatus.Retrying)]
    [InlineData(ProtoStatus.DeadLettered, ExecutionStatus.DeadLettered)]
    [InlineData(ProtoStatus.Failed,       ExecutionStatus.Failed)]
    public void Map_Status_MapsByOrdinal(ProtoStatus proto, ExecutionStatus expected)
    {
        var p = Valid();
        p.ExecutionStatus = proto;

        var env = AgentMapper.Map(p);

        Assert.NotNull(env);
        Assert.Equal(expected, env!.ExecutionStatus);
    }

    [Fact]
    public void Map_EmptyBrokerType_DefaultsToAgent()
    {
        var proto = Valid();
        proto.BrokerType = ""; // proto3 default

        var env = AgentMapper.Map(proto);

        Assert.NotNull(env);
        Assert.Equal("Agent", env!.BrokerType);
    }

    [Fact]
    public void Map_LongStackTrace_TruncatedToCap()
    {
        var proto = Valid();
        proto.ErrorDetails = new ProtoErrorDetails
        {
            ExceptionType       = "Boom",
            Message             = "m",
            TruncatedStackTrace = new string('x', AgentMapper.StackTraceCap + 500),
        };

        var env = AgentMapper.Map(proto);

        Assert.NotNull(env);
        Assert.NotNull(env!.ErrorDetails);
        Assert.Equal(AgentMapper.StackTraceCap, env.ErrorDetails!.TruncatedStackTrace!.Length);
    }
}
