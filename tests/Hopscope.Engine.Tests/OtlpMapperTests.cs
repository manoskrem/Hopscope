using Hopscope.Domain.Events;
using Hopscope.Infrastructure.Providers.Otlp;

namespace Hopscope.Engine.Tests;

/// <summary>
/// Unit tests for <see cref="OtlpMapper"/>. No live broker / gRPC required — the mapper
/// takes primitives (byte ids, string dicts), so spans are simulated directly.
/// This is the provider that introduces real per-trace Failed + ErrorDetails, so the
/// error-span mapping and the id-validation guard are covered thoroughly.
/// </summary>
public sealed class OtlpMapperTests
{
    private static readonly DateTimeOffset FixedNow =
        new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static readonly IReadOnlyDictionary<string, string> NoAttrs =
        new Dictionary<string, string>(StringComparer.Ordinal);

    // 16-byte trace id, 8-byte span ids (valid widths).
    private static byte[] Trace16(byte fill = 0xAB)
    {
        var b = new byte[16];
        for (var i = 0; i < 16; i++) b[i] = (byte)(fill + i);
        return b;
    }

    private static byte[] Span8(byte fill = 0x11)
    {
        var b = new byte[8];
        for (var i = 0; i < 8; i++) b[i] = (byte)(fill + i);
        return b;
    }

    // =========================================================================
    // Identity: hex encoding + validation guard (the must-fix)
    // =========================================================================

    [Fact]
    public void SpanToEnvelope_ValidIds_ProducesHexTraceAndHop()
    {
        var env = OtlpMapper.SpanToEnvelope(
            Trace16(), Span8(), parentSpanId: [], spanName: "op",
            resourceServiceName: "svc", attributes: NoAttrs,
            statusCode: 0, exceptionEvent: null, ts: FixedNow);

        Assert.NotNull(env);
        Assert.Equal(32, env!.TraceId.Length);   // 16 bytes → 32 hex chars
        Assert.Equal(16, env.HopId.Length);       // 8 bytes  → 16 hex chars
        // Lower-hex only.
        Assert.DoesNotContain(env.TraceId, c => char.IsUpper(c));
    }

    [Theory]
    [InlineData(0)]    // empty trace id
    [InlineData(8)]    // wrong length (too short)
    [InlineData(15)]   // off-by-one
    public void SpanToEnvelope_InvalidTraceIdLength_ReturnsNull(int traceLen)
    {
        var env = OtlpMapper.SpanToEnvelope(
            new byte[traceLen], Span8(), [], "op", "svc", NoAttrs, 0, null, FixedNow);
        Assert.Null(env);
    }

    [Theory]
    [InlineData(0)]    // empty span id
    [InlineData(4)]    // wrong length
    public void SpanToEnvelope_InvalidSpanIdLength_ReturnsNull(int spanLen)
    {
        var env = OtlpMapper.SpanToEnvelope(
            Trace16(), new byte[spanLen], [], "op", "svc", NoAttrs, 0, null, FixedNow);
        Assert.Null(env);
    }

    [Fact]
    public void SpanToEnvelope_AllZeroTraceId_ReturnsNull()
    {
        var env = OtlpMapper.SpanToEnvelope(
            new byte[16], Span8(), [], "op", "svc", NoAttrs, 0, null, FixedNow);
        Assert.Null(env);   // all-zero trace id is invalid per the proto
    }

    [Fact]
    public void SpanToEnvelope_AllZeroSpanId_ReturnsNull()
    {
        var env = OtlpMapper.SpanToEnvelope(
            Trace16(), new byte[8], [], "op", "svc", NoAttrs, 0, null, FixedNow);
        Assert.Null(env);
    }

    // =========================================================================
    // ParentHopId: the proto null↔empty seam
    // =========================================================================

    [Fact]
    public void SpanToEnvelope_EmptyParent_ParentHopIdNull()
    {
        var env = OtlpMapper.SpanToEnvelope(
            Trace16(), Span8(), parentSpanId: [], spanName: "root",
            resourceServiceName: "svc", attributes: NoAttrs,
            statusCode: 0, exceptionEvent: null, ts: FixedNow);

        Assert.NotNull(env);
        Assert.Null(env!.ParentHopId);
    }

    [Fact]
    public void SpanToEnvelope_AllZeroParent_ParentHopIdNull()
    {
        var env = OtlpMapper.SpanToEnvelope(
            Trace16(), Span8(), parentSpanId: new byte[8], spanName: "root",
            resourceServiceName: "svc", attributes: NoAttrs,
            statusCode: 0, exceptionEvent: null, ts: FixedNow);

        Assert.NotNull(env);
        Assert.Null(env!.ParentHopId);   // all-zero parent == root
    }

    [Fact]
    public void SpanToEnvelope_RealParent_ParentHopIdIs16Hex()
    {
        var parent = Span8(0x22);
        var env = OtlpMapper.SpanToEnvelope(
            Trace16(), Span8(), parentSpanId: parent, spanName: "child",
            resourceServiceName: "svc", attributes: NoAttrs,
            statusCode: 0, exceptionEvent: null, ts: FixedNow);

        Assert.NotNull(env);
        Assert.NotNull(env!.ParentHopId);
        Assert.Equal(16, env.ParentHopId!.Length);
        Assert.Equal(OtlpMapper.ToHexLower(parent), env.ParentHopId);
    }

    // =========================================================================
    // ExecutionStatus + ErrorDetails — REAL per-trace Failed data
    // =========================================================================

    [Fact]
    public void SpanToEnvelope_ErrorStatusWithExceptionEvent_FailedWithErrorDetails()
    {
        var env = OtlpMapper.SpanToEnvelope(
            Trace16(), Span8(), [], "charge",
            resourceServiceName: "payment-svc", attributes: NoAttrs,
            statusCode: 2,   // STATUS_CODE_ERROR
            exceptionEvent: ("PaymentGatewayException", "Gateway timeout", "at Pay.Charge()"),
            ts: FixedNow);

        Assert.NotNull(env);
        Assert.Equal(ExecutionStatus.Failed, env!.ExecutionStatus);
        Assert.NotNull(env.ErrorDetails);
        Assert.Equal("PaymentGatewayException", env.ErrorDetails!.ExceptionType);
        Assert.Equal("Gateway timeout",         env.ErrorDetails.Message);
        Assert.Equal("at Pay.Charge()",         env.ErrorDetails.TruncatedStackTrace);
    }

    [Fact]
    public void SpanToEnvelope_ErrorStatusNoExceptionEvent_FailedWithSynthesizedDetails()
    {
        var env = OtlpMapper.SpanToEnvelope(
            Trace16(), Span8(), [], "checkout",
            resourceServiceName: "svc", attributes: NoAttrs,
            statusCode: 2, exceptionEvent: null, ts: FixedNow);

        Assert.NotNull(env);
        Assert.Equal(ExecutionStatus.Failed, env!.ExecutionStatus);
        Assert.NotNull(env.ErrorDetails);
        Assert.Equal("SpanError", env.ErrorDetails!.ExceptionType);
        Assert.Equal("checkout",  env.ErrorDetails.Message);   // falls back to span name
        Assert.Null(env.ErrorDetails.TruncatedStackTrace);
    }

    [Theory]
    [InlineData(0)]   // STATUS_CODE_UNSET
    [InlineData(1)]   // STATUS_CODE_OK
    public void SpanToEnvelope_NonErrorStatus_SuccessNoErrorDetails(int statusCode)
    {
        var env = OtlpMapper.SpanToEnvelope(
            Trace16(), Span8(), [], "op", "svc", NoAttrs, statusCode, null, FixedNow);

        Assert.NotNull(env);
        Assert.Equal(ExecutionStatus.Success, env!.ExecutionStatus);
        Assert.Null(env.ErrorDetails);
    }

    [Fact]
    public void SpanToEnvelope_LongStackTrace_TruncatedToCap()
    {
        var hugeStack = new string('x', OtlpMapper.StackTraceCap + 5000);
        var env = OtlpMapper.SpanToEnvelope(
            Trace16(), Span8(), [], "op", "svc", NoAttrs,
            statusCode: 2,
            exceptionEvent: ("E", "m", hugeStack),
            ts: FixedNow);

        Assert.NotNull(env);
        Assert.NotNull(env!.ErrorDetails!.TruncatedStackTrace);
        Assert.Equal(OtlpMapper.StackTraceCap, env.ErrorDetails.TruncatedStackTrace!.Length);
    }

    // =========================================================================
    // Source / Destination / topology kinds
    // =========================================================================

    [Fact]
    public void SpanToEnvelope_ResourceServiceName_BecomesSource()
    {
        var env = OtlpMapper.SpanToEnvelope(
            Trace16(), Span8(), [], "op",
            resourceServiceName: "order-svc", attributes: NoAttrs,
            statusCode: 0, exceptionEvent: null, ts: FixedNow);

        Assert.NotNull(env);
        Assert.Equal("order-svc", env!.Source);
    }

    [Fact]
    public void SpanToEnvelope_NoResourceName_FallsBackToServiceNameAttr()
    {
        var attrs = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["service.name"] = "attr-svc",
        };
        var env = OtlpMapper.SpanToEnvelope(
            Trace16(), Span8(), [], "op",
            resourceServiceName: null, attributes: attrs,
            statusCode: 0, exceptionEvent: null, ts: FixedNow);

        Assert.NotNull(env);
        Assert.Equal("attr-svc", env!.Source);
    }

    [Fact]
    public void SpanToEnvelope_NoSourceAnywhere_FallsBackToOtlpService()
    {
        var env = OtlpMapper.SpanToEnvelope(
            Trace16(), Span8(), [], "op", null, NoAttrs, 0, null, FixedNow);

        Assert.NotNull(env);
        Assert.Equal("otlp-service", env!.Source);
    }

    [Fact]
    public void SpanToEnvelope_MessagingAttrs_DestinationKindTopic_BrokerFromSystem()
    {
        var attrs = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["messaging.system"]           = "kafka",
            ["messaging.destination.name"] = "orders",
        };
        var env = OtlpMapper.SpanToEnvelope(
            Trace16(), Span8(), [], "send", "producer-svc", attrs, 0, null, FixedNow);

        Assert.NotNull(env);
        Assert.Equal("orders", env!.Destination);
        Assert.Equal("kafka",  env.BrokerType);
        Assert.True(env.PayloadMetadata.TryGetValue("destinationKind", out var dk));
        Assert.Equal("Topic", dk);
    }

    [Fact]
    public void SpanToEnvelope_NoMessaging_DestinationKindService_BrokerOtlp()
    {
        var env = OtlpMapper.SpanToEnvelope(
            Trace16(), Span8(), [], "GET /api", "web-svc", NoAttrs, 0, null, FixedNow);

        Assert.NotNull(env);
        Assert.Equal("GET /api", env!.Destination);   // falls back to span name
        Assert.Equal("OTLP", env.BrokerType);
        Assert.True(env.PayloadMetadata.TryGetValue("destinationKind", out var dk));
        Assert.Equal("Service", dk);
    }

    // =========================================================================
    // PayloadMetadata is an allowlist — never wholesale attribute copy / bodies
    // =========================================================================

    [Fact]
    public void SpanToEnvelope_PayloadMetadata_AllowlistOnly_NoBodyLeak()
    {
        const string sensitive = "super-secret-card-number";
        var attrs = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["messaging.system"] = "rabbitmq",   // allowlisted
            ["http.request.body"] = sensitive,    // NOT allowlisted — must not appear
            ["db.statement"]      = sensitive,    // NOT allowlisted
        };
        var env = OtlpMapper.SpanToEnvelope(
            Trace16(), Span8(), [], "op", "svc", attrs, 0, null, FixedNow);

        Assert.NotNull(env);
        foreach (var (k, v) in env!.PayloadMetadata)
        {
            Assert.False(v.Contains(sensitive, StringComparison.Ordinal),
                $"PayloadMetadata[{k}] leaked a non-allowlisted attribute value.");
        }
        // The allowlisted one IS present.
        Assert.True(env.PayloadMetadata.TryGetValue("messaging.system", out var sys));
        Assert.Equal("rabbitmq", sys);
    }

    // =========================================================================
    // Hex helpers
    // =========================================================================

    [Fact]
    public void ToHexLower_KnownBytes_LowerHex()
    {
        Assert.Equal("00ff10ab", OtlpMapper.ToHexLower([0x00, 0xFF, 0x10, 0xAB]));
    }

    [Fact]
    public void ToHexLower_Empty_ReturnsEmpty()
    {
        Assert.Equal(string.Empty, OtlpMapper.ToHexLower([]));
    }

    [Theory]
    [InlineData(new byte[] { 0, 0, 0 }, true)]
    [InlineData(new byte[] { 0, 1, 0 }, false)]
    public void IsAllZero_DetectsZeroRuns(byte[] bytes, bool expected)
    {
        Assert.Equal(expected, OtlpMapper.IsAllZero(bytes));
    }

    // =========================================================================
    // OtlpProvider.CanHandle
    // =========================================================================

    [Theory]
    [InlineData("OTLP",          true)]
    [InlineData("otlp",          true)]
    [InlineData("OpenTelemetry", true)]
    [InlineData("opentelemetry", true)]
    [InlineData("Kafka",         false)]
    [InlineData("RabbitMQ",      false)]
    [InlineData("",              false)]
    public void Provider_CanHandle_CaseInsensitive(string brokerType, bool expected)
    {
        var provider = new OtlpProvider(new OtlpChannelBridge());
        var source   = new Hopscope.Application.Abstractions.IngestionSource(
            brokerType, "n/a", new Dictionary<string, string>());

        Assert.Equal(expected, provider.CanHandle(source));
    }

    [Fact]
    public void Provider_CreateIngestor_ReturnsIngestorWithNameOtlp()
    {
        var provider = new OtlpProvider(new OtlpChannelBridge());
        var source   = new Hopscope.Application.Abstractions.IngestionSource(
            "OTLP", "n/a", new Dictionary<string, string>());

        var ingestor = provider.CreateIngestor(source);

        Assert.NotNull(ingestor);
        Assert.Equal("OTLP", ingestor.Name);
    }
}
