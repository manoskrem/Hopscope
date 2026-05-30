using Hopscope.Application.Abstractions;

namespace Hopscope.Infrastructure.Providers.Otlp;

/// <summary>
/// <see cref="IBrokerProvider"/> for the OTLP gRPC receiver.
/// Compile-time DI-registered in <c>Hopscope.Host/Program.cs</c> — no reflection,
/// no runtime scanning. AOT-safe.
///
/// The ingestor created here does NOT open a socket — it drains the shared
/// <see cref="OtlpChannelBridge"/> that <see cref="OtlpTraceService"/> (hosted by
/// Kestrel on port 4317) writes into.
/// </summary>
public sealed class OtlpProvider : IBrokerProvider
{
    private readonly OtlpChannelBridge _bridge;

    public OtlpProvider(OtlpChannelBridge bridge)
    {
        _bridge = bridge;
    }

    /// <inheritdoc/>
    public string BrokerType => "OTLP";

    /// <inheritdoc/>
    /// <remarks>
    /// Accepts "OTLP", "otlp", "OpenTelemetry", "opentelemetry" — ordinal ignore-case.
    /// </remarks>
    public bool CanHandle(IngestionSource source) =>
        string.Equals(source.BrokerType, "OTLP",          StringComparison.OrdinalIgnoreCase) ||
        string.Equals(source.BrokerType, "OpenTelemetry", StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc/>
    public IEventIngestor CreateIngestor(IngestionSource source)
        => new OtlpIngestor(_bridge);
}
