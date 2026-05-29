namespace Hopscope.Application.Abstractions;

/// <summary>
/// Broker-agnostic factory (the Provider Pattern seam). ADD A BROKER = implement this +
/// register one DI line in the Host. Nothing else changes.
/// </summary>
public interface IBrokerProvider
{
    /// <summary>e.g. "RabbitMQ".</summary>
    string BrokerType { get; }

    bool CanHandle(IngestionSource source);

    IEventIngestor CreateIngestor(IngestionSource source);
}
