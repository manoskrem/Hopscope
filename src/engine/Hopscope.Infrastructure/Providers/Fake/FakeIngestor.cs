using Hopscope.Application.Abstractions;
using Hopscope.Domain.Events;

namespace Hopscope.Infrastructure.Providers.Fake;

/// <summary>
/// Synthetic <see cref="IEventIngestor"/> for Phase 1 smoke-testing.
///
/// Emits a realistic multi-broker, multi-hop topology with seeded <see cref="Random"/>
/// so the graph is reproducible across runs. Includes:
///   - Multi-hop traces with proper ParentHopId chains.
///   - All four <see cref="ExecutionStatus"/> values, including at least one
///     <see cref="ExecutionStatus.DeadLettered"/> hop with <see cref="ErrorDetails"/>.
///   - Three broker types (RabbitMQ, Kafka, Redis) with matching destinationKind hints.
///   - A configurable emit delay (default: random 100–300 ms) to simulate real traffic.
///
/// AOT-safe: no reflection, no LINQ Expressions, no dynamic codegen.
/// </summary>
public sealed class FakeIngestor : IEventIngestor
{
    public string Name => "Fake";

    // Seeded for reproducibility across runs.
    private static readonly Random Rng = new(42);

    // -----------------------------------------------------------------------
    // Static topology — a small but realistic service mesh
    // -----------------------------------------------------------------------

    // Services (sources)
    private const string SvcOrder    = "order-svc";
    private const string SvcPayment  = "payment-svc";
    private const string SvcInventory = "inventory-svc";
    private const string SvcNotify   = "notification-svc";
    private const string SvcAudit    = "audit-svc";

    // RabbitMQ exchanges / queues
    private const string ExOrders    = "orders.exchange";
    private const string ExPayments  = "payments.exchange";
    private const string QDeadLetter = "orders.dlq";

    // Kafka topics
    private const string TopicEvents = "domain-events";
    private const string TopicAudit  = "audit-log";

    // Redis channel
    private const string ChanNotify  = "notifications:channel";

    // -----------------------------------------------------------------------
    // Trace templates — each defines one full multi-hop trace pattern
    // -----------------------------------------------------------------------
    private sealed record HopTemplate(
        string Source,
        string Destination,
        string BrokerType,
        string DestKind,
        ExecutionStatus Status,
        string? ErrorType   = null,
        string? ErrorMsg    = null);

    private sealed record TraceTemplate(string TracePrefix, HopTemplate[] Hops);

    private static readonly TraceTemplate[] Traces =
    [
        // --- Trace A: happy-path order flow (RabbitMQ) ---
        new("order-flow",
        [
            new(SvcOrder,    ExOrders,    "RabbitMQ", "Exchange", ExecutionStatus.Success),
            new(ExOrders,    SvcPayment,  "RabbitMQ", "Service",  ExecutionStatus.Success),
            new(SvcPayment,  ExPayments,  "RabbitMQ", "Exchange", ExecutionStatus.Success),
            new(ExPayments,  SvcInventory,"RabbitMQ", "Service",  ExecutionStatus.Success),
        ]),

        // --- Trace B: payment retry then dead-letter (RabbitMQ) ---
        new("payment-dlq",
        [
            new(SvcOrder,    ExOrders,    "RabbitMQ", "Exchange", ExecutionStatus.Success),
            new(ExOrders,    SvcPayment,  "RabbitMQ", "Service",  ExecutionStatus.Retrying),
            new(SvcPayment,  QDeadLetter, "RabbitMQ", "Queue",    ExecutionStatus.DeadLettered,
                "PaymentGatewayException", "Gateway timeout after 3 retries"),
        ]),

        // --- Trace C: domain event fan-out (Kafka) ---
        new("domain-event",
        [
            new(SvcOrder,    TopicEvents, "Kafka", "Topic",   ExecutionStatus.Success),
            new(TopicEvents, SvcAudit,    "Kafka", "Service", ExecutionStatus.Success),
            new(TopicEvents, SvcNotify,   "Kafka", "Service", ExecutionStatus.Success),
            new(SvcNotify,   ChanNotify,  "Redis", "Topic",   ExecutionStatus.Success),
        ]),

        // --- Trace D: audit write failure (Kafka) ---
        new("audit-fail",
        [
            new(SvcInventory, TopicAudit, "Kafka", "Topic",   ExecutionStatus.Success),
            new(TopicAudit,   SvcAudit,   "Kafka", "Service", ExecutionStatus.Failed,
                "DatabaseException", "Connection pool exhausted"),
        ]),

        // --- Trace E: notification Redis failure ---
        new("notify-fail",
        [
            new(SvcPayment,  ChanNotify, "Redis", "Topic",   ExecutionStatus.Failed,
                "RedisTimeoutException", "RESP write timed out after 500ms"),
        ]),
    ];

    // -----------------------------------------------------------------------
    // IEventIngestor
    // -----------------------------------------------------------------------
    public async IAsyncEnumerable<EventEnvelope> StreamAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken ct)
    {
        var run = 0; // loop counter — appended to TraceId so each cycle is unique

        while (!ct.IsCancellationRequested)
        {
            run++;

            foreach (var template in Traces)
            {
                if (ct.IsCancellationRequested) yield break;

                var traceId   = $"{template.TracePrefix}-{run}";
                string? parentHopId = null;

                for (var i = 0; i < template.Hops.Length; i++)
                {
                    if (ct.IsCancellationRequested) yield break;

                    var hop    = template.Hops[i];
                    var hopId  = $"{traceId}-hop{i + 1}";

                    var metadata = new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["destinationKind"] = hop.DestKind,
                        ["routingKey"]      = $"{hop.Source}.{hop.Destination}",
                        ["contentType"]     = "application/json",
                    };

                    ErrorDetails? errorDetails = null;
                    if (hop.ErrorType is not null)
                    {
                        errorDetails = new ErrorDetails(
                            ExceptionType:        hop.ErrorType,
                            Message:              hop.ErrorMsg ?? "Unknown error",
                            TruncatedStackTrace:  $"   at {hop.Source}.Handler.Process()");
                    }

                    yield return new EventEnvelope
                    {
                        TraceId         = traceId,
                        HopId           = hopId,
                        ParentHopId     = parentHopId,
                        Source          = hop.Source,
                        Destination     = hop.Destination,
                        BrokerType      = hop.BrokerType,
                        Timestamp       = DateTimeOffset.UtcNow,
                        ExecutionStatus = hop.Status,
                        ErrorDetails    = errorDetails,
                        PayloadMetadata = metadata,
                    };

                    parentHopId = hopId; // each hop is the parent of the next in the chain

                    // Random inter-hop delay: 50–150 ms.
                    var delayMs = Rng.Next(50, 150);
                    await Task.Delay(delayMs, ct).ConfigureAwait(false);
                }

                // Inter-trace delay: 100–300 ms.
                var traceDelayMs = Rng.Next(100, 300);
                await Task.Delay(traceDelayMs, ct).ConfigureAwait(false);
            }
        }
    }
}
