using Hopscope.Application.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace Hopscope.Infrastructure.Providers.Fake;

/// <summary>
/// Fallback wiring for the synthetic <see cref="FakeIngestor"/>. Call this ONCE after all
/// real <c>Add&lt;Broker&gt;Ingestion(config)</c> calls in <c>Program.cs</c>: it registers the
/// FakeIngestor only when no real <see cref="IEventIngestor"/> has been registered, so
/// Phase-1 smoke-testing still works with no broker configured (real data wins otherwise).
/// </summary>
public static class FakeIngestionServiceCollectionExtensions
{
    public static IServiceCollection AddFakeIngestionIfNoneRegistered(this IServiceCollection services)
    {
        // Reflection-free scan of the collection — just a ServiceType comparison.
        foreach (var descriptor in services)
        {
            if (descriptor.ServiceType == typeof(IEventIngestor))
                return services; // a real ingestor is already registered
        }

        services.AddSingleton<IEventIngestor, FakeIngestor>();
        return services;
    }
}
