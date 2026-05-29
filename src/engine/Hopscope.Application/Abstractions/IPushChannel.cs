using Hopscope.Domain.Topology;

namespace Hopscope.Application.Abstractions;

/// <summary>Fans deltas to connected UI clients (raw <c>System.Net.WebSockets</c> impl).</summary>
public interface IPushChannel
{
    ValueTask BroadcastAsync(GraphDelta delta, CancellationToken ct);

    ValueTask SendSnapshotAsync(string connectionId, GraphSnapshot snapshot, CancellationToken ct);
}
