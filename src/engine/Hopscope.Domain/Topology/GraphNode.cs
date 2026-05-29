namespace Hopscope.Domain.Topology;

/// <summary>A node on the live topology canvas (a service, exchange, topic, or queue).</summary>
public sealed record GraphNode(string Id, NodeKind Kind, string Label, string BrokerType);
