namespace Hopscope.Domain.Topology;

/// <summary>
/// Wire wrapper sent over the raw WebSocket connection. The UI uses
/// <c>Kind</c> to distinguish an initial snapshot from an incremental delta.
/// Exactly one of <see cref="Snapshot"/> or <see cref="Delta"/> is non-null per frame.
///
/// AOT note: registered on <c>AppJsonSerializerContext</c> in the same change-set.
/// </summary>
public sealed record PushFrame(
    string Kind,
    GraphSnapshot? Snapshot,
    GraphDelta? Delta);
