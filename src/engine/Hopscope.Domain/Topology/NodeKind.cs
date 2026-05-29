namespace Hopscope.Domain.Topology;

/// <summary>The kind of a topology node, used by the UI to pick the right glyph.</summary>
public enum NodeKind
{
    Service = 0,
    Exchange = 1,
    Topic = 2,
    Queue = 3
}
