namespace Hopscope.Domain.Events;

/// <summary>
/// Per-hop lifecycle outcome.
/// Ordinals are wire-congruent with the proto <c>ExecutionStatus</c> enum
/// (SUCCESS=0, RETRYING=1, DEAD_LETTERED=2, FAILED=3) — do not reorder.
/// </summary>
public enum ExecutionStatus
{
    Success = 0,
    Retrying = 1,
    DeadLettered = 2,
    Failed = 3
}
