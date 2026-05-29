namespace Hopscope.Domain.Events;

/// <summary>
/// Failure detail attached to an <see cref="EventEnvelope"/>; non-null iff the hop
/// failed or was dead-lettered. The stack trace is truncated metadata — never a
/// message body (RAM + privacy guard, enforced by the contract).
/// </summary>
public sealed record ErrorDetails(string ExceptionType, string Message, string? TruncatedStackTrace);
