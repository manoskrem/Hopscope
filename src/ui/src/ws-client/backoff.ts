// Exponential backoff with full-magnitude jitter, used to space out reconnect
// attempts so a burst (or a persistent sequence gap) can't cause a reconnect storm.

export interface BackoffOptions {
  baseMs: number;
  maxMs: number;
  factor: number;
  /** Jitter as a fraction of the computed delay, in [0, 1]. */
  jitter: number;
}

export const DEFAULT_BACKOFF: BackoffOptions = {
  baseMs: 500,
  maxMs: 10_000,
  factor: 2,
  jitter: 0.25,
};

/**
 * Delay before reconnect attempt `attempt` (1-based: the first retry is attempt 1).
 * The base delay grows exponentially (capped at `maxMs`), then ±`jitter` is applied.
 * `rand` is injectable for deterministic tests (defaults to Math.random).
 */
export function nextBackoffMs(
  attempt: number,
  opts: BackoffOptions = DEFAULT_BACKOFF,
  rand: () => number = Math.random,
): number {
  const steps = Math.max(0, attempt - 1);
  const exp = Math.min(opts.maxMs, opts.baseMs * opts.factor ** steps);
  const range = exp * opts.jitter;
  const delta = (rand() * 2 - 1) * range; // [-range, +range]
  return Math.max(0, Math.round(exp + delta));
}
