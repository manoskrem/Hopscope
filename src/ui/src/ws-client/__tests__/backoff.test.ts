import { describe, expect, it } from 'vitest';
import { nextBackoffMs, type BackoffOptions } from '../backoff';

const NO_JITTER: BackoffOptions = { baseMs: 1000, maxMs: 8000, factor: 2, jitter: 0 };
const QUARTER: BackoffOptions = { baseMs: 1000, maxMs: 8000, factor: 2, jitter: 0.25 };

describe('nextBackoffMs', () => {
  it('grows exponentially from the base (attempt is 1-based)', () => {
    expect(nextBackoffMs(1, NO_JITTER, () => 0.5)).toBe(1000);
    expect(nextBackoffMs(2, NO_JITTER, () => 0.5)).toBe(2000);
    expect(nextBackoffMs(3, NO_JITTER, () => 0.5)).toBe(4000);
  });

  it('caps at maxMs', () => {
    expect(nextBackoffMs(10, NO_JITTER, () => 0.5)).toBe(8000);
    expect(nextBackoffMs(100, NO_JITTER, () => 0.5)).toBe(8000);
  });

  it('applies jitter within ±jitter of the base delay', () => {
    // rand 0 → -range, rand 1 → +range, rand 0.5 → 0
    expect(nextBackoffMs(1, QUARTER, () => 0)).toBe(750);
    expect(nextBackoffMs(1, QUARTER, () => 1)).toBe(1250);
    expect(nextBackoffMs(1, QUARTER, () => 0.5)).toBe(1000);
  });

  it('never returns a negative delay', () => {
    const big: BackoffOptions = { baseMs: 1000, maxMs: 8000, factor: 2, jitter: 2 };
    expect(nextBackoffMs(1, big, () => 0)).toBeGreaterThanOrEqual(0);
  });
});
