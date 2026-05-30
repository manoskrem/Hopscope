import { describe, expect, it } from 'vitest';
import { classifySequence } from '../gap';

describe('classifySequence', () => {
  it('treats lastSeq + 1 as in order', () => {
    expect(classifySequence(5, 6)).toBe('inOrder');
  });

  it('treats anything beyond lastSeq + 1 as a gap', () => {
    expect(classifySequence(5, 8)).toBe('gap');
    expect(classifySequence(5, 7)).toBe('gap');
  });

  it('treats a duplicate (== lastSeq) as stale', () => {
    expect(classifySequence(5, 5)).toBe('stale');
  });

  it('treats anything below lastSeq as stale', () => {
    expect(classifySequence(5, 3)).toBe('stale');
  });

  it('handles the snapshot boundary: snapshot seq S, next delta S+1 is in order', () => {
    const snapshotSeq = 10;
    expect(classifySequence(snapshotSeq, 11)).toBe('inOrder');
    // a delta at or below the snapshot cursor is already reflected
    expect(classifySequence(snapshotSeq, 10)).toBe('stale');
    expect(classifySequence(snapshotSeq, 9)).toBe('stale');
  });
});
