// Sequence-gap classification. The engine stamps a monotonic `sequence` on every
// delta; the snapshot carries the cursor it is current as of. After a snapshot at
// sequence S, the next valid delta is S+1.

export type SequenceClass = 'inOrder' | 'gap' | 'stale';

/**
 * Classify an incoming delta sequence against the last applied sequence.
 *   inOrder  — exactly lastSeq + 1: apply it, advance the cursor.
 *   gap      — beyond lastSeq + 1: a delta was missed → reconnect for a fresh snapshot.
 *   stale    — <= lastSeq: already reflected (duplicate or pre-snapshot) → ignore.
 *              Safe because upserts are idempotent.
 */
export function classifySequence(lastSeq: number, incoming: number): SequenceClass {
  if (incoming === lastSeq + 1) return 'inOrder';
  if (incoming > lastSeq + 1) return 'gap';
  return 'stale';
}
