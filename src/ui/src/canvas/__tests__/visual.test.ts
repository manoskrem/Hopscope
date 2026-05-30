import { describe, expect, it } from 'vitest';
import { ExecutionStatus, NodeKind } from '../../contract/wire';
import { nodeVisual, statusVisual } from '../visual';

describe('nodeVisual', () => {
  it('maps each kind to a distinct class and glyph', () => {
    const classes = [NodeKind.Service, NodeKind.Exchange, NodeKind.Topic, NodeKind.Queue].map(
      (k) => nodeVisual(k).className,
    );
    expect(new Set(classes).size).toBe(4); // all distinct
  });

  it('falls back safely for an unknown kind', () => {
    const v = nodeVisual(99 as NodeKind);
    expect(v.className).toBe('kind-unknown');
    expect(v.glyph).toBeTruthy();
  });
});

describe('statusVisual', () => {
  it('maps each status to a distinct class', () => {
    const classes = [
      ExecutionStatus.Success,
      ExecutionStatus.Retrying,
      ExecutionStatus.DeadLettered,
      ExecutionStatus.Failed,
    ].map((s) => statusVisual(s).className);
    expect(classes).toEqual(['ok', 'retry', 'dlq', 'fail']);
  });

  it('flags only DeadLettered and Failed as errors', () => {
    expect(statusVisual(ExecutionStatus.Success).isError).toBe(false);
    expect(statusVisual(ExecutionStatus.Retrying).isError).toBe(false);
    expect(statusVisual(ExecutionStatus.DeadLettered).isError).toBe(true);
    expect(statusVisual(ExecutionStatus.Failed).isError).toBe(true);
  });

  it('falls back safely for an unknown status', () => {
    expect(statusVisual(42 as ExecutionStatus).className).toBe('unknown');
  });
});
