import { describe, expect, it } from 'vitest';
import { ExecutionStatus, NodeKind, type GraphDelta, type GraphSnapshot } from '../../contract/wire';
import { applyDelta, applySnapshot, EMPTY_STATE } from '../reducer';

const node = (id: string, kind: NodeKind = NodeKind.Service) => ({
  id,
  kind,
  label: id,
  brokerType: 'RabbitMQ',
});
const edge = (sourceId: string, targetId: string, count: number, lastStatus: ExecutionStatus) => ({
  id: `${sourceId}->${targetId}`,
  sourceId,
  targetId,
  lastStatus,
  count,
});

describe('applySnapshot', () => {
  it('replaces all state and sets the sequence', () => {
    const snap: GraphSnapshot = {
      nodes: [node('a'), node('orders', NodeKind.Exchange)],
      edges: [edge('a', 'orders', 5, ExecutionStatus.Success)],
      sequence: 42,
    };
    const state = applySnapshot(snap);
    expect(state.nodes.size).toBe(2);
    expect(state.edges.size).toBe(1);
    expect(state.sequence).toBe(42);
    expect(state.nodes.get('orders')?.kind).toBe(NodeKind.Exchange);
  });

  it('replaces (does not merge with) prior content', () => {
    const first = applySnapshot({ nodes: [node('a'), node('b')], edges: [], sequence: 1 });
    const second = applySnapshot({ nodes: [node('c')], edges: [], sequence: 2 });
    expect([...second.nodes.keys()]).toEqual(['c']);
    // first is untouched
    expect([...first.nodes.keys()].sort()).toEqual(['a', 'b']);
  });
});

describe('applyDelta', () => {
  it('upserts nodes and edges by id', () => {
    const base = applySnapshot({ nodes: [node('a')], edges: [], sequence: 1 });
    const delta: GraphDelta = {
      upsertNodes: [node('orders', NodeKind.Exchange)],
      upsertEdges: [edge('a', 'orders', 1, ExecutionStatus.Success)],
      sequence: 2,
    };
    const next = applyDelta(base, delta);
    expect(next.nodes.size).toBe(2);
    expect(next.edges.size).toBe(1);
    expect(next.sequence).toBe(2);
  });

  it('uses last-write-wins on count and status (never sums)', () => {
    let state = applyDelta(EMPTY_STATE, {
      upsertNodes: [],
      upsertEdges: [edge('a', 'b', 10, ExecutionStatus.Success)],
      sequence: 1,
    });
    state = applyDelta(state, {
      upsertNodes: [],
      upsertEdges: [edge('a', 'b', 13, ExecutionStatus.Failed)],
      sequence: 2,
    });
    const e = state.edges.get('a->b');
    expect(e?.count).toBe(13); // replaced, NOT 10 + 13
    expect(e?.lastStatus).toBe(ExecutionStatus.Failed);
    expect(state.edges.size).toBe(1);
  });

  it('is immutable — the previous state is not mutated', () => {
    const base = applySnapshot({ nodes: [node('a')], edges: [], sequence: 1 });
    const next = applyDelta(base, {
      upsertNodes: [node('b')],
      upsertEdges: [],
      sequence: 2,
    });
    expect(base.nodes.size).toBe(1); // unchanged
    expect(next.nodes.size).toBe(2);
    expect(next.nodes).not.toBe(base.nodes); // new Map reference
  });
});
