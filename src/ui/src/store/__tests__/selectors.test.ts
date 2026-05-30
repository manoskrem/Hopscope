import { describe, expect, it } from 'vitest';
import { ExecutionStatus, NodeKind, type GraphSnapshot } from '../../contract/wire';
import { applyDelta, applySnapshot } from '../reducer';
import { connectedEdgeIds, errorEdges, neighborNodeIds, structureKey } from '../selectors';

const node = (id: string) => ({ id, kind: NodeKind.Service, label: id, brokerType: 'RabbitMQ' });
const edge = (s: string, t: string, count: number, status: ExecutionStatus) => ({
  id: `${s}->${t}`,
  sourceId: s,
  targetId: t,
  lastStatus: status,
  count,
});

const scene: GraphSnapshot = {
  nodes: [node('a'), node('orders'), node('q.fulfil'), node('q.dlx')],
  edges: [
    edge('a', 'orders', 100, ExecutionStatus.Success),
    edge('orders', 'q.fulfil', 80, ExecutionStatus.Retrying),
    edge('orders', 'q.dlx', 5, ExecutionStatus.DeadLettered),
    edge('q.fulfil', 'a', 3, ExecutionStatus.Failed),
  ],
  sequence: 10,
};

describe('structureKey', () => {
  it('is stable when only an edge count or status changes', () => {
    const before = applySnapshot(scene);
    const after = applyDelta(before, {
      upsertNodes: [],
      upsertEdges: [edge('a', 'orders', 999, ExecutionStatus.Failed)], // same pair, new count/status
      sequence: 11,
    });
    expect(structureKey(after)).toBe(structureKey(before));
  });

  it('changes when a new node/edge appears', () => {
    const before = applySnapshot(scene);
    const after = applyDelta(before, {
      upsertNodes: [node('q.notify')],
      upsertEdges: [edge('q.fulfil', 'q.notify', 1, ExecutionStatus.Success)],
      sequence: 11,
    });
    expect(structureKey(after)).not.toBe(structureKey(before));
  });
});

describe('errorEdges', () => {
  it('returns only DeadLettered and Failed edges', () => {
    const ids = errorEdges(applySnapshot(scene))
      .map((e) => e.id)
      .sort();
    expect(ids).toEqual(['orders->q.dlx', 'q.fulfil->a']);
  });
});

describe('connectedEdgeIds', () => {
  it('includes edges in both directions', () => {
    const ids = connectedEdgeIds(applySnapshot(scene), 'a');
    expect([...ids].sort()).toEqual(['a->orders', 'q.fulfil->a']);
  });
});

describe('neighborNodeIds', () => {
  it('includes the node itself plus upstream and downstream neighbors', () => {
    const ids = neighborNodeIds(applySnapshot(scene), 'orders');
    expect([...ids].sort()).toEqual(['a', 'orders', 'q.dlx', 'q.fulfil']);
  });
});
