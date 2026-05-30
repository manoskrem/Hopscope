import { describe, expect, it } from 'vitest';
import { ExecutionStatus, NodeKind, type GraphEdge, type GraphNode } from '../../contract/wire';
import { layoutGraph, type XY } from '../layout';

const node = (id: string): GraphNode => ({ id, kind: NodeKind.Service, label: id, brokerType: 'RabbitMQ' });
const edge = (s: string, t: string): GraphEdge => ({
  id: `${s}->${t}`,
  sourceId: s,
  targetId: t,
  lastStatus: ExecutionStatus.Success,
  count: 1,
});

const nodes = [node('a'), node('b'), node('c')];
const edges = [edge('a', 'b'), edge('b', 'c')];

describe('layoutGraph', () => {
  it('assigns a position to every node', () => {
    const pos = layoutGraph(nodes, edges);
    expect(pos.size).toBe(3);
    for (const n of nodes) {
      const p = pos.get(n.id)!;
      expect(Number.isFinite(p.x)).toBe(true);
      expect(Number.isFinite(p.y)).toBe(true);
    }
  });

  it('is deterministic for the same input', () => {
    const a = layoutGraph(nodes, edges);
    const b = layoutGraph(nodes, edges);
    expect([...a.entries()]).toEqual([...b.entries()]);
  });

  it('lays a chain out left-to-right (a before b before c on the x axis)', () => {
    const pos = layoutGraph(nodes, edges);
    expect(pos.get('a')!.x).toBeLessThan(pos.get('b')!.x);
    expect(pos.get('b')!.x).toBeLessThan(pos.get('c')!.x);
  });

  it('honors pinned positions over the computed layout', () => {
    const pinned = new Map<string, XY>([['b', { x: 999, y: -42 }]]);
    const pos = layoutGraph(nodes, edges, pinned);
    expect(pos.get('b')).toEqual({ x: 999, y: -42 });
  });

  it('ignores edges that reference a missing node', () => {
    const pos = layoutGraph([node('a')], [edge('a', 'ghost')]);
    expect(pos.size).toBe(1);
    expect(pos.has('a')).toBe(true);
  });
});
