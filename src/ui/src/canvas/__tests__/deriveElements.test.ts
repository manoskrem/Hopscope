import { describe, expect, it } from 'vitest';
import { ExecutionStatus, NodeKind, type GraphEdge, type GraphNode } from '../../contract/wire';
import { applySnapshot, type GraphState } from '../../store/reducer';
import { neighborNodeIds, connectedEdgeIds } from '../../store/selectors';
import { deriveElements } from '../deriveElements';
import { NO_SELECTION, type Selection } from '../types';
import type { XY } from '../layout';

const node = (id: string, kind: NodeKind = NodeKind.Service): GraphNode => ({
  id,
  kind,
  label: id,
  brokerType: 'RabbitMQ',
});
const edge = (s: string, t: string, status: ExecutionStatus): GraphEdge => ({
  id: `${s}->${t}`,
  sourceId: s,
  targetId: t,
  lastStatus: status,
  count: 7,
});

const state = applySnapshot({
  nodes: [node('a'), node('orders', NodeKind.Exchange), node('q.dlx', NodeKind.Queue)],
  edges: [
    edge('a', 'orders', ExecutionStatus.Success),
    edge('orders', 'q.dlx', ExecutionStatus.DeadLettered),
  ],
  sequence: 1,
});

const positions = new Map<string, XY>([
  ['a', { x: 0, y: 0 }],
  ['orders', { x: 200, y: 0 }],
  ['q.dlx', { x: 400, y: 0 }],
]);

describe('deriveElements', () => {
  it('produces one RF node per store node and one edge per store edge', () => {
    const { nodes, edges } = deriveElements(state, positions, NO_SELECTION);
    expect(nodes).toHaveLength(3);
    expect(edges).toHaveLength(2);
    expect(nodes.every((n) => n.type === 'hop')).toBe(true);
    expect(edges.every((e) => e.type === 'status')).toBe(true);
  });

  it('attaches a status-specific arrow marker to each edge', () => {
    const { edges } = deriveElements(state, positions, NO_SELECTION);
    const byId = Object.fromEntries(edges.map((e) => [e.id, e.markerEnd]));
    expect(byId['a->orders']).toBe('url(#hop-arrow-ok)');
    expect(byId['orders->q.dlx']).toBe('url(#hop-arrow-dlq)');
  });

  it('uses positions from the layout map', () => {
    const { nodes } = deriveElements(state, positions, NO_SELECTION);
    expect(nodes.find((n) => n.id === 'orders')!.position).toEqual({ x: 200, y: 0 });
  });

  it('dims unrelated nodes/edges and highlights connected ones when focused', () => {
    const selection: Selection = {
      focusedId: 'orders',
      nodeIds: neighborNodeIds(state, 'orders'),
      edgeIds: connectedEdgeIds(state, 'orders'),
    };
    const { nodes, edges } = deriveElements(state, positions, selection);

    const orders = nodes.find((n) => n.id === 'orders')!;
    expect(orders.data.focused).toBe(true);
    expect(orders.data.dimmed).toBe(false);

    // 'a' and 'q.dlx' are neighbors of 'orders' → not dimmed
    expect(nodes.find((n) => n.id === 'a')!.data.dimmed).toBe(false);

    const connected = edges.find((e) => e.id === 'orders->q.dlx')!;
    expect(connected.data!.highlighted).toBe(true);
    expect(connected.data!.dimmed).toBe(false);
  });

  it('drops edges whose endpoints are not present as nodes', () => {
    const dangling: GraphState = {
      nodes: new Map([['a', node('a')]]),
      edges: new Map([['a->ghost', edge('a', 'ghost', ExecutionStatus.Success)]]),
      sequence: 1,
    };
    const { edges } = deriveElements(dangling, new Map([['a', { x: 0, y: 0 }]]), NO_SELECTION);
    expect(edges).toHaveLength(0);
  });
});
