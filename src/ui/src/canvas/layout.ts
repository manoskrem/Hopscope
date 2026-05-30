import dagre from '@dagrejs/dagre';
import type { GraphEdge, GraphNode } from '../contract/wire';

export interface XY {
  x: number;
  y: number;
}

export const NODE_W = 184;
export const NODE_H = 64;

/**
 * Hierarchical left-to-right layout via dagre (the engine sends no coordinates).
 * `pinned` positions (e.g. user-dragged) override the computed layout so they
 * survive a re-layout. Every node in `nodes` gets a position.
 */
export function layoutGraph(
  nodes: GraphNode[],
  edges: GraphEdge[],
  pinned?: ReadonlyMap<string, XY>,
): Map<string, XY> {
  const g = new dagre.graphlib.Graph();
  g.setGraph({ rankdir: 'LR', nodesep: 44, ranksep: 96, marginx: 28, marginy: 28 });
  g.setDefaultEdgeLabel(() => ({}));

  for (const node of nodes) g.setNode(node.id, { width: NODE_W, height: NODE_H });
  for (const edge of edges) {
    if (g.hasNode(edge.sourceId) && g.hasNode(edge.targetId)) {
      g.setEdge(edge.sourceId, edge.targetId);
    }
  }

  dagre.layout(g);

  const out = new Map<string, XY>();
  for (const node of nodes) {
    const pin = pinned?.get(node.id);
    if (pin) {
      out.set(node.id, pin);
      continue;
    }
    const pos = g.node(node.id);
    // dagre reports the node center; React Flow positions by top-left.
    out.set(node.id, pos ? { x: pos.x - NODE_W / 2, y: pos.y - NODE_H / 2 } : { x: 0, y: 0 });
  }
  return out;
}
