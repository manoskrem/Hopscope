import type { GraphState } from '../store/reducer';
import type { XY } from './layout';
import type { HopFlowEdge, HopFlowNode, Selection } from './types';
import { statusVisual } from './visual';

/**
 * Pure projection of the store state + computed positions + current selection
 * into React Flow node/edge arrays. RF-free (only type imports from @xyflow/react)
 * so it is unit-testable without the React Flow runtime.
 */
export function deriveElements(
  state: GraphState,
  positions: ReadonlyMap<string, XY>,
  selection: Selection,
): { nodes: HopFlowNode[]; edges: HopFlowEdge[] } {
  const hasFocus = selection.focusedId !== null;

  const nodes: HopFlowNode[] = [];
  for (const node of state.nodes.values()) {
    nodes.push({
      id: node.id,
      type: 'hop',
      position: positions.get(node.id) ?? { x: 0, y: 0 },
      data: {
        label: node.label,
        kind: node.kind,
        brokerType: node.brokerType,
        dimmed: hasFocus && !selection.nodeIds.has(node.id),
        focused: selection.focusedId === node.id,
      },
    });
  }

  const edges: HopFlowEdge[] = [];
  for (const edge of state.edges.values()) {
    // Skip dangling edges (endpoint not yet known) — RF would warn otherwise.
    if (!state.nodes.has(edge.sourceId) || !state.nodes.has(edge.targetId)) continue;
    const v = statusVisual(edge.lastStatus);
    edges.push({
      id: edge.id,
      type: 'status',
      source: edge.sourceId,
      target: edge.targetId,
      markerEnd: `url(#hop-arrow-${v.className})`,
      data: {
        status: edge.lastStatus,
        count: edge.count,
        dimmed: hasFocus && !selection.edgeIds.has(edge.id),
        highlighted: hasFocus && selection.edgeIds.has(edge.id),
      },
    });
  }

  return { nodes, edges };
}
