import { ExecutionStatus, type GraphEdge } from '../contract/wire';
import type { GraphState } from './reducer';

/**
 * A stable key describing the topology STRUCTURE: the set of node ids plus the
 * set of source→target connections. It changes when nodes/edges are added,
 * removed, or rewired — but NOT when an edge's count or status updates. The
 * canvas keys layout recomputation on this so high-frequency count/status deltas
 * don't cause the graph to jump around.
 */
export function structureKey(state: GraphState): string {
  const nodeIds = [...state.nodes.keys()].sort();
  const edgePairs = [...state.edges.values()].map((e) => `${e.sourceId}>${e.targetId}`).sort();
  return `${nodeIds.join(',')}|${edgePairs.join(',')}`;
}

/** Edges whose most recent hop is in an error state (DeadLettered or Failed). */
export function errorEdges(state: GraphState): GraphEdge[] {
  const out: GraphEdge[] = [];
  for (const edge of state.edges.values()) {
    if (edge.lastStatus === ExecutionStatus.DeadLettered || edge.lastStatus === ExecutionStatus.Failed) {
      out.push(edge);
    }
  }
  return out;
}

/** Ids of edges touching `nodeId` in either direction. */
export function connectedEdgeIds(state: GraphState, nodeId: string): Set<string> {
  const ids = new Set<string>();
  for (const edge of state.edges.values()) {
    if (edge.sourceId === nodeId || edge.targetId === nodeId) ids.add(edge.id);
  }
  return ids;
}

/** `nodeId` plus its immediate (1-hop) neighbors, upstream and downstream. */
export function neighborNodeIds(state: GraphState, nodeId: string): Set<string> {
  const ids = new Set<string>([nodeId]);
  for (const edge of state.edges.values()) {
    if (edge.sourceId === nodeId) ids.add(edge.targetId);
    if (edge.targetId === nodeId) ids.add(edge.sourceId);
  }
  return ids;
}
