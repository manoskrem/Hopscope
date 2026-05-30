import type { GraphDelta, GraphEdge, GraphNode, GraphSnapshot } from '../contract/wire';

/** Normalized, immutable view of the topology. `sequence` is -1 before the first snapshot. */
export interface GraphState {
  readonly nodes: ReadonlyMap<string, GraphNode>;
  readonly edges: ReadonlyMap<string, GraphEdge>;
  readonly sequence: number;
}

export const EMPTY_STATE: GraphState = {
  nodes: new Map(),
  edges: new Map(),
  sequence: -1,
};

/** A snapshot is the new ground truth — replace everything. */
export function applySnapshot(snapshot: GraphSnapshot): GraphState {
  const nodes = new Map<string, GraphNode>();
  for (const node of snapshot.nodes) nodes.set(node.id, node);

  const edges = new Map<string, GraphEdge>();
  for (const edge of snapshot.edges) edges.set(edge.id, edge);

  return { nodes, edges, sequence: snapshot.sequence };
}

/**
 * A delta upserts nodes/edges by id with LAST-WRITE-WINS semantics. The engine's
 * `edge.count` is authoritative — we replace, never accumulate locally. Returns a
 * fresh state (new Maps) so subscribers get a stable, changed reference.
 */
export function applyDelta(state: GraphState, delta: GraphDelta): GraphState {
  const nodes = new Map(state.nodes);
  for (const node of delta.upsertNodes) nodes.set(node.id, node);

  const edges = new Map(state.edges);
  for (const edge of delta.upsertEdges) edges.set(edge.id, edge);

  return { nodes, edges, sequence: delta.sequence };
}
