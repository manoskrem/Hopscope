// TypeScript mirror of the engine's wire contract.
//
// camelCase JSON; enums travel as INTEGERS — the engine's System.Text.Json
// source-gen context has no JsonStringEnumConverter, so NodeKind/ExecutionStatus
// arrive as 0..3, not strings. Keep this file congruent with:
//   Hopscope.Domain/Topology/{NodeKind,GraphNode,GraphEdge,GraphSnapshot,GraphDelta,PushFrame}.cs
//   Hopscope.Domain/Events/ExecutionStatus.cs

/** Congruent with Hopscope.Domain.Topology.NodeKind (integer wire values). */
export const NodeKind = {
  Service: 0,
  Exchange: 1,
  Topic: 2,
  Queue: 3,
} as const;
export type NodeKind = (typeof NodeKind)[keyof typeof NodeKind];

/** Congruent with Hopscope.Domain.Events.ExecutionStatus (integer wire values). */
export const ExecutionStatus = {
  Success: 0,
  Retrying: 1,
  DeadLettered: 2,
  Failed: 3,
} as const;
export type ExecutionStatus = (typeof ExecutionStatus)[keyof typeof ExecutionStatus];

export interface GraphNode {
  id: string;
  kind: NodeKind;
  label: string;
  brokerType: string;
}

export interface GraphEdge {
  id: string;
  sourceId: string;
  targetId: string;
  /** Status of the most recent hop on this edge. */
  lastStatus: ExecutionStatus;
  /** Engine-authoritative running total — REPLACE by id, never accumulate locally. */
  count: number;
}

export interface GraphSnapshot {
  nodes: GraphNode[];
  edges: GraphEdge[];
  /** The delta cursor this snapshot is current as of. */
  sequence: number;
}

export interface GraphDelta {
  upsertNodes: GraphNode[];
  upsertEdges: GraphEdge[];
  /** Monotonic; the next valid delta after sequence S is S+1. */
  sequence: number;
}

export type PushFrameKind = 'snapshot' | 'delta';

export interface PushFrame {
  kind: PushFrameKind;
  snapshot: GraphSnapshot | null;
  delta: GraphDelta | null;
}

/**
 * Parse + minimally validate a raw WebSocket text frame. Returns null for
 * anything that isn't a well-formed frame — a malformed frame must never crash
 * the reducer; the socket layer simply ignores it.
 */
export function parseFrame(raw: string): PushFrame | null {
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    return null;
  }
  if (typeof value !== 'object' || value === null) return null;
  const frame = value as Record<string, unknown>;

  if (frame.kind === 'snapshot' && isSnapshot(frame.snapshot)) {
    return { kind: 'snapshot', snapshot: frame.snapshot, delta: null };
  }
  if (frame.kind === 'delta' && isDelta(frame.delta)) {
    return { kind: 'delta', snapshot: null, delta: frame.delta };
  }
  return null;
}

function isSnapshot(value: unknown): value is GraphSnapshot {
  if (typeof value !== 'object' || value === null) return false;
  const s = value as Record<string, unknown>;
  return Array.isArray(s.nodes) && Array.isArray(s.edges) && typeof s.sequence === 'number';
}

function isDelta(value: unknown): value is GraphDelta {
  if (typeof value !== 'object' || value === null) return false;
  const d = value as Record<string, unknown>;
  return Array.isArray(d.upsertNodes) && Array.isArray(d.upsertEdges) && typeof d.sequence === 'number';
}
