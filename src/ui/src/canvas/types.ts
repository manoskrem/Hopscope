import type { Edge, Node } from '@xyflow/react';
import type { ExecutionStatus, NodeKind } from '../contract/wire';

export interface HopNodeData extends Record<string, unknown> {
  label: string;
  kind: NodeKind;
  brokerType: string;
  /** Faded because another node is focused and this one isn't connected. */
  dimmed: boolean;
  /** This is the focused node. */
  focused: boolean;
}
export type HopFlowNode = Node<HopNodeData, 'hop'>;

export interface HopEdgeData extends Record<string, unknown> {
  status: ExecutionStatus;
  count: number;
  dimmed: boolean;
  highlighted: boolean;
}
export type HopFlowEdge = Edge<HopEdgeData, 'status'>;

/** The current connection-highlight focus. */
export interface Selection {
  focusedId: string | null;
  nodeIds: ReadonlySet<string>;
  edgeIds: ReadonlySet<string>;
}

export const NO_SELECTION: Selection = {
  focusedId: null,
  nodeIds: new Set(),
  edgeIds: new Set(),
};
