import { useCallback, useEffect, useMemo, useRef } from 'react';
import {
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  useEdgesState,
  useNodesState,
  useReactFlow,
  type NodeMouseHandler,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import '../styles/reactflow-overrides.css';

import { connectedEdgeIds, neighborNodeIds, structureKey, useGraphState } from '../store';
import { deriveElements } from './deriveElements';
import { layoutGraph, type XY } from './layout';
import { edgeTypes } from './edgeTypes';
import { nodeTypes } from './nodeTypes';
import { MARKER_STATUSES, statusVisual } from './visual';
import { NO_SELECTION, type HopFlowEdge, type HopFlowNode, type Selection } from './types';

export interface CanvasProps {
  focusedId: string | null;
  onFocus: (id: string | null) => void;
}

/** Status-colored arrow markers, referenced by edges via url(#hop-arrow-<status>). */
function ArrowMarkers() {
  return (
    <svg className="hop-markers" aria-hidden>
      <defs>
        {MARKER_STATUSES.map((status) => {
          const v = statusVisual(status);
          return (
            <marker
              key={v.className}
              id={`hop-arrow-${v.className}`}
              markerWidth="12"
              markerHeight="12"
              refX="9"
              refY="4"
              orient="auto"
              markerUnits="userSpaceOnUse"
            >
              <path d="M0,0 L9,4 L0,8 Z" fill={v.color} />
            </marker>
          );
        })}
      </defs>
    </svg>
  );
}

function CanvasInner({ focusedId, onFocus }: CanvasProps) {
  const state = useGraphState();
  const { fitView } = useReactFlow();

  const positionsRef = useRef(new Map<string, XY>());
  const pinnedRef = useRef(new Map<string, XY>());
  const prevKeyRef = useRef('');

  const [rfNodes, setNodes, onNodesChange] = useNodesState<HopFlowNode>([]);
  const [rfEdges, setEdges, onEdgesChange] = useEdgesState<HopFlowEdge>([]);

  const key = useMemo(() => structureKey(state), [state]);

  const selection: Selection = useMemo(() => {
    if (focusedId === null || !state.nodes.has(focusedId)) return NO_SELECTION;
    return {
      focusedId,
      nodeIds: neighborNodeIds(state, focusedId),
      edgeIds: connectedEdgeIds(state, focusedId),
    };
  }, [state, focusedId]);

  // Store -> React Flow. Re-run dagre ONLY when the topology structure changes;
  // count/status/selection updates reuse existing positions, so the graph never
  // jumps around on high-frequency deltas.
  useEffect(() => {
    if (key !== prevKeyRef.current) {
      prevKeyRef.current = key;
      positionsRef.current = layoutGraph(
        [...state.nodes.values()],
        [...state.edges.values()],
        pinnedRef.current,
      );
    }
    const { nodes, edges } = deriveElements(state, positionsRef.current, selection);
    setNodes(nodes);
    setEdges(edges);
  }, [state, selection, key, setNodes, setEdges]);

  // Re-frame the graph whenever the structure (hence layout) changes.
  const fitKey = state.nodes.size === 0 ? '' : key;
  useEffect(() => {
    if (fitKey === '') return;
    const handle = requestAnimationFrame(() => fitView({ duration: 350, padding: 0.22 }));
    return () => cancelAnimationFrame(handle);
  }, [fitKey, fitView]);

  const onNodeClick = useCallback<NodeMouseHandler>((_event, node) => onFocus(node.id), [onFocus]);
  const onPaneClick = useCallback(() => onFocus(null), [onFocus]);

  return (
    <ReactFlow
      nodes={rfNodes}
      edges={rfEdges}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      nodeTypes={nodeTypes}
      edgeTypes={edgeTypes}
      onNodeClick={onNodeClick}
      onPaneClick={onPaneClick}
      nodesDraggable={false}
      nodesConnectable={false}
      elementsSelectable={false}
      minZoom={0.2}
      maxZoom={2.5}
      fitView
    >
      <ArrowMarkers />
      <Background variant={BackgroundVariant.Dots} gap={22} size={1} color="#16201d" />
      <MiniMap
        pannable
        zoomable
        className="hop-minimap"
        maskColor="rgba(7,11,10,0.72)"
        nodeColor="#1d2a26"
        nodeStrokeColor="#2a3a34"
      />
      <Controls showInteractive={false} className="hop-controls" />
    </ReactFlow>
  );
}

export function Canvas(props: CanvasProps) {
  return (
    <ReactFlowProvider>
      <CanvasInner {...props} />
    </ReactFlowProvider>
  );
}
