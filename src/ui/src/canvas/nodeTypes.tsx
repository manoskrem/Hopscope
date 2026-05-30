import { memo, type CSSProperties } from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import { nodeVisual } from './visual';
import type { HopFlowNode } from './types';

function HopNodeComponent({ data }: NodeProps<HopFlowNode>) {
  const v = nodeVisual(data.kind);
  const classes = ['hop-node', v.className];
  if (data.dimmed) classes.push('is-dimmed');
  if (data.focused) classes.push('is-focused');

  return (
    <div className={classes.join(' ')} style={{ '--accent': v.accentVar } as CSSProperties}>
      <Handle type="target" position={Position.Left} className="hop-handle" />
      <div className="hop-node__bar">
        <span className="hop-node__kind">{v.kindLabel}</span>
        <span className="hop-node__glyph" aria-hidden>
          {v.glyph}
        </span>
      </div>
      <div className="hop-node__label" title={data.label}>
        {data.label}
      </div>
      <div className="hop-node__broker">{data.brokerType}</div>
      <Handle type="source" position={Position.Right} className="hop-handle" />
    </div>
  );
}

/** Memoized so frequent data updates don't re-render unchanged nodes. */
export const HopNode = memo(HopNodeComponent);

/** Module-scope constant — React Flow v12 re-mounts every node if this identity changes. */
export const nodeTypes = { hop: HopNode };
