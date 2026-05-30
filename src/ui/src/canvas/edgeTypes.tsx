import { memo, type CSSProperties } from 'react';
import { BaseEdge, EdgeLabelRenderer, getSmoothStepPath, type EdgeProps } from '@xyflow/react';
import { ExecutionStatus } from '../contract/wire';
import { statusVisual } from './visual';
import type { HopFlowEdge } from './types';

function StatusEdgeComponent(props: EdgeProps<HopFlowEdge>) {
  const { id, sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, markerEnd, data } =
    props;

  const [path, labelX, labelY] = getSmoothStepPath({
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    borderRadius: 10,
  });

  const v = statusVisual(data?.status ?? ExecutionStatus.Success);
  const dimmed = data?.dimmed ?? false;
  const highlighted = data?.highlighted ?? false;

  const edgeClass = ['hop-edge', v.className];
  if (dimmed) edgeClass.push('is-dimmed');
  if (highlighted) edgeClass.push('is-highlighted');

  const labelClass = ['hop-edge__label', v.className];
  if (dimmed) labelClass.push('is-dimmed');

  return (
    <>
      <BaseEdge
        id={id}
        path={path}
        markerEnd={markerEnd}
        className={edgeClass.join(' ')}
        style={{ stroke: v.color, '--glow': v.glow } as CSSProperties}
      />
      <EdgeLabelRenderer>
        <div
          className={labelClass.join(' ')}
          style={{ transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)` }}
        >
          {v.label} <span className="hop-edge__count">×{data?.count ?? 0}</span>
        </div>
      </EdgeLabelRenderer>
    </>
  );
}

export const StatusEdge = memo(StatusEdgeComponent);

/** Module-scope constant — see the note on nodeTypes. */
export const edgeTypes = { status: StatusEdge };
