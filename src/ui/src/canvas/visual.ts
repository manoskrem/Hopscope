import { ExecutionStatus, NodeKind } from '../contract/wire';

// ---- node kinds -----------------------------------------------------------

export interface NodeVisual {
  kindLabel: string;
  glyph: string;
  /** CSS custom-property reference (applied to HTML nodes). */
  accentVar: string;
  className: string;
}

export function nodeVisual(kind: NodeKind): NodeVisual {
  switch (kind) {
    case NodeKind.Service:
      return { kindLabel: 'SERVICE', glyph: '◇', accentVar: 'var(--kind-service)', className: 'kind-service' };
    case NodeKind.Exchange:
      return { kindLabel: 'EXCHANGE', glyph: '⬡', accentVar: 'var(--kind-exchange)', className: 'kind-exchange' };
    case NodeKind.Topic:
      return { kindLabel: 'TOPIC', glyph: '≈', accentVar: 'var(--kind-topic)', className: 'kind-topic' };
    case NodeKind.Queue:
      return { kindLabel: 'QUEUE', glyph: '▤', accentVar: 'var(--kind-queue)', className: 'kind-queue' };
    default:
      return { kindLabel: 'NODE', glyph: '○', accentVar: 'var(--dim)', className: 'kind-unknown' };
  }
}

// ---- execution status -----------------------------------------------------

export interface StatusVisual {
  label: string;
  /** Concrete hex (SVG strokes/markers can't reliably use CSS vars). Mirrors theme.css. */
  color: string;
  glow: string;
  /** 'ok' | 'retry' | 'dlq' | 'fail' | 'unknown' — also the SVG marker suffix. */
  className: string;
  isError: boolean;
}

export function statusVisual(status: ExecutionStatus): StatusVisual {
  switch (status) {
    case ExecutionStatus.Success:
      return { label: 'OK', color: '#37f08a', glow: 'rgba(55,240,138,0.5)', className: 'ok', isError: false };
    case ExecutionStatus.Retrying:
      return { label: 'RETRY', color: '#ffb547', glow: 'rgba(255,181,71,0.5)', className: 'retry', isError: false };
    case ExecutionStatus.DeadLettered:
      return { label: 'DLQ', color: '#ff49d0', glow: 'rgba(255,73,208,0.6)', className: 'dlq', isError: true };
    case ExecutionStatus.Failed:
      return { label: 'FAIL', color: '#ff4d4d', glow: 'rgba(255,77,77,0.65)', className: 'fail', isError: true };
    default:
      return { label: '?', color: '#7d958a', glow: 'transparent', className: 'unknown', isError: false };
  }
}

/** Statuses that get an SVG arrow marker defined on the canvas. */
export const MARKER_STATUSES: ExecutionStatus[] = [
  ExecutionStatus.Success,
  ExecutionStatus.Retrying,
  ExecutionStatus.DeadLettered,
  ExecutionStatus.Failed,
];
