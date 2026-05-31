import { Fragment } from 'react';
import { type HopNode, type TraceView, type ErrorDetails } from '../contract/wire';
import { statusVisual } from '../canvas/visual';
import '../styles/trace-panel.css';

interface TraceDetailPanelProps {
  trace: TraceView | null;
  loading: boolean;
  error: string | null;
  edgeLabel: string | null;
  onClose: () => void;
}

/** Format an ISO-8601 UTC string compactly: HH:MM:SS.mmm (date prefix only when not today). */
function formatTimestamp(iso: string): string {
  try {
    const d = new Date(iso);
    const now = new Date();
    const sameDay =
      d.getFullYear() === now.getFullYear() &&
      d.getMonth() === now.getMonth() &&
      d.getDate() === now.getDate();
    const time = d.toISOString().slice(11, 23); // HH:MM:SS.mmm
    if (sameDay) return time;
    return `${d.toISOString().slice(0, 10)} ${time}`;
  } catch {
    return iso;
  }
}

interface HopCardProps {
  node: HopNode;
  depth: number;
}

function HopCard({ node, depth }: HopCardProps) {
  const { envelope } = node;
  const v = statusVisual(envelope.executionStatus);
  const hasMetadata = Object.keys(envelope.payloadMetadata).length > 0;

  return (
    <div className={`hop-group${depth > 0 ? ' hop-group--child' : ''}`}>
      <div className={`hop-card hop-card--${v.className}`}>
        <div className="hop-card__route" title={`${envelope.source} → ${envelope.destination}`}>
          {envelope.source} → {envelope.destination}
        </div>

        <div className="hop-card__meta">
          <span className="hop-card__timestamp">{formatTimestamp(envelope.timestamp)}</span>
          <span className={`hop-pill hop-pill--${v.className}`}>{v.label}</span>
        </div>

        {hasMetadata && (
          <div className="hop-card__meta-table">
            {Object.entries(envelope.payloadMetadata).map(([k, val]) => (
              <Fragment key={k}>
                <span className="hop-card__meta-key" title={k}>
                  {k}
                </span>
                <span className="hop-card__meta-val" title={val}>
                  {val}
                </span>
              </Fragment>
            ))}
          </div>
        )}

        {envelope.errorDetails && <ErrorBlock details={envelope.errorDetails} />}
      </div>

      {node.children.length > 0 &&
        node.children.map((child) => (
          <HopCard key={child.envelope.hopId} node={child} depth={depth + 1} />
        ))}
    </div>
  );
}

interface ErrorBlockProps {
  details: ErrorDetails;
}

function ErrorBlock({ details }: ErrorBlockProps) {
  return (
    <div className="hop-card__error">
      <div className="hop-card__error-type" title={details.exceptionType}>
        {details.exceptionType}
      </div>
      <div className="hop-card__error-msg">{details.message}</div>
      {details.truncatedStackTrace && (
        <pre className="hop-card__error-stack">{details.truncatedStackTrace}</pre>
      )}
    </div>
  );
}

function LoadingState() {
  return (
    <div className="trace-panel__loading" aria-label="Loading trace">
      <div className="trace-panel__skel" />
      <div className="trace-panel__skel" />
      <div className="trace-panel__skel" />
    </div>
  );
}

export function TraceDetailPanel({ trace, loading, error, edgeLabel, onClose }: TraceDetailPanelProps) {
  return (
    <section className="rail-section trace-panel">
      <div className="trace-panel__header">
        <div className="trace-panel__title">
          <div className="trace-panel__edge-label">TRACE</div>
          {edgeLabel && (
            <div className="trace-panel__route" title={edgeLabel}>
              {edgeLabel}
            </div>
          )}
        </div>
        <button
          type="button"
          className="trace-panel__close"
          onClick={onClose}
          aria-label="Close trace panel"
          title="Close"
        >
          ×
        </button>
      </div>

      {loading && <LoadingState />}

      {!loading && error && <div className="trace-panel__error">{error}</div>}

      {!loading && !error && trace && (
        <div className="hop-tree">
          {trace.roots.length === 0 ? (
            <div className="trace-panel__empty">— no hops in this trace —</div>
          ) : (
            trace.roots.map((root) => (
              <HopCard key={root.envelope.hopId} node={root} depth={0} />
            ))
          )}
        </div>
      )}

      {!loading && !error && !trace && (
        <div className="trace-panel__empty">— select an edge to drill down —</div>
      )}
    </section>
  );
}
