import { ExecutionStatus, type GraphEdge } from '../contract/wire';
import { statusVisual } from '../canvas';

interface ErrorPanelProps {
  errors: GraphEdge[];
  focusedId: string | null;
  onFocus: (id: string | null) => void;
  onSelectEdge?: (source: string, target: string) => void;
}

export function ErrorPanel({ errors, focusedId, onFocus, onSelectEdge }: ErrorPanelProps) {
  return (
    <section className="rail-section">
      <h3 className="rail-h">ERRORS ({errors.length})</h3>
      {errors.length === 0 ? (
        <div className="rail-empty">— none —</div>
      ) : (
        <div className="err-list">
          {errors.map((edge) => {
            const v = statusVisual(edge.lastStatus);
            const active = focusedId === edge.sourceId;
            const statusText = edge.lastStatus === ExecutionStatus.DeadLettered ? 'DEADLETTERED' : 'FAILED';
            return (
              <button
                type="button"
                key={edge.id}
                className={`err-card err-card--${v.className}${active ? ' is-active' : ''}`}
                onClick={() => {
                  onFocus(active ? null : edge.sourceId);
                  onSelectEdge?.(edge.sourceId, edge.targetId);
                }}
                title={`Focus connections of ${edge.sourceId}`}
              >
                <span className="err-card__top">
                  <span className="err-card__route">
                    {edge.sourceId} → {edge.targetId}
                  </span>
                  <span className="err-card__count">×{edge.count}</span>
                </span>
                <span className="err-card__stat" style={{ color: v.color }}>
                  {statusText}
                </span>
              </button>
            );
          })}
        </div>
      )}
    </section>
  );
}
