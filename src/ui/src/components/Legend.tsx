import { ExecutionStatus, NodeKind } from '../contract/wire';
import { nodeVisual, statusVisual } from '../canvas';

const KINDS = [NodeKind.Service, NodeKind.Exchange, NodeKind.Topic, NodeKind.Queue];
const STATUSES = [
  ExecutionStatus.Success,
  ExecutionStatus.Retrying,
  ExecutionStatus.DeadLettered,
  ExecutionStatus.Failed,
];
const STATUS_NAME: Record<string, string> = {
  ok: 'Success',
  retry: 'Retrying',
  dlq: 'DeadLettered',
  fail: 'Failed',
};

export function Legend() {
  return (
    <section className="rail-section">
      <h3 className="rail-h">NODE KINDS</h3>
      {KINDS.map((kind) => {
        const v = nodeVisual(kind);
        return (
          <div className="legend-row" key={v.className}>
            <span className="legend-swatch legend-swatch--node" style={{ borderLeftColor: v.accentVar }} />
            <span className="legend-name">{v.kindLabel.charAt(0) + v.kindLabel.slice(1).toLowerCase()}</span>
            <span className="legend-glyph" style={{ color: v.accentVar }}>
              {v.glyph}
            </span>
          </div>
        );
      })}

      <h3 className="rail-h rail-h--spaced">STATUS</h3>
      {STATUSES.map((status) => {
        const v = statusVisual(status);
        return (
          <div className="legend-row" key={v.className}>
            <span className={`legend-swatch legend-swatch--edge legend-swatch--${v.className}`} style={{ background: v.color }} />
            <span className="legend-name">{STATUS_NAME[v.className]}</span>
            {v.isError && <span className="legend-alarm">⚠</span>}
          </div>
        );
      })}
    </section>
  );
}
