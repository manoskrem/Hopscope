import type { ConnectionStatus as Status, StatusInfo } from '../ws-client';

const LABELS: Record<Status, string> = {
  idle: 'IDLE',
  connecting: 'CONNECTING',
  open: 'LIVE',
  reconnecting: 'RECONNECTING',
  closed: 'OFFLINE',
};

interface ConnectionStatusProps {
  status: Status;
  info: StatusInfo;
  sequence: number;
}

export function ConnectionStatus({ status, info, sequence }: ConnectionStatusProps) {
  return (
    <span className={`conn conn--${status}`} role="status" aria-live="polite">
      <span className="conn__dot" aria-hidden />
      <span className="conn__label">{LABELS[status]}</span>
      {status === 'open' && sequence >= 0 && <span className="conn__meta">seq {sequence}</span>}
      {status === 'reconnecting' && (
        <span className="conn__meta">
          attempt {info.attempt}
          {info.nextRetryMs != null ? ` · ${(info.nextRetryMs / 1000).toFixed(1)}s` : ''}
        </span>
      )}
    </span>
  );
}
