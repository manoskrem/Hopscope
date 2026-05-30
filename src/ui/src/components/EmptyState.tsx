import type { ConnectionStatus as Status } from '../ws-client';

function message(status: Status): string {
  switch (status) {
    case 'reconnecting':
      return 'Reconnecting to the engine…';
    case 'closed':
      return 'Disconnected from the engine.';
    case 'idle':
    case 'connecting':
      return 'Connecting to the engine…';
    default:
      return 'Waiting for traffic…';
  }
}

export function EmptyState({ status }: { status: Status }) {
  return (
    <div className="empty">
      <div className="empty__pulse" aria-hidden />
      <div className="empty__title">HOPSCOPE</div>
      <div className="empty__msg">{message(status)}</div>
      <div className="empty__hint">
        Publish messages to a broker — or run the engine with the synthetic ingestor — and the
        topology builds itself.
      </div>
    </div>
  );
}
