import { useEffect, useMemo, useState } from 'react';
import { Canvas } from './canvas';
import { AppShell } from './components/AppShell';
import { ConnectionStatus } from './components/ConnectionStatus';
import { EmptyState } from './components/EmptyState';
import { ErrorPanel } from './components/ErrorPanel';
import { Legend } from './components/Legend';
import { errorEdges, graphStore, useGraphState } from './store';
import { HopscopeSocket, type ConnectionStatus as Status, type StatusInfo } from './ws-client';

export function App() {
  const state = useGraphState();
  const [focusedId, setFocusedId] = useState<string | null>(null);
  const [status, setStatus] = useState<Status>('idle');
  const [statusInfo, setStatusInfo] = useState<StatusInfo>({ lastSequence: -1, attempt: 0 });

  // One live connection for the app's lifetime. The WS snapshot-on-connect frame
  // bootstraps the store; deltas stream in after. URL is derived from location
  // (same-origin via the Vite/nginx proxy), so no engine host is hardcoded.
  useEffect(() => {
    const socket = new HopscopeSocket({
      onSnapshot: (snapshot) => graphStore.applySnapshot(snapshot),
      onDelta: (delta) => graphStore.applyDelta(delta),
      onStatus: (next, info) => {
        setStatus(next);
        setStatusInfo(info);
      },
    });
    socket.connect();
    return () => socket.close();
  }, []);

  const errors = useMemo(() => errorEdges(state), [state]);
  const hasData = state.nodes.size > 0;

  return (
    <AppShell
      nodeCount={state.nodes.size}
      edgeCount={state.edges.size}
      errorCount={errors.length}
      header={<ConnectionStatus status={status} info={statusInfo} sequence={state.sequence} />}
      rail={
        <>
          <Legend />
          <ErrorPanel errors={errors} focusedId={focusedId} onFocus={setFocusedId} />
        </>
      }
    >
      <Canvas focusedId={focusedId} onFocus={setFocusedId} />
      {!hasData && <EmptyState status={status} />}
    </AppShell>
  );
}
