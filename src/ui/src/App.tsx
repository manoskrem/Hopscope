import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Canvas } from './canvas';
import { AppShell } from './components/AppShell';
import { ConnectionStatus } from './components/ConnectionStatus';
import { EmptyState } from './components/EmptyState';
import { ErrorPanel } from './components/ErrorPanel';
import { Legend } from './components/Legend';
import { TraceDetailPanel } from './components/TraceDetailPanel';
import { fetchTrace, fetchTraces } from './api/client';
import type { TraceView } from './contract/wire';
import { errorEdges, graphStore, useGraphState } from './store';
import { HopscopeSocket, type ConnectionStatus as Status, type StatusInfo } from './ws-client';

export function App() {
  const state = useGraphState();
  const [focusedId, setFocusedId] = useState<string | null>(null);
  const [status, setStatus] = useState<Status>('idle');
  const [statusInfo, setStatusInfo] = useState<StatusInfo>({ lastSequence: -1, attempt: 0 });

  // Trace drill-down state
  const [selectedTrace, setSelectedTrace] = useState<TraceView | null>(null);
  const [traceLoading, setTraceLoading] = useState(false);
  const [traceError, setTraceError] = useState<string | null>(null);
  const [traceEdgeLabel, setTraceEdgeLabel] = useState<string | null>(null);

  // Race-guard: ignore resolved fetches if a newer selection started
  const selectionSeqRef = useRef(0);

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

  const selectEdge = useCallback(async (source: string, target: string) => {
    const seq = ++selectionSeqRef.current;
    setTraceEdgeLabel(`${source} → ${target}`);
    setTraceLoading(true);
    setTraceError(null);
    setSelectedTrace(null);

    try {
      const summaries = await fetchTraces({ source, target });

      if (seq !== selectionSeqRef.current) return; // superseded

      if (summaries.length === 0) {
        setTraceLoading(false);
        setTraceError('No retained trace for this edge (it may have been evicted).');
        return;
      }

      // Pick worst-then-newest: sort by worstStatus desc, then lastTimestamp desc
      const sorted = [...summaries].sort((a, b) => {
        if (b.worstStatus !== a.worstStatus) return b.worstStatus - a.worstStatus;
        return b.lastTimestamp.localeCompare(a.lastTimestamp);
      });

      const picked = sorted[0];
      const traceView = await fetchTrace(picked.traceId);

      if (seq !== selectionSeqRef.current) return; // superseded

      if (traceView === null) {
        setTraceLoading(false);
        setTraceError('No retained trace for this edge (it may have been evicted).');
        return;
      }

      setSelectedTrace(traceView);
      setTraceLoading(false);
    } catch (err) {
      if (seq !== selectionSeqRef.current) return;
      setTraceLoading(false);
      setTraceError(err instanceof Error ? err.message : String(err));
    }
  }, []);

  const closeTrace = useCallback(() => {
    selectionSeqRef.current++;
    setSelectedTrace(null);
    setTraceError(null);
    setTraceLoading(false);
    setTraceEdgeLabel(null);
  }, []);

  const showTracePanel = traceLoading || traceError !== null || selectedTrace !== null;

  return (
    <AppShell
      nodeCount={state.nodes.size}
      edgeCount={state.edges.size}
      errorCount={errors.length}
      header={<ConnectionStatus status={status} info={statusInfo} sequence={state.sequence} />}
      rail={
        <>
          {showTracePanel && (
            <TraceDetailPanel
              trace={selectedTrace}
              loading={traceLoading}
              error={traceError}
              edgeLabel={traceEdgeLabel}
              onClose={closeTrace}
            />
          )}
          <Legend />
          <ErrorPanel errors={errors} focusedId={focusedId} onFocus={setFocusedId} onSelectEdge={selectEdge} />
        </>
      }
    >
      <Canvas focusedId={focusedId} onFocus={setFocusedId} onSelectEdge={selectEdge} />
      {!hasData && <EmptyState status={status} />}
    </AppShell>
  );
}
