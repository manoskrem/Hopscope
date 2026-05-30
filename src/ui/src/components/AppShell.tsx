import type { ReactNode } from 'react';
import '../styles/app.css';

interface AppShellProps {
  nodeCount: number;
  edgeCount: number;
  errorCount: number;
  header: ReactNode;
  rail: ReactNode;
  children: ReactNode;
}

export function AppShell({ nodeCount, edgeCount, errorCount, header, rail, children }: AppShellProps) {
  return (
    <div className="app">
      <header className="app__header">
        <span className="app__brand">
          HOP<b>SCOPE</b>
        </span>
        <span className="app__meta">
          topology <span className="app__sep">·</span> nodes <b>{nodeCount}</b>
          <span className="app__sep">·</span> edges <b>{edgeCount}</b>
          {errorCount > 0 && (
            <>
              <span className="app__sep">·</span> <span className="app__err">⚠ {errorCount}</span>
            </>
          )}
        </span>
        <span className="app__spacer" />
        {header}
      </header>
      <div className="app__body">
        <main className="app__stage">{children}</main>
        <aside className="app__rail">{rail}</aside>
      </div>
    </div>
  );
}
