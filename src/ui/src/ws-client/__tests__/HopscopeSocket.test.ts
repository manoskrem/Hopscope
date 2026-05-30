import { beforeEach, describe, expect, it, vi } from 'vitest';
import { HopscopeSocket, type ConnectionStatus, type SocketLike } from '../HopscopeSocket';
import type { GraphDelta, GraphSnapshot } from '../../contract/wire';

// --- a fake WebSocket the test drives by hand -------------------------------
class FakeSocket implements SocketLike {
  sent: string[] = [];
  closed = false;
  onopen: ((ev: unknown) => void) | null = null;
  onclose: ((ev: unknown) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  constructor(public url: string) {}
  send(data: string): void {
    this.sent.push(data);
  }
  close(): void {
    this.closed = true;
  }
  emitOpen(): void {
    this.onopen?.({});
  }
  emitFrame(obj: unknown): void {
    this.onmessage?.({ data: typeof obj === 'string' ? obj : JSON.stringify(obj) });
  }
  emitClose(): void {
    this.onclose?.({ code: 1006 });
  }
}

// --- a manual timer queue ---------------------------------------------------
function makeTimers() {
  let id = 0;
  const queue = new Map<number, () => void>();
  return {
    setTimeoutFn: (fn: () => void) => {
      const handle = ++id;
      queue.set(handle, fn);
      return handle;
    },
    clearTimeoutFn: (handle: number) => queue.delete(handle),
    pending: () => queue.size,
    runAll: () => {
      const fns = [...queue.values()];
      queue.clear();
      for (const fn of fns) fn();
    },
  };
}

function snapshotFrame(sequence: number): { kind: 'snapshot'; snapshot: GraphSnapshot; delta: null } {
  return { kind: 'snapshot', snapshot: { nodes: [], edges: [], sequence }, delta: null };
}
function deltaFrame(sequence: number): { kind: 'delta'; snapshot: null; delta: GraphDelta } {
  return { kind: 'delta', snapshot: null, delta: { upsertNodes: [], upsertEdges: [], sequence } };
}

describe('HopscopeSocket', () => {
  let sockets: FakeSocket[];
  let timers: ReturnType<typeof makeTimers>;
  let statuses: ConnectionStatus[];
  let snapshots: GraphSnapshot[];
  let deltas: GraphDelta[];

  function makeSocket() {
    sockets = [];
    return new HopscopeSocket({
      url: 'ws://test/ws',
      socketFactory: (url) => {
        const s = new FakeSocket(url);
        sockets.push(s);
        return s;
      },
      setTimeoutFn: timers.setTimeoutFn,
      clearTimeoutFn: timers.clearTimeoutFn,
      rand: () => 0.5,
      onStatus: (s) => statuses.push(s),
      onSnapshot: (s) => snapshots.push(s),
      onDelta: (d) => deltas.push(d),
    });
  }

  beforeEach(() => {
    timers = makeTimers();
    statuses = [];
    snapshots = [];
    deltas = [];
  });

  it('goes connecting → open and creates exactly one socket', () => {
    const sock = makeSocket();
    sock.connect();
    expect(statuses).toEqual(['connecting']);
    expect(sockets).toHaveLength(1);

    sockets[0].emitOpen();
    expect(statuses).toEqual(['connecting', 'open']);
    expect(sock.currentStatus).toBe('open');
  });

  it('applies the snapshot then in-order deltas, ignoring stale ones', () => {
    const sock = makeSocket();
    sock.connect();
    sockets[0].emitOpen();

    sockets[0].emitFrame(snapshotFrame(10));
    expect(snapshots).toHaveLength(1);
    expect(sock.lastSequence).toBe(10);

    sockets[0].emitFrame(deltaFrame(11));
    expect(deltas.map((d) => d.sequence)).toEqual([11]);
    expect(sock.lastSequence).toBe(11);

    sockets[0].emitFrame(deltaFrame(11)); // duplicate → stale → ignored
    sockets[0].emitFrame(deltaFrame(5)); // old → stale → ignored
    expect(deltas.map((d) => d.sequence)).toEqual([11]);
  });

  it('ignores deltas that arrive before the first snapshot', () => {
    const sock = makeSocket();
    sock.connect();
    sockets[0].emitOpen();

    sockets[0].emitFrame(deltaFrame(5)); // no baseline yet
    expect(deltas).toHaveLength(0);

    sockets[0].emitFrame(snapshotFrame(10));
    expect(snapshots).toHaveLength(1);
  });

  it('reconnects for a fresh snapshot on a forward gap (does not apply the gapped delta)', () => {
    const sock = makeSocket();
    sock.connect();
    sockets[0].emitOpen();
    sockets[0].emitFrame(snapshotFrame(10));

    sockets[0].emitFrame(deltaFrame(20)); // gap (expected 11)
    expect(deltas).toHaveLength(0); // gapped delta NOT applied
    expect(sockets[0].closed).toBe(true); // socket torn down
    expect(timers.pending()).toBe(1); // reconnect scheduled
    expect(sock.currentStatus).toBe('reconnecting');

    timers.runAll(); // fire the reconnect
    expect(sockets).toHaveLength(2);
    sockets[1].emitOpen();
    sockets[1].emitFrame(snapshotFrame(25)); // fresh baseline
    expect(sock.lastSequence).toBe(25);
  });

  it('reconnects with backoff on an unintentional drop and resets attempt on reopen', () => {
    const sock = makeSocket();
    sock.connect();
    sockets[0].emitOpen();

    sockets[0].emitClose(); // network drop
    expect(sock.currentStatus).toBe('reconnecting');
    expect(timers.pending()).toBe(1);

    timers.runAll();
    expect(sockets).toHaveLength(2);
    sockets[1].emitOpen(); // recovered
    expect(sock.currentStatus).toBe('open');

    // a second drop after recovery is treated as attempt 1 again (backoff reset)
    sockets[1].emitClose();
    expect(timers.pending()).toBe(1);
  });

  it('does not reconnect after an intentional close', () => {
    const sock = makeSocket();
    sock.connect();
    sockets[0].emitOpen();

    sock.close();
    expect(sock.currentStatus).toBe('closed');
    expect(sockets[0].closed).toBe(true);
    expect(timers.pending()).toBe(0);

    // a late close event from the now-detached socket must be a no-op
    sockets[0].emitClose();
    expect(timers.pending()).toBe(0);
  });

  it('derives the ws URL from location when no explicit url is given', () => {
    const calls: string[] = [];
    const sock = new HopscopeSocket({
      location: { protocol: 'https:', host: 'app.example.com' },
      socketFactory: (url) => {
        calls.push(url);
        return new FakeSocket(url);
      },
      setTimeoutFn: timers.setTimeoutFn,
      clearTimeoutFn: timers.clearTimeoutFn,
    });
    sock.connect();
    expect(calls).toEqual(['wss://app.example.com/ws']);
    vi.clearAllMocks();
  });
});
