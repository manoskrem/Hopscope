import { parseFrame, type GraphDelta, type GraphSnapshot } from '../contract/wire';
import { classifySequence } from './gap';
import { DEFAULT_BACKOFF, nextBackoffMs, type BackoffOptions } from './backoff';
import { resolveWsUrl, type UrlLike } from './url';

export type ConnectionStatus = 'idle' | 'connecting' | 'open' | 'reconnecting' | 'closed';

export interface StatusInfo {
  lastSequence: number;
  attempt: number;
  nextRetryMs?: number;
  reason?: string;
}

export interface SocketHandlers {
  onSnapshot?(snapshot: GraphSnapshot): void;
  onDelta?(delta: GraphDelta): void;
  onStatus?(status: ConnectionStatus, info: StatusInfo): void;
}

/** Minimal structural shape of a WebSocket — lets tests inject a fake. */
export interface SocketLike {
  send(data: string): void;
  close(code?: number, reason?: string): void;
  onopen: ((ev: unknown) => void) | null;
  onclose: ((ev: unknown) => void) | null;
  onerror: ((ev: unknown) => void) | null;
  onmessage: ((ev: { data: unknown }) => void) | null;
}

export type SocketFactory = (url: string) => SocketLike;

export interface HopscopeSocketOptions extends SocketHandlers {
  /** Explicit URL; if omitted it is derived from `location`. */
  url?: string;
  location?: UrlLike;
  socketFactory?: SocketFactory;
  setTimeoutFn?: (fn: () => void, ms: number) => number;
  clearTimeoutFn?: (handle: number) => void;
  rand?: () => number;
  backoff?: BackoffOptions;
}

const defaultFactory: SocketFactory = (url) =>
  new WebSocket(url) as unknown as SocketLike;

/**
 * Drives one logical connection to the engine's /ws endpoint:
 *   - snapshot-on-connect establishes the sequence baseline;
 *   - in-order deltas advance the cursor, stale deltas are dropped, a forward gap
 *     triggers a reconnect for a fresh snapshot;
 *   - drops/errors reconnect with exponential backoff + jitter (attempt resets on
 *     a successful open, so isolated gaps reconnect fast but persistent failures
 *     back off).
 * Deltas that arrive before the first snapshot are ignored (no baseline yet).
 */
export class HopscopeSocket {
  private readonly url: string;
  private readonly factory: SocketFactory;
  private readonly setTimeoutFn: (fn: () => void, ms: number) => number;
  private readonly clearTimeoutFn: (handle: number) => void;
  private readonly rand: () => number;
  private readonly backoff: BackoffOptions;
  private readonly handlers: SocketHandlers;

  private socket: SocketLike | null = null;
  private timer: number | undefined;
  private status: ConnectionStatus = 'idle';
  private intentional = false;
  private hasSnapshot = false;
  private lastSeq = -1;
  private attempt = 0;

  constructor(opts: HopscopeSocketOptions = {}) {
    const loc = opts.location ?? (typeof window !== 'undefined' ? window.location : undefined);
    this.url = opts.url ?? (loc ? resolveWsUrl(loc) : 'ws://localhost/ws');
    this.factory = opts.socketFactory ?? defaultFactory;
    this.setTimeoutFn =
      opts.setTimeoutFn ?? ((fn, ms) => setTimeout(fn, ms) as unknown as number);
    this.clearTimeoutFn =
      opts.clearTimeoutFn ?? ((h) => clearTimeout(h as unknown as ReturnType<typeof setTimeout>));
    this.rand = opts.rand ?? Math.random;
    this.backoff = opts.backoff ?? DEFAULT_BACKOFF;
    this.handlers = { onSnapshot: opts.onSnapshot, onDelta: opts.onDelta, onStatus: opts.onStatus };
  }

  get currentStatus(): ConnectionStatus {
    return this.status;
  }

  get lastSequence(): number {
    return this.lastSeq;
  }

  /** Open the connection. Resets the attempt counter (fresh, intentional connect). */
  connect(): void {
    this.intentional = false;
    this.attempt = 0;
    this.openSocket();
  }

  /** Close intentionally — no reconnect will be scheduled. */
  close(): void {
    this.intentional = true;
    this.clearTimer();
    this.teardownSocket();
    this.setStatus('closed');
  }

  // ---------------------------------------------------------------------------

  private openSocket(): void {
    this.hasSnapshot = false;
    this.setStatus(this.attempt === 0 ? 'connecting' : 'reconnecting');

    const socket = this.factory(this.url);
    this.socket = socket;
    socket.onopen = () => this.handleOpen();
    socket.onmessage = (ev) => this.handleMessage(ev.data);
    socket.onclose = () => this.handleDrop('close');
    socket.onerror = () => this.handleDrop('error');
  }

  private handleOpen(): void {
    this.attempt = 0; // a successful open clears prior failure backoff
    this.setStatus('open');
  }

  private handleMessage(data: unknown): void {
    if (typeof data !== 'string') return;
    const frame = parseFrame(data);
    if (!frame) return;

    if (frame.kind === 'snapshot' && frame.snapshot) {
      this.hasSnapshot = true;
      this.lastSeq = frame.snapshot.sequence;
      this.handlers.onSnapshot?.(frame.snapshot);
      return;
    }

    if (frame.kind === 'delta' && frame.delta) {
      if (!this.hasSnapshot) return; // no baseline yet — wait for the snapshot
      switch (classifySequence(this.lastSeq, frame.delta.sequence)) {
        case 'inOrder':
          this.lastSeq = frame.delta.sequence;
          this.handlers.onDelta?.(frame.delta);
          break;
        case 'stale':
          break; // already reflected — ignore
        case 'gap':
          this.scheduleReconnect('gap'); // do NOT apply; reconnect for a fresh snapshot
          break;
      }
    }
  }

  private handleDrop(reason: string): void {
    if (this.intentional) return;
    this.scheduleReconnect(reason);
  }

  private scheduleReconnect(reason: string): void {
    if (this.intentional) return;
    this.teardownSocket(); // detaches handlers first, so the close() below can't re-enter
    this.attempt += 1;
    const delay = nextBackoffMs(this.attempt, this.backoff, this.rand);
    this.status = 'reconnecting';
    this.handlers.onStatus?.('reconnecting', {
      lastSequence: this.lastSeq,
      attempt: this.attempt,
      nextRetryMs: delay,
      reason,
    });
    this.clearTimer();
    this.timer = this.setTimeoutFn(() => this.openSocket(), delay);
  }

  private teardownSocket(): void {
    const s = this.socket;
    if (!s) return;
    s.onopen = s.onclose = s.onerror = s.onmessage = null;
    try {
      s.close();
    } catch {
      // already closing/closed — ignore
    }
    this.socket = null;
  }

  private clearTimer(): void {
    if (this.timer !== undefined) {
      this.clearTimeoutFn(this.timer);
      this.timer = undefined;
    }
  }

  private setStatus(status: ConnectionStatus): void {
    this.status = status;
    this.handlers.onStatus?.(status, { lastSequence: this.lastSeq, attempt: this.attempt });
  }
}
