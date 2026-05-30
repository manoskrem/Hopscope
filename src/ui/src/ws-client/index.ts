export { HopscopeSocket } from './HopscopeSocket';
export type {
  ConnectionStatus,
  SocketHandlers,
  SocketLike,
  SocketFactory,
  StatusInfo,
  HopscopeSocketOptions,
} from './HopscopeSocket';
export { classifySequence, type SequenceClass } from './gap';
export { nextBackoffMs, DEFAULT_BACKOFF, type BackoffOptions } from './backoff';
export { resolveWsUrl, type UrlLike } from './url';
