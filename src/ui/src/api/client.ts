import type { TraceView, TraceSummary } from '../contract/wire';

/**
 * Fetch a single trace by id. The id may contain literal '/' characters
 * (e.g. RabbitMQ vhost "/:orders.dlq:7") — encode each path segment
 * individually so slashes are preserved as path separators and the catch-all
 * route on the engine matches correctly.
 *
 * Returns null on 404 (trace evicted / unknown).
 * Throws on any other non-OK status.
 */
export async function fetchTrace(id: string): Promise<TraceView | null> {
  const encodedPath = id.split('/').map(encodeURIComponent).join('/');
  const res = await fetch(`/trace/${encodedPath}`);
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(`GET /trace/${encodedPath} → ${res.status} ${res.statusText}`);
  return (await res.json()) as TraceView;
}

/**
 * Fetch the list of retained trace summaries. Omits params whose values are
 * empty/undefined so the query string stays clean.
 *
 * Throws on non-OK.
 */
export async function fetchTraces(params: {
  status?: string;
  source?: string;
  target?: string;
  limit?: number;
}): Promise<TraceSummary[]> {
  const qs = new URLSearchParams();
  if (params.status) qs.set('status', params.status);
  if (params.source) qs.set('source', params.source);
  if (params.target) qs.set('target', params.target);
  if (params.limit !== undefined) qs.set('limit', String(params.limit));

  const query = qs.toString();
  const url = query ? `/traces?${query}` : '/traces';
  const res = await fetch(url);
  if (!res.ok) throw new Error(`GET ${url} → ${res.status} ${res.statusText}`);
  return (await res.json()) as TraceSummary[];
}
