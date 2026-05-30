// Derive the WebSocket URL from the page's own origin. The UI is always served
// same-origin with the engine (Vite proxy in dev, nginx in prod), so we never
// hardcode an engine host — wss when the page is https, ws otherwise.

export interface UrlLike {
  protocol: string;
  host: string;
}

export function resolveWsUrl(loc: UrlLike, path = '/ws'): string {
  const scheme = loc.protocol === 'https:' ? 'wss' : 'ws';
  return `${scheme}://${loc.host}${path}`;
}
