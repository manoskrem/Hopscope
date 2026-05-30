import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';

// Same-origin model: the browser always talks to its OWN origin. In dev, Vite
// proxies /ws (WebSocket) and /snapshot to the engine's published host port
// (docker-compose maps engine 8080 -> host 8085). In production an nginx
// reverse-proxy plays the same role (see Dockerfile + nginx.conf). The engine
// therefore needs no CORS, and the client never hardcodes an engine host.
const ENGINE = process.env.HOPSCOPE_ENGINE_ORIGIN ?? 'http://localhost:8085';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/ws': { target: ENGINE, ws: true, changeOrigin: true },
      '/snapshot': { target: ENGINE, changeOrigin: true },
    },
  },
  test: {
    environment: 'jsdom',
    setupFiles: ['./src/test/setup.ts'],
    include: ['src/**/*.{test,spec}.{ts,tsx}'],
  },
});
