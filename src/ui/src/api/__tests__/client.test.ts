import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { fetchTrace, fetchTraces } from '../client';
import { ExecutionStatus } from '../../contract/wire';
import type { TraceView, TraceSummary } from '../../contract/wire';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function mockFetch(status: number, body: unknown): void {
  vi.stubGlobal(
    'fetch',
    vi.fn().mockResolvedValue({
      ok: status >= 200 && status < 300,
      status,
      statusText: status === 200 ? 'OK' : status === 404 ? 'Not Found' : 'Internal Server Error',
      json: () => Promise.resolve(body),
    }),
  );
}

const sampleTrace: TraceView = {
  traceId: 'abc-123',
  hopCount: 1,
  roots: [
    {
      envelope: {
        traceId: 'abc-123',
        hopId: 'hop-1',
        parentHopId: null,
        source: 'orders-service',
        destination: 'orders.dlq',
        brokerType: 'RabbitMQ',
        payloadMetadata: { 'x-death-count': '3' },
        timestamp: '2024-01-01T12:00:00.000Z',
        executionStatus: ExecutionStatus.DeadLettered,
        errorDetails: null,
      },
      children: [],
    },
  ],
};

const sampleSummaries: TraceSummary[] = [
  {
    traceId: 'abc-123',
    hopCount: 1,
    worstStatus: ExecutionStatus.DeadLettered,
    hasError: true,
    lastTimestamp: '2024-01-01T12:00:00.000Z',
  },
];

// ---------------------------------------------------------------------------

beforeEach(() => {
  vi.restoreAllMocks();
});

afterEach(() => {
  vi.unstubAllGlobals();
});

// ---------------------------------------------------------------------------
// fetchTrace
// ---------------------------------------------------------------------------

describe('fetchTrace', () => {
  it('returns null on a 404 response', async () => {
    mockFetch(404, null);
    const result = await fetchTrace('abc-123');
    expect(result).toBeNull();
  });

  it('parses and returns the body on 200', async () => {
    mockFetch(200, sampleTrace);
    const result = await fetchTrace('abc-123');
    expect(result).toEqual(sampleTrace);
  });

  it('throws on a non-200 non-404 status', async () => {
    mockFetch(500, null);
    await expect(fetchTrace('abc-123')).rejects.toThrow('500');
  });

  it('encodes a slashed id per-segment, preserving slashes as path separators', async () => {
    mockFetch(200, sampleTrace);
    const fetchSpy = vi.mocked(fetch);

    // id that simulates rmq-activity:/:orders.dlq:7
    await fetchTrace('rmq-activity:/:orders.dlq:7');

    const calledUrl = fetchSpy.mock.calls[0][0] as string;
    // Each segment is encoded individually; slashes between segments survive
    expect(calledUrl).toBe('/trace/rmq-activity%3A/%3Aorders.dlq%3A7');
  });

  it('encodes a simple id (no slashes) correctly', async () => {
    mockFetch(200, sampleTrace);
    const fetchSpy = vi.mocked(fetch);

    await fetchTrace('trace id with spaces');
    const calledUrl = fetchSpy.mock.calls[0][0] as string;
    expect(calledUrl).toBe('/trace/trace%20id%20with%20spaces');
  });
});

// ---------------------------------------------------------------------------
// fetchTraces
// ---------------------------------------------------------------------------

describe('fetchTraces', () => {
  it('builds the expected /traces?... query from params', async () => {
    mockFetch(200, sampleSummaries);
    const fetchSpy = vi.mocked(fetch);

    await fetchTraces({ status: 'failed', source: 'orders-service', target: 'orders.dlq', limit: 10 });

    const calledUrl = fetchSpy.mock.calls[0][0] as string;
    expect(calledUrl).toContain('/traces?');
    expect(calledUrl).toContain('status=failed');
    expect(calledUrl).toContain('source=orders-service');
    expect(calledUrl).toContain('target=orders.dlq');
    expect(calledUrl).toContain('limit=10');
  });

  it('omits empty params from the query string', async () => {
    mockFetch(200, sampleSummaries);
    const fetchSpy = vi.mocked(fetch);

    await fetchTraces({ source: 'svc-a' });
    const calledUrl = fetchSpy.mock.calls[0][0] as string;
    expect(calledUrl).toBe('/traces?source=svc-a');
    expect(calledUrl).not.toContain('status');
    expect(calledUrl).not.toContain('target');
    expect(calledUrl).not.toContain('limit');
  });

  it('uses /traces with no query string when no params given', async () => {
    mockFetch(200, []);
    const fetchSpy = vi.mocked(fetch);

    await fetchTraces({});
    const calledUrl = fetchSpy.mock.calls[0][0] as string;
    expect(calledUrl).toBe('/traces');
  });

  it('returns the parsed array on 200', async () => {
    mockFetch(200, sampleSummaries);
    const result = await fetchTraces({});
    expect(result).toEqual(sampleSummaries);
  });

  it('throws on non-OK status', async () => {
    mockFetch(503, null);
    await expect(fetchTraces({})).rejects.toThrow('503');
  });
});
