import { describe, it, expect, vi } from 'vitest';
import { render, fireEvent } from '@testing-library/react';
import { TraceDetailPanel } from '../TraceDetailPanel';
import { ExecutionStatus } from '../../contract/wire';
import type { TraceView } from '../../contract/wire';

// ---------------------------------------------------------------------------
// Fixture: 3-hop chain Success → Retrying → DeadLettered (with ErrorDetails)
// ---------------------------------------------------------------------------

const trace3Hop: TraceView = {
  traceId: 'trace-xyz',
  hopCount: 3,
  roots: [
    {
      envelope: {
        traceId: 'trace-xyz',
        hopId: 'hop-1',
        parentHopId: null,
        source: 'payment-service',
        destination: 'payments.exchange',
        brokerType: 'RabbitMQ',
        payloadMetadata: { 'x-correlation-id': 'corr-001' },
        timestamp: '2024-06-15T10:00:00.000Z',
        executionStatus: ExecutionStatus.Success,
        errorDetails: null,
      },
      children: [
        {
          envelope: {
            traceId: 'trace-xyz',
            hopId: 'hop-2',
            parentHopId: 'hop-1',
            source: 'payments.exchange',
            destination: 'payments.retry-queue',
            brokerType: 'RabbitMQ',
            payloadMetadata: { 'x-retry-count': '1' },
            timestamp: '2024-06-15T10:00:01.000Z',
            executionStatus: ExecutionStatus.Retrying,
            errorDetails: null,
          },
          children: [
            {
              envelope: {
                traceId: 'trace-xyz',
                hopId: 'hop-3',
                parentHopId: 'hop-2',
                source: 'payments.retry-queue',
                destination: 'payments.dlq',
                brokerType: 'RabbitMQ',
                payloadMetadata: { 'x-death-count': '3' },
                timestamp: '2024-06-15T10:00:05.000Z',
                executionStatus: ExecutionStatus.DeadLettered,
                errorDetails: {
                  exceptionType: 'PaymentGatewayException',
                  message: 'Gateway timeout after 3 retries',
                  truncatedStackTrace: 'at PaymentGateway.Charge()\nat PaymentWorker.Process()',
                },
              },
              children: [],
            },
          ],
        },
      ],
    },
  ],
};

// ---------------------------------------------------------------------------
// Tests
// Use queries scoped to the rendered container — prevents cross-test pollution
// when multiple renders accumulate in the document body.
// ---------------------------------------------------------------------------

describe('TraceDetailPanel', () => {
  it('renders all three source→destination hops', () => {
    const { getAllByText } = render(
      <TraceDetailPanel
        trace={trace3Hop}
        loading={false}
        error={null}
        edgeLabel="payment-service → payments.dlq"
        onClose={vi.fn()}
      />,
    );

    // Route divs have split text nodes; use title attribute via getAllByText with exact:false
    expect(getAllByText(/payment-service.*payments\.exchange/i).length).toBeGreaterThan(0);
    expect(getAllByText(/payments\.exchange.*payments\.retry-queue/i).length).toBeGreaterThan(0);
    expect(getAllByText(/payments\.retry-queue.*payments\.dlq/i).length).toBeGreaterThan(0);
  });

  it('shows the DeadLettered status label', () => {
    const { getAllByText } = render(
      <TraceDetailPanel
        trace={trace3Hop}
        loading={false}
        error={null}
        edgeLabel={null}
        onClose={vi.fn()}
      />,
    );
    // statusVisual(DeadLettered) → label 'DLQ' — there is exactly one DLQ pill in this render
    const dlqPills = getAllByText('DLQ');
    expect(dlqPills.length).toBeGreaterThan(0);
  });

  it('shows the ErrorDetails exceptionType and message for the DeadLettered hop', () => {
    const { getAllByText } = render(
      <TraceDetailPanel
        trace={trace3Hop}
        loading={false}
        error={null}
        edgeLabel={null}
        onClose={vi.fn()}
      />,
    );

    expect(getAllByText('PaymentGatewayException').length).toBeGreaterThan(0);
    expect(getAllByText('Gateway timeout after 3 retries').length).toBeGreaterThan(0);
  });

  it('shows loading skeleton state when loading=true', () => {
    const { getByLabelText } = render(
      <TraceDetailPanel
        trace={null}
        loading={true}
        error={null}
        edgeLabel="a → b"
        onClose={vi.fn()}
      />,
    );

    expect(getByLabelText('Loading trace')).toBeInTheDocument();
  });

  it('shows the error message when error is set', () => {
    const { getByText } = render(
      <TraceDetailPanel
        trace={null}
        loading={false}
        error="No retained trace for this edge (it may have been evicted)."
        edgeLabel="a → b"
        onClose={vi.fn()}
      />,
    );

    expect(
      getByText('No retained trace for this edge (it may have been evicted).'),
    ).toBeInTheDocument();
  });

  it('calls onClose when the close button is clicked', () => {
    const onClose = vi.fn();
    const { container } = render(
      <TraceDetailPanel
        trace={trace3Hop}
        loading={false}
        error={null}
        edgeLabel="a → b"
        onClose={onClose}
      />,
    );

    // Query scoped to this render's container to avoid picking up buttons from prior tests
    const closeButton = container.querySelector('.trace-panel__close') as HTMLElement;
    expect(closeButton).not.toBeNull();
    fireEvent.click(closeButton);
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('renders the edge label in the header', () => {
    const { getAllByText } = render(
      <TraceDetailPanel
        trace={null}
        loading={false}
        error={null}
        edgeLabel="payment-service → payments.dlq"
        onClose={vi.fn()}
      />,
    );
    expect(getAllByText('payment-service → payments.dlq').length).toBeGreaterThan(0);
  });

  it('shows status pills for all three statuses present in the chain', () => {
    const { getAllByText } = render(
      <TraceDetailPanel
        trace={trace3Hop}
        loading={false}
        error={null}
        edgeLabel={null}
        onClose={vi.fn()}
      />,
    );
    // Success → 'OK', Retrying → 'RETRY', DeadLettered → 'DLQ'
    expect(getAllByText('OK').length).toBeGreaterThan(0);
    expect(getAllByText('RETRY').length).toBeGreaterThan(0);
    expect(getAllByText('DLQ').length).toBeGreaterThan(0);
  });
});
