// Package sink streams captured envelopes to the engine's gRPC Ingestion service.
//
// It mirrors, client-side, the discipline the engine applies server-side: a bounded
// drop-oldest buffer decouples capture from the network so the kernel reader never
// blocks, and a single long-lived Ingestion.Stream is kept open and re-established
// with bounded exponential backoff on any error. Envelopes render on the canvas as
// they arrive — the stream is only half-closed on shutdown.
package sink

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	contractsv1 "github.com/hopscope/agent/internal/contracts/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Config configures a Sink. Zero values fall back to the defaults below.
type Config struct {
	Target     string        // engine gRPC endpoint, e.g. "engine:4318"
	BufferSize int           // bounded buffer capacity (drop-oldest when full)
	MinBackoff time.Duration // first reconnect delay
	MaxBackoff time.Duration // reconnect delay ceiling
	Logger     *slog.Logger
}

const (
	defaultBufferSize = 2048 // matches the engine's AgentChannelBridge capacity
	defaultMinBackoff = 250 * time.Millisecond
	defaultMaxBackoff = 30 * time.Second
	// A stream that stayed up at least this long is considered healthy, so the next
	// reconnect starts from MinBackoff again rather than the grown value.
	healthyConnFloor = 5 * time.Second
)

// Sink buffers envelopes and ships them to the engine. The buffer is written by a
// single producer (the capture loop) via Enqueue and drained by Run.
type Sink struct {
	cfg     Config
	ch      chan *contractsv1.EventEnvelope
	dropped atomic.Int64
	log     *slog.Logger
}

// New returns a ready Sink. dialer is optional (nil → real gRPC); it exists so tests
// can inject a fake connection.
func New(cfg Config) *Sink {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = defaultBufferSize
	}
	if cfg.MinBackoff <= 0 {
		cfg.MinBackoff = defaultMinBackoff
	}
	if cfg.MaxBackoff < cfg.MinBackoff {
		cfg.MaxBackoff = defaultMaxBackoff
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Sink{
		cfg: cfg,
		ch:  make(chan *contractsv1.EventEnvelope, cfg.BufferSize),
		log: cfg.Logger,
	}
}

// Enqueue offers an envelope to the buffer without ever blocking. When the buffer is
// full the OLDEST envelope is dropped (DropOldest) so the capture loop is never
// stalled — a dropped hop is preferable to back-pressuring the kernel reader. Safe
// for a SINGLE producer goroutine. Returns false if the envelope could not be queued.
func (s *Sink) Enqueue(env *contractsv1.EventEnvelope) bool {
	select {
	case s.ch <- env:
		return true
	default:
	}
	// Full: evict the oldest, then try once more.
	select {
	case <-s.ch:
		s.dropped.Add(1)
	default:
	}
	select {
	case s.ch <- env:
		return true
	default:
		return false
	}
}

// Dropped returns the number of envelopes evicted under back-pressure since startup.
func (s *Sink) Dropped() int64 { return s.dropped.Load() }

// Run drains the buffer to the engine until ctx is cancelled, reconnecting with
// bounded exponential backoff. It never returns an error for transient network
// failures — only ctx cancellation ends it (mirroring the engine's never-throws
// ingestor discipline).
func (s *Sink) Run(ctx context.Context) error {
	backoff := s.cfg.MinBackoff
	for ctx.Err() == nil {
		uptime, err := s.streamOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if uptime >= healthyConnFloor {
			backoff = s.cfg.MinBackoff // the connection was healthy; reset
		}
		s.log.Warn("agent: ingestion stream ended; reconnecting",
			"err", err, "backoff", backoff.String())
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		backoff = nextBackoff(backoff, s.cfg.MaxBackoff)
	}
	return ctx.Err()
}

// streamOnce dials, opens the client stream, and forwards buffered envelopes until
// ctx is cancelled or a send fails. It returns how long the connection stayed up.
func (s *Sink) streamOnce(ctx context.Context) (time.Duration, error) {
	conn, err := grpc.NewClient(s.cfg.Target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	client := contractsv1.NewIngestionClient(conn)
	stream, err := client.Stream(ctx)
	if err != nil {
		return 0, err
	}
	start := time.Now()
	s.log.Info("agent: connected to engine", "target", s.cfg.Target)

	for {
		select {
		case <-ctx.Done():
			if ack, e := stream.CloseAndRecv(); e == nil && ack != nil {
				s.log.Info("agent: stream closed", "accepted", ack.GetAccepted())
			}
			return time.Since(start), ctx.Err()
		case env := <-s.ch:
			if err := stream.Send(env); err != nil {
				return time.Since(start), err
			}
		}
	}
}

// nextBackoff doubles cur, clamped to max.
func nextBackoff(cur, max time.Duration) time.Duration {
	next := cur * 2
	if next > max {
		return max
	}
	return next
}
