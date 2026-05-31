package sink

import (
	"testing"
	"time"

	contractsv1 "github.com/hopscope/agent/internal/contracts/v1"
)

func TestNextBackoff(t *testing.T) {
	min, max := 250*time.Millisecond, 30*time.Second
	seq := []time.Duration{min}
	cur := min
	for i := 0; i < 12; i++ {
		next := nextBackoff(cur, max)
		if next < cur {
			t.Fatalf("backoff must be monotone non-decreasing: %v -> %v", cur, next)
		}
		if next > max {
			t.Fatalf("backoff must be clamped to %v, got %v", max, next)
		}
		cur = next
		seq = append(seq, cur)
	}
	if cur != max {
		t.Fatalf("backoff must reach the ceiling %v, ended at %v", max, cur)
	}
}

// TestEnqueueDropOldest verifies the buffer never blocks the (single) producer and
// drops the OLDEST envelope when full, keeping the freshest hops.
func TestEnqueueDropOldest(t *testing.T) {
	s := New(Config{Target: "unused", BufferSize: 2})

	mk := func(id string) *contractsv1.EventEnvelope {
		return &contractsv1.EventEnvelope{HopId: id}
	}

	if !s.Enqueue(mk("a")) || !s.Enqueue(mk("b")) {
		t.Fatal("first two enqueues should succeed")
	}
	// Buffer is full (cap 2). This must not block and must evict the oldest ("a").
	if !s.Enqueue(mk("c")) {
		t.Fatal("enqueue on a full buffer must still succeed via drop-oldest")
	}
	if got := s.Dropped(); got != 1 {
		t.Fatalf("Dropped() = %d, want 1", got)
	}

	// Drain: the remaining two must be the newest ("b","c"); "a" was evicted.
	first := <-s.ch
	second := <-s.ch
	if first.GetHopId() != "b" || second.GetHopId() != "c" {
		t.Fatalf("buffer held %q,%q; want b,c (oldest dropped)", first.GetHopId(), second.GetHopId())
	}
}

func TestNewAppliesDefaults(t *testing.T) {
	s := New(Config{Target: "engine:4318"})
	if cap(s.ch) != defaultBufferSize {
		t.Errorf("buffer cap = %d, want default %d", cap(s.ch), defaultBufferSize)
	}
	if s.cfg.MinBackoff != defaultMinBackoff || s.cfg.MaxBackoff != defaultMaxBackoff {
		t.Errorf("backoff defaults not applied: min=%v max=%v", s.cfg.MinBackoff, s.cfg.MaxBackoff)
	}
}
