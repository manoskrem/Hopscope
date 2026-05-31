// Package mapper turns a captured Redis command (verb + first key + the client
// process that issued it) into the normalized EventEnvelope the engine ingests.
//
// It deliberately mirrors the in-proc RedisMapper's *shape* — a Redis node is a
// Redis node regardless of how it was tapped, so the canvas stays one coherent
// picture (the Phase-5 invariant: the engine cannot tell an agent-sourced hop from
// an in-proc-provider hop). The one place the agent's richer kernel-level knowledge
// shows through is Source: the real client process (from bpf_get_current_comm),
// which the management-API provider could never identify. That flows through the
// existing Service→Topic model, not a new node type, with provenance in metadata.
//
// The Redis value is never an input to this package and never appears in its output.
package mapper

import (
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	contractsv1 "github.com/hopscope/agent/internal/contracts/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	brokerType   = "Redis"
	defaultDB    = 0              // SELECT/db tracking is deferred — first slice assumes db 0.
	keyDepth     = 1              // first colon segment, matching the in-proc Redis provider.
	fallbackComm = "redis-client" // Source must be non-empty or the engine rejects the envelope.
	capturedBy   = "agent-ebpf"
)

// Mapper is safe for concurrent use; the only mutable state is a monotonic counter
// that keeps every captured command on a distinct HopId so the edge Count grows.
type Mapper struct {
	counter atomic.Int64
}

// New returns a ready Mapper.
func New() *Mapper { return &Mapper{} }

// FromCommand builds the EventEnvelope for one captured Redis command.
//
//	Source       = the client process (comm), sanitized; fallback "redis-client"
//	Destination  = KeyPrefix(firstKey) — the Redis Topic node (identical to Phase-4)
//	HopId        = fresh per call (counter) so the source→dest edge accumulates Count
//	TraceId      = stable per (source, prefix) so high-frequency commands don't flood
//	               the aggregator's trace LRU with singleton traces
//	BrokerType   = "Redis"; ExecutionStatus = SUCCESS (errors are a follow-up slice)
//
// firstKey may be "" (keyless commands like PING) → Destination "keys:*".
func (m *Mapper) FromCommand(comm, verb, firstKey string, pid uint32, now time.Time) *contractsv1.EventEnvelope {
	source := sanitizeComm(comm)
	prefix := KeyPrefix(firstKey, keyDepth)
	n := m.counter.Add(1)

	hopID := "agent-redis:" + source + ":" + prefix + ":" + strconv.FormatInt(n, 10)
	traceID := "redis-activity:" + source + ":" + prefix

	meta := map[string]string{
		"destinationKind": "Topic",
		"sourceKind":      "Service",
		"redisEvent":      strings.ToLower(verb),
		"keyPrefix":       prefix,
		"db":              strconv.Itoa(defaultDB),
		"capturedBy":      capturedBy,
		"clientComm":      source,
		"pid":             strconv.FormatUint(uint64(pid), 10),
	}

	return &contractsv1.EventEnvelope{
		TraceId:         traceID,
		HopId:           hopID,
		ParentHopId:     "", // empty == null (proto3 has no null; the engine maps it back)
		Source:          source,
		Destination:     prefix,
		BrokerType:      brokerType,
		PayloadMetadata: meta,
		Timestamp:       timestamppb.New(now.UTC()),
		ExecutionStatus: contractsv1.ExecutionStatus_SUCCESS,
		ErrorDetails:    nil,
	}
}

// sanitizeComm trims the NUL padding of a fixed-width kernel comm and guarantees a
// non-empty result (the engine rejects an envelope with an empty Source).
func sanitizeComm(comm string) string {
	comm = strings.TrimRight(comm, "\x00")
	comm = strings.TrimSpace(comm)
	if comm == "" {
		return fallbackComm
	}
	return comm
}
