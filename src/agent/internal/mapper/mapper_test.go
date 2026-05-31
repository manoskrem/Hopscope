package mapper

import (
	"bytes"
	"testing"
	"time"

	contractsv1 "github.com/hopscope/agent/internal/contracts/v1"
	"github.com/hopscope/agent/internal/resp"
	"google.golang.org/protobuf/proto"
)

func TestFromCommand_GoldenShape(t *testing.T) {
	m := New()
	now := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)

	env := m.FromCommand("redis-cli", "SET", "user:1", 4242, now)

	if env.GetSource() != "redis-cli" {
		t.Errorf("Source = %q, want redis-cli", env.GetSource())
	}
	if env.GetDestination() != "user:*" {
		t.Errorf("Destination = %q, want user:*", env.GetDestination())
	}
	if env.GetBrokerType() != "Redis" {
		t.Errorf("BrokerType = %q, want Redis", env.GetBrokerType())
	}
	if env.GetExecutionStatus() != contractsv1.ExecutionStatus_SUCCESS {
		t.Errorf("ExecutionStatus = %v, want SUCCESS", env.GetExecutionStatus())
	}
	if env.GetParentHopId() != "" {
		t.Errorf("ParentHopId = %q, want empty", env.GetParentHopId())
	}
	if env.GetErrorDetails() != nil {
		t.Errorf("ErrorDetails = %v, want nil", env.GetErrorDetails())
	}
	if env.GetTraceId() != "redis-activity:redis-cli:user:*" {
		t.Errorf("TraceId = %q", env.GetTraceId())
	}

	meta := env.GetPayloadMetadata()
	wantMeta := map[string]string{
		"destinationKind": "Topic",
		"sourceKind":      "Service",
		"redisEvent":      "set",
		"keyPrefix":       "user:*",
		"db":              "0",
		"capturedBy":      "agent-ebpf",
		"clientComm":      "redis-cli",
		"pid":             "4242",
	}
	for k, want := range wantMeta {
		if got := meta[k]; got != want {
			t.Errorf("metadata[%q] = %q, want %q", k, got, want)
		}
	}
}

func TestFromCommand_FreshHopStableTrace(t *testing.T) {
	m := New()
	now := time.Now()

	a := m.FromCommand("redis-cli", "GET", "user:9", 1, now)
	b := m.FromCommand("redis-cli", "GET", "user:9", 1, now)

	if a.GetHopId() == b.GetHopId() {
		t.Errorf("HopIds must differ per observation (edge Count): %q == %q", a.GetHopId(), b.GetHopId())
	}
	if a.GetTraceId() != b.GetTraceId() {
		t.Errorf("TraceId must be stable per (source, prefix): %q != %q", a.GetTraceId(), b.GetTraceId())
	}
}

func TestFromCommand_EmptyCommFallback(t *testing.T) {
	m := New()
	// A NUL-padded / empty comm must not yield an empty Source (the engine would reject it).
	env := m.FromCommand("\x00\x00\x00", "PING", "", 0, time.Now())
	if env.GetSource() != "redis-client" {
		t.Errorf("Source = %q, want redis-client fallback", env.GetSource())
	}
	if env.GetDestination() != "keys:*" {
		t.Errorf("Destination = %q, want keys:* for keyless command", env.GetDestination())
	}
}

// TestPipelinePrivacy is the end-to-end body-free assertion across resp + mapper:
// a real SET with a secret value, parsed and mapped, must not contain the value
// anywhere in the serialized envelope.
func TestPipelinePrivacy(t *testing.T) {
	const secret = "supersecretvalue"
	buf := []byte("*3\r\n$3\r\nSET\r\n$6\r\nuser:1\r\n$16\r\n" + secret + "\r\n")

	verb, key, ok := resp.Parse(buf)
	if !ok {
		t.Fatal("parse failed")
	}
	env := New().FromCommand("redis-cli", verb, key, 7, time.Now())

	blob, err := proto.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if bytes.Contains(blob, []byte(secret)) {
		t.Fatalf("value %q leaked into the serialized envelope", secret)
	}
}
