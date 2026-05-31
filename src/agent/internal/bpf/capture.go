//go:build linux

package bpf

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cilium/ebpf/link"
	"github.com/cilium/ebpf/ringbuf"
	"github.com/cilium/ebpf/rlimit"
)

// Event is one captured Redis send: the issuing process and the bounded request
// prefix. Data is a private copy (the ring-buffer sample is reused after Read).
type Event struct {
	PID  uint32
	Comm string
	Data []byte
}

// Capture loads the eBPF program, attaches the tcp_sendmsg kprobe, and streams
// decoded Events. It requires CAP_BPF/CAP_PERFMON (or privileged) and kernel BTF.
type Capture struct {
	objs   bpfObjects
	link   link.Link
	reader *ringbuf.Reader
}

// NewCapture loads + attaches. The caller must Close the returned Capture.
func NewCapture() (*Capture, error) {
	if err := rlimit.RemoveMemlock(); err != nil {
		return nil, fmt.Errorf("remove memlock rlimit: %w", err)
	}

	var objs bpfObjects
	if err := loadBpfObjects(&objs, nil); err != nil {
		return nil, fmt.Errorf("load bpf objects (need kernel BTF at /sys/kernel/btf): %w", err)
	}

	kp, err := link.Kprobe("tcp_sendmsg", objs.RedisTcpSendmsg, nil)
	if err != nil {
		objs.Close()
		return nil, fmt.Errorf("attach kprobe tcp_sendmsg: %w", err)
	}

	rd, err := ringbuf.NewReader(objs.Events)
	if err != nil {
		kp.Close()
		objs.Close()
		return nil, fmt.Errorf("open ring buffer: %w", err)
	}

	return &Capture{objs: objs, link: kp, reader: rd}, nil
}

// Run reads events until ctx is cancelled, invoking handle for each decoded Event.
// handle must not block (it should hand off to a buffered sink).
func (c *Capture) Run(ctx context.Context, handle func(Event)) error {
	go func() {
		<-ctx.Done()
		c.reader.Close() // unblocks the Read below
	}()

	for {
		rec, err := c.reader.Read()
		if err != nil {
			if errors.Is(err, ringbuf.ErrClosed) || ctx.Err() != nil {
				return ctx.Err()
			}
			continue // transient read error — keep going
		}
		if ev, ok := decode(rec.RawSample); ok {
			handle(ev)
		}
	}
}

// Close detaches the probe and releases the program/maps.
func (c *Capture) Close() error {
	c.reader.Close()
	c.link.Close()
	return c.objs.Close()
}

// decode turns a raw ring-buffer sample into an Event, copying out only the captured
// prefix (Len bytes) and the NUL-trimmed comm.
func decode(raw []byte) (Event, bool) {
	var re bpfRedisEvent
	if err := binary.Read(bytes.NewReader(raw), binary.LittleEndian, &re); err != nil {
		return Event{}, false
	}
	n := int(re.Len)
	if n < 0 || n > len(re.Data) {
		n = len(re.Data)
	}
	return Event{
		PID:  re.Pid,
		Comm: commString(re.Comm),
		Data: append([]byte(nil), re.Data[:n]...),
	}, true
}

// commString converts a fixed-width, NUL-padded kernel comm into a Go string.
func commString(b [16]int8) string {
	buf := make([]byte, 0, len(b))
	for _, c := range b {
		if c == 0 {
			break
		}
		buf = append(buf, byte(c))
	}
	return string(buf)
}
