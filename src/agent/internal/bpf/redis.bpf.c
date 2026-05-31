//go:build ignore

// SPDX-License-Identifier: GPL-2.0
//
// (The go:build constraint above keeps the Go toolchain from treating this as a cgo C
// source — it is compiled only by clang via bpf2go, never by `go build`.)
//
// Redis send-path capture (Phase-5 no-code agent, first slice).
//
// A kprobe on tcp_sendmsg fires for EVERY TCP send on the host kernel — regardless
// of which container or namespace issued it — so the agent observes a target's Redis
// commands with no change to the target. We keep only sends to the Redis port, copy a
// bounded PREFIX of the request bytes plus the issuing process (comm/pid) into a ring
// buffer, and let user space parse the RESP command + first key. The value (arg2+) is
// never parsed there, so it never leaves the host.

#include "vmlinux_min.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_core_read.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_endian.h>

#define DATA_LEN   256
#define REDIS_PORT 6379

// iter_type enum values on kernels >= 6.x: ITER_UBUF = 0, ITER_IOVEC = 1.
#define ITER_UBUF  0
#define ITER_IOVEC 1

// Shared event — laid out identically to the bpf2go-generated Go struct (bpfRedisEvent).
struct redis_event {
	__u32 pid;
	__u16 dport;
	__u16 pad;
	__u32 len;
	char  comm[16];
	__u8  data[DATA_LEN];
};

// Force struct redis_event into the program BTF so `bpf2go -type redis_event` emits it.
const struct redis_event *unused_redis_event __attribute__((unused));

struct {
	__uint(type, BPF_MAP_TYPE_RINGBUF);
	__uint(max_entries, 1 << 20); // 1 MiB
} events SEC(".maps");

SEC("kprobe/tcp_sendmsg")
int BPF_KPROBE(redis_tcp_sendmsg, struct sock *sk, struct msghdr *msg, __u64 size)
{
	__u16 dport = bpf_ntohs(BPF_CORE_READ(sk, __sk_common.skc_dport));
	if (dport != REDIS_PORT)
		return 0;

	__u64 total = BPF_CORE_READ(msg, msg_iter.count);
	if (total == 0)
		return 0;

	// Resolve the user buffer base. The iov_iter union changed across kernels:
	//   * ITER_UBUF (single buffer, modern fast path) → __ubuf_iovec.iov_base
	//   * ITER_IOVEC (segment array)                  → __iov[0].iov_base (legacy: iov[0])
	// CO-RE field-existence guards keep reads of absent fields unreachable (and thus
	// not relocated) on kernels that lack them. This is the iteration point if a future
	// runner kernel changes the layout again.
	void *base = 0;
	__u8 itype = BPF_CORE_READ(msg, msg_iter.iter_type);

	if (itype == ITER_UBUF && bpf_core_field_exists(((struct iov_iter *)0)->__ubuf_iovec)) {
		base = BPF_CORE_READ(msg, msg_iter.__ubuf_iovec.iov_base);
	} else {
		const struct iovec *iov = 0;
		if (bpf_core_field_exists(((struct iov_iter *)0)->__iov))
			iov = BPF_CORE_READ(msg, msg_iter.__iov);
		else
			iov = BPF_CORE_READ(msg, msg_iter.iov);
		base = BPF_CORE_READ(iov, iov_base);
	}
	if (!base)
		return 0;

	struct redis_event *e = bpf_ringbuf_reserve(&events, sizeof(*e), 0);
	if (!e)
		return 0;

	e->pid   = bpf_get_current_pid_tgid() >> 32;
	e->dport = dport;
	e->pad   = 0;
	bpf_get_current_comm(&e->comm, sizeof(e->comm));

	// Copy a bounded prefix. Cap at DATA_LEN-1 and mask so the verifier sees a value
	// strictly within the buffer (256 & 255 == 0 would read nothing — hence -1).
	__u32 n = total < DATA_LEN ? (__u32)total : (DATA_LEN - 1);
	n &= (DATA_LEN - 1);
	if (bpf_probe_read_user(&e->data, n, base) != 0) {
		bpf_ringbuf_discard(e, 0);
		return 0;
	}
	e->len = n;

	bpf_ringbuf_submit(e, 0);
	return 0;
}

char LICENSE[] SEC("license") = "GPL";
