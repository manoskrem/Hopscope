// Package bpf loads the Redis-RESP eBPF capture program and streams decoded events.
//
// The go:generate directive below compiles redis.bpf.c into a CO-RE object and emits the
// Go loader wrappers (bpf_bpfel.go / bpf_bpfeb.go) plus the bpfRedisEvent type. `go generate
// ./...` runs it inside the Docker build, where clang + libbpf headers are available; the
// generated wrappers and .o files are gitignored (build artifacts, not source). On non-Linux
// dev machines this file is the only member of the package, so it still compiles.
//
// -D__TARGET_ARCH_x86: BPF_KPROBE needs the target kernel arch for PT_REGS access. The agent
// runs amd64-only (the nightly runner / x86 kernels — BTF+kprobe are unreliable on the
// Apple-Silicon dev Docker, by design), so x86 is correct everywhere it actually runs.
package bpf

//go:generate go run github.com/cilium/ebpf/cmd/bpf2go -type redis_event -cc clang -cflags "-O2 -g -Wall -D__TARGET_ARCH_x86" bpf redis.bpf.c
