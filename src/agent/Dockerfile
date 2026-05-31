# syntax=docker/dockerfile:1
# Build context: repo root (built via deploy/docker-compose.agent.yml with context "..").
#
# The Hopscope no-code agent: an eBPF Redis-RESP sniffer that streams normalized
# EventEnvelopes to the engine. The build stage compiles redis.bpf.c into a CO-RE
# object (clang) and generates the Go loader (bpf2go) via `go generate`, then builds a
# static binary. The runtime image is distroless/static — the agent carries no native
# libraries; kernel BTF comes from the host mount at runtime, not the image. It must run
# privileged (or with CAP_BPF/CAP_PERFMON) with /sys/kernel/btf available.

# ---- build ----
FROM golang:1.25-bookworm AS build
# eBPF toolchain: clang/llvm compile the BPF program; libbpf-dev supplies <bpf/*.h>;
# linux-libc-dev + libelf-dev back the codegen and cilium/ebpf.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        clang llvm libbpf-dev linux-libc-dev libelf-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src/agent
# Cache the module layer on manifest-only changes.
COPY src/agent/go.mod src/agent/go.sum ./
RUN go mod download

COPY src/agent/ ./
# Compile redis.bpf.c → CO-RE object + Go loader wrappers, then build the static binary.
RUN go generate ./...
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/hopscope-agent ./cmd/hopscope-agent

# ---- runtime: static binary; runs privileged with /sys/kernel/btf mounted ----
FROM gcr.io/distroless/static-debian12:latest AS final
COPY --from=build /out/hopscope-agent /hopscope-agent
ENTRYPOINT ["/hopscope-agent"]
