# `hopscope-agent` — the no-code interception agent

`hopscope-agent` taps message flows at the **kernel**, with no change to the target
application or broker, and streams normalized events to the Hopscope engine over gRPC.
This first slice captures **Redis (RESP)** traffic.

It is the second half of the no-code seam: the engine already hosts the client-streaming
`hopscope.v1.Ingestion/Stream` RPC (port `4318`); the agent is the client.

## How it works

```
tcp_sendmsg (eBPF fentry, kernel-global)
   │  bounded prefix of the request + issuing process (comm/pid)
   ▼
ring buffer ──► RESP parse (verb + first key ONLY) ──► EventEnvelope ──► gRPC Stream ──► engine
```

- **Capture** (`internal/bpf`): a CO-RE eBPF **fentry** hook on `tcp_sendmsg` fires for every
  TCP send on the host kernel — regardless of which container issued it — keeps only sends to the
  Redis port, and copies a bounded **prefix** of the request bytes plus the issuing process
  (`bpf_get_current_comm`) into a ring buffer. fentry gives typed args from BTF, so one bytecode
  loads on any little-endian arch (amd64 + arm64).
- **Parse** (`internal/resp`): extracts the command verb and the **first key/channel only**.
- **Map** (`internal/mapper`): builds an `EventEnvelope` shaped exactly like the in-proc Redis
  provider's (a `Redis` Topic keyed by the key-prefix), but with the real **client process** in
  `Source` — knowledge the management-API provider never had. Provenance (`capturedBy`,
  `clientComm`, `pid`) goes in `payloadMetadata`.
- **Sink** (`internal/sink`): a bounded drop-oldest buffer feeds one long-lived `Ingestion.Stream`,
  reconnecting with bounded backoff so capture never blocks.

## Metadata only — never message bodies

The RESP parser reads **at most the verb and the first key**; the value (arg2+) is never parsed,
so it never enters an envelope. `payloadMetadata` carries routing metadata only. This is enforced
by tests (`internal/resp`, `internal/mapper`) — including an end-to-end assertion that a secret
value never appears in a serialized envelope.

## Platform support & when to use the agent

The agent is **one** of Hopscope's ingestion paths, and the only one that touches the kernel.
Pick the path that fits where you're running:

| Path | Kernel requirement | Runs on |
|---|---|---|
| **Broker providers** (RabbitMQ / Redis / Kafka) | none (userspace) | any OS, any laptop — Windows/WSL2 included |
| **OTLP receiver** (services push traces) | none (userspace) | any OS, any laptop |
| **eBPF agent** (this) — no instrumentation | **kernel BTF** | Linux with BTF: modern servers, managed Kubernetes nodes, Linux dev hosts, Docker Desktop for Mac |

The agent is the *no-instrumentation* option for Linux/cluster environments where you can't or
won't touch the target services. If you're developing locally on a kernel without BTF (some
**Windows/WSL2** setups, older/locked-down distros), use a **broker provider** or **OTLP** instead —
same canvas, zero kernel requirement — and run the agent in your cluster/CI.

**Containers share the host kernel — the agent image cannot carry its own.** eBPF loads into the
Docker VM's kernel, so what matters is whether *that* kernel has BTF, not the image. Check any host:

```bash
ls -l /sys/kernel/btf/vmlinux            # on a Linux host
# or through Docker (Mac/Windows), which checks the Docker VM's kernel:
docker run --rm --privileged -v /sys/kernel/btf:/sys/kernel/btf:ro alpine ls /sys/kernel/btf/vmlinux
```

If that file exists the agent runs; if not, it exits at startup pointing you to the provider/OTLP
paths (never silently). BTF has been default-on in mainline since ~5.2 and in major distros since
~2020, so modern server/cluster targets almost always qualify.

## Build & run

The agent loads eBPF and therefore needs **Linux with kernel BTF** and **CAP_BPF/CAP_PERFMON**
(or `--privileged`). Thanks to fentry it is **arch-portable** — it runs on any little-endian
kernel with BTF, including **Docker Desktop on Apple Silicon** (whose LinuxKit kernel ships BTF)
and the amd64 CI runner.

```bash
# Standalone no-code proof stack (engine with NO Redis provider, Redis with keyevents OFF):
docker compose -f deploy/docker-compose.agent.yml up -d --build --wait
bash deploy/e2e/agent-redis-check.sh   # Redis hops render from kernel capture alone
```

Configuration (env): `HOPSCOPE_AGENT_TARGET` (engine gRPC endpoint, default `engine:4318`).

## Layout

```
cmd/hopscope-agent/   entrypoint: wire capture → mapper → sink
internal/contracts/v1 committed Go bindings of contracts/proto/event.proto
internal/bpf/         redis.bpf.c + vmlinux_min.h (CO-RE) + bpf2go loader; Linux-only
internal/resp/        pure RESP parser (verb + first key); unit-tested
internal/mapper/      RawEvent → EventEnvelope; mirrors the engine's RedisMapper; unit-tested
internal/sink/        gRPC client: bounded drop-oldest buffer + reconnect/backoff
```

## Regenerating generated code

- **Go bindings** (`internal/contracts/v1/*.pb.go`, committed): from the frozen contract —
  ```bash
  protoc --proto_path=contracts/proto \
    --go_out=src/agent      --go_opt=module=github.com/hopscope/agent \
    --go-grpc_out=src/agent --go-grpc_opt=module=github.com/hopscope/agent \
    contracts/proto/event.proto
  ```
- **eBPF loader** (`internal/bpf/bpf_bpf*.go` + `.o`, gitignored): `go generate ./...` (needs
  clang + libbpf headers). This runs automatically in the agent's Docker build.

## Tests

```bash
cd src/agent
go test ./internal/resp/... ./internal/mapper/... ./internal/sink/...   # pure, no kernel
```

Kernel/eBPF integration is proven in the nightly `agent-ebpf-e2e` workflow (real kernel + BTF +
privileged), not per-PR.

## Roadmap

This slice captures Redis commands (status `Success`). Next: recv-side error replies
(`-ERR` → `Failed` + `ErrorDetails`), then AMQP and Kafka wire sniffers — each proven with that
broker's in-process provider OFF.
