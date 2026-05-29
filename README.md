# Hopscope

**Real-time visual event-stream debugger for microservices.** Hopscope maps asynchronous message flows (RabbitMQ / Kafka / Redis) onto a live, interactive canvas — showing service dependencies, message propagation, and where a trace died — **without intrusive code changes** to your services.

Ships as a microscopic OCI container (`<60 MB` image, `<35 MB` idle RAM, sub-second boot) for local `docker compose` sprints and production Kubernetes / ECS.

## How it works

1. **Tap** — broker-native APIs + OpenTelemetry by default; an optional eBPF/proxy sidecar for true zero-touch capture.
2. **Correlate** — a lock-free in-memory engine stitches hops into traces via correlation IDs and lifecycle status.
3. **Visualise** — graph deltas stream over WebSockets to a React Flow canvas that spawns nodes (services / exchanges / topics / queues) and edges (message hops) as traffic flows.

## Stack

| Layer | Tech |
|---|---|
| Core engine + push server | C# **.NET 10 Native AOT** · raw WebSockets |
| No-code agent (opt-in, Phase 5) | **Go** (`cilium/ebpf`) → gRPC |
| UI | **React 19** + **React Flow** + TypeScript |
| Packaging | Chiseled-AOT container · `docker compose` / K8s |

## Status

🚧 **Scaffolding (Phase 0).** Architecture is locked — see [`docs/architecture.md`](docs/architecture.md). Contribution and build conventions live in [`CONTRIBUTING.md`](CONTRIBUTING.md).

## Quick start (once the engine skeleton lands)

```bash
docker compose -f deploy/docker-compose.yml up
# open the UI, publish messages to the sample broker, watch the topology build itself
```

## Repository layout

```
contracts/   event.proto — the cross-language contract (source of truth)
src/engine/  C# .NET 10 AOT engine (Clean Architecture)
src/agent/   Go eBPF sidecar (Phase 5)
src/ui/      React + React Flow frontend
deploy/      docker-compose + k8s manifests
docs/        architecture & design
```
