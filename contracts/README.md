# `contracts/` — the cross-language seam

`proto/event.proto` is the **single source of truth** for the normalized event that flows
from every ingestor into the Hopscope engine. The C# engine and the Go eBPF agent (Phase 5)
must never drift from it. The change protocol below keeps the two sides congruent.

## What lives here
- `proto/event.proto` — the wire contract (`package hopscope.v1`). Authoritative for the
  gRPC boundary between the Go agent (client) and the engine (server).

The in-process counterpart is `src/engine/Hopscope.Domain/Events/EventEnvelope.cs`. The two
must stay congruent: same fields, same enum ordinals, same null↔empty mapping.

## Regenerating bindings (wired in Phase 5, not Phase 0)

Phase 0 ships the `.proto` as the frozen contract artifact only — no gRPC code is generated
yet (keeps the engine's dependency surface minimal and the AOT publish gate clean). When the
`RemoteAgentIngestor` lands in Phase 5:

- **C# (engine):** add `Grpc.Tools` + `Grpc.AspNetCore` to `Hopscope.Infrastructure` and
  reference this `.proto` as a `<Protobuf>` item — bindings generate on build into the
  `Hopscope.Contracts.V1` namespace (`option csharp_namespace`). Verify the generated code
  trims clean under `PublishAot=true`.
- **Go (agent):** `protoc --go_out --go-grpc_out` into the package named by `option
  go_package`.

## Change protocol (every contract edit)
1. Edit `event.proto` first — new field numbers only, **never reuse** a number.
2. Regenerate **both** sides in the same change.
3. Keep `EventEnvelope.cs` (+ `ExecutionStatus`/`ErrorDetails`) congruent.
4. Register any new wire type on `AppJsonSerializerContext` (AOT — no reflection fallback).
5. Preserve the null↔empty mapping in both normalizers. Keep `payload_metadata` body-free.
