## What & why
<!-- One or two sentences. Link the roadmap phase (architecture.md §5) if relevant. -->

## Type
<!-- feat / fix / docs / refactor / chore / test / build / ci -->

## Checklist
- [ ] Branch named `type/scope-summary`; PR targets `main`.
- [ ] Commits use Conventional Commits and are **signed off** (`git commit -s` — DCO).
- [ ] `dotnet publish src/engine/Hopscope.Host -c Release /p:PublishAot=true` is **warning-free**.
- [ ] Engine image still **< 60 MB** (`docker build` + `docker images`).
- [ ] No AOT-breaking patterns introduced (Minimal APIs only; STJ source-gen; no reflection
      plugin loading; no LINQ `Expressions` in the hot path).
- [ ] If `contracts/proto/event.proto` changed: **both** C# and Go regenerated, `EventEnvelope`
      kept congruent, new field numbers only (none reused), `payload_metadata` still body-free.
- [ ] New wire types registered on `AppJsonSerializerContext`.
- [ ] Verified with real brokers via `docker compose` where applicable (not just unit tests).

## Notes for reviewers
<!-- Anything non-obvious: trade-offs, follow-ups, measured footprint numbers. -->
