# Contributing to Hopscope

Thanks for your interest! Hopscope is a real-time visual event-stream debugger with a
microscopic .NET 10 Native AOT engine. This guide covers how we branch, commit, and what
every change must pass before it can merge.

> Hopscope is **open-core**: this repository is the free, Apache-2.0 engine + UI. Enterprise
> features (SSO, team collaboration, historical cloud storage) live in a separate private
> repository and are not covered here.

## Prerequisites
- **.NET 10 SDK** (`10.0.x`) — engine.
- **Docker** — footprint gate + local broker stack (`deploy/docker-compose.yml`).
- **Node 22 + pnpm/npm** — UI (`src/ui`, Phase 3+).
- **Go 1.22+** — eBPF agent (`src/agent`, Phase 5+).

## Branching model — trunk-based (GitHub Flow)
- **`main` is the trunk and is always releasable.** Never push to it directly.
- Cut a **short-lived branch off `main`**, open a PR, get it green + reviewed, **squash-merge**,
  delete the branch. Each commit on `main` is then one self-contained unit of work.
- **Branch naming:** `type/scope-summary`, scoped to the roadmap phase where it helps:
  ```
  feat/phase1-state-aggregator
  feat/phase4-redis-provider
  fix/aggregator-hopid-dedupe
  docs/architecture-phase5
  chore/ci-aot-gate
  ```
- External contributors: **fork → branch → PR** against `main`.

## Commits — Conventional Commits + DCO
- Use [Conventional Commits](https://www.conventionalcommits.org/): `feat:`, `fix:`, `docs:`,
  `refactor:`, `chore:`, `test:`, `build:`, `ci:`. This drives the changelog and versioning.
- **Sign off every commit** (Developer Certificate of Origin):
  ```bash
  git commit -s -m "feat(engine): add bounded channel back-pressure"
  ```
  The `Signed-off-by:` line certifies you wrote the code or have the right to submit it under
  the project license.

## What every change MUST pass (these are the merge gates)
Hopscope's whole premise is a microscopic, AOT-clean binary. CI enforces, and you should run
locally before pushing:

1. **AOT publish is warning-free** — a single trim/AOT warning is a build failure:
   ```bash
   dotnet publish src/engine/Hopscope.Host -c Release /p:PublishAot=true   # zero warnings
   ```
2. **Engine image < 60 MB**:
   ```bash
   docker build -f src/engine/Hopscope.Host/Dockerfile -t hopscope-engine:dev .
   docker images hopscope-engine:dev   # must be < 60 MB
   ```
3. **AOT commandments:** Minimal APIs only (no MVC/controllers/FastEndpoints);
   `System.Text.Json` source-gen only (register new wire types on `AppJsonSerializerContext`);
   no runtime plugin loading / reflection-emit; no `System.Linq.Expressions` in the hot path.
4. **Contract discipline:** if you touch `contracts/proto/event.proto`, regenerate **both** the
   C# and Go sides in the same change and keep `Hopscope.Domain/Events/EventEnvelope.cs`
   congruent. `payload_metadata` carries headers/keys/type-names only — **never message bodies**.
5. **Real-broker verification** where applicable — unit tests are necessary but not sufficient;
   verify against real brokers via `docker compose`.

## The contract is guarded
`contracts/proto/` is the single source of truth shared by the engine and the Go agent.
Changes there require maintainer review (see `.github/CODEOWNERS`) to prevent silent drift.

## Pull requests
- Fill in the PR template checklist.
- Keep PRs focused — one phase/chunk per PR.
- A green CI (AOT gate + image-size gate) and one maintainer approval are required to merge.
