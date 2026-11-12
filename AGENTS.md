# distlib

A distributed library: members share items over iroh, a Raft core group keeps the signed
membership log, and custodians keep every item available. Rust workspace under `crates/`,
Svelte UI under `crates/distlib-ui/web`.

Engineering standards, tooling and the git workflow: [`CLAUDE.md`](CLAUDE.md). It applies to
every agent, not only Claude.

Design and decisions: [`docs/distlib-plan.md`](docs/distlib-plan.md) is the design, the current
phase's plan is in [`docs/plan-phases/`](docs/plan-phases/), and every departure from a plan is a
row in [`docs/plan-deltas.md`](docs/plan-deltas.md).

## Code Review Rules

Review against `CLAUDE.md` and the rules below. The project is before v1: renames and format
changes need no compatibility layer or migration.

### Consensus

- The membership fold (`MembershipState::apply` and what it calls) is deterministic: every node
  reaches the same verdict from the log alone. Anything node-local in it (clocks, Raft metrics,
  the catalogue, configuration) is a P0.
- A change to what the fold accepts, or to any encoding on the wire or on disk, changes a golden
  encoding test in the same PR.
- Raft reconfiguration keeps quorum throughout: a voter stays seated until its removal is
  durably committed, and both halves of a joint configuration count.

### Runtime

- Shutdown order in `crates/distlib/src/runtime.rs` is load-bearing: the heartbeat goodbye and
  `endpoint.close()` stay adjacent (P4-2).
- Log levels follow consequence: `error` when a task stops or stays stale, `warn` when it
  recovers on its own.

### Plans

- A behaviour that departs from `docs/plan-phases/` comes with its row in `docs/plan-deltas.md`.
