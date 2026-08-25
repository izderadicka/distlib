# Deltas from the design plan

[`distlib-plan.md`](distlib-plan.md) is the original design and is **never edited**. This file is
the living record of every deviation from it, organised by the phase in which the deviation was
discovered.

**The rule:** any PR that changes a design assumption updates this file in the same PR. A delta
recorded weeks later is archaeology; a delta recorded alongside the code that depends on it is
documentation.

Each entry carries the section of `distlib-plan.md` it contradicts, what the doc says, what we
actually do, and why. "Carried forward" lists constraints discovered in one phase that a later
phase must design around.

---

## Cross-cutting

### C1 — iroh reached 1.0 (discovered in Phase 0, affects every phase)

The design doc was written against the iroh 0.10x series. Verified against crates.io and
docs.rs on 2026-08-23:

| §  | Doc says | Reality |
|---|---|---|
| §3 | "the iroh family moves fast (0.10x)" | **`iroh 1.0.3`** — stable and semver-committed. The compatible set is `iroh-blobs 0.103.0`, `iroh-gossip 0.101.0`, `iroh-docs 0.101.0`, `iroh-relay 1.0.3`, `irpc 0.17.0`; `iroh-docs 0.101` itself pins `iroh-blobs ^0.103` and `iroh-gossip ^0.101`, so the set is coherent. The exact-pin policy still stands, but the churn it guards against is much reduced. |
| §4.1, §8 | `NodeId`, `NodeAddr` | Renamed **`EndpointId`** (an alias of `PublicKey`) and **`EndpointAddr`**. |
| §3 | `discovery` | Renamed **`address_lookup()`** on the endpoint builder. |
| §5.1, §9 | "allowlist hook at connection accept" (mechanism unspecified) | iroh 1.0 provides **`EndpointHooks::after_handshake(&Connection) -> AfterHandshakeOutcome`**, which fires after the TLS handshake on both incoming *and* outgoing connections. `Reject { error_code: VarInt, reason: Vec<u8> }` closes the connection. This is the mechanism. |
| —  | MSRV unstated | The iroh family requires **Rust 1.91**. |

Three consequences that outlive Phase 0:

1. **The allowlist hook is endpoint-level, not per-protocol.** Phases 2–5 register
   `ProtocolHandler`s we do not author (iroh-blobs, iroh-docs, iroh-gossip). An allowlist check
   inside our own handler would protect `distlib/ping/0` and nothing else. `EndpointHooks` is the
   only place that covers all of them. **Do not move this check into a protocol handler.**
2. **`after_handshake` also gates our own outgoing connections.** This is correct for "do not talk
   to expelled members", but see the Phase 1 carry-forward below.
3. **`SecretKey::generate()` takes no RNG argument** in 1.0, so no `rand` dependency is needed.

---

## Phase 0 — Skeleton & transport

| # | §  | Doc says | We do | Why |
|---|---|---|---|---|
| P0-1 | §9 vs §8 | §9 names the binary crate `distlib-daemon`; §8 shows `crates/distlib/` | **`crates/distlib`** | §8's layout and the user-facing command name agree with each other; §9 was a slip. |
| P0-2 | §4.1 | keep `member_id` distinct from `node_id` so multi-device is possible later | **`MemberId` only — no `NodeId` newtype** | v1 defines them as equal. The name now shadows a type iroh renamed (`NodeId` → `EndpointId`), so introducing it buys a Phase 2 affordance at the price of permanent reader confusion. Phase 2 multi-device introduces `DeviceId` at the point it means something. |
| P0-3 | §9 | acceptance is "two containers on separate networks" | **in-process relay (`iroh::test_utils::run_relay_server`) plus `Builder::clear_ip_transports()`** | Makes the relay path structural instead of hoping loopback does not upgrade to a direct connection, and keeps the suite free of third-party infrastructure. `docker/` arrives in Phase 3 with the real binary. The in-process relay serves self-signed certificates, so the test endpoints need `CaTlsConfig::insecure_skip_verify()` and an `iroh-relay` dev-dependency — test-only, never on a production path. |
| P0-4 | §8 | integration tests live in `tests/` at the workspace root | `crates/distlib-net/tests/` | A single-crate test directory needs no extra scaffolding. Root `tests/` is reserved for the Phase 1+ multi-crate cluster harness that actually needs it. |
| P0-5 | §12 | openraft listed as a settled choice | **deliberately unpinned in Phase 0** | `0.9.25` (stable) vs `0.10.0-alpha.34` is a real decision with consequences, and Phase 0 does not depend on it. Decided at the start of Phase 1. |
| P0-6 | §2 | platforms are Linux, Windows and macOS | **CI tests Linux only** | Deliberate, for turnaround speed while the codebase is small. The matrix is kept as a one-element list so widening it is a one-line change. See the carry-forward below. |
| P0-7 | §5.2 | `item_id = BLAKE3(tag \|\| n \|\| sorted[ len(h_i) \|\| h_i ])` | **the per-element length prefix is dropped**: `BLAKE3(tag \|\| n \|\| sorted[ h_i ])` | Every `h_i` is a 32-byte BLAKE3 hash, so the concatenation is already unambiguous and the prefix distinguishes nothing. The forward-compatibility argument does not hold either: a later scheme admitting variable-length elements would carry its own domain tag, and the tag is what prevents a cross-version collision. The count is kept — one field, and it makes the pre-image self-describing. **This changes every item id**, which is free now and would not be once a catalogue exists. |
| P0-8 | §5.1, §9 | "allowlist hook at connection accept" | **two hooks, not one**: `before_connect` (refuse to dial a non-member) plus `after_handshake` (refuse a verified non-member) | `after_handshake` alone satisfies the acceptance criterion, but it fires only after packets have been exchanged, so an expelled member would learn the node is running. `before_connect` costs ~10 lines and closes that. `after_handshake` remains the authoritative check — it is the first point at which the peer's identity is proven rather than claimed — and is the only one that covers incoming connections. |

### Carried forward to Phase 1

- **Outgoing connections are gated by the same allowlist** (consequence 2 of C1). The join
  flow in §4.3 has a joiner connecting to a core node that is not yet in the joiner's log, and the
  joiner is not yet in the core node's log either. This needs a designed bootstrap exemption —
  scoped to the join ALPN and the addresses carried in the join ticket — rather than a workaround
  discovered while debugging. `crates/distlib-net/tests/allowlist.rs::outgoing_to_unknown_refused`
  pins the current behaviour so the exemption is a deliberate change.

  Note the exemption has to be made in **two** places, not one: `before_connect` refuses to dial a
  non-member before any packet is sent, and `after_handshake` refuses the verified peer. The join
  ALPN must be admitted by both.
- **openraft version choice** (P0-5).
- **Windows and macOS are unverified** (P0-6). §2 claims all three platforms, but CI exercises
  only Linux. The first code that actually diverges is the `0600` key-file handling in
  `distlib-core::identity` — `cfg(unix)` permissions with a Windows fallback — so that fallback
  compiles and runs untested for now. Widen `.github/workflows/ci.yml`'s `test` matrix before
  claiming cross-platform support anywhere user-facing (README, release artefacts).

---

## Phase 1 — Membership log (Raft core)

*Not started.*

## Phase 2 — Catalogue & library basics

*Not started.*

## Phase 3 — API + UI

*Not started.*

## Phase 4 — Availability + community metadata

*Not started.*

## Phase 5 — Custodianship & quotas

*Not started.*

## Phase 6 — Hardening & release

*Not started.*
