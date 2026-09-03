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
- ~~Expulsion must close connections that are already open (§4.4).~~ **Discharged.**
  `Allowlist::changed()` is exposed and `AllowlistHooks::evict_expelled` consumes it, closing live
  connections to removed members with the same `NOT_A_MEMBER` code a refused handshake gets. The
  hooks keep a `WeakConnectionHandle` per peer, which is what the `EndpointHooks` docs prescribe
  for looking a connection up later without disabling close-on-drop.
- **openraft version choice** (P0-5).
- **Windows and macOS are unverified** (P0-6). §2 claims all three platforms, but CI exercises
  only Linux. The first code that actually diverges is the `0600` key-file handling in
  `distlib-core::identity` — `cfg(unix)` permissions with a Windows fallback — so that fallback
  compiles and runs untested for now. Widen `.github/workflows/ci.yml`'s `test` matrix before
  claiming cross-platform support anywhere user-facing (README, release artefacts).

---

## Phase 1 — Membership log (Raft core)

| # | §  | Doc says | We do | Why |
|---|---|---|---|---|
| P1-1 | §4.2 | "Append-only log of **signed** events"; followers "verify the core's signatures" — the scheme is not specified | **Each event is signed by the member who proposed it**, in an envelope carrying `proposer` and `at`, verified when the event is applied | The transport already authenticates *who served us the log* (iroh connections are mutually authenticated), so what a signature adds is attribution of the entry itself: a compromised core node cannot invent a `MemberExpelled` and attribute it to somebody else. It gives §4.3's "proposer is recorded — auditability over ceremony" real weight for ~64 bytes and one verify per entry. It does **not** defend against a core node that refuses to serve entries or serves a stale prefix — that is Raft's problem, and outside §2's threat model either way. |
| P1-2 | §4.2 | `MemberAdded { invited_by }` and `MemberExpelled { proposed_by }` | **Both fields dropped** | The signing envelope already carries an authenticated `proposer`, which is the same member. Keeping both would let them disagree with no rule for which wins — and only the signed one means anything. |
| P1-3 | §4.2 | `MemberRecord` carries `joined_at` and `last_changed` | **Both fields dropped** | They are the proposer's clock, which nothing verifies and nothing keeps in step, so they can never be authoritative. Nothing derived from the log reads them, and they are not lost: each event's envelope carries `at`, so a UI wanting "joined on" reads it from the `MemberAdded` entry. Keeping them in the projection would invite exactly the mistake the `Timestamp` docs warn against — comparing timestamps to decide what happened first, when log order is the only truth. |
| P1-4 | §12, P0-5 | openraft listed as settled, version unstated | **`=0.9.25` with the `storage-v2` feature** | Latest stable. `0.10` has been `0.10.0-alpha.34` for months with no sign of landing, and the exact-pin policy exists precisely to avoid depending on something that can break without semver. `storage-v2` is not a preference: `impl<T> Sealed for T {}` is gated on it, so without that feature `RaftLogStorage` and `RaftStateMachine` cannot be implemented at all. Closes P0-5. |
| P1-5 | §4.1 | — | **`RawMemberId` in `distlib-core`; `MemberId` deliberately has no `Default`** | `openraft::NodeId` requires `Default`, which an ed25519 key cannot sensibly have — `PublicKey` validates that its bytes decompress to a curve point. Giving `MemberId` one would be a hazard, not a convenience: `#[serde(default)]` on any config or wire struct would then silently produce *a member* rather than an error, in a system where membership is the security boundary. openraft needs the bound only so its own `testing::Suite` can build placeholder log ids — a test-harness artifact, not a domain concept. `RawMemberId` is the unvalidated byte form, sited in core rather than consensus so the workspace has one such type and one validation point (`MemberId::try_from`) for ids arriving from any framework or wire. |
| P1-6 | §4.5 | "log + state machine persisted in redb" | **`redb = "=4.2.0"`** | Resolved against the tree rather than guessed: `iroh-blobs 0.103` and `iroh-docs 0.101` both use redb 4.2.0. (`iroh-docs` additionally pulls 3.1.3 behind its `redb-v2-migration` feature — not the live engine.) Matching it means Phase 2 adds no second storage engine. |
| P1-7 | §4.5 | "Snapshots: trivial (state = the full membership table; it's small)" | **a persistent state machine**: `apply` commits before returning, and snapshots exist to catch peers up rather than to recover from | The doc is right that the state is small, which is what makes this affordable — one commit per apply, and recovery is a read rather than a log replay from the last snapshot. openraft offers either arrangement; this is the one with less to reason about. |
| P1-8 | §4.2 | — | **a committed event the rules reject is skipped, not fatal** | By the time an event reaches the state machine Raft has committed it, so every node sees it. Returning a storage error would make one malformed proposal a simultaneous fatal failure on every node in the group. `MembershipState::apply` is deterministic and leaves state untouched on error, so skipping is equally consistent and merely loud: the event is logged at `warn` and the log id still advances. Proposals are validated before submission; this is the backstop. |
| P1-9 | — | — | **`distlib-core` gains a `testing` feature** enabling `RawMemberId: From<u64>` | openraft's conformance suite requires `NodeId: From<u64>` to mint ids in its fixtures. An integer is not a member, so the impl is gated out of production builds entirely. Note this is a *weaker* concession than `Default` would have been (P1-5): a feature flag could not have made `Default` safe, because the hazard there — `#[serde(default)]` inventing a member — lives in code paths a production build compiles. |
| P1-10 | §4.5 | "Snapshots: trivial" | **a stored snapshot is never allowed to move backwards** | openraft spawns `build_snapshot` onto its own task while the state machine worker keeps running, so a builder started at an older log id can still be in flight when a newer snapshot is installed from the leader. Overwriting blindly would regress `get_current_snapshot` — and because openraft purges the log up to an installed snapshot, the entries needed to bridge the gap are already gone, leaving the node unable to catch anyone up. `build_snapshot` therefore skips the store when a newer snapshot is present, while still returning what it built. |
| P1-11 | §5.1, P0-4 | `alpn::registered()` is "every ALPN this build accepts" | **it is the set [`crate::Node`] serves, and `configure` takes the ALPNs as a parameter** | An endpoint must advertise exactly what its router handles: negotiating a protocol and then having no handler for it means accepting the connection and refusing every stream. `distlib-net` cannot serve `distlib/raft/0` — the handler needs a `Raft`, which lives in `distlib-consensus`, and that crate depends on `distlib-net` rather than the reverse. So the caller that builds the router now says which ALPNs it serves. |
| P1-12 | §4.2, §4.5 | — | **the bootstrap seed is kept until a `GroupFounded` is applied, then the log wins outright** | The circularity the plan predicted, now in code: core nodes cannot replicate the founding entry without connecting, and cannot connect without an allowlist. So the follow task publishes the log-derived allowlist only once the group exists; before that it leaves the seed alone, and a node whose log has nothing to say would otherwise enforce an empty allowlist and never reach anyone. After founding the seed is never consulted again, so a member who was only ever in configuration stops being admitted. Both directions are pinned by tests, and each fails under the opposite mutation. |
| P1-13 | §4.3 | "That member submits `ProposeAdd` to any core node" | **a proposal made on a follower is forwarded to the leader**, over the existing `distlib/raft/0` ALPN | openraft's `client_write` does not forward: it returns `ForwardToLeader` and leaves it to the application. Without forwarding only the founder could ever grow the group, because every core node admitted afterwards is a follower, and so is a node that restarted while somebody else held the term. The forwarded reply carries a message rather than a typed openraft error — a follower has no use for the leader's own `ForwardToLeader` pointing back at itself. |
| P1-14 | §4.2 | — | **`R = Result<(), ConsensusError>`: a write returns the state machine's verdict** | Committing and applying are different things here. P1-8 makes a committed event whose rules do not hold a skip rather than a fatal error, so a successful `client_write` says only that the entry reached the log. Without the verdict, `propose` reported success for a change that never happened — and any member could fill the log with entries no node will ever apply. `ConsensusError` had to become serialisable to travel back, which also removed `postcard::Error` from a domain type. |
| P1-15 | §3, §5.1 | — | **connections are pooled per (peer, protocol) in `distlib-net`, not per peer** | ALPN is negotiated once in the TLS handshake: `Connection::alpn()` reads it from handshake data, and iroh's router resolves a handler once and gives it the whole connection. Streams multiplex within a protocol, never across, so raft and the phase 1b log protocol each hold their own connection to a peer. That is iroh's model rather than a choice — avoiding it would mean multiplexing our protocols under one ALPN with our own dispatch. The cost is bounded: iroh keeps address lookup, hole punching and path selection per *remote*, so a second connection to a known peer is a handshake over an established path. `ping` deliberately does not pool — a liveness probe over a cached connection answers the wrong question. |
| P1-16 | §3, §4.3 | `net.allowlist` configures who a node talks to | **`net.allowlist` is gone; `[consensus] core` replaces it** | The allowlist is now derived from the log, so configuring it would be configuring a cache. What configuration still has to supply is the *founding* core group, and only until a group exists: founders cannot replicate the first entry without reaching each other, and cannot reach each other without an allowlist. So `[consensus] core` is read once, before `GroupFounded` applies, and never again. Unlike anything else that names a member it carries addresses, because the log that would otherwise supply them is exactly what those addresses are needed to fetch. No migration handling for the old key: nothing was ever released with it, so `deny_unknown_fields` refusing it is enough. |
| P1-17 | §9.1 | `distlib init-group` founds a group | **`distlib run --found-group` founds it instead** | Two constraints rule out a standalone command. redb takes an exclusive file lock, so a second process cannot open the node's database while it runs — `init-group` would have to be run with the node stopped. And the founder must stay up afterwards to replicate what it wrote, which a command that founds and exits cannot do. As a flag on `run` both fall away: the founder starts, founds, and serves. The other founders just `run`. |
| P1-18 | §9.1 | `distlib members` lists the group | **Superseded by P1-27.** For one release `members` needed the node stopped, because a running node holds the database exclusively. | The lock was real; treating it as the end of the story was not. Kept as a row rather than deleted, because the reasoning is what motivated bringing §7.1's API forward. |
| P1-19 | §9.1 | — | **`distlib whoami` added** | Founding needs every founder's id and address before any group exists to ask, so the exchange happens out of band — and nothing printed them in a form that could be pasted. `whoami` creates the identity if there is none and prints the `[consensus] core` line, rendered by the same code that writes the starter config so the two cannot drift. It prints no address when `bind_addr_v4` has no fixed port: an OS-chosen port is gone after the next restart, and a line the founder pastes has to keep working. It never prints the secret key. |
| P1-20 | §4.2, §4.3, §4.4 | membership events are committed by the core group; the only stated rule is that admission is invitation-only | **Per-event proposer rules, enforced in `MembershipState::apply`**: a `PledgeChanged` may only be proposed by the member whose pledge it is, and a `CoreGroupChanged` only by a current core member. `MemberAdded` and `MemberExpelled` stay open to any member, as §4.3 and §4.4 say. | Being a member was the only check, which left two holes. A pledge is a promise about the proposer's *own* storage and §5.5 makes custodian assignment depend on it, so anyone rewriting anyone else's could move everybody's data. The core group is the set of Raft voters, so a non-voter rewriting it could remove every voter but themselves. The rules live in `apply` because that is the one place every node runs identically — a check at the API or in a protocol handler would be a second opinion only that node holds, and two nodes disagreeing about whether an entry applied is a split membership. |
| P1-21 | §4.2 | — | **Every proposal carries the membership it was made against**: `MembershipState` tracks `changed_at`, the log index of the last entry that changed it; the signing envelope carries the value its proposer saw, and `apply` refuses a mismatch. | Raft's guarantee stops at the voters. §4.2's non-core followers *pull* the log, so a follower is eventually consistent and can act on a group that has moved on. This makes that self-announcing: a stale node finds out when it tries to propose, rather than silently proposing against a membership that no longer exists. The log's own index rather than a counter of our own, so there is one monotonic number and an error can name an index that `members` also reports. The last *membership change* rather than the log head, because Raft commits a blank entry at every leader election and comparing heads would invalidate every proposal in flight during one. |
| P1-22 | §4.2, §4.5 | one dedicated ALPN for the Raft RPC; `distlib/memberlog/0` for log distribution to followers | **`distlib/raft/0` carries openraft's RPCs and nothing else, and is refused to any peer that is not a current voter. Proposals moved to `distlib/memberlog/0`**, which every member may speak. | The two have different audiences, and one ALPN serving both meant a node that accepted proposals from any member was also serving `Vote` and `AppendEntries` to them — so any member in the allowlist could disrupt a term. Splitting them makes the audience the protocol boundary. A node whose voter set is empty accepts from the members it was *configured to found with*, and only until the log says a group exists. "Voter set empty" alone was the first answer and was wrong: a node that is never initialised has an empty voter set for its whole life, so it would have served consensus to every member in its allowlist forever — enough for one member to send it a self-signed `GroupFounded` and make it evict the real group. The gate is re-checked per RPC rather than once per connection, because the voter set moves under a long-lived connection and §4.4 makes the same argument for the allowlist: refusing the *next* connection is not enough while the current one is open. |
| P1-23 | §4.5 | "Core group membership changes use openraft's joint-consensus mechanism, recorded as `CoreGroupChanged`" | **Deferred: `CoreGroupChanged` changes the projection only.** No `raft.change_membership` call exists, so committing one does not promote or demote a Raft voter. | The event, its authorisation (P1-20) and its projection are all in place; what is missing is the consensus half. Worth stating rather than leaving implied, because P1-20's rationale — "a non-voter rewriting it could remove every voter but themselves" — reads as though the committed core set governs Raft, and today it does not: `RaftProtocol` asks openraft's own membership, which after `initialize` never changes. Joint consensus has failure modes of its own and deserves its own change rather than being tacked onto an authorisation fix. Until then a group's voters are its founders. |
| P1-24 | §9, §7.1 | `distlib-api` (axum: JSON-RPC + SSE + embedded UI) is phase 3 | **The JSON-RPC half arrives in phase 1b**, with the `group.*` and `node.status` methods from §7.1. SSE, `library.*` and the embedded UI stay in phase 3. | Phase 1 needs a way to commit a `MemberAdded` from outside a test: the running node holds the redb lock and the Raft, so no second process can reach them, and "stop the node to add a member" is not a group anyone can run. The alternative was a throwaway control channel — a Unix socket was considered and rejected — which would have been a second thing to discard the moment §7.1 arrived. Method names are §7.1's verbatim, so phase 3 extends this rather than renaming it. |
| P1-25 | §7.1 | "localhost by default" | **A bearer token always; loopback by default.** The token lives in `<data-dir>/api.token` at mode 0600, held in `secrecy::SecretString` and never logged. `[api] enabled` switches the listener off, and `[api] bind_addr` may be set to anything — binding off loopback warns rather than refuses. | Whoever can call this makes the node propose as itself. Narrower than holding the node's key — nothing proposed escapes the group's rules, and every proposal is signed and attributed — but not nothing. Loopback alone would trust every process on the machine, so the token is unconditional. The reverse does not hold: a node on a server or in a container has to be reachable from elsewhere, so loopback is the default and not a constraint. **There is no TLS**, so a remote listener wants a reverse proxy until there is; TLS and whatever authentication belongs beside it come with phase 3's UI. Also: a second node on one host needs its own `[api] bind_addr`, exactly as it needs its own `[net] bind_addr_v4`. |
| P1-26 | §3 | — | **hyper, not reqwest, wherever this API is called** | reqwest's `rustls` feature pulls `aws-lc-rs` — a C toolchain — to satisfy a TLS stack that a loopback HTTP call never uses, and feature unification with iroh's copy makes the no-provider build panic on the first request. hyper and hyper-util are already in the tree via axum, so this costs zero new packages. The caller lives in the tests until the CLI needs one: a client in the crate with no consumer is public surface committed to before anything asks for it. |
| P1-27 | §9.1, §7.1 | `group.members` / `group.propose_add` / `group.propose_expel` / `group.pledge_set` are API methods | **`distlib admit`, `distlib expel` and `distlib pledge` on the command line**, and `members` and `status` ask the running node before reading its database. | A group that could be founded and never changed was not usable by hand. Verbs rather than `member add` and `member expel`, so there is no `member`/`members` pair a tired typist can confuse. Asking twice is what retires P1-18: only one of the two sources can answer at a time — the node holds the database exclusively while it runs — so trying the API first and the files second works whether or not it is up, and `status` gains the Raft state and current leader, which only a running node knows. |
| P1-28 | §4.2, §4.5 | "peers fetch missing suffix from any core node over a dedicated ALPN" | **`distlib/memberlog/0` gains `From { cursor }`**, answering with the events in `(cursor, up_to]` *and their log indices*, where `up_to` is the serving node's **applied** index rather than the last event's. Every answer also carries the current core group **with addresses**, and the leader if known. | Three things the sketch leaves open. Indices travel because `MembershipState::apply` needs one — the freshness check (P1-21) is against a log index, so a follower folding without them could not compute it. `up_to` is the applied index because most entries carry no membership event: a cursor taken from the last event would re-request the gap between them forever, and only entries the state machine has applied may be served at all, since anything beyond is committed but not yet part of anybody's membership. Addresses travel because §4.5 says `CoreGroupChanged` tells followers where to fetch from, and our variant carries member ids and none — so a follower could learn *who* to ask and still have no way to reach them. |
| P1-29 | §4.5 | "Snapshots: trivial (state = the full membership table; it's small)" | **A follower whose cursor predates the purge watermark is told `TooFarBehind`, not sent a snapshot.** | Serving the state itself is a second transfer path, a second thing for a follower to verify, and a second way for the two to disagree — for a case that needs 5,000 membership events first, since that is openraft's snapshot threshold and nothing purges before one. Told plainly rather than left to fail obscurely, so when a group does get there the error names the problem. |

### Still to come in Phase 1

- The Raft network layer and the wiring that makes the allowlist come from the log — PRs 4 and 5.
  The Phase 0 carry-forwards below stay open until then.
- ~~openraft's `testing::Suite` has not run yet.~~ **Discharged.** It runs in the state-machine
  PR, covering the log store retroactively: 34 conformance cases against both stores sharing one
  database, as a real node runs them.

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
