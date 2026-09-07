# Phase 2 — Catalogue & library basics

Sequencing plan for §9's Phase 2. Written before any code, and revised in place if a PR proves part
of it wrong — an entry that turned out to be mistaken is more useful corrected than deleted.

---

## Context

Phase 1 is complete and merged: identity, transport, the membership log, and a test suite split
into a fast lane and a slow one. The group can be founded, members admitted and expelled, and every
node derives what it will talk to from the committed log. There is no catalogue, no content
transfer and no UI.

Phase 2 is §9's "Catalogue & library basics": items and their metadata converge across the group
via CRDT sync, every node answers queries from a local SQLite + tantivy projection, files move as
content-addressed blobs, and all of it is reachable from the CLI. §9's acceptance criterion is the
target: *node A adds 3 ebooks; node B (fresh join) syncs the catalogue, searches by author,
downloads a file, and after restart still serves it.*

**This document is sequencing** — what gets built, in what order, in which PR, and what each PR has
to demonstrate. Design deviations from [`distlib-plan.md`](../distlib-plan.md) still go into
[`plan-deltas.md`](../plan-deltas.md), in the PR that causes them. There must not be two sources of
truth for a deviation.

---

## Ground truth established before planning

Verified against the vendored crate sources in `~/.cargo/registry`, not against docs.rs prose.

1. **The compatible set is real.** `iroh-docs 0.101.0` requires `iroh 1.0`, `iroh-blobs 0.103`,
   `iroh-gossip 0.101.0`, `irpc 0.17` — exactly the set already pinned in the root manifest, and
   `iroh-gossip 0.101.0` is already a production dependency of `distlib-consensus` building against
   `iroh 1.0.3`. No version conflict, no second endpoint stack.

2. **`iroh-blobs 0.103` carries a first-party quality warning.** Its README line 3, verbatim: *"this
   version of iroh-blobs is not yet considered production quality. For now, if you need production
   quality, use iroh-blobs 0.35"*. We cannot take that advice: 0.35 predates iroh 1.0, and
   `iroh-docs 0.101` hard-requires blobs 0.103. **Adopting either means adopting the caveat.** This
   is the one finding worth a decision rather than a shrug — see [Risk](#risk-stated-once) below.

3. **`iroh-docs` is alive.** No deprecation anywhere in the crate; 0.101.0 was released to track
   iroh 1.0; the repo is active. §5.1's "wrap, don't expose" plan stands.

4. **Defaults pull a second redb major.** `iroh-docs` default features include `redb-v2-migration`,
   which adds `redb 3.1` alongside `redb 4`. That migration path exists only to read stores written
   by iroh-docs 0.94–0.98, which we have never written. Turning it off is safe and *loud*: opening
   such a store without the feature returns an explicit error naming the feature. `rpc` is also off
   — it gates only `Store::connect`/`listen` (cross-process transport we do not use) and drags in
   an unused TLS stack.

   ```toml
   iroh-docs  = { version = "=0.101.0", default-features = false, features = ["fs-store", "metrics"] }
   iroh-blobs = { version = "=0.103.0", default-features = false, features = ["fs"] }
   ```

   With that, one redb 4 in the graph, matching our `=4.2.0`. **Watch `constant_time_eq = "<0.4.3"`**
   in iroh-blobs — an upper-bound-only requirement in a workspace that `=`-pins everything; it can
   move under `cargo update`. Note it in the manifest comment.

5. **`iroh-docs`' `Author` wraps an `iroh::SecretKey`, and `AuthorId` is its `PublicKey`** — with
   `impl From<SecretKey> for Author`. So the node's own key can be its docs author, making
   `AuthorId == MemberId` by construction. This is load-bearing for §5.3 and settled in D2 below.

6. **`iroh::protocol::RouterBuilder::accept` takes `impl Into<Box<dyn DynProtocolHandler>>`**, so
   handlers can be boxed and handed to whoever builds the router. This is what makes the ownership
   refactor (2a-1) cheap.

---

## The structural decision: who owns the process's iroh runtime

Today `MembershipNode::start` ([node.rs:238](../../crates/distlib-consensus/src/node.rs)) builds the
`Router` and spawns the `Gossip` instance, because P1-11 established that `distlib-net` cannot serve
`distlib/raft/0`. That was right for one subsystem. It stops being right now:

- `iroh-docs` **requires** a `Gossip` handed to it at construction, and requires all three of
  docs/blobs/gossip accepted on the **same** router. There cannot be two `Gossip` instances.
- Phases 3–5 add more handlers still.

So something above consensus must own endpoint, gossip, blobs store, docs and router. It cannot be
a crate that consensus's own tests depend on, or the dependency graph cycles.

**Decision: `crates/distlib` gains a `[lib]` target exposing a `Runtime`.** The binary crate already
does this assembly by hand in `commands.rs::run`; making it a library target costs one manifest
section, changes no dependency direction, and lets phase-2 integration tests spawn several full
nodes in-process without a new crate. `main.rs` becomes a thin caller.

`Runtime` owns the shutdown order, and **keeps today's**: abort the membership node's tasks → stop
Raft (or the follow loop) → docs → blobs store → gossip → router → endpoint. Router last, as
[node.rs:712-735](../../crates/distlib-consensus/src/node.rs) has it now. This matters because
2a-1's acceptance is "no behaviour change", and `commands.rs`'s ordered shutdown and the
expelled-node exit path (P1-40) both depend on the current order. If a later PR wants to shut the
router first — a defensible choice, since it stops new connections arriving mid-teardown — that is
its own change with its own justification, not a side effect of moving ownership.

`MembershipNode` loses its `router` field and gains:

```rust
pub fn protocols(&self) -> Vec<(Vec<u8>, Box<dyn DynProtocolHandler>)>
```

and takes a `Gossip` rather than spawning one. **Add a test that `protocols()`' ALPNs equal
`alpns(is_core)`** — the drift P1-11 warns about becomes impossible to introduce silently.

Layering for the new code (matches §8; no new crate beyond the two §8 already names):

| Crate | Gains |
|---|---|
| `distlib-core` | catalogue record types, `Namespace` kind, catalogue key encode/decode |
| `distlib-net` | `blobs.rs`: open the `FsStore`, the `BlobsProtocol` handler, fetch-by-hash-from-peers |
| `distlib-sync` *(new)* | the only crate that touches `iroh-docs`: namespaces, typed read/write, projection stream |
| `distlib-store` *(new)* | SQLite read model + tantivy index + the projection task |
| `distlib` | `Runtime`, the `library.*` CLI verbs |

Record types live in `distlib-core` so `distlib-store` never depends on `distlib-sync`; sync maps
records to docs keys, store projects them into SQLite.

**Membership enforcement is free and must stay that way.** `AllowlistHooks` sits on the *endpoint*
(C1 consequence 1, [hooks.rs](../../crates/distlib-net/src/hooks.rs)). The docs and blobs handlers
inherit it. **Do not add a per-handler allowlist check.**

**`AddressBook` must be widened in 2a-2, not later.**
[addresses.rs](../../crates/distlib-net/src/addresses.rs) is the existing answer to "a third-party
protocol dials a bare id" (P1-39), and it currently holds **core nodes only** — the only addresses
anything records. Both new crates have gossip's problem:

- `iroh-blobs`' downloader takes `EndpointId` with **no** address.
- `iroh-docs`' `start_sync` takes `Vec<EndpointAddr>`, so bootstrap is fine — but it then joins a
  gossip swarm where neighbours arrive as bare ids, and its *internal* downloader (the one that
  fetches entry values) is the blobs downloader above.

So under `relay_mode = "disabled"` the first thing that breaks is a **follower-to-follower catalogue
sync in 2a-2**, not a media download in 2b. That is P1-39 again, in a phase where no test would
notice — it was found by hand last time. **2a-2 widens the book to learn every member's address**,
with the mutation check that pins it. Verify the failure exists before fixing it: two followers,
relays disabled, no addresses beyond the core group.

---

## Design decisions taken up front

**D1 — Namespace distribution: through the membership log, not the ticket.**
§5.1 says namespace secrets are "distributed via the join flow". Today's ticket (P1-36) carries
group id + core addresses + relay config and is prefixed as a versioned format. It does not need to
change: a joiner fetches the membership log over `distlib/memberlog/0` *before* it can sync
anything, so the log is already a channel that reaches exactly the members and nobody else.

Add one event, `NamespaceCreated { kind: Namespace, secret: NamespaceSecret }`, proposable only by
a core member (the P1-20 pattern), applied into `MembershipState`. Founding commits a
`NamespaceCreated { Catalogue }` immediately after `GroupFounded`. Phase 4 reuses it verbatim for
`community`; §6.2's `works` likewise. This is a delta against §5.1 and gets an entry.

Consistent with §5.1's own position: the namespace secret is **not** the security boundary — the
allowlist is. It is a bearer token of convenience that only members can read.

**D2 — The docs author is the node's own key, so `AuthorId == MemberId`.**
`Author` wraps an `iroh::SecretKey` and `AuthorId` is its public key, so passing the node's secret
key gives an author id whose bytes *are* the member id. This makes §5.3's per-author keys
(`rating/{item_id}/{member_id}`) automatic rather than a mapping to maintain, and makes "who wrote
this entry" answerable without storing it.

The cost, stated rather than buried: the same ed25519 key then signs iroh's TLS handshakes and docs
entries. The messages are domain-separated by protocol and neither is an oracle for the other, so
this is acceptable — but it is a real cross-protocol reuse and belongs in the delta entry, with the
alternative (a derived author key, which loses the identity and gains nothing we need) named.

Do **not** use `DefaultAuthorStorage::Persistent` — it writes a `default-author` file that can
disagree with the node key and hard-errors on mismatch. The author is derived from the node key
every start; there is nothing to persist and nothing to fall out of step.

**D3 — No second clock. `created` / `last_modified` / `modified_by` / `added_by` are dropped as
catalogue keys.**
§5.2 lists all four as stored keys. Every one is a self-reported value sitting next to iroh-docs'
*own* entry timestamp and author — the exact mistake P1-3 rejected for `MemberRecord`. Instead the
projection derives them from entry metadata: `last_modified` = max `entry.timestamp()` over the
item's keys, `modified_by` = the author of that entry, and `created` / `added_by` = the timestamp
and author of `item/{id}/type` specifically — written once at creation, with no reason ever to be
rewritten. Not "the earliest entry": timestamps are writer clocks and LWW can retire the entry that
was written first, so "earliest surviving" and "first written" are not the same thing.

This leaves one clock, and it is the same clock docs already uses to resolve last-writer-wins — so
what the UI shows and what the merge did can never disagree. Delta against §5.2.

**D4 — `item_id` is §5.2 as amended by P0-7, and `ItemId::from_content_hashes` already implements
it.** [`id.rs`](../../crates/distlib-core/src/id.rs) has it, tested, with zero production callers.
Phase 2 is its first caller. Anyone reading §5.2 alone will compute a different fingerprint (the
per-element length prefix is gone) — the phase doc says so once, here.

**D5 — Media blobs are referenced, never docs values.** Docs entry values stay under ~1 KB (§5.1),
so `item/{id}/file/{blob_hash}` holds a small JSON description and the media blob is fetched
separately by the download flow. This keeps docs' automatic value-download from ever pulling a
40-chapter audiobook onto a node that only wanted the catalogue.

---

## Sub-phases

Ten PRs. Each ends compiling, tested, and — from 2a-4 on — demo-able by hand. One PR at a time,
review before the next.

### 2.0 — Groundwork (1 PR)

Closes the papercut from the by-hand run: `distlib run` silently minted an identity in an empty data
directory and never said which config it had loaded, which cost an evening on node b.

- `run` logs the resolved config file path at startup (and says so when there is none).
- `run` refuses a data directory with no identity, naming `init` / `join` / `whoami` in the error.
  Identity creation stays in the commands whose job it is.
- Fix the timing numbers that disagree across `README.md`, `.cargo/config.toml` and `ci.yml`.

**Acceptance:** [`manual-check.md`](../manual-check.md)'s node-b step cannot go wrong the way it did.

### 2.1 — The core group can move (3 PRs) — closes P1-23

> **Revised while building it.** The plan had 2.1-1 wire `change_membership` and 2.1-2 add the
> address case. Two facts moved the boundary. First, enacting a change means handing openraft a
> node map, and the log had nowhere to put one — so the address had to come *first*, not second,
> or the intermediate state would add voters with no address, which under `relay_mode =
> "disabled"` is a voter nothing can reach. Second, openraft requires a node in a proposed config
> to already be a learner, and a promoted node is still `Role::Follower` at runtime: it advertises
> no `distlib/raft/0` and its router has no `RaftProtocol`, so enacting a promotion creates a voter
> that counts toward quorum and can never answer. P1-30 justified "promotion needs a restart" with
> "it costs nothing today because that event does not move Raft's voters anyway" — that
> justification expires here, so promotion becomes its own PR rather than a footnote in this one.

Not catalogue work, but it is the carried-forward item with a live failure mode: a core node that
changes IP or port in a `relay_mode = "disabled"` group is out of its own group permanently, with
refounding the only way back.

- **2.1-1** *(done, delta P2-2)* — **the log becomes the source of truth for core addressing.** `GroupFounded` and
  `CoreGroupChanged` carry an address per core node; `MembershipState` holds the core group as a
  map; `core_addresses()` reads that projection rather than openraft's `StoredMembership`, which
  also fixes a follower answering "no core nodes" about the group it follows. No
  `change_membership` yet. Delta P2-2.

- **2.1-2** *(done, delta P2-3)* — wire `CoreGroupChanged` to `raft.change_membership`, for
  **address changes and removals only**. The event already commits, is authorised (P1-20) and projects; what is missing
  is the consensus half.

  A promotion is refused at apply time until 2.1-3 lands, so the projection and Raft can never
  disagree about who votes: an addition simply never commits.

  Enactment is a reconciliation loop on the leader — diff Raft's voters and nodes against the
  projection, emit `SetNodes` for moved addresses and `RemoveVoters` (`retain: false`, since P1-22
  refuses `distlib/raft/0` to non-voters) for departures. It must wake on **both** the membership
  watch and `Raft::metrics()`: a node that becomes leader *after* the event applied never sees the
  membership watch fire again, and without that the "retry on the next opportunity, including
  after a restart" recovery path does not exist. Being a diff rather than a command is what makes
  it restart-safe with nothing extra persisted — and it also fixes a latent bug, since
  `MemberExpelled` already drops a voter from the projection while leaving it a Raft voter for
  ever.

  **On `SetNodes`:** openraft's own docs say "do not use unless you know what you are doing",
  because updating an address can point at a different node and elect two leaders. That hazard
  does not exist here, and the reason belongs in the delta: `RaftClient::endpoint_addr` builds
  `EndpointAddr::new(member.endpoint_id())`, so the socket is a hint the QUIC handshake overrides
  — dialling a wrong address reaches nobody rather than the wrong somebody. That is exactly the
  "ensure connection to the correct node is the `RaftNetwork`'s responsibility" clause in the same
  openraft doc, discharged structurally.

- **2.1-3** — **promotion**, which needs a node to be able to start voting without a restart:
  advertising `distlib/raft/0`, gaining a `RaftProtocol`, and openraft's learner-before-voter
  requirement. Closes the half of P1-30 that 2.1-2 leaves open.

  **The claim that must hold, and the one with real openraft risk:** a `CoreGroupChanged` that
  commits but whose `change_membership` then fails must not leave the projection and Raft's voter
  set disagreeing — that split is precisely what P1-23 says reads as true today and is not.
  **Raft wins**, because it is the only one of the two that decides who can commit anything: the
  projection is a view, and a node whose `change_membership` failed must retry it from the applied
  log on the next opportunity (including after a restart) rather than reporting success. State the
  recovery path in the PR: what a node does when it restarts with a committed `CoreGroupChanged`
  that Raft never enacted.

  Failure modes to test: a change proposed during an election, a change that would remove the
  proposer, a node restarting mid-change.

  The CLI and API surface — `distlib core set <member> [--addr ...] [--relay ...]`, `distlib core
  remove <member>`, `group.propose_core` — lands with whichever of 2.1-2/2.1-3 makes it usable.
  `AddressBook` and the memberlog's core-group answer follow automatically, since both already
  read the projection.

**Acceptance:** a three-node group; restart one core node on a different port with
`relay_mode = "disabled"`; submit its new address; the group converges and the moved node is
dialable again — plus a paragraph in `manual-check.md`. **Still owed by 2.1-3**: 2.1-2 pins the
mechanism (openraft's own membership takes the new address, and drops a departed voter) but not
the end-to-end restart, which needs the CLI verb to submit one by hand.

### 2a — The catalogue converges (4 PRs)

**Note the blobs store arrives here, not in 2b.** `iroh-docs` cannot be constructed without an
`iroh_blobs::api::Store` — docs entry *values* live in it. So 2a wires the store as docs' backing;
2b is where media files and transfer arrive.

- **2a-1 — Ownership refactor. No behaviour change.**
  `crates/distlib` gains `[lib]` and `Runtime`; `Gossip` is spawned by `Runtime` and passed to
  `MembershipNode::start`; `MembershipNode` loses its `router` and gains `protocols()`; the
  consensus test harness (`tests/common/mod.rs`) builds the router from `protocols()` in the one
  place it already builds a node. Add the ALPN-agreement test.
  **Acceptance:** the entire existing suite passes unchanged, including the by-hand runbook.

- **2a-2 — `distlib-sync`.** iroh-docs + blobs store wired into `Runtime`; `NamespaceCreated` (D1);
  the catalogue namespace opened from the log; `start_sync` against the members it should sync with;
  catalogue record types and key encode/decode in `distlib-core` (§5.2 field-level keys, per-file
  keys under `file/{blob_hash}`); a typed write path and a projection event stream over `LiveEvent`.

  Data dir gains `docs/` and `blobs/` — via new `DataDir` accessors, as
  [paths.rs](../../crates/distlib-core/src/paths.rs) instructs, and **fix the existing
  contradiction** while there: `raft.redb` sits at the root with no accessor while the doc promises
  `raft/`.

  The `NamespaceSecret` now lives in `raft.redb`. Give that file the `0600` treatment the node key
  and API token already get ([private_file.rs](../../crates/distlib-core/src/private_file.rs)), or
  confirm in the PR that it already has it. CLAUDE.md's rule about secrets is standing and this is
  the first secret the log has ever carried.

  **Acceptance:** two in-process nodes; A writes an item; B sees it. Then the same between two
  *followers* with relays disabled, which is the address-book case above. A member expelled
  mid-sync stops receiving entries — the allowlist hooks doing their job on a handler we did not
  author, which is the claim worth pinning.

  **Note for the reviewer of 2b-1:** this acceptance already moves blobs, because docs entry values
  *are* blobs. What it exercises is iroh-docs' internal downloader, not our code.

- **2a-3 — `distlib-store`.** SQLite schema (`items`, `item_files`, `members`) + the projection task
  consuming 2a-2's stream + tantivy index + `admin.reindex`. Projection must be **idempotent**
  (replay N times = replay once) — proptest, fast lane, per §10. Cold start = full docs replay.
  **Acceptance:** kill and restart a node; its SQLite state matches a node that never restarted.

- **2a-4 — Query surface.** `library.search` and `library.item` as JSON-RPC methods (§7.1 names
  verbatim), `distlib search` and `distlib item` on the CLI, reading SQLite/tantivy only.
  **Acceptance:** by hand — two nodes, add metadata on one, search by author on the other.

### 2b — Content moves (3 PRs)

- **2b-1 — Media blobs.** `distlib-net::blobs`: the `BlobsProtocol` on the router, and fetch by hash
  from a set of member providers — **our** path, `Store::downloader` driven by a provider list we
  choose. Blobs already moved in 2a-2, but that was iroh-docs fetching its own entry values through
  machinery we do not call. Do not let this acceptance pass on that path: the test must add a blob
  that is *not* a docs entry value and fetch it through `distlib-net::blobs`.
  **Acceptance:** a media blob added on A is fetched by B with `relay_mode = "disabled"` and no
  address supplied at the call site.

- **2b-2 — `library.add`.** Hash the file set → blobs → `ItemId::from_content_hashes` over the
  `role: content` files only (D4) → catalogue entry. Exact-dup guard: compute the fingerprint before
  creating, and on a hit report the existing item and offer to contribute missing files rather than
  creating a second one (§6.1). `distlib add`.
  **Acceptance:** adding the identical file set twice on two nodes converges on one item, with no
  coordination.

- **2b-3 — `library.download` + the phase acceptance.** Provider selection from the availability we
  have (members holding the blob; §5.6's gossip index is phase 4, so v1 asks the members it knows),
  fetch, verify (inherent to BLAKE3 transfer), export, register as a holder. `distlib download`.
  **Acceptance — §9's criterion, end to end, as a slow-lane test beside
  [`founding.rs`](../../crates/distlib/tests/founding.rs) because "after restart still serves it"
  needs real processes:** node A adds 3 ebooks; node B joins fresh, syncs the catalogue, searches by
  author, downloads a file, restarts, and still serves it.

---

## Carried forward from Phase 1 — take or defer, explicitly

| Item | Phase 2 |
|---|---|
| **P1-23** — `CoreGroupChanged` moves nobody; no address can change | **Taken**, sub-phase 2.1 |
| **P1-29** — a follower too far behind cannot recover (`TooFarBehind`) | **Deferred.** The fix is serving the membership state itself, i.e. snapshot transfer, and it needs 5,000 membership events to reach. It belongs with whatever else needs snapshot transfer, not bolted onto the entry path. Re-state it in Phase 2's carry-forward. |
| **P1-35** — gossip announces *and* a 30 s timer polls | **Deferred.** Phase 2 adds a second gossip consumer (docs), which is new evidence about what the load actually looks like. Revisit once there is a catalogue generating traffic, rather than tuning it blind now. |
| **P0-6** — Windows and macOS unverified | **Deferred, and getting worse.** Phase 2 adds three new on-disk stores. The CI matrix is a one-element list precisely so widening it is a one-line change; it should be widened before anything user-facing claims cross-platform support. Not a Phase 2 blocker, but say so in the carry-forward rather than letting it go quiet for a third phase. |

---

## Testing and the lanes

The split holds: `slow-tests` on by default, `cargo test-fast` during development, `cargo test-all`
before a task is done.

**Fast lane** (must stay ~5 s): catalogue key encode/decode round-trips, `ItemId` fingerprinting
(already there), SQLite projection idempotency (proptest), tantivy query construction, the
`protocols()`/`alpns()` agreement test, `NamespaceCreated` authorisation rules.

**Slow lane**: everything with more than one node, every blob transfer, the 2.1 address-change test,
and the 2b-3 acceptance test.

Two things to watch, both found in the ground-truth pass:

- **`FsStore` spawns its own multi-threaded tokio runtime** rather than using the ambient one. An
  in-process cluster of five nodes is therefore five extra runtimes. Use `MemStore` in tests that do
  not need persistence, and keep `.config/nextest.toml`'s `test-threads = 4` honest by measuring
  after 2a-2 rather than assuming.
- **Two error idioms at the boundary**: iroh-docs returns `anyhow::Result`, iroh-blobs 0.103 returns
  `n0-error` types. Both get wrapped in `thiserror` enums at the crate edge (`SyncError`,
  `StoreError`), per CLAUDE.md — no `anyhow` in library code, and `.unwrap()` nowhere.

---

## Risk, stated once

`iroh-blobs 0.103` tells us it is not production quality and points at 0.35, which is unreachable
from iroh 1.0. There is no version of this phase that avoids it, short of abandoning iroh-docs and
hand-rolling range-based set reconciliation — which §5.1 rejected for good reason ("weeks of subtle
work"), and which would still leave us needing a blob store.

So: take it, pin it exactly, and keep the seam. §5.1's "wrap, don't expose" rule is what makes this
survivable — `distlib-sync` is the only crate that touches iroh-docs, `distlib-net::blobs` the only
one that touches iroh-blobs, and the rest of the codebase sees typed records and hashes. If either
crate has to be replaced, one layer is replaced. **This rule is the mitigation, so it is not a
stylistic preference and PRs should be reviewed against it.**

Record it as a Phase 2 delta in `plan-deltas.md` in the PR that adds the dependency, so the decision
is dated and attributable rather than inferred later from a version number.

---

## Verification

Per PR, before review:

- `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` — clean.
- `cargo test-all` — green. `cargo test-fast` still ~5 s; if a PR moves it, say so in the PR body.
- Any test asserting a new mechanism gets a **mutation check**: delete the line the test is about,
  confirm the test fails, restore. The flaky-test work is the precedent for why — a test that passes
  with its subject deleted is testing a second mechanism, not the first.

At the end of the phase:

- §9's Phase 2 acceptance as an automated slow-lane test (2b-3).
- The same criteria added to [`manual-check.md`](../manual-check.md) as a by-hand §10, driven from
  the CLI in the same shape as the existing §9 runbook — because the by-hand run is what found
  P1-39, P1-40 and P1-41, none of which any test noticed.
