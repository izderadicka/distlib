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

Thirteen PRs. Each ends compiling, tested, and — from 2a-4 on — demo-able by hand. One PR at a
time, review before the next.

### 2.0 — Groundwork (1 PR)

Closes the papercut from the by-hand run: `distlib run` silently minted an identity in an empty data
directory and never said which config it had loaded, which cost an evening on node b.

- `run` logs the resolved config file path at startup (and says so when there is none).
- `run` refuses a data directory with no identity, naming `init` / `join` / `whoami` in the error.
  Identity creation stays in the commands whose job it is.
- Fix the timing numbers that disagree across `README.md`, `.cargo/config.toml` and `ci.yml`.

**Acceptance:** [`manual-check.md`](../manual-check.md)'s node-b step cannot go wrong the way it did.

### 2.1 — The core group can move (2 PRs, done)

> **Revised while building it.** The plan had 2.1-1 wire `change_membership` and 2.1-2 add the
> address case. Two facts moved the boundary. First, enacting a change means handing openraft a
> node map, and the log had nowhere to put one — so the address had to come *first*, not second,
> or the intermediate state would add voters with no address, which under `relay_mode =
> "disabled"` is a voter nothing can reach. Second, openraft requires a node in a proposed config
> to already be a learner, and a promoted node is still `Role::Follower` at runtime: it advertises
> no `distlib/raft/0` and its router has no `RaftProtocol`, so enacting a promotion creates a voter
> that counts toward quorum and can never answer. P1-30 justified "promotion needs a restart" with
> "it costs nothing today because that event does not move Raft's voters anyway" — that
> justification expires here, so promotion becomes its own PR — now [2.3](#23--promotion-1-pr),
> after the approval rules in 2.2, because promotion is itself a change to who votes and should be
> governed by the same rule from the moment it becomes possible rather than being retrofitted.

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

  A promotion is refused at apply time until 2.3 lands, so the projection and Raft can never
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

**Acceptance:** 2.1-2 pins the mechanism — openraft's own membership takes a new address, and
drops a departed voter — with both halves mutation-checked. The end-to-end version (restart a core
node on a different port under `relay_mode = "disabled"`, submit its new address by hand, watch the
group converge) needs a CLI verb to submit one, and lands with 2.3.

### 2.2 — Approval before a membership change takes effect (2 PRs)

Found while reviewing 2.1-2, and worth doing next because 2.1-2 is what gave it teeth.

**Today any member can expel anyone, including every core member, on a single signed proposal.**
`MembershipState::authorise` constrains exactly two events — `PledgeChanged` to the member's own
pledge, and `CoreGroupChanged` to a core proposer. `MemberExpelled` is open to every member against
every member. §4.4 step 2 says otherwise: *"requires acknowledgment by a configurable quorum of core
nodes (default: majority) before commit"*. That was never implemented, and P1-20 records
`MemberExpelled` as "open to any member, **as §4.3 and §4.4 say**" — true of §4.4's step 1, which
says any member may *submit*, and silent about step 2, which says a core quorum *commits*. So it is
an undocumented deviation rather than a decision.

Two things make it urgent rather than tidy-up:

1. **2.1-2 gave it consequences.** Expelling a core member now genuinely removes them from
   openraft's voter set, so a sequence of single proposals shrinks the group's ability to commit
   anything.
2. **The empty-core guard is bypassable, and the result is a group that can never change again.**
   `CoreGroupChanged` refuses to empty the core group and has a test saying so; `MemberExpelled`
   walks straight past it — `self.core.remove(member)` with no floor. Verified by probe rather than
   by reading: expelling the last voter returns `Ok(())` and leaves zero voters.

   2.1-2's reconciliation loop does *not* make this worse — it treats an empty core group as "no
   instruction" rather than as "remove every voter", and there is a unit test saying so, because
   openraft refuses an empty membership and a group with no voters could never commit the event
   that would restore them. What actually happens is quieter and worse: Raft keeps the voters it
   had, those voters are no longer members, so the allowlist refuses their connections, replication
   stops, and nothing can ever be committed again — including the re-admission that would fix it.
   `CoreGroupChanged` cannot restore the core group either, since it requires a core proposer and
   there are none. **The hole is in the expulsion rule, and 2.2-1 is where a floor goes.**

Note where this sits relative to §2's threat model, which assumes members do not attack the
protocol: the case this really guards is not an attacker but a **foot-gun**. A mistyped id in
`distlib expel` removes a voter today, and nothing asks.

#### The rule

> **Changing who votes takes a majority of voters. Everything else takes one.**

| | approvals |
|---|---|
| add a member | 1 |
| expel a follower | 1 |
| change a core node's address | 1 |
| **expel a core member** | **majority of the current core** |
| **demote a core member** | **majority of the current core** |
| **promote a follower** (2.3) | **majority of the current core** |
| change your own pledge | none — it is the member's own (P1-20) |

Expelling a core member and demoting one are the same decision reached by two routes, so they must
cost the same or the ceremony is bypassable in two cheap steps. Once a majority has demoted somebody
the person left is a follower, and expelling them for one approval is right: the decision that
mattered already happened.

```rust
fn approvals_needed(&self, event: &MembershipEvent) -> usize {
    if self.changes_the_voters(event) {
        self.core.len() / 2 + 1
    } else {
        1
    }
}

fn changes_the_voters(&self, event: &MembershipEvent) -> bool {
    match event {
        MemberExpelled { member, .. } => self.is_core(member),
        // Any difference either way. Additions cannot commit until 2.3, but the
        // rule that governs them should not arrive with them.
        CoreGroupChanged { core } => !names_exactly(core, self.core.keys()),
        _ => false,
    }
}
```

**The path to the core group is follower → core member, and that is structural rather than a
convention.** `MemberAdded` cannot make a voter — it does not touch `core` — which is precisely why
admission stays at one approval: it can never be a governance decision. Promotion is always a
separate `CoreGroupChanged`. Founders are the exception, and they are a different event.

#### Design, with the calls made

- **`MemberAdded` and `MemberExpelled` become proposals.** Applying one records it as pending rather
  than changing the membership. The change happens in the fold when the last approval applies — no
  node "notices" a threshold and proposes a follow-up, because two nodes would both do it and the
  log is the only thing that should decide.
- **`Approved { proposal: u64 }` names the log index of the proposal**, not its subject. Two pending
  proposals about the same person stay unambiguous, and the log index is already the currency here
  (`changed_at`). The CLI reads `12  expel  core-1  (1 of 2)` and then `distlib approve 12`.
- **A core proposer's own proposal counts as their approval.** This is what keeps today's behaviour:
  `distlib admit` and `distlib expel` run against a core node still take effect in one step, so the
  existing acceptance test and the by-hand runbook are unchanged. What changes is a *follower*
  proposing, and a core member being expelled.
- **The target's own approval is refused.** Nobody votes on their own expulsion.
- **An approver must be core at the moment the approval applies**, and the threshold is evaluated
  then, against the core as it stands then — not as it stood when the proposal was made. Promoting a
  fourth voter into a group of three needs 2 approvals, not 3. Joint consensus makes it easy to
  argue either way, so it is written down.
- **Every rule is re-checked when the last approval lands**, not only when the proposal was made.
  The group can move in between, and that gap is where this kind of thing goes wrong.
- **~~An expulsion that would empty the core is refused at apply time.~~ Not built, because the
  thresholds subsume it** — the deviation 2.2-1 records. Expelling the last voter needs a majority
  of a core of one; the only core member is the one being expelled; and the target cannot approve
  their own expulsion. So it can be proposed and never decided. Verified by mutation rather than
  asserted: deleting any one of those three rules re-opens the hole and
  `the_last_voter_cannot_be_expelled` fails, whereas a separate floor would have been a line no test
  could reach. The hole the sub-phase was partly written to close is closed by a rule that has a
  reason, not by a special case.
- **Only approvals from current voters count**, on the same principle that measures the threshold
  against the current core. An approval from somebody since demoted or expelled is not a voter's
  agreement, and counting it would let a shrinking core carry decisions on the word of people who
  have left it.
- **A repeat approval is idempotent, not refused.** A proposal can become sufficient without anybody
  approving it — a shrinking core lowers the threshold under one already sitting there — and nothing
  re-examines a pending proposal on its own, since deciding several at once on one membership change
  would be a surprising cascade. An existing approver saying so again is what makes it reachable.
- **A duplicate proposal is accepted, not refused — until 2.2-3.** Two members expelling the same
  peer split their approvals and neither reaches its threshold. This was taken as a self-healing
  wart, since one more approval on either decides it, and refusing would have left a stuck slot
  nothing could clear. That is right as far as it goes and **understates the case for the same
  subject being proposed repeatedly**: a member who keeps asking manufactures a new pending entry
  each time, spreading approvals thinner the more they ask. 2.2-3 makes it one per subject.
- **Majority is computed, not configured.** §4.4 says "configurable", and it cannot be a config file
  key: the fold must reach the same verdict on every node, so a per-node setting could split the
  membership. Configurable means *in the log*, which needs a policy event we do not have. §5.5 needs
  exactly the same machinery for its weight cap, so it gets built once, there. Deviation recorded.
- **~~No expiry.~~ Wrong, and 2.2-3 is the correction.** The reasoning was that timestamps are not
  authoritative (P1-3), so any expiry would have to be counted in log entries, which is arbitrary —
  and that what remained was "bounded by how many people are genuinely under discussion". **The
  first half stands; the second is false.** `subject()` is `None` for `CoreGroupChanged`, so the
  prune that clears a proposal when its member is expelled or re-admitted never touches one: a
  stalled demotion — and, once 2.3 lands, a stalled *promotion* — sits in the map for the life of
  the group with nothing able to remove it. "Arbitrary constant" was also the wrong objection.
  Determinism is the property that matters, because every node must fold to the same map, and a
  count of log entries has it while a clock does not.
- **A withdrawal event, in 2.2-2 with its surface.** The deliberate way out, as against 2.2-3's
  automatic one.
- **A two-voter core group can no longer remove one of its voters.** A majority is two and the one
  being removed does not get a say. This is a real behaviour change, not a pre-existing condition:
  two healthy voters commit normally and simply cannot shrink. It is the price of "one member cannot
  seize a group", and the way out is to grow to three, or for the departing voter to agree to a
  demotion — which they may, since standing down is resigning rather than being removed. For the
  same reason a core member cannot propose their own expulsion, so a graceful exit is
  demote-then-expel.

#### The three PRs

- **2.2-1 — the state machine only (done).** `Approved`, the pending state in `MembershipState`,
  the thresholds, the approval rules, and `changes_the_voters` written symmetrically so 2.3 inherits
  it. Fast-lane tests throughout, each rule mutation-checked. Nothing user-visible changes for a
  core operator, which is what keeps it reviewable. Two deviations from the above, both recorded in
  P2-4: the empty-core floor is **not** built, because the thresholds already refuse what it would
  have caught; and duplicate proposals are accepted rather than refused, which is what defers the
  withdrawal event to 2.2-2 rather than needing it here.
- **2.2-2 — the surface.** `distlib pending` and `distlib approve <index>`; `group.propose_expel`
  keeps its §7.1 name and gains `group.approve`; pending proposals appear in `node.status`; the
  withdrawal event lands with `distlib withdraw`. **The gap it closes:** after 2.2-1 a follower's
  `distlib admit` prints `admitted` and the admission then waits for a core member with nothing
  saying so, and the joiner holding the ticket cannot connect until somebody approves it. The
  acceptance test and `manual-check.md` grow a genuine two-operator core expulsion.

- **2.2-3 — bounding the pending set.** Three findings from reviewing 2.2-1, with one combined
  answer. **They are one change because each one alone is worse than all three together**: expiry
  without one-per-subject still lets duplicates pile up inside the window; one-per-subject without
  expiry re-creates the stuck slot that made duplicates acceptable in the first place; and
  withdrawal alone (2.2-2) does nothing about a proposer who has gone away.

  1. **A pending proposal can stay forever.** Nothing removes one that never reaches its threshold
     unless the member it is about is expelled or re-admitted — and for `CoreGroupChanged` not even
     then, because `subject()` is `None`. Two costs, and the second is the serious one. It is
     unbounded state that `MembershipState` re-encodes into redb on *every* apply, so it is write
     amplification on the hot path rather than only memory. And **a stale proposal stays live**: one
     made against a five-voter core group can be approved into effect a year later by people who
     never saw the discussion. The rules are re-checked, so it is not unsound — but "somebody once
     proposed this" is not the same as "the group is deciding this now".
  2. **The same action proposed repeatedly makes a new pending entry each time**, splitting the
     approvals it needs. The worst case is exactly the one that is never pruned: a member who keeps
     asking to join the core group.
  3. **Compaction.** Verified rather than assumed: the map holds the *event*, not a reference into
     the log, and `MembershipState` is what gets snapshotted, so a pending proposal survives
     compaction correctly and an index is never reused. What compaction costs is the **audit
     lookup** — after the log is trimmed there is no way back to the original signed envelope for
     entry 47, only to the projection's copy of it. Consequences: `distlib pending` must show
     everything a decision needs (proposer, event, approvals, index) so nobody has to go to the log
     for it, and this is the argument for expiry counted in **log-index distance** rather than
     anything else, since `changed_at` advances monotonically and compaction neither renumbers nor
     reuses.

  So: **at most one pending proposal per subject** — widened so the core group is a subject of its
  own — with a second refused naming the existing one rather than superseding it, because
  superseding would let anyone reset accumulated approvals by re-proposing. Plus **expiry after a
  fixed number of committed entries**, evaluated in the fold, which bounds the map, unsticks a slot
  whose proposer has vanished, and stops a decision being carried by a proposal nobody remembers.
  The constant lives in code, not config, for the same reason the majority does — and joins §5.5's
  policy event when that lands.

  **Test gap to close while here:** the restart path carries pending proposals
  (`a_proposal_still_waiting_for_approvals_survives_a_restart`), but the **snapshot-install** path is
  different code and nothing asserts a node that installs a snapshot inherits pending proposals it
  can still approve.

**Ordering.** After 2.2-2, because expiry that silently drops a proposal is only tolerable once
`distlib pending` exists to show what is waiting and why. Before 2.3, because promotion is the case
that is both unprunable and duplicable, so the bound should exist before promotion can pend at all.

**Acceptance:** a three-node group; a follower proposes expelling a core member; one approval is not
enough; a second core approval applies it, and 2.1-2's reconciliation loop drops the voter from
openraft. Plus the same by hand in `manual-check.md`, which is the first time the runbook has needed
two operators to agree on anything.

### 2.3 — Promotion (1 PR)

Closes P1-23 and the half of P1-30 that 2.1-2 leaves open. **After 2.2** — all three of it — so a
promotion is governed by the majority rule from the moment it becomes possible, and so a promotion
that stalls is bounded rather than permanent. A pending `CoreGroupChanged` is the one kind nothing
prunes, which makes 2.2-3 a prerequisite rather than a preference.

- **Promotion** needs a node to be able to start voting without a restart: advertising
  `distlib/raft/0`, gaining a `RaftProtocol`, and satisfying openraft's learner-before-voter
  requirement. `ConsensusError::PromotionUnsupported` goes away in the same change.

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

- **The CLI and API surface for the core group** — `distlib core set <member> [--addr ...]
  [--relay ...]`, `distlib core remove <member>`, `group.propose_core` — lands here, because this
  is the first sub-phase where a core-group change is something an operator can usefully submit by
  hand. Each of them is a proposal that 2.2's rules then govern. `AddressBook` and the memberlog's
  core-group answer need no work: both already read the projection.

**Acceptance:** the end-to-end version 2.1 could not do. A three-node group; restart one core node
on a different port under `relay_mode = "disabled"`; submit its new address by hand; the group
converges and the moved node is dialable again. Then promote a follower and watch it start voting
without a restart. Plus a paragraph in `manual-check.md` for both.

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
| **P1-23** — `CoreGroupChanged` moves nobody; no address can change | **Taken**, sub-phases 2.1 (addressing, enactment) and 2.3 (promotion) |
| **P1-29** — a follower too far behind cannot recover (`TooFarBehind`) | **Deferred.** The fix is serving the membership state itself, i.e. snapshot transfer, and it needs 5,000 membership events to reach. It belongs with whatever else needs snapshot transfer, not bolted onto the entry path. Re-state it in Phase 2's carry-forward. |
| **P1-35** — gossip announces *and* a 30 s timer polls | **Deferred.** Phase 2 adds a second gossip consumer (docs), which is new evidence about what the load actually looks like. Revisit once there is a catalogue generating traffic, rather than tuning it blind now. |
| **§4.4's core-quorum policy** — never implemented; any member can expel anyone on one proposal | **Taken**, sub-phase 2.2. Not a Phase 1 carry-forward but a Phase 1 omission, found while reviewing 2.1-2: P1-20 recorded `MemberExpelled` as open to any member "as §4.3 and §4.4 say", which is true of §4.4's step 1 and silent about step 2. |
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
