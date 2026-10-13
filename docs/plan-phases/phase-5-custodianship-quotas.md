# Phase 5 — Custodianship & quotas

Sequencing plan for §9's Phase 5. Written before any code, and revised in place if a PR proves part
of it wrong — an entry that turned out to be mistaken is more useful corrected than deleted.

---

## Context

**Status: planned, not started.**

Phase 4 is complete and merged, and so is the refactor series that followed it
([`plan-refactor-complexity.md`](../plan-refactor-complexity.md)). Every member now says what it
holds in a heartbeat, the library shows who is online with an item, and ratings, reviews, bookmarks
and wishes work end to end.

Phase 5 is §9's "Custodianship & quotas", the part of §5.5 that makes distlib a library rather than
a swarm:

- an assignment of custodians to every item, computed from the membership log alone;
- a pin manager that fetches what this node is custodian of and releases what it no longer is,
  within its pledge;
- the 2× rule: a member's pledge covers twice what it has added;
- storage challenges, their results gossiped, and the evidence shown in the members list and
  attachable to an expulsion proposal.

§9's acceptance is the target: *5 nodes, item with `replicas=3`; kill a custodian → next-ranked node
auto-fetches within one cycle; a node that deletes its pinned data fails challenges from 2 peers and
shows up red in the members view.*

**Decisions taken with Ivan when this plan was started** (2026-10-13), each with the facts it was
taken on:

- **Assignment is a weighted consistent-hash ring, not §5.5's rendezvous hashing** — measured in
  ground truth 9. A delta, P5-1, in the PR that builds it.
- **Custody comes from the log; a grace top-up covers a custodian that stays away.** §9's "kill a
  custodian" and §5.5's "going offline changes nobody's custodian set" disagree, and both are kept:
  custodians are what the log says, and an item whose live copies stay below `replicas` for longer
  than a grace period is fetched as an *extra* copy by the next members on the ring, and released
  when the custodians are back (D7).
- **Everything §5.5 describes is in scope**, not only §9's checklist: the weight cap with group
  settings in the log, prioritized throttled recovery, pledge-capped overflow, and the
  `MemberLeaving` decommission.
- **A challenge is iroh-blobs' own verified chunk probe** (ground truth 8), not a new protocol.

**The scale target is phase 4's, still Ivan's:** about 100k items must be easy, 10M must work
without trouble, and some nodes will hold a large share of the library. Every cost below is checked
against it.

The phase also takes, explicitly:

- **C9** — `PENDING_EXPIRY` becomes a group setting (D2).
- **C26** — superseded heartbeat base lists are collected once GC runs (5a-1).
- **C8, in part** — "who added this" exists for quotas (D9). `created` and `modified_by` stay open.

**This document is sequencing** — what gets built, in what order, in which PR, and what each PR has
to demonstrate. Design deviations from [`distlib-plan.md`](../distlib-plan.md) still go into
[`plan-deltas.md`](../plan-deltas.md), in the PR that causes them.

---

## Ground truth established before planning

Verified against the tree and the vendored crate sources in `~/.cargo/registry` — iroh-blobs
0.103.1, iroh-docs 0.101.0, openraft 0.9.25 — and, for the costs, measured.

1. **Pledges exist, and are the member's own.** `MemberRecord::pledge_bytes`
   ([`event.rs:71`](../../crates/distlib-consensus/src/event.rs)), set by `PledgeChanged`
   (`event.rs:108`), which only the member itself may propose (`state.rs:279-322`) and which needs
   no approvals. Nothing reads a pledge yet.

2. **Nothing group-wide is configurable in the log.** The events are `GroupFounded`, `MemberAdded`,
   `MemberExpelled`, `PledgeChanged`, `CoreGroupChanged`, `Approved`, `Withdrawn`
   ([`event.rs:92-143`](../../crates/distlib-consensus/src/event.rs)). The one group-wide number
   there is, `PENDING_EXPIRY = 128` (`state.rs:62`), is a constant — C9. §5.5's `cap` "stored in the
   Raft log as an absolute byte value" therefore needs a new event, and a change to the snapshot
   format, which is free before a release (C23).

3. **`replicas` is a catalogue field nobody writes.** `Item::replicas: Option<u32>`
   ([`catalogue.rs:291`](../../crates/distlib-core/src/catalogue.rs)); `library.edit_metadata`
   refuses it as "not metadata" ([`methods.rs:532`](../../crates/distlib-api/src/methods.rs)). Any
   member could write one, so a per-item value would let a member ask the whole group to store its
   items ten times.

4. **An item's size is what its author says.** `FileRecord::size`
   ([`catalogue.rs:76`](../../crates/distlib-core/src/catalogue.rs)) is written by the adder and
   checked by nobody; a fetch learns the real size, since BLAKE3 verification covers it.

5. **Who wrote a catalogue entry is known.** The catalogue's iroh-docs author is the node's own key,
   so `AuthorId == MemberId` by construction
   ([`catalogue.rs:186-193`](../../crates/distlib-sync/src/catalogue.rs)). Reads use
   `single_latest_per_key` (`:369`, `:492`), but iroh-docs keeps **one entry per key and author**, so
   every member who wrote a file key is still there to be counted. A second `library.add` of the same
   files writes only the keys that are missing (`add_missing_files`), so a file key usually has one
   author — two only when two members added the same files at the same moment.

6. **What a node holds is already tracked, and so is who else holds what.** `Holdings` is the set of
   items whose every content file is complete here
   ([`availability.rs:36-66`](../../crates/distlib-sync/src/availability.rs)); the availability index
   maps each item to the members that announced it (phase 4's D3, C28). The live copy count of an
   item is therefore a lookup.

7. **Nothing is ever deleted today, and turning GC on unprotected would delete downloads.**
   - The blob store is opened with defaults (`FsStore::load`,
     [`runtime.rs:89`](../../crates/distlib/src/runtime.rs)), and the default is `gc: None`
     (`iroh-blobs store/fs/options.rs:124`). Deletion is `pub(crate)` (`api/blobs.rs:159-171`); the
     public way to delete is to untag a blob and let GC collect it.
   - GC is `GcConfig { interval, add_protected }` (`store/gc.rs:138-153`), where `add_protected` is a
     callback run before each pass that adds hashes to keep.
   - **What is tagged today** (phase 4's ground truth 7, re-checked): media added by `library.add` has
     an automatic tag (`add_path`, [`catalogue.rs:437-445`](../../crates/distlib-sync/src/catalogue.rs));
     the heartbeat base list has a named tag (`statement.rs:102`); **downloads have none**, and the
     catalogue's own entry values only a temporary one.
   - iroh-docs can protect what its documents reference — `Builder::protect_handler`
     (`iroh-docs protocol.rs:95`) — and **distlib does not call it**.

   So a GC pass today would collect every download and the content of every catalogue entry. 5a-1
   exists to make that impossible before anything is untagged on purpose.

8. **A challenge needs no protocol of ours.** `iroh_blobs::get::request::get_chunk_probe(connection,
   hash, chunk)` (`get/request.rs:287-319`) requests one 1 KiB chunk over the blobs ALPN every node
   already serves, and the transfer verifies it against the blob's root hash. A challenger needs only
   the hash, which the catalogue gives everyone — not a copy of the data.

9. **Assignment costs, measured** on this machine (12 threads), with throwaway code in the
   scratchpad:

   | | Per unit | 100k items × 100 members | 10M × 100 | 10M × 1,000 |
   |---|---|---|---|---|
   | §5.5's rendezvous hashing, one thread | 58 ns per (item, member) | 0.6 s | ~58 s | ~10 min |
   | the same, 12 threads | 6.8 ns | 0.07 s | ~7 s | ~70 s |
   | weighted ring, one thread | 100–160 ns per item | 0.01 s | ~1 s | ~1 s |

   Rendezvous hashing ranks every member for every item, so its cost grows with both; the ring's
   grows with items only. **Proportionality**, the ring's weakness: with 1,024 points for a member at
   the cap, load per unit of weight stayed within 0.83–1.14 of the mean across 1,000 members of
   weights from 10 % to 100 % of the cap; with 100 points, 0.49–1.46. Hence D1's 1,024.

10. **Fetching already prefers whoever is known to hold the data.** `Blobs::fetch` takes an ordered
    provider list and tries it in order
    ([`blobs.rs:122-160`](../../crates/distlib-net/src/blobs.rs)); the pin manager hands it the
    item's online holders first, as `library.download` does.

---

## The structural decision: custody is computed, never replicated

Who is custodian of what is a pure function of two inputs every node already has: the membership
log (members, pledges, group settings) and the catalogue (items and their sizes). **Nothing about
custody is written anywhere** — no assignment table in the log, no pin list in the catalogue. Every
node computes the same answer from the same inputs, and computes it again when an input changes.

Two consequences, both accepted:

- **The catalogue converges, so assignments do too.** A node that has not yet received an item
  computes no custodians for it; one that has received an item whose files are still arriving
  assigns it once its sizes are known. Assignments agree once the catalogue does.
- **Availability decides only order and extras**, never custody: the live copy count orders
  recovery (D8) and triggers the grace top-up (D7). A custodian going offline changes no
  assignment — §5.5's principle, kept.

---

## Design decisions taken up front

**D1 — A weighted consistent-hash ring (P5-1).**

```text
points(m)    = max(1, round(1024 × w_eff(m) / cap))
point(m, i)  = first 8 bytes of BLAKE3("distlib.ring.v1" || member_id || i),   i < points(m)
position(it) = first 8 bytes of BLAKE3("distlib.ring.v1" || item_id)
custodians(it) = walking clockwise from position(it), the first `replicas` distinct members
                 that still have room (D5)
```

- **Deterministic**: integers and hashes only, no floating point in anything that decides placement.
- **Minimal reshuffle**: a member's weight changing moves only that member's points; an item moves
  only if a point it would have reached appeared or vanished. Joining moves about `replicas / N` of
  items, each by one custodian.
- **Proportional within about ±15 %** at 1,024 points at the cap (ground truth 9). `cap` is absolute
  (D2), so a join changes nobody else's point count — the reason §5.5 insists on it, and it holds for
  the ring exactly as for rendezvous hashing.
- **Pure, in `distlib-core`**, property-tested in the fast lane for §10's three properties.

**D2 — Group settings in the log: one event, approved like a change of voters.**

```text
GroupSettingsChanged {
  replicas            u8     default 3
  cap_bytes           u64    default 2 TB
  min_pledge_bytes    u64    default 100 GB   (§5.5)
  pending_expiry      u64    default 128      (C9)
  grace_secs          u32    default 3600     (D7)
  leaving_secs        u32    default 7 days   (D6)
  challenge_secs      u32    default 7 days   (D10)
}
```

Whole, not per field: one event, one approval, one place the defaults live. A group that never sends
one runs on the defaults. It needs a majority of the core group, like a change of voters
(`changes_the_voters`, `state.rs:378`), because every member's storage depends on it. The projection,
`node.status` and a read-only settings section on the Node page show it; `group.settings_set`
proposes it.

**`replicas` is the group's, not the item's** (ground truth 3). `Item::replicas` stays unwritten;
honouring it needs a rule about who may set it — C34.

**D3 — Effective weight.**

```text
w_eff(m) = min(pledge(m), cap) × leaving_factor(m)
```

`leaving_factor` is 1, or falls in whole-hour steps to 0 over `leaving_secs` after a `MemberLeaving`
(D6). A member with no pledge has no points and holds nothing for anybody — which the 2× rule (D9)
stops being a free ride.

**D4 — An item's size is the sum of its content files, as the catalogue says.** Covers and other
roles do not count, as they do not count towards holding it (ground truth 6). A size is the author's
claim (ground truth 4); a member that lies about it skews its own quota and its custodians' loads,
and the custodian that fetches the file finds out. Logged, not acted on — C35.

**D5 — Pledge-capped overflow, in ring order.** Items are placed in order of `position(item)`. Each
takes the first `replicas` distinct members clockwise **whose assigned bytes plus this item's size
fit their pledge**; a full member is skipped and the walk goes on. Everyone computes the same loads
from the same inputs, so everyone skips the same members.

- **An item that finds no room** after a full turn of the ring gets fewer custodians and is reported
  under-replicated, not forced onto someone.
- **The cost of being deterministic**: inserting one item early in ring order can push a later item
  off a full member, and that one the next. The cascade runs only through members that are full,
  and is measured in 5b-1's property test; if it churns, D5 falls back to "a full member stops at
  its pledge" and the open question 4 of §11 is answered the other way.

**D6 — `MemberLeaving`, proposed by the member itself.** No approvals, like a pledge. Its weight steps
down hourly to zero over `leaving_secs`, from the event's own timestamp, so custodian tasks migrate
its items while it still serves them. Hourly steps keep nodes whose clocks differ by minutes in
agreement for all but those minutes. Leaving changes only weight: the member stays a member until it
is expelled or, later, leaves by an event of its own. **Expulsion stays abrupt**, as §5.5 has it.

**D7 — The grace top-up.** When an item has had fewer than `replicas` live copies (ground truth 6)
for longer than `grace_secs`, each node checks whether it is among the first *missing* members on
the ring after the item's custodians, skipping members that are offline or full. If it is, it fetches
the item as an **extra** copy, from its spare pledge — the bytes its custodial assignments leave
free. It releases the extra once the item has had `replicas` live copies for `grace_secs` again.

- **Per observer, like availability itself.** Two nodes may disagree about who is online and both
  fetch, or neither; the next check settles it. Extras are opportunistic, never counted as custody.
- **The grace is what keeps restarts free.** A custodian that restarts is back long before the
  grace runs out; only one that stays away costs anybody a fetch.
- This is how §9's "kill a custodian → next-ranked node auto-fetches" holds without breaking §5.5.

**D8 — The pin manager: tags, a diff, and a prioritized, throttled queue.**

- **A tag per reason**: `pin/<item>` for custody, `extra/<item>` for D7. Releasing deletes the tag;
  GC collects the blob once nothing else keeps it (5a-1).
- **The diff** runs on every membership change, catalogue item change, pledge or settings change,
  and availability change relevant to D7: wanted pins against held tags.
- **Recovery is prioritized**: fetches are ordered by live copies ascending, so an item down to one
  copy goes before one at two (§5.5's mitigation 4). At most two fetches per providing peer, and four
  in all; the queue is reordered, not restarted, when counts change.
- **Releasing is never urgent.** A release waits until the item has `replicas` live copies without
  this node, so a reshuffle never drops an item below its target on the way to a new custodian.

**D9 — The 2× rule, counted from authorship.**

```text
added_bytes(m)    = Σ size of each content file whose file key has an entry by m (ground truth 5)
required_pledge(m) = max(min_pledge_bytes, 2 × added_bytes(m))
```

A file key written by two members counts against both. That is rare (ground truth 5) and errs
towards more storage, not less. The read model gains a table of `(member, blob, size)` from every
author's entry, not only the latest. Enforcement is **soft and local**, as §5.5 says:
`library.add` refuses an add that would leave the pledge below the requirement, with the pledge it
would need, and `group.pledge_set` refuses to go below it. Another node does not police it; the
members list shows each member's pledge beside its requirement.

**D10 — Challenges: a verified chunk probe, signed and gossiped.**

- **Who**: each member challenges the custodians of the items it added (D9's authorship), §5.5's
  rule. Each pair (item, custodian) about once per `challenge_secs`, at a random moment, and only
  once the custodian has had `grace_secs` to fetch it. An offline custodian is skipped, not failed —
  that is availability's business.
- **How**: `get_chunk_probe` on a random chunk of a random content file (ground truth 8), within
  30 s.
- **Results** are `SignedChallengeResult { challenger, custodian, item, blob, chunk, passed, at }`
  on a topic of their own beside the heartbeat's, so the beat's format and size budget stay as phase
  4 left them. Each node keeps what it hears in a local table — evidence has to survive a restart to
  be attached to anything.
- **Red** in the Node page's members list: failures from at least two distinct challengers within
  `challenge_secs`, and no pass since. The evidence — who, what, when — is shown on the member.
- **Attached to an expulsion through the CLI and the API**, because the UI's admin is read-only
  (C15): `distlib expel <member> --with-evidence`, and `group.propose_expel`'s `with_evidence`,
  write a summary of it into the reason — counts, challengers and the latest failure, within
  `MAX_REASON`'s 512 bytes ([`event.rs:157`](../../crates/distlib-consensus/src/event.rs)). The
  decision stays human and core-group (§5.5).
- **What it cannot catch**, accepted in §5.5: a custodian that fetches the chunk from another holder
  on demand passes. The 30 s bound is the only limit.

---

## Sub-phases

Fourteen PRs. Each ends compiling, tested, and demonstrable by hand with the CLI or a page. One PR at
a time, review before the next.

### 5a — foundations (3 PRs)

- **5a-1 — GC on, and nothing lost to it.** Every download gets a tag; the catalogue registers
  iroh-docs' protect handler; GC runs on an interval (an hour), with `add_protected` keeping
  whatever the documents reference. Closes C26: superseded base lists are collected.
  **Acceptance:** a test runs a real GC pass on a node that has added a file, downloaded one, and
  synced a catalogue — all three survive, readable, and a deliberately untagged blob is gone.
  Mutation-checked: without the download tag the download is collected; without the protect handler
  the catalogue's content is.
  **Watch for:** this is the phase's risk (below). It ships before anything untags on purpose.

- **5a-2 — the ring (D1, P5-1).** `distlib-core::custody`: points, positions, `custodians(item)`,
  pure and without overflow yet.
  **Acceptance, as property tests:** the same inputs give the same custodians in any input order;
  adding a member moves only items that reach one of its points, each by one custodian; with 1,024
  points at the cap, load per unit of weight stays within ±20 % on random weights; `replicas`
  distinct members whenever there are that many with points.

- **5a-3 — group settings (D2, C9).** `GroupSettingsChanged`, its approval rule, the snapshot format,
  `group.settings_set`, and the settings in `node.status`, the CLI and a read-only Node page section.
  `PENDING_EXPIRY` reads the setting.
  **Acceptance:** a three-founder group changes `replicas` with two approvals; one approval is not
  enough; a follower sees the new value; a group that never sets any reports the defaults.

### 5b — custody (6 PRs)

- **5b-1 — assignments, with overflow (D3–D5).** A task that computes every item's custodians from
  membership and the read model's item sizes, again on every input change; `library.item` and the
  item page show the custodians, this node's own share in `node.status`.
  **Acceptance:** five in-process nodes agree on every item's custodians; raising one member's pledge
  moves items only onto it; a full member is skipped identically everywhere; an item that fits
  nowhere is reported under-replicated. Property test: the D5 cascade on random insert orders, its
  size recorded.

- **5b-2 — the pin manager (D8).** Pins and releases by tag, prioritized and throttled.
  **Acceptance:** a node made custodian fetches the item without anybody asking for it; demoted from
  custody, it releases only after the item has its copies elsewhere, and the next GC pass frees the
  space; a queue of many items fetches the one-copy items first.

- **5b-3 — the grace top-up (D7).**
  **Acceptance:** §9's first half — five nodes, `replicas=3`; a custodian stopped; with `grace_secs`
  at 2 s in the test, the next member on the ring fetches within one cycle; the custodian back, the
  extra is released after the grace. A custodian restarted within the grace causes no fetch.

- **5b-4 — `MemberLeaving` (D6).** The event, the hourly ramp, `group.leave` and a Node page button.
  **Acceptance:** with `leaving_secs` short, a leaving member's items move to others step by step,
  each new custodian fetching while the leaver still serves; nothing is under-replicated on the way.

- **5b-5 — the 2× rule (D9).** The authorship table, `added_bytes` and `required_pledge`, the
  refusals in `library.add` and `group.pledge_set`, and the requirement in the members list.
  **Acceptance:** a member with a 100 GB pledge adding files totalling 60 GB is refused, told 120 GB;
  raising the pledge lets the add through; two members adding the same file at once both carry it.

- **5b-6 — custody in the UI.** The item page's custodians with their online state, the Node page's
  pledge, requirement, and the bytes this node holds as custodian, as extra, and by download.
  **Acceptance:** Vitest per view; one e2e path from adding an item to seeing its custodians.

### 5c — challenges (5 PRs)

- **5c-1 — the probe and its schedule (D10).** Challenging this node's own items' custodians at
  random moments, results in a local table.
  **Acceptance:** a custodian holding the item passes; one whose blob was deleted underneath it
  fails; an offline one is skipped, not failed; the schedule spreads pairs over `challenge_secs`.

- **5c-2 — signed results, gossiped.** `SignedChallengeResult`, its topic, verification, and every
  node's table filled from what it hears.
  **Acceptance:** three nodes see the same results; a forged or replayed result, or one by a
  non-member, is dropped.

- **5c-3 — evidence: red in the members list, attached to an expel proposal.**
  **Acceptance:** two challengers' failures turn a member red on every node; a pass clears it;
  `distlib expel --with-evidence` proposes with a reason that names the failures and fits
  `MAX_REASON`. Vitest per view.

- **5c-4 — §9's acceptance, as one test.** Five nodes, `replicas=3`: a custodian stopped, the next
  member fetches within one cycle; a node whose pinned blob is deleted fails challenges from two
  peers and is red everywhere.

- **5c-5 — by-hand check.** [`manual-check.md`](../manual-check.md) grows within its one story, and
  what it finds is fixed before the phase is called done.

---

## Carried forward — take or defer, explicitly

Every open C-number appears here once.

| # | Item | Phase 5 |
|---|---|---|
| **C1** | A full peer offer is O(N²) dials | **Unchanged.** |
| **C3** | The sweep reads the whole document every five seconds | **Deferred.** Still cost only. |
| **C6** | A field blinks out of the read model while its newest value is in flight | **Unchanged.** |
| **C7** | The read model is replayed in full at every start | **Deferred**, and relied on again: 5b-5's table fills from the replay. |
| **C8** | `added_by`, `created`, `modified_by` have nowhere to come from | **In part, by 5b-5**: authorship for quotas. `created` and `modified_by` stay open. |
| **C9** | `PENDING_EXPIRY` is one fixed count | **Closed by 5a-3.** |
| **C13** | Gossip does not change sides when a node is promoted | **Deferred.** |
| **C15**–**C19** | Read-only admin; Windows file privacy and service; clearing a field; downloads as in-memory tasks | **Unchanged.** |
| **C21** | Title sort | **Unchanged.** |
| **C22** | Consensus test peers cannot restart | **Closed by #132**: `Peer::restart`. |
| **C23**, **C24** | Release publishing; the expelled leader's lost answer | **Unchanged.** C23 is what lets 5a-3 change the snapshot format freely. |
| **C26** | Superseded base lists stay on the publisher's disk | **Closed by 5a-1.** |
| **C27** | Heartbeat traffic not measured above N = 50 | **No phase.** |
| **C28** | The `item → members` map lives in memory | **No phase.** D7 and D8 read it; they add nothing to it. |
| **C29**–**C33** | One wish comment per member; a failed gossip dial strands a peer; a write missing both paths; presence traffic above 1,000 members; iroh-docs' `AlreadySyncing` wedge | **Unchanged.** |

**New in phase 5:**

| # | Item | Where it goes |
|---|---|---|
| **C34** | **`Item::replicas` is not honoured** — one group-wide `replicas` (D2) | **No phase.** Needs a rule about who may ask the group to store an item more times. |
| **C35** | **A false `FileRecord::size` is logged, not acted on** (D4) | **No phase.** The fetching custodian knows the real size; correcting the catalogue's claim is a later decision. |

---

## Testing and the lanes

**Fast lane:** the ring's three properties and the overflow cascade, as property tests; the settings
event's approval rule; the leaving ramp's steps; `added_bytes` and `required_pledge`; the schedule's
spread; signing round trips and tampering of challenge results; the D7 decision on given counts.

**Slow lane:** GC keeping what it must (5a-1); five-node assignment agreement; pin and release;
the grace top-up; leaving; the challenge outcomes; §9's acceptance.

Multi-node tests run with the settings shortened through `GroupSettingsChanged` itself — a grace of
2 s, a challenge period of seconds — so what is tested is the setting, not a test-only path. They
**assert "within", never an exact sequence**, as phase 4's do.

Every mechanism is mutation-checked. UI: Vitest first, Playwright for the happy paths only.

---

## Risk, stated once

**The first time distlib deletes anything.** Until now no blob has ever been removed: GC is off, and
every download, catalogue value and base list stays for ever (ground truth 7). Phase 5 has to
release what a node is no longer custodian of, and the only public way to delete is GC. A GC pass
collects whatever is not tagged or protected, and today that includes every download and the
content of every catalogue entry.

The fences, each a rule PRs are reviewed against:

- **5a-1 ships alone, first**, and its test runs a real GC pass over every kind of blob distlib
  writes. A new kind of blob in a later PR extends that test.
- **Releases wait for copies elsewhere** (D8): nothing a node releases leaves an item below
  `replicas` live copies.
- **Only the pin manager deletes tags**, and only its own two prefixes. A download, an add and the
  documents are never untagged by anything in this phase.
- **`add_protected` errs towards keeping**: if the documents' hashes cannot be read, the pass is
  aborted (`ProtectOutcome::Abort`), not run without them.

---

## Verification

Per PR, before review:

- `cargo fmt --all` and `cargo clippy --workspace --all-targets --all-features -- -D warnings` —
  clean.
- `cargo test-all` — green.
- Quick hand-written mutations of the PR's key behaviours with `scripts/mutate.py`; results in the
  PR body.
- For UI PRs: `npm run check`, `npx vitest run` and `npm run e2e`.

At the end of the phase: the thorough mutation pass (`mutate.py --every-block` and `cargo mutants`
on the phase's diff), and [`manual-check.md`](../manual-check.md) grows within its one story:

- the group sets `replicas=2` and a short grace through the settings;
- dave's item page shows its two custodians; bob, one of them, fetched it without asking;
- bob stopped: after the grace, a third member fetches it; bob back, the extra goes;
- erin, under-pledged, is refused an add and told what pledge she needs;
- bob deletes a pinned file by hand: after challenges from two members he is red on the Node page,
  and `distlib expel bob --with-evidence` proposes with the evidence as its reason.
