# Phase 5 — Custodianship & quotas

Sequencing plan for §9's Phase 5. Written before any code, and revised in place if a PR proves part
of it wrong — an entry that turned out to be mistaken is more useful corrected than deleted.

---

## Context

**Status: a draft, under discussion.** Ivan's first review (#133) reshaped it: rendezvous hashing
stays, `replicas` belongs to the group, and a node holds an item for one of three reasons — it added
it, it is custodian, or it downloaded it — which the plan has to bind together. What is still open
is under [Open questions](#open-questions), each with a proposal.

Phase 4 is complete and merged, and so is the refactor series that followed it
([`plan-refactor-complexity.md`](../plan-refactor-complexity.md)). Every member says what it holds in
a heartbeat, the library shows who is online with an item, and ratings, reviews, bookmarks and
wishes work end to end.

Phase 5 is §9's "Custodianship & quotas", the part of §5.5 that makes distlib a library rather than
a swarm:

- custodians for every item, computed from the membership log alone;
- **one model of why a node keeps an item** — it added it, it is custodian, or it downloaded it —
  with a budget for each and a clear order of what goes first when space runs out (D3);
- a pin manager that fetches what this node is custodian of and keeps it while it is;
- the quota rule: a member's pledge grows with what it adds (D9);
- storage challenges, their results gossiped, and the evidence shown in the members list and
  attachable to an expulsion proposal.

§9's acceptance is the target: *5 nodes, item with `replicas=3`; kill a custodian → next-ranked node
auto-fetches within one cycle; a node that deletes its pinned data fails challenges from 2 peers and
shows up red in the members view.*

**Decided so far**, with the facts each was taken on:

- **Rendezvous hashing, as §5.5 has it** — Ivan, at the review. The goal for this version is groups
  of thousands of members. Finding one item's custodians is a pass over the members, and the
  expensive question — "what am I custodian of" — is asked rarely and in the background (D1, D5;
  ground truth 9). A weighted ring was considered and set aside for a later version.
- **`replicas` is a group property, set when the group is founded** — Ivan, at the review (D2).
  A per-item value would let any member ask the whole group to store its items a hundred times.
- **The creator keeps what it added; a download is a cache** — Ivan, at the review (D3). What was
  exported is the user's and outside distlib; the copy in the store may go when space is short.
- **Custody comes from the log; a grace top-up covers a custodian that stays away** (D7). §9's "kill a
  custodian" and §5.5's "going offline changes nobody's custodian set" are both kept.
- **Membership churn must not turn into storage churn** — Ivan, at the review (D5): custody is
  recomputed only once membership has settled, weights move in steps, and an item a node stops being
  custodian of becomes cache rather than being deleted.
- **Everything §5.5 describes is in scope**: the weight cap, prioritized throttled recovery,
  pledge-capped overflow and the `MemberLeaving` decommission.
- **A challenge is iroh-blobs' own verified chunk probe** (ground truth 8), not a new protocol.

**The scale target, revised at the review:** groups of **thousands of members**, with phase 4's
items target — about 100k easy, 10M working — and some nodes holding a large share of the library.

The phase also takes, explicitly:

- **C9** — `PENDING_EXPIRY` becomes a group setting (D2).
- **C26** — superseded heartbeat base lists are collected once GC runs (5a-1).
- **C8, in part** — "who added this" exists, for the creator's copy and for quotas (D3, D9).

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
   there is, `PENDING_EXPIRY = 128` (`state.rs:62`), is a constant — C9. Settings set at founding
   change `GroupFounded` and the snapshot format, which is free before a release (C23).

3. **`replicas` is also a catalogue field, which nobody writes.** `Item::replicas: Option<u32>`
   ([`catalogue.rs:291`](../../crates/distlib-core/src/catalogue.rs)); `library.edit_metadata`
   refuses it as "not metadata" ([`methods.rs:532`](../../crates/distlib-api/src/methods.rs)). D2
   removes it.

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

7. **Nothing is ever deleted today; turning GC on decides what can be.**
   - The blob store is opened with defaults (`FsStore::load`,
     [`runtime.rs:89`](../../crates/distlib/src/runtime.rs)), and the default is `gc: None`
     (`iroh-blobs store/fs/options.rs:124`). Deletion is `pub(crate)` (`api/blobs.rs:159-171`); the
     public way to delete is to untag a blob and let GC collect it — see
     [Tags](#tags-how-keeping-and-releasing-work).
   - GC is `GcConfig { interval, add_protected }` (`store/gc.rs:138-153`), where `add_protected` is a
     callback run before each pass that adds hashes to keep.
   - **What is tagged today** (phase 4's ground truth 7, re-checked): media added by `library.add` has
     an automatic, uniquely named tag that nothing ever deletes (`add_path`,
     [`catalogue.rs:437-445`](../../crates/distlib-sync/src/catalogue.rs)); the heartbeat base list
     has a named tag (`statement.rs:102`); **downloads have none**, and the catalogue's own entry
     values only a temporary one.
   - iroh-docs can protect what its documents reference — `Builder::protect_handler`
     (`iroh-docs protocol.rs:95`) — and **distlib does not call it**.

   So a GC pass today would collect every download — acceptable once downloads are a cache with a
   policy (D3), not before — and **the content of every catalogue entry, which is never acceptable**.

8. **A challenge needs no protocol of ours.** `iroh_blobs::get::request::get_chunk_probe(connection,
   hash, chunk)` (`get/request.rs:287-319`) requests one 1 KiB chunk over the blobs ALPN every node
   already serves, and the transfer verifies it against the blob's root hash. A challenger needs only
   the hash, which the catalogue gives everyone — not a copy of the data.

9. **Rendezvous hashing's cost, measured** on this machine (12 threads), with throwaway code in the
   scratchpad: **58 ns per (item, member) score on one thread, 6.8 ns on twelve.**

   | Question | Cost | 1,000 members | 5,000 members |
   |---|---|---|---|
   | Custodians of one known item | one score per member | ~58 µs | ~0.3 ms |
   | "What am I custodian of?", 100k items | every item × every member | ~6 s, ~0.7 s on 12 threads | ~29 s, ~3.4 s |
   | the same, 1M items | | ~58 s, ~7 s | ~5 min, ~34 s |
   | the same, 10M items | | ~10 min, ~70 s | ~48 min, ~6 min |

   The first is what the item page, the grace top-up and a challenger ask, and it is cheap at any
   size. The second is what the pin manager needs, and it is the one D5 keeps rare, debounced and in
   the background. An incremental version — keeping each item's top scores, so a join costs one
   score per item — brings it to O(items) per change; kept in reserve until a measurement asks for
   it (C36).

10. **Fetching already prefers whoever is known to hold the data.** `Blobs::fetch` takes an ordered
    provider list and tries it in order
    ([`blobs.rs:122-160`](../../crates/distlib-net/src/blobs.rs)); the pin manager hands it the
    item's online holders first, as `library.download` does.

---

## The structural decision: custody is computed, never replicated

Who is custodian of what is a pure function of two inputs every node already has: the membership
log (members, pledges, group settings) and the catalogue (items, their sizes and their creators).
**Nothing about custody is written anywhere** — no assignment table in the log, no pin list in the
catalogue. Every node computes the same answer from the same inputs.

Two consequences, both accepted:

- **The catalogue converges, so assignments do too.** A node that has not yet received an item
  computes no custodians for it. Assignments agree once the catalogue does.
- **Availability decides only order and extras**, never custody: the live copy count orders
  recovery (D8) and triggers the grace top-up (D7). A custodian going offline changes no
  assignment — §5.5's principle, kept.

---

## Why a node keeps an item

The heart of the phase, and the part Ivan asked to be bound together. A node keeps an item for one
of three reasons, and may have several at once:

| Reason | Who | Kept | Counts against |
|---|---|---|---|
| **Own** | the member that added it — its creator | always, while the member is a member | nothing: it is the member's contribution, and what the quota rule (D9) is measured on |
| **Custody** | the members rendezvous hashing assigns (D1) | while assigned | the pledge |
| **Cache** | anyone who downloaded it; a grace top-up's extra (D7); a former custodian | while there is room | the pledge, after custody |

- **A blob goes only when no reason is left.** A download of an item this node is also custodian of
  is kept as long as the custody is; losing custody of an item still in the cache leaves it cached.
- **The pledge is the space a member offers the group**: custody first, cache in whatever custody
  leaves. When custody needs room, the cache gives it up, least recently used first.
- **What the user exported is the user's.** `library.download` writes a file outside the store; the
  copy in the store is the cache, and freeing it never touches the export.

---

## Tags: how keeping and releasing work

iroh-blobs keeps a blob for as long as something says to:

- **A tag** is a persistent name pointing at a hash, stored with the blobs. A blob any tag points at
  is kept; deleting the tag is how anything is ever released.
- **A temporary tag** protects a blob for as long as the handle is held in memory — during an import
  or a download, before a real tag is set.
- **The protected set**: before each GC pass, `add_protected` is asked for hashes to keep without a
  tag. iroh-docs answers it with every hash its documents reference, once the protect handler is
  registered (ground truth 7).
- **A GC pass** deletes every blob that is neither tagged nor protected.

distlib gives each reason of the table above its own tag, one per content blob of the item:

```text
own/<item>/<blob>        set by library.add, kept while the member is a member
custody/<item>/<blob>    set and deleted by the pin manager
cache/<item>/<blob>      set by library.download and by a grace top-up; evicted under pressure
```

The tag *is* the reason: listing `custody/` is the pin manager's view of what it holds, and a blob
shared by two items, or held for two reasons, has two tags and stays until both are gone. Releasing
custody of an item deletes its `custody/` tags and sets `cache/` ones in the same step, so the item
falls into the cache rather than out of the store. The catalogue's own content needs no tag: the
protect handler keeps it. Cache eviction needs to know what was used least recently, so the cache's
last-used times live in a small local table beside the tags.

---

## Design decisions taken up front

**D1 — Rendezvous hashing, as §5.5 writes it.**

```text
score(item, m) = −w(m) / ln(h(item, m)),   h ∈ (0, 1) from BLAKE3("distlib.custody.v1" || item || m)
custodians(item) = the item's creator, then the members with the highest scores, up to `replicas`
                   distinct members in all, skipping members that are full (D6)
```

- **Deterministic**: the score is compared as a float, but every node computes the same float from
  the same bytes; ties, astronomically rare, break by member id.
- **Minimal reshuffle**: a member joining takes only the items where it outscores somebody, and one
  slot in each; leaving or a weight change moves only that member's items.
- **Proportional**: a member's expected share is its weight's share.
- **Pure, in `distlib-core`**, property-tested in the fast lane for §10's three properties.
- **The creator first** is open question Q1: it makes the creator's own copy count as one of the
  `replicas`, which is also what makes the quota rule's 2× come out (D9).

**D2 — Group settings, set when the group is founded.** `GroupFounded` gains them:

```text
replicas            u8     default 3
cap_bytes           u64    default 2 TB
weight_step_bytes   u64    default 10 GB    (D5)
min_pledge_bytes    u64    default 100 GB   (§5.5)
pending_expiry      u64    default 128      (C9)
settle_secs         u32    default 1 h      (D5)
grace_secs          u32    default 1 h      (D7)
leaving_secs        u32    default 7 days   (D6)
challenge_secs      u32    default 7 days   (D10)
```

A founder that sets none gets the defaults. `node.status`, the CLI and a read-only section of the
Node page show them. `Item::replicas` is removed from the catalogue schema (ground truth 3).
**Whether any of these may change after founding is open question Q4**; the proposal is no, not in
this phase.

**D3 — Effective weight.**

```text
w(m) = step × floor(min(pledge(m), cap) × leaving_factor(m) / step)
```

The cap is §5.5's mitigation 1: absolute, so a join changes nobody else's weight. The step is D5's:
small pledge edits move nothing. `leaving_factor` is 1, or falls hourly to 0 over `leaving_secs`
after a `MemberLeaving` (D6). A member with no pledge has no weight and is custodian of nothing.

**D4 — An item's size is the sum of its content files, as the catalogue says.** Covers and other
roles do not count, as they do not count towards holding it (ground truth 6). A size is the author's
claim (ground truth 4); a member that lies about it skews its own quota and its custodians' loads,
and the custodian that fetches the file finds out. Logged, not acted on — C35.

**D5 — Membership churn does not become storage churn.** Ivan's concern at the review: custody that
follows every change could get out of hand in a group whose membership moves often. Five fences:

1. **Custody waits for membership to settle.** The pin manager recomputes custody only once the
   membership has not changed for `settle_secs` — and at least once a day, so a group that never
   settles still moves. Ten changes in an afternoon are one reshuffle in the evening. Nodes agree
   on the result because they compute it from the same settled membership; they may start it a few
   seconds apart.
2. **Weights move in steps** (D3): a pledge edited from 500 GB to 505 GB moves nothing.
3. **Losing custody does not delete anything.** The item becomes cache (the tag table above), so a
   change that is undone costs nothing, and the new custodian can fetch it from the old one.
4. **Releases wait for copies elsewhere**: a node does not even demote an item to cache until it has
   `replicas` live copies without this node.
5. **Fetching is throttled and prioritized** (D8), so a big reshuffle is a slow background job, not a
   storm.

The recompute itself is the expensive question of ground truth 9: a few seconds at 1,000 members and
100k items, run in the background, once per settled change.

**D6 — Pledge-capped overflow, and `MemberLeaving`.**

- **Overflow**: when a member's custody would exceed its pledge, its lowest-scoring items go to the
  next-ranked member instead. Everyone computes the same loads from the same settled inputs, so
  everyone skips the same members. Items are placed in order of their id, so the result does not
  depend on the order items arrived; an item that fits nowhere gets fewer custodians and is reported
  under-replicated. **A cascade** — one item pushing the next off a full member — is possible, and
  5b-1 measures it with a property test.
- **`MemberLeaving`**, proposed by the member itself, needs no approvals, like a pledge. Its weight
  steps down hourly to zero over `leaving_secs` from the event's timestamp, so custodian tasks move
  its items while it still serves them. Hourly steps keep nodes whose clocks differ by minutes in
  agreement for all but those minutes. **Expulsion stays abrupt**, as §5.5 has it.

**D7 — The grace top-up.** When an item has had fewer than `replicas` live copies (ground truth 6)
for longer than `grace_secs`, each node checks whether it is among the first members after the
item's custodians in rendezvous order, skipping members that are offline or full. If it is, it
fetches the item as **cache** — not custody — and the cache keeps it while there is room. Once the
custodians are back, nothing has to happen: the extra copy simply ages out of the cache.

- **Per observer, like availability itself.** Two nodes may disagree about who is online and both
  fetch, or neither; the next check settles it. Extras never count as custody.
- **The grace is what keeps restarts free.** A custodian that restarts is back long before the
  grace runs out.
- This is how §9's "kill a custodian → next-ranked node auto-fetches" holds without breaking §5.5.

**D8 — The pin manager: a diff, and a prioritized, throttled queue.**

- **The diff**: custody wanted (D1, after D5's settling) against `custody/` tags held.
- **Recovery is prioritized**: fetches go in order of live copies ascending, so an item down to one
  copy goes before one at two (§5.5's mitigation 4). At most two fetches per providing peer, four
  in all; the queue is reordered, not restarted, when counts change.
- **An item already in the cache is promoted, not fetched**: a former custodian, a downloader or a
  top-up that became custodian swaps the tag.

**D9 — The quota rule.**

**Where the 2× comes from**: §5.5 of [`distlib-plan.md`](../distlib-plan.md) — "required pledge ≥
max(minimum, 2 × bytes of content you've added)". It is stated there without a derivation, and one
fits `replicas = 3` exactly: every byte a member adds costs the group three bytes of storage. If
the creator's own copy is one of them (D1, Q1), the other two sit with other members, so a member
that adds *A* bytes should offer to hold about *2A* of other members' items in return.

So the proposal is the rule with its reason built in:

```text
added_bytes(m)     = Σ size of each content file whose file key has an entry by m (ground truth 5)
required_pledge(m) = max(min_pledge_bytes, (replicas − 1) × added_bytes(m))
```

— 2× at `replicas = 3`, and still fair if a group chooses 2 or 5. A file key written by two members
counts against both: rare (ground truth 5), and it errs towards more storage. Enforcement is **soft
and local**, as §5.5 says: `library.add` refuses an add that would leave the pledge below the
requirement, telling the member the pledge it would need, and `group.pledge_set` refuses to go below
it. The members list shows each member's pledge beside its requirement.

**D10 — Challenges: a verified chunk probe, signed and gossiped.**

- **Who**: each member challenges the custodians of the items it created, §5.5's rule. Each pair
  (item, custodian) about once per `challenge_secs`, at a random moment, and only once the custodian
  has had `grace_secs` to fetch it. An offline custodian is skipped, not failed.
- **How**: `get_chunk_probe` on a random chunk of a random content file (ground truth 8), within
  30 s.
- **Results** are `SignedChallengeResult { challenger, custodian, item, blob, chunk, passed, at }`
  on a topic of their own beside the heartbeat's, so the beat's format and size budget stay as phase
  4 left them. Each node keeps what it hears in a local table — evidence has to survive a restart.
- **Red** in the Node page's members list: failures from at least two distinct challengers within
  `challenge_secs`, and no pass since. The evidence — who, what, when — is shown on the member.
- **Attached to an expulsion through the CLI and the API**, because the UI's admin is read-only
  (C15): `distlib expel <member> --with-evidence`, and `group.propose_expel`'s `with_evidence`,
  write a summary into the reason within `MAX_REASON`'s 512 bytes
  ([`event.rs:157`](../../crates/distlib-consensus/src/event.rs)). The decision stays human and
  core-group (§5.5).
- **What it cannot catch**, accepted in §5.5: a custodian that fetches the chunk from another holder
  on demand passes. The 30 s bound is the only limit.

---

## Open questions

Each with a proposal; none blocks 5a.

- **Q1 — Does the creator's copy count as one of the `replicas`?** Proposed: yes. Custodians are the
  creator plus the top `replicas − 1` others; once the creator is no longer a member, the top
  `replicas`. It makes the creator accountable for its own items, saves a copy per item, and gives
  D9's 2× its meaning. If two members created the same files, both keep them and the first by score
  counts.
- **Q2 — Is the cache bounded only by the pledge?** Proposed: yes — custody first, cache in what is
  left, nothing beyond. A member that wants a larger cache raises its pledge. The alternative, a
  local `cache_bytes` in the node's config, lets a member keep downloads without offering the space
  to the group.
- **Q3 — Does the creator's own copy count against its pledge?** Proposed: no (the table above): the
  pledge is what a member holds *for others*, and D9 already ties it to what the member adds.
- **Q4 — May settings change after founding?** Proposed: not in this phase. Changing `replicas` or
  `cap` reshuffles every item at once; a group that needs it can found again. A
  `GroupSettingsChanged` needing a core majority is the shape if it is wanted — C37.
- **Q5 — `settle_secs` and the daily bound.** An hour and a day are guesses; a measurement on a
  group with real churn will set them.

---

## Sub-phases

Fifteen PRs. Each ends compiling, tested, and demonstrable by hand with the CLI or a page. One PR at
a time, review before the next.

### 5a — foundations (3 PRs)

- **5a-1 — GC on, and nothing lost to it.** `own/` tags for what `library.add` imports, replacing the
  automatic ones; `cache/` tags for downloads; the catalogue registers iroh-docs' protect handler;
  GC runs hourly. Closes C26: superseded base lists are collected.
  **Acceptance:** a real GC pass on a node that has added a file, downloaded one and synced a
  catalogue — all three survive, readable, and a deliberately untagged blob is gone. Mutation-checked:
  without the protect handler the catalogue's content is collected; without the `own/` tag the added
  file is.
  **Watch for:** this is the phase's risk (below). Nothing untags on purpose until it is in.

- **5a-2 — rendezvous scoring (D1, D3).** `distlib-core::custody`: the score, weights with the cap and
  the step, and `custodians(item)` for one item — pure, without overflow yet.
  **Acceptance, as property tests:** the same inputs give the same custodians in any input order; a
  member joining moves only items where it now outscores somebody, each by one slot; load is
  proportional to weight within a stated tolerance over random weights; a pledge change within one
  step moves nothing.

- **5a-3 — group settings at founding (D2, C9).** `GroupFounded` carries them, `Item::replicas` goes,
  and the settings show in `node.status`, the CLI and the Node page. `PENDING_EXPIRY` reads the
  setting.
  **Acceptance:** a group founded with `replicas = 2` reports it on every node, followers included; a
  group founded without settings reports the defaults.

### 5b — keeping items (7 PRs)

- **5b-1 — custody, settled, with overflow (D1, D5, D6).** Custodians of every item, recomputed in the
  background once membership has settled; `library.item` and the item page show them, this node's
  share in `node.status`.
  **Acceptance:** five in-process nodes agree on every item's custodians; a burst of membership
  changes inside `settle_secs` causes one recompute; a full member is skipped identically
  everywhere. Property test: the overflow cascade on random inputs, its size recorded.

- **5b-2 — the pin manager (D8).** Custody tags set and released; a released item falls into the
  cache; fetches prioritized and throttled.
  **Acceptance:** a node made custodian fetches the item without anybody asking for it; losing
  custody, it keeps the item as cache until the item has its copies elsewhere; a queue fetches the
  one-copy items first; an item already cached is promoted without a fetch.

- **5b-3 — the cache and its eviction (D3, Q2).** Last-used times, and eviction least recently used
  first when custody needs room.
  **Acceptance:** a node at its pledge evicts the oldest cached item to take on a new custody, never
  an own or custody item; the exported file is untouched.

- **5b-4 — the grace top-up (D7).**
  **Acceptance:** §9's first half — five nodes, `replicas = 3`, `grace_secs` a few seconds at
  founding; a custodian stopped, the next member in rendezvous order fetches within one cycle. A
  custodian restarted within the grace causes no fetch.

- **5b-5 — `MemberLeaving` (D6).** The event, the hourly ramp, `group.leave` and a Node page button.
  **Acceptance:** with `leaving_secs` short, a leaving member's items move step by step, each new
  custodian fetching while the leaver still serves; nothing is under-replicated on the way.

- **5b-6 — the quota rule (D9).** The authorship table, `added_bytes` and `required_pledge`, the
  refusals in `library.add` and `group.pledge_set`, and the requirement in the members list.
  **Acceptance:** at `replicas = 3`, a member with a 100 GB pledge adding files totalling 60 GB is
  refused and told 120 GB; raising the pledge lets the add through.

- **5b-7 — keeping, in the UI.** The item page's custodians with their online state; the Node page's
  pledge, requirement, and bytes held as own, custody and cache.
  **Acceptance:** Vitest per view; one e2e path from adding an item to seeing its custodians.

### 5c — challenges (5 PRs)

- **5c-1 — the probe and its schedule (D10).**
  **Acceptance:** a custodian holding the item passes; one whose blob was deleted underneath it
  fails; an offline one is skipped, not failed; the schedule spreads pairs over `challenge_secs`.

- **5c-2 — signed results, gossiped.**
  **Acceptance:** three nodes see the same results; a forged or replayed result, or one by a
  non-member, is dropped.

- **5c-3 — evidence: red in the members list, attached to an expel proposal.**
  **Acceptance:** two challengers' failures turn a member red on every node; a pass clears it;
  `distlib expel --with-evidence` proposes with a reason that names the failures and fits
  `MAX_REASON`. Vitest per view.

- **5c-4 — §9's acceptance, as one test.** Five nodes, `replicas = 3`: a custodian stopped, the next
  member fetches within one cycle; a node whose pinned blob is deleted fails challenges from two
  peers and is red everywhere.

- **5c-5 — by-hand check.** [`manual-check.md`](../manual-check.md) grows within its one story, and
  what it finds is fixed before the phase is called done.

---

## Carried forward — take or defer, explicitly

Every open C-number appears here once.

| # | Item | Phase 5 |
|---|---|---|
| **C1** | A full peer offer is O(N²) dials | **Unchanged** — and worth measuring at the thousands of members this phase now targets. |
| **C3** | The sweep reads the whole document every five seconds | **Deferred.** Still cost only. |
| **C6** | A field blinks out of the read model while its newest value is in flight | **Unchanged.** |
| **C7** | The read model is replayed in full at every start | **Deferred**, and relied on again: 5b-6's table fills from the replay. |
| **C8** | `added_by`, `created`, `modified_by` have nowhere to come from | **In part, by 5b-6**: authorship, for the creator's copy and quotas. `created` and `modified_by` stay open. |
| **C9** | `PENDING_EXPIRY` is one fixed count | **Closed by 5a-3.** |
| **C13** | Gossip does not change sides when a node is promoted | **Deferred.** |
| **C15**–**C19** | Read-only admin; Windows file privacy and service; clearing a field; downloads as in-memory tasks | **Unchanged.** |
| **C21** | Title sort | **Unchanged.** |
| **C22** | Consensus test peers cannot restart | **Closed by #132**: `Peer::restart`. |
| **C23**, **C24** | Release publishing; the expelled leader's lost answer | **Unchanged.** C23 is what lets 5a-3 change `GroupFounded` freely. |
| **C26** | Superseded base lists stay on the publisher's disk | **Closed by 5a-1.** |
| **C27** | Heartbeat traffic not measured above N = 50 | **No phase**, but thousands of members makes it pressing; C32 is its sharper form. |
| **C28** | The `item → members` map lives in memory | **No phase.** D7 and D8 read it; they add nothing to it. |
| **C29**–**C33** | One wish comment per member; a failed gossip dial strands a peer; a write missing both paths; presence traffic above 1,000 members; iroh-docs' `AlreadySyncing` wedge | **Unchanged.** C32 matters at the new target. |

**New in phase 5:**

| # | Item | Where it goes |
|---|---|---|
| **C34** | **One `replicas` for the whole group** (D2) | **No phase.** Per-item values need a rule about who may ask the group to store an item more times. |
| **C35** | **A false `FileRecord::size` is logged, not acted on** (D4) | **No phase.** The fetching custodian knows the real size; correcting the catalogue's claim is a later decision. |
| **C36** | **"What am I custodian of" is a full pass** — items × members (ground truth 9) | **No phase.** Incremental top-score tracking when a group measures it slow. |
| **C37** | **Settings are fixed at founding** (Q4) | **No phase.** A `GroupSettingsChanged` with core approval, if a group needs it. |

---

## Testing and the lanes

**Fast lane:** rendezvous scoring's three properties, the weight step and the overflow cascade, as
property tests; the leaving ramp's steps; `added_bytes` and `required_pledge`; tag transitions
between own, custody and cache; the eviction order; the schedule's spread; signing round trips and
tampering of challenge results; the D7 decision on given counts.

**Slow lane:** GC keeping what it must (5a-1); five-node custody agreement and settling; pin,
release into the cache and eviction; the grace top-up; leaving; the challenge outcomes; §9's
acceptance.

Multi-node tests found their groups with short settings — a grace and a settle of seconds — so what
is tested is the setting, not a test-only path. They **assert "within", never an exact sequence**,
as phase 4's do.

Every mechanism is mutation-checked. UI: Vitest first, Playwright for the happy paths only.

---

## Risk, stated once

**The first time distlib deletes anything.** Until now no blob has ever been removed: GC is off
(ground truth 7). Phase 5 needs it — to evict the cache and to collect what custody released — and a
GC pass deletes whatever is neither tagged nor protected. Today that would include the content of
every catalogue entry, without which the catalogue cannot be read.

The fences, each a rule PRs are reviewed against:

- **5a-1 ships alone, first**, and its test runs a real GC pass over every kind of blob distlib
  writes. A new kind of blob in a later PR extends that test.
- **Only two things delete tags**: the pin manager its `custody/` ones, and the cache its `cache/`
  ones. `own/` tags and the documents' protection are never removed by anything in this phase.
- **Releases wait for copies elsewhere** (D5), and a release lands in the cache rather than in the
  bin.
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

- the group is founded with `replicas = 2` and short settle and grace;
- dave adds a book: it is his own, and one more member is its custodian and fetched it unasked;
- bob, custodian, stopped: after the grace, the next member fetches it into its cache; bob back,
  nothing moves;
- erin, under-pledged, is refused an add and told what pledge she needs;
- bob deletes a pinned file by hand: after challenges from two members he is red on the Node page,
  and `distlib expel bob --with-evidence` proposes with the evidence as its reason.
