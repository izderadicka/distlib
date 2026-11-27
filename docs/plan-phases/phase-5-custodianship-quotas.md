# Phase 5 — Custodianship & quotas

Sequencing plan for §9's Phase 5. Written before any code, and revised in place if a PR proves part
of it wrong — an entry that turned out to be mistaken is more useful corrected than deleted.

---

## Context

**Status: agreed in discussion with Ivan, in progress — 5a done: 5a-1 (#134), 5a-2 (#135),
5a-3a (#136), 5a-3b (#137); 5b-1a (#138), 5b-1b (#139), 5b-2a (#141), 5b-2b (#142), 5b-2c (#143),
5b-3a (#144), 5b-3b (#145 to #147), 5b-3c (#148), 5b-4 (#149), 5b-5a (#150), 5b-5b (#151), 5b-6 (#152),
5b-7 (#153); 5c-0 (#155), 5c-1a (#156), 5c-1b (#157), 5c-1c (#158), 5c-1d (#159), 5c-2a (#161), 5c-2b (#162), 5c-2c (#163), 5c-3a (#164), 5c-3b (#165), 5c-4 (#166), 5c-5a (#167); #160; 5d in progress, before 5c-5b: 5d-1 (#168), CI flakes (#169), 5d-2 (#170), 5d-3 (#171), 5d-4a (#172), 5d-4b (#173), a catalogue error says why (#174), 5d-5 (#175), 5d-6 (#176), 5d-7a (#177), 5d-7b (#178), 5d-8a (#179), 5d-8b (#180); before 5c-5b, the dry run's fixes (P5-49); 5c-5b planned from Ivan's run and #184 (P5-50); 5c-5b1 (P5-51), 5c-5b2 (P5-52), 5c-5b3 (P5-53), 5c-5b4 (P5-54); #183's fixes: 5d-9a (#191, P5-55), 5d-9b (#193, P5-56); 5e planned from #194 and #185 (P5-57); 5e-2 (#196, P5-58), 5e-3 (#197), 5e-4 (#198), 5e-5 (#199, P5-59), 5e-6 (#201), 5e-7a (#202, P5-60), 5e-7b (#203, P5-61), 5e-8 (#204); C58 closed (P5-62); 5e-9 (#206, P5-63); 5f planned from #200 (#207, P5-64); 5f-6 (#209, P5-65); 5f-2 (#213, P5-66); 5f-7 planned (P5-67), 5f-7a (#214), 5f-7b (#215), 5f-7c (#216), 5f-7d (#217); 5f-3 (#218); 5f-4 (#219, P5-68); 5f-5 (#220); 5g planned (P5-69); 5g-1 (#221); 5g-2 (#222, P5-70), ordered epochs (#223, P5-71); 5g-3 (P5-72); the mutation pass: 5h-1 (#226, P5-73), 5h-2 (#227), 5h-3a (#228), 5h-3b (#229), 5h-4 (#230), 5h-5 (#231); 5i planned (P5-74); 5i-1 (P5-75), 5i-2 (#234), 5i-3 (#235), 5i-4.** Four rounds of review on #133 shaped it;
what they decided is listed below, each with the facts it was decided on, and what is still a guess
is under [Open questions](#open-questions).

Membership lifecycle correction in **#160**: preserve quorum through durable retirement,
preserve acknowledged logs on re-promotion, derive replay authority only from signed core
transitions, and let seatless sources serve history/referrals. See **P5-33–P5-37** in
[`plan-deltas.md`](../plan-deltas.md); these supersede the promotion/log-clearing and blanket
seatless-refusal assumptions in P2-8 and P3-25.

**5d** finishes what #154 still asks for and adds the hooks v1 needs so that live groups can be
upgraded later ([`plan-upgrades.md`](../plan-upgrades.md)); it goes before 5c-5b, so that Ivan's
manual run checks the final rules (P5-38).

Phase 4 is complete and merged, and so is the refactor series that followed it
([`plan-refactor-complexity.md`](../plan-refactor-complexity.md)). Every member says what it holds in
a heartbeat, the library shows who is online with an item, and ratings, reviews, bookmarks and
wishes work end to end.

Phase 5 is §9's "Custodianship & quotas", the part of §5.5 that makes distlib a library rather than
a swarm:

- custodians for every item, computed from the membership log alone;
- **one model of why a node keeps an item** — it created it, it is custodian, or it downloaded it —
  with two budgets, and a clear order of what goes first when space runs out;
- a pin manager that fetches what this node is custodian of, throttled, and keeps it while it is;
- the quota rule: a member's pledge grows with what it adds;
- checking that custodians keep what they should: what they announce against what they should hold,
  and, where the group wants it, random probes whose failures are recorded and shown.

§9's acceptance is the target, reworded where the discussion changed it (P5-1, in 5c-4): *5 nodes,
`replicas = 3`; expel a custodian → the next-ranked member fetches the item; stop all but one copy's
holder → after the emergency delay another member fetches an emergency copy; a node that deletes its
pinned data fails probes from 2 peers and shows up red in the members list.*

### Decided in discussion

- **Rendezvous hashing, as §5.5 has it** (round 1). The goal for this version is groups of
  thousands of members. A weighted ring was measured and set aside for a later version.
- **"Am I custodian of this item?" stops early** (round 3, measured — ground truth 9): about a dozen
  scores per item at 1,000 members, growing only with the logarithm of the group, so recomputing
  custody for every item costs about two seconds per million items on one thread, a quarter of a
  second on twelve.
- **`replicas` is a group property, set when the group is founded**, and settings do not change after
  founding in this phase (rounds 1–2).
- **The creator keeps what it added, and its copy is one of the `replicas`** (rounds 1–2).
- **Two budgets** (round 2): the pledge, in the log, weighs custody; a local storage limit, larger
  than the pledge, bounds everything, and pressure on it evicts the cache. *(P5-25: the pledge
  weighs custody but no longer cuts it; custody past it is kept.)*
- **Membership changes are settled and throttled locally** (round 3): custody is recomputed once
  membership has been quiet for a while; no custody epochs in the log for now.
- **Joins rebalance, throttled** (round 3): a new member takes its fair share, at a rate.
- **Custody never follows liveness** (round 3): an offline custodian changes no assignment. Only an
  item down to its **last live copy** gets an emergency copy, after a delay the group sets.
- **Fetching has a rate limit in local config, never below a minimum the group sets** (round 3).
- **Custody is checked in two layers** (rounds 3–4): what custodians announce against what they
  should hold, always; random probes, optional per group.
- **Probes are random samples by holders; a failure means repair first, blame second** (round 4).
- **Only failures are recorded, one key per (custodian, prober), cleared by that prober's next pass**
  (round 4), and probes split their effort between random sampling and re-checking suspects.
- **Overflow and `MemberLeaving` are out of phase 5** (round 4): there is plenty to do. A load
  factor shows when a group's custody outgrows its weight (D9).
- **A probe is iroh-blobs' own verified chunk probe** (ground truth 8), not a new protocol.
- **The creator is never ranked as a custodian of its own item** (round 5): its copy is the first
  replica, and the ranking fills the rest (D1).
- **The quota warns before it refuses** (round 5): a big red warning when under-pledged, a refusal
  only when far under (D8).
- **Recomputing custody runs on all cores with rayon, above a size where that pays** (round 5;
  5a-2).

**The scale target:** groups of **thousands of members**, with phase 4's items target — about 100k
easy, 10M working — and some nodes holding a large share of the library.

The phase also takes, explicitly:

- **C9** — `PENDING_EXPIRY` becomes a group setting (D2).
- **C26** — superseded heartbeat base lists are collected once GC runs (5a-1).
- **C8, in part** — "who added this" exists, for the creator's copy and for quotas (D1, D8).

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
   item, and whether a given member announces it, are lookups.

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
   policy, not before — and **the content of every catalogue entry, which is never acceptable**.

8. **A probe needs no protocol of ours.** `iroh_blobs::get::request::get_chunk_probe(connection,
   hash, chunk)` (`get/request.rs:287-319`) requests one 1 KiB chunk over the blobs ALPN every node
   already serves, and the transfer verifies it against the blob's root hash. A prober needs only
   the hash, which the catalogue gives everyone — and anyone can repeat a probe to check a claim.

9. **Rendezvous hashing's cost, measured** on this machine, with throwaway code in the scratchpad:
   58 ns per (item, member) score on one thread, 6.8 ns on twelve.

   | Question | How | Cost |
   |---|---|---|
   | Custodians of one known item | score every member, keep the top | ~58 µs at 1,000 members, ~0.3 ms at 5,000 |
   | **Am I custodian of this item?** | score myself, then the others **until `replicas` of them outscore me** | **~12 scores per item at 1,000 members** (measured again in 5a-2, mixed weights; the first throwaway measurement's 6–7 was wrong): 1M items in ~2 s on one thread, ~0.25 s on 12 |
   | Everyone's custody at once | every item × every member | 1M items × 1,000 members ~58 s, ~7 s on 12 threads |

   The early stop works because a member that is not custodian of an item meets `replicas` higher
   scores quickly: about 2·ln(N) scores on average, since an item where this member happens to score
   high takes longer to rule out. A score is one keyed BLAKE3 of one 64-byte block, ~71 ns. **So recomputing a node's own custody after a
   membership change is cheap**; only a rule that needs *everyone's* custody — overflow was one —
   would pay the last row, and phase 5 has none.

10. **Fetching already prefers whoever is known to hold the data.** `Blobs::fetch` takes an ordered
    provider list and tries it in order
    ([`blobs.rs:122-160`](../../crates/distlib-net/src/blobs.rs)); the pin manager hands it the
    item's online holders first, as `library.download` does.

---

## The structural decisions

**Custody is computed, never replicated.** Who is custodian of what is a pure function of two inputs
every node already has: the membership log (members, pledges, group settings) and the catalogue
(items, their sizes and their creators). Nothing about custody is written anywhere. Every node
computes the same answer from the same inputs, and a node whose view lags computes a slightly
different one until it catches up — which costs at most a fetch that ends up as cache, or one that
happens a little later.

**Custody never follows liveness.** A custodian that goes offline changes nobody's assignment —
§5.5's principle, kept, and the main fence against data flowing back and forth (D5). Availability
answers a different question:

| | Answers | Used for |
|---|---|---|
| **Availability** (phase 4) | who holds an item *now* | downloading; the emergency copy (D6); fetch order (D7); checking custodians (D10) |
| **Custody** (this phase) | who *promised* to keep it | what a node keeps; which probes are worth making |

Custody is not a discovery mechanism: a download asks availability, as it does today.

---

## Why a node keeps an item

A node keeps an item for one of three reasons, and may have several at once:

| Reason | Who | Kept | Counts against |
|---|---|---|---|
| **Own** | the member that added it — its creator | always, while the member is a member | the storage limit; it is what the quota rule (D8) measures |
| **Custody** | the members rendezvous hashing assigns (D1) | while assigned | the pledge, and the storage limit |
| **Cache** | anyone who downloaded it; an emergency copy (D6); a former custodian | while there is room | the storage limit only |

Own and custody are **mandatory**: nothing evicts them. Cache is **optional**: it is what goes when
space is short.

- **A blob goes only when no reason is left.** A download of an item this node is also custodian of
  is kept as long as the custody is; losing custody of an item still in the cache leaves it cached.
- **Two budgets, one shared with the group and one not:**
  - **The pledge** is in the log: the space a member offers the group, which weighs its custody and
    which the quota rule measures (D8).
  - **The storage limit** is local, `[storage] limit_bytes` in the node's config, never shared: the
    disk space distlib may use on this machine for all three reasons together. It must be larger than
    the pledge — the difference is room for own items and the cache.
  - **Pressure** is own + custody + cache above the limit, and it evicts the cache, least recently
    used first, until the total fits. *(Changed in 5e-8: the items with the most real copies on
    other online members go first, counted up to `replicas`; then least recently used.)*
  - **Mandatory alone over the limit** evicts the whole cache and is then reported in `node.status`
    and on the Node page; `library.add` refuses an add that would make it worse, and
    `group.pledge_set` refuses a pledge that is not below the limit. *(P5-26: a custody fetch that
    would not fit even with the whole cache gone waits instead, evicting nothing; only the
    emergency copy goes past the limit.)*
  - **Custody past the pledge** — possible, since custody follows weight and not pledge exactly (D9)
    — is kept all the same, and `node.status`, `distlib status` and the Node page say so. *(P5-25:
    5b-7 cut it at the pledge and left the rest a copy short; undone in 5c-1b.)*
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

distlib gives each reason its own tag, one per content blob of the item:

```text
own/<item>/<blob>        set by library.add, kept while the member is a member
custody/<item>/<blob>    set and deleted by the pin manager
emergency/<item>/<blob>  set by an emergency copy, moved to cache/ once it is not needed (5e-7)
cache/<item>/<blob>      set by library.download; evicted under pressure
```

The tag *is* the reason: listing `custody/` is the pin manager's view of what it holds, and a blob
shared by two items, or held for two reasons, has two tags and stays until both are gone. Releasing
custody of an item deletes its `custody/` tags and sets `cache/` ones in the same step, so the item
falls into the cache rather than out of the store. The catalogue's own content needs no tag: the
protect handler keeps it. The cache's last-used times live in a small local table beside the tags,
for eviction.

---

## Design decisions

**D1 — Rendezvous hashing, as §5.5 writes it, with the creator first.**

```text
score(item, m)   = −w(m) / ln(h(item, m)),   h ∈ (0, 1) from keyed BLAKE3(item || m), keyed by
                   derive_key("distlib.custody.v1")
custodians(item) = the top `replicas − 1` by score among members other than the creator,
                   while the creator is a member; the top `replicas` by score once it is not
```

- **The creator is never ranked.** Its copy is the item's first replica by being its own, so the
  ranking skips it and fills the remaining `replicas − 1` slots from the other members. A creator that
  would have scored in the top is therefore never custodian of its own item, never counts it twice,
  and never holds a `custody/` tag beside its `own/` one. Its own items count against its storage
  limit, not its pledge — the pledge is for other members' items, which is what D8 measures.
- **The creator leaving** — expelled — turns its slot into one more ranked slot: the member ranked
  next fetches the item, one copy, D7's rate.
- **Deterministic, but for near-ties**: every node computes the same hash and the same `h` from the
  same bytes; ties, astronomically rare, break by member id. `ln` is the platform's math library,
  which may differ in the last bit between platforms (P5-4), so two nodes can order a near-tie
  differently: about one item in 10¹⁷ per recompute, costing one copy too many or too few.
  Accepted; `libm::log` would make it exact.
- **Minimal reshuffle**: a member joining takes only the items where it outscores somebody, one slot
  in each; an expulsion or a weight change moves only that member's items.
- **Proportional**: a member's expected share is its weight's share.
- **"Am I custodian" stops early** (ground truth 9): the node scores itself, then the others in a fixed
  order, skipping the creator, and stops at `replicas − 1` higher scores.
- **Two creators** — two members who added the same files at once (ground truth 5) — both keep their
  copy as their own, and both are skipped by the ranking: the item has one copy more than
  `replicas`, which is rare and errs towards safety.
- **Pure, in `distlib-core`**, property-tested in the fast lane for §10's three properties.

**D2 — Group settings, set when the group is founded.** `GroupFounded` gains them:

```text
replicas              u8     default 3
cap_bytes             u64    default 2 TB      (D3)
weight_step_bytes     u64    default 10 GB     (D3)
min_pledge_bytes      u64    default 100 GB    (§5.5)
pending_expiry        u64    default 128       (C9)
settle_secs           u32    default 1 h       (D4)
max_settle_secs       u32    default 1 day     (D4)
emergency_after_secs  u32    default 1 day     (D6)
min_transfer_rate     u64    default 1 MB/s    (D7, P5-12)
probes                bool   default on        (D11)
probe_every_secs      u32    default 5 min     (D11)
failure_window_secs   u32    default 7 days    (D12)
```

A founder that sets none gets the defaults. `node.status`, the CLI and a read-only section of the
Node page show them. `Item::replicas` is removed from the catalogue schema (ground truth 3). They do
not change after founding: changing `replicas` or `cap` reshuffles every item at once, and a group
that needs it can found again (C37).

**D3 — Effective weight.**

```text
w(m) = step × floor(min(pledge(m), cap) / step)
```

The cap is §5.5's mitigation 1: absolute, so a join changes nobody else's weight. The step means a
pledge edited from 500 GB to 505 GB moves nothing. A member with no pledge has no weight and is
custodian of nothing beyond its own items.

**D4 — Membership changes are settled before custody follows them.** The pin manager recomputes this
node's custody only once the membership has gone `settle_secs` without a change; each new join,
expulsion or pledge change restarts the wait, so ten changes in an afternoon are one reshuffle in
the evening. `max_settle_secs` is the longest it waits regardless, so a large group whose membership
never stays still still moves. Nodes compute from the same settled membership and agree on the
result; they may start a few seconds apart, and a lagging view costs at most a fetch that ends as
cache. Custody epochs in the log would make the switch-over exact, and are not needed yet (C40).

**D5 — No data flows back and forth.** Every source of bulk transfer, and what bounds it:

| Trigger | Traffic | Bound |
|---|---|---|
| A new item | `replicas − 1` copies, once | — it is the point |
| An expulsion | the expelled member's custody, to others | D7's rate |
| A join | the newcomer's fair share, taken from others | D7's rate; D4's settling |
| A pledge change | that member's items, in or out | D3's step; D4; D7 |
| A custodian offline | nothing | custody ignores liveness |
| An item at its last live copy | one emergency copy | D6's delay |
| A change that is undone | nothing | losing custody leaves the item in the cache |

A node does not even demote an item from custody to cache until the item has `replicas` live copies
elsewhere.

**D6 — The emergency copy.** When an item has had **one** live copy (ground truth 6) for
`emergency_after_secs`, each node checks whether it is the first member after the item's custodians
in score order that is online and has room. If it is, it fetches the item as **cache**. When the
custodians come back nothing is recomputed: the extra copy simply ages out of the cache.
*(Changed in 5e-7a, P5-60: kept under its own mandatory reason, past the storage limit, and moved
to the cache once the members obliged to keep the item hold `replicas` copies.)*

- **Per observer, like availability itself.** Two nodes may disagree about who is online and both
  fetch, or neither; the next check settles it.
- **The delay keeps absence cheap.** A laptop off overnight costs nobody a fetch; a member away for a
  week costs one copy of each item where it was the last holder. Long absence is a human matter —
  the members list shows it, and expulsion moves custody properly.

**D7 — The pin manager: a diff, and a throttled, prioritized queue.**

- **The diff**: this node's custody (D1, after D4) against its `custody/` tags.
- **The rate** *(changed in review of 5b-2a, P5-12: node-wide limits instead, below)*:
  `[custody] fetch_rate` in the node's config, never below the group's
  `min_fetch_rate` — a lower value is raised to it, with a warning. A member cannot opt out of its
  share by throttling to nothing, and D10 shows one that falls behind.
- **Recovery is prioritized**: fetches go in order of live copies ascending, so an item down to one
  copy goes before one at two (§5.5's mitigation 4). At most two at a time from one providing peer
  *(changed in review of 5b-2a, P5-12: a cap on fetches at once in total)*; the queue is reordered,
  not restarted, when counts change.
- **An item already in the cache is promoted, not fetched.**

**D8 — The quota rule.** §5.5 of [`distlib-plan.md`](../distlib-plan.md) says "required pledge ≥
max(minimum, 2 × bytes of content you've added)", without a derivation. One fits: at `replicas = 3`
every byte a member adds costs the group three bytes; the creator holds one (D1), so the other two sit
with other members, and a member that adds *A* bytes should hold about *2A* of other members' items
in return.

```text
added_bytes(m)     = Σ size of each content file whose file key has an entry by m (ground truth 5)
required_pledge(m) = max(min_pledge_bytes, (replicas − 1) × added_bytes(m))
```

2× at `replicas = 3`, and still fair if a group founds with 2 or 5. A file key written by two members
counts against both: rare, and it errs towards more storage.

Enforcement is **soft and local**, as §5.5 says, and **warns before it refuses**:

- **Under-pledged** — pledge below the requirement: the add or the pledge change goes through, with a
  big red warning that names the pledge needed — in the CLI's output, on the add page, and beside the
  member in the members list.
- **Far under-pledged** — the requirement more than twice the pledge: `library.add` refuses an add
  that would get there, and `group.pledge_set` refuses a pledge that would, both telling the member
  the pledge needed.

The factor of two is a constant, not a group setting, until a group needs another (Q9).

**D9 — The load factor, instead of overflow.** Custody follows weight, and weight is the pledge
capped, so when large members are capped the rest are asked for more than their pledges. Everyone
can compute the group's **load factor** cheaply:

```text
demand = Σ over items of size × (ranked custodian slots: replicas − 1, or replicas once the creator is gone)
load   = demand / Σ w(m)
```

The Node page shows it and warns above 90 %: raise the cap, or pledges. Above 100 % some items have
a copy short, visibly (the table above). §5.5's overflow — moving a full member's lowest-scoring
items to the next member — would need everyone's custody (ground truth 9's last row) and cascades;
it is out of this phase (C38).

**D10 — Checking custody, layer one: promises against claims. Always on.** Each node knows, for the
items it holds, who their other custodians are (D1, one known item at a time) and whether each
announces the item in its heartbeat (ground truth 6). So every holder watches its co-custodians:

- **Coverage** per member: of the items this node holds and that member should hold too, the share
  it announces, counting only assignments older than `max_settle_secs` so a member still fetching is
  not blamed.
- It catches the free-rider that never fetches, a node out of space, a stuck pin manager, and one
  throttled below its share. It costs nothing on the network: the heartbeats already carry it.
- It is this node's own view, shown in its members list, and not published. A member with low
  coverage is a suspect for layer two.

**D11 — Checking custody, layer two: probes. Optional per group** (`probes` in D2).

- **What layer one cannot see**: a member that announces an item it no longer has — a lost disk with
  the database intact.
- **Who**: holders, by random sampling. Every `probe_every_secs`, a node picks a random item it holds
  and probes one of the item's other custodians that announces it, with `get_chunk_probe` on a
  random chunk of a random content file (ground truth 8), within 30 s. An unreachable member is
  skipped, not failed — that is availability's business.
- **Effort is split**: three probes in four are random sampling; the fourth re-checks a suspect — this
  node's own recorded failures first, to confirm or clear them, then members with failures recorded
  by others, then members with low coverage (D10). With no suspects, all four are random.
- **A failure means repair first, blame second.** The failed copy counts as missing, so an item down
  to its last real copy gets D6's emergency copy. Then the failure is recorded (D12).

**D12 — Failures, recorded in the catalogue.**

- **Only failures**, as Ivan put it: a pass is what is expected. One key per pair:
  `probe/<custodian>/<prober>`, written by the prober when its probe of that custodian fails —
  item, blob, chunk, time, and a count — and **cleared by the same prober** when its next probe of
  that custodian passes.
- **Bounded by pairs, not items**: at most one key per (custodian, prober) that ever saw a failure;
  in a healthy group, close to nothing. A cleared key is a small iroh-docs tombstone.
- **Red** in the members list: failures from at least two distinct probers, each within
  `failure_window_secs`. Only a prober can clear its own key, so another member's pass denies a
  suspicion by not adding a second failure.
- **The suspects view** orders members by active failures and distinct probers — the candidates for
  expulsion — with the latest evidence and a **check now** button, which runs a probe from this node.
- **Expulsion with the evidence goes through the CLI and the API**, because the UI's admin is
  read-only (C15): `distlib expel <member> --with-evidence` and `group.propose_expel`'s
  `with_evidence` write a summary into the reason, within `MAX_REASON`'s 512 bytes
  ([`event.rs:157`](../../crates/distlib-consensus/src/event.rs)). The decision stays human and
  core-group (§5.5).
- **What probes cannot catch**, accepted in §2 and §5.5: a member that fetches a chunk from another
  holder on demand passes. §2's members are semi-trusted — free-riding and negligence are what must
  be detected, not active attacks.

---

## Open questions

The design is agreed; these numbers are guesses until something is measured:

- **Q5 — `settle_secs` and `max_settle_secs`** (D4): an hour and a day.
- **Q6 — `emergency_after_secs`** (D6): a day.
- **Q7 — the probe rate and split** (D11): one probe per node every five minutes, three in four
  random. At 1,000 members that is about 290,000 probes a day across the group, each a 1 KiB chunk.
- **Q8 — `min_transfer_rate`** (D7, P5-12; was `min_fetch_rate`): 1 MB/s is about 86 GB a day — enough for a newcomer's share in a
  modest group, slow for a large one. A measurement of a real rebalance will set it.
- **Q9 — "far under-pledged"** (D8): a requirement more than twice the pledge. With the defaults, a
  member pledging 100 GB adds up to 50 GB freely, is warned up to 100 GB, and is refused beyond.

---

## Sub-phases

Twenty-six PRs — fourteen planned, 5a-3, 5b-1 and 5c-3 each split in two, 5b-2 and 5c-2 in three, 5c-1 in four, and 5c-0 and 5b-7 added in review. Each ends compiling,
tested, and demonstrable by hand with the CLI or a page. One PR at a time, review before the next.

### 5a — foundations (4 PRs)

- **5a-1 — GC on, and nothing lost to it.** *Done in #134; deltas P5-2 and P5-3.* `own/` tags for what `library.add` imports, replacing the
  automatic ones; `cache/` tags for downloads; the catalogue registers iroh-docs' protect handler;
  GC runs hourly. Closes C26: superseded base lists are collected.
  **Acceptance:** a real GC pass on a node that has added a file, downloaded one and synced a
  catalogue — all three survive, readable, and a deliberately untagged blob is gone. Mutation-checked:
  without the protect handler the catalogue's content is collected; without the `own/` tag the added
  file is.
  **Watch for:** this is the first risk (below). Nothing untags on purpose until it is in.

- **5a-2 — rendezvous scoring (D1, D3).** *Done; deltas P5-4 and P5-5. Parallel from 1,024 items,
  measured.* `distlib-core::custody`: the score, weights with the cap and
  the step, `custodians(item)` for one item with the creator skipped, the early-stopping
  `is_custodian(item, me)`, and `custody_of(me, items)` — the whole pass that 5b-1 runs — all pure.
  The pass runs on rayon's pool once the item count is above a threshold, sequentially below it; the
  PR measures both and sets the threshold where parallel starts to win. rayon 1.12 is already built,
  for tantivy's search index; it becomes a direct dependency of `distlib-core`.
  **Acceptance, as property tests:** the same inputs give the same custodians in any input order; a
  member joining moves only items where it now outscores somebody, each by one slot; load is
  proportional to weight within a stated tolerance over random weights; a pledge change within one
  step moves nothing; `is_custodian` agrees with `custodians` on every item; the creator is never
  among its own item's custodians, and its expulsion adds exactly one; `custody_of` gives the same
  set in parallel as sequentially.

- **5a-3 — group settings at founding (D2, C9).** `GroupFounded` carries them, `Item::replicas` goes,
  and the settings show in `node.status`, the CLI and the Node page. `PENDING_EXPIRY` reads the
  setting. *Split in two:* **5a-3a** *(done; delta P5-6)* — the settings in the log, set from a
  founder's `[group]` config section, `pending_expiry` from them, `node.status` and `distlib status`,
  a warning when `[group]` disagrees with the log; **5a-3b** *(done; delta P5-7)* — `Item::replicas`
  removed (catalogue, read model, item page) and the Node page's read-only settings; the snapshot
  test now checks that the settings travel in a snapshot.
  **Acceptance:** a group founded with `replicas = 2` reports it on every node, followers included; a
  group founded without settings reports the defaults.

### 5b — keeping items (13 PRs)

- **5b-1 — this node's custody, settled (D1, D4, D9).** Recomputed in the background once membership
  has settled; `library.item` and the item page show an item's custodians; `node.status` shows this
  node's custody and the group's load factor.
  **Acceptance:** five in-process nodes agree on every item's custodians; a burst of membership
  changes inside `settle_secs` causes one recompute, and a group that never settles recomputes at
  `max_settle_secs`; the load factor matches a hand-computed one.
  *Split in two:* **5b-1a** *(done in #138; delta P5-8)* — every author of an item's file keys read from the
  catalogue into the read model's `file_authors` table, shown as `added_by` by `library.item` and
  `distlib item`; **5b-1b** *(done; deltas P5-9 and P5-10)* — the custody service, in a new
  `distlib-custody` crate, with the custodians in `library.item` and `distlib item`, and this node's
  custody and the load factor in `node.status` and `distlib status`. The membership at a start, or
  the first group a new node learns, is taken as settled at once; a new or changed item is checked
  at once; a replay of the read model, or a missed event, runs the whole pass again. An item's size
  is all of its files. The item page's custodians move to 5b-6.

- **5b-2 — the pin manager (D5, D7).** Custody tags set and released; a released item falls into the
  cache, and only once it has its copies elsewhere; fetches throttled and prioritized.
  **Acceptance:** a node made custodian fetches the item without anybody asking; losing custody, it
  keeps the item as cache; the queue fetches one-copy items first; the fetch rate holds within a
  tolerance and is raised to the group minimum; an item already cached is promoted without a fetch.
  *Split in three:* **5b-2a** *(done; delta P5-11)* — the pin manager in `distlib-custody`: `custody/`
  tags on every file of each item this node keeps, fetched up to ten at once in `library.download`'s
  provider order, given up after 60 s without progress and asked again when availability changes;
  an item already here is only tagged; a lost item falls into the cache once `replicas` online
  members other than this node announce it. Its acceptance is the first, second and last points
  above. The pin manager's state is one enum per item, and fetches run in a `JoinSet` up to a
  fixed ten at once (review of #141).
  **5b-2b** *(done; delta P5-13)* — the queue and the download limit: `max_fetches` in the node's config, ten by
  default, replacing the fixed ten (Ivan: a cap per providing peer can come later); items with the
  fewest live copies first, reordered rather than restarted; and **one node-wide download limit**
  for every blob fetch, custody's and `library.download`'s, enforced in `distlib_net::Blobs` by
  pacing on the downloader's progress stream, whose backpressure slows the transfer itself (Ivan:
  pacing, not byte-exact throttling). **The group's minimum is symmetric** (Ivan, review of
  #141): one group setting, `min_fetch_rate` renamed for both directions, below which neither a
  node's download nor its upload limit may be set; both limits are optional. Its acceptance is the
  two remaining points, with the fetch rate as the download limit. `node.status` shows how many
  items are in each pin state (review of #142).
  **5b-2c** *(done; delta P5-14)* — the upload limit: every blob this node serves, paced through iroh-blobs' provider
  throttle (`ThrottleMode::Intercept`) where `BlobsProtocol` is built, never below the group's
  minimum. **Acceptance:** a node limited to a rate serves a large file at that rate within a
  tolerance, to two downloaders at once together.

- **5b-3 — the storage limit and the cache's eviction** (4 PRs; deltas P5-15 to P5-17). Custody over the
  pledge comes later, in its own PR (Ivan).
  **5b-3a** *(done; delta P5-15)* — `[storage] limit_bytes`, unset meaning no limit; own + custody +
  cache counted from the blob store's tags, a blob kept for two reasons once under the stronger;
  the bytes for each reason, the limit and whether own and custody alone are over it in
  `node.status` and `distlib status`. **Acceptance:** an add that would grow own and custody past
  the limit is refused, and one that grows nothing goes through even over it; a pledge not below
  the limit is refused.
  **5b-3b** *(done)* — in two (delta P5-16). **5b-3b-1** *(done)* — last-used times in the read model
  (all the cache equally old after `db/` is deleted), stamped by a download, a release into the
  cache and a blob served; collection every ten minutes by default, its last pass in
  `node.status`, an error logged when passes stop (C42; distlib cannot run its own pass on
  iroh-blobs 0.103.1). **5b-3b-2** *(done; delta P5-17)* — eviction least recently used first before a custody fetch, a
  download or an add; a download refused when the whole cache is not enough, a custody fetch
  made anyway; holdings no longer announce what was evicted. **Acceptance:** a node at its limit
  evicts the oldest cached item to take on a new custody or a new download, never an own or
  custody item; the exported file is untouched.
  **5b-3c** *(done)* — the Node page's storage: the limit and the bytes held as own, custody and
  cache, and a warning when own and custody alone are over the limit. The page also reloads on
  `catalogue.item_added` and `download.finished`, which change what is kept; a custody fetch has
  no event of its own and shows at the next reload. The last collection pass is in `node.status`
  only. **Acceptance:** Vitest for the view.

- **5b-4 — the emergency copy (D6).** *(done; delta P5-18)* A second task of the pin manager, fetching
  as cache whatever the limit; only members with a pledge copy.
  **Acceptance:** five nodes, `replicas = 3`, `emergency_after_secs` a few seconds at founding; an
  item's holders stopped down to one, the next member in score order fetches an emergency copy after
  the delay; one custodian stopped of three causes no fetch; a holder back within the delay causes
  none.

- **5b-5 — the minimum pledge and the quota rule (D8)**, split in two (Ivan; delta P5-19):
  - **5b-5a — the quota rule, and the minimum as a floor.** *(done; delta P5-19)* `added_bytes`
    (from 5b-1a's `file_authors` table) and `required_pledge`, the warnings and refusals in
    `library.add` and `group.pledge_set` and in the CLI, the requirement in `group.members`; a
    pledge below `min_pledge_bytes` refused by the log.
    **Acceptance:** at `replicas = 3`, a member with a 100 GB pledge adding files totalling 60 GB is
    warned and told 120 GB, and the add goes through; adding 110 GB is refused, told 220 GB;
    raising the pledge clears the warning; lowering it to below half the requirement is refused.
  - **5b-5b — no downloads without the minimum pledge.** *(done; delta P5-20)* `library.download` refuses while this
    node pledges less than `min_pledge_bytes`, naming the pledge needed; and a node refuses to
    serve an item's content files to a member pledging less, per get request, so the catalogue's
    entries and the availability base lists still reach it.
    **Acceptance:** a member at pledge 0 is refused a download with the pledge needed; one that
    skips its own check (a test requester) is refused the content by every serving node, and still
    syncs the catalogue; pledging the minimum lets both through.

- **5b-6 — keeping, in the UI.** *(done; delta P5-21)* The item page's custodians with their online state; the Node page's
  pledge, requirement and load factor (its storage is 5b-3c's); D8's warning on the add page and
  each member's requirement in the members list (from 5b-5a's API).
  **Acceptance:** Vitest per view; one e2e path, over two nodes, from adding an item to seeing its
  custodians.

- **5b-7 — custody over the pledge (D9).** *(done; delta P5-22; undone in 5c-1b, P5-25)* Added in review of 5b-3a (P5-15).
  Custody cuts what this node keeps at its pledge: the assigned items by this node's score, highest
  first, each kept while it fits in what is left. What is over is not fetched, and is reported in
  `node.status` (`custody.over_pledge`), `distlib status` and the Node page.
  **Acceptance:** a node pledging room for one of two books fetches the one it scores higher, says
  the other is over its pledge, and never tags it.

### 5c — checking custody (12 PRs)

- **5c-0 — the whole log, kept (C43).** *(done; delta P5-23)* The log is never purged: `max_in_snapshot_log_to_keep` at
  `u64::MAX`, so openraft still snapshots but deletes nothing. Every signed event, approval and
  refusal stays on the core nodes as the group's audit history, and a follower can always read
  from index 0, so P1-29's `TooFarBehind` can no longer happen. Decided with Ivan after measuring:
  about 600 bytes per event on disk, so a busy group of 1,000 members (about 3,800 entries a year)
  holds about 230 MB after a hundred years; a joiner verifies the whole log once. *(Measured in
  P5-23: 299 bytes an entry, about 120 MB a century, a joiner of 100,000 entries caught up in 6 s.)*
  **Acceptance:** a member joining after the leader has taken a snapshot reads every entry from the
  founding one onward, settings included, and the log's first index stays 1; the measurement
  repeated at 100,000 entries — size on disk, and how long a joiner takes to catch up.
  *Paged* (P5-23): one answer held the whole log, which past about 27,000 events would not fit
  in an answer at all; it now holds a thousand events or 4 MiB of them, whichever comes first, and
  a follower asks for the next at once.

- **5c-1 — coverage from heartbeats (D10).** *(done)* Per member, of the items this node holds and that member
  should hold too, the share it announces; shown in the members list.
  *Split in four* (P5-24, P5-25): **5c-1a** *(done)* — coverage worked out and in `group.members`;
  **5c-1b** *(done)* — 5b-7's cut undone, so *should hold* is everything a member is custodian of,
  and the heartbeat's count over the pledge goes with it; **5c-1c** *(done)* — custody fetches
  stop at the storage limit rather than going past it; **5c-1d** *(done)* — the members list on the Node page and in
  `distlib members`.
  **Acceptance:** a custodian that holds its items shows full coverage; one whose pin manager is
  stopped falls behind once its assignments are older than `max_settle_secs`; one still within that
  time is not blamed.

- **5c-2 — probes (D11), and failures in the catalogue (D12).** *(done)* Random sampling with the suspect
  share, the repair hook, failure keys written and cleared; off when the group says so.
  *Split in three* (P5-27, P5-28): **5c-2a** *(done)* — random probes, and failure keys written and cleared;
  **5c-2b** *(done)* — the failed copy counted as missing, for the emergency copy and for letting go of
  custody; **5c-2c** *(done)* — one probe in four re-checking a suspect.
  **Acceptance:** a custodian holding the item passes and nothing is written; one whose blob was
  deleted underneath it fails, its key appears on every node, and the item gets its emergency copy if
  that was its last real one; its next pass clears the key; an unreachable custodian is skipped; a
  group founded with `probes` off makes no probes.

- **5c-3 — the suspects view and expulsion with evidence (D12).** *(done)* Red, the suspects view, check now,
  and `distlib expel --with-evidence`.
  *Split in two* (P5-30): **5c-3a** *(done)* — red, the suspects view and check now; **5c-3b** *(done)* —
  `distlib expel --with-evidence` and `group.propose_expel`'s `with_evidence`.
  **Acceptance:** failures from two probers turn a member red on every node; one prober's failure
  alone does not; `distlib expel --with-evidence` proposes with a reason that names the failures and
  fits `MAX_REASON`. Vitest per view.

- **5c-4 — §9's acceptance, as one test, reworded (P5-1).** *(done)* Five nodes, `replicas = 3`: an expelled
  custodian's items are fetched by the next-ranked member; an item down to one holder gets an
  emergency copy after the delay; a node whose pinned blob is deleted fails probes from two peers and
  is red everywhere. *As one story* (P5-1): the custodian whose data is gone is red, and is the one
  expelled, with the evidence; its data is emptied in place rather than deleted.

- **5c-5 — by-hand check.** [`manual-check.md`](../manual-check.md) grows within its one story, and
  what it finds is fixed before the phase is called done.
  *Split in two* (P5-32): **5c-5a** *(done)* — the check written, its command-line half run end to
  end first, and what that found fixed; **5c-5b** — what Ivan's run of the whole check finds,
  run after 5d (P5-38), its command-line half dry-run again first (P5-49).
  Ivan's run and Codex's (#184) found what follows, fixed in four PRs (P5-50):
  - **5c-5b1 — the Node page and the pledge.** *Done (P5-51).* A new event, `probes.changed`, published when a
    probe failure is recorded or cleared (`Catalogue::probe_failures_changed`), and the Node page
    reloads on it: §8 promises red and back without a reload, and the page showed neither (#184).
    A pledge field on the Node page (`group.pledge_set`). The under-pledged warning, on the add
    page and in the CLI, says the add went through and when adds will be refused (twice the
    pledge, D8), and the page's links to the field. The tab title names the node.
    **Acceptance:** Vitest for both probe transitions, the pledge field and the title; a Rust test
    that a recorded and a cleared failure each publish the event.
  - **5c-5b2 — a lone node, and quieter logs.** *Done (P5-52).* Bob, left alone in a normal run, logged four
    lines a dead peer every 0.5 s, its own endpoint answering `endpoint stopping`.
    *Diagnose first:* reproduce, and look for noq's `I/O error` — the QUIC endpoint's driver
    dying on a socket error, leaving the node deaf. If it does, the cause is fixed, or reported
    upstream with the node stopping loudly rather than running deaf. Then: openraft's retry of an
    unreachable peer grows from 0.5 s to about 8 s (`RaftNetwork::backoff`); custody logs each
    item it starts or stops keeping, since its only log line is the whole pass, which read
    `items: 0` from before any item existed; the shutdown stops Raft straight after closing the
    endpoint, the membership's watchers stopped before. manual-check.md: carol's own expulsion
    line in §11 (a voter does not print a follower's `it will stop following`), and the known
    harmless lines — "a message will be ignored because vote changed" at founding, and gossip's
    `failed to send` at a stop, if the new order leaves it.
    **Acceptance:** whatever the diagnosis needs as a test; a test of the backoff's intervals.
  - **5c-5b3 — wishes.** *Done (P5-53).* The wishes as a table (title, type, status, wished by, when, fulfilled
    by); "Make a wish" under it, on a page of its own; each wish on a page of its own, with its
    details, Fulfil and its comments (kept, Ivan). **Acceptance:** Vitest per page.
  - **5c-5b4 — the upload.** *Done (P5-54): measured, no change to D5; a relative data
    directory fixed.* 1 GB through the page took 15 s or more on one machine, the download
    less: the bytes are written to a staged file, then copied and hashed into the blob store.
    Measured first, receiving and importing timed apart; if the copy is the cost, the upload
    streams into the blob store as it arrives — a change to D5's staging, proposed to Ivan
    before it is made.

### 5d — v1 readiness: the rest of #154, and upgrade hooks (9 PRs)

#160 fixed #154's findings 1, 2 and 7. What is left, and what v1 needs so that a group can later
run old and new versions side by side ([`plan-upgrades.md`](../plan-upgrades.md)):

- **5d-1 — the plan.** The upgrade note, its questions decided (Ivan), and this section.
- **5d-2 — fold rules (#154's 8 and expiry).** *Done (#170, P5-40).* A member is admitted with
  pledge 0, and so is every founder, the signing one too (P5-40): a pledge is set only by its owner
  (D8).
  A proposal expires by log index (Ivan): it cannot be approved at an index past where it was made
  plus its expiry, whatever came between. Both change what the fold accepts, so they go in before
  v1, when that would need a raise.
- **5d-3 — node-local fixes.** *Done (#171, P5-41).* A stale `CoreGroupChanged` is refused rather
  than retried (Ivan; the plan said rebuilt), so it cannot undo a change made in between (#154's
  stale retry). A snapshot built before a
  promotion's reset cannot be written after it (#154's 5): a generation checked in the writing
  transaction.
- **5d-4 — golden encoding tests.** Fixed bytes for every type that goes on the wire or to disk,
  so that a change to an encoding fails a test rather than a live group. Two PRs (Ivan, P5-42),
  openraft's types included:
  - **5d-4a — the consensus crate.** *Done (#172).* The log's events, signed; the projection;
    the raft, memberlog and gossip RPCs; what redb keeps.
  - **5d-4b — core and sync.** *Done (#173).* The catalogue's and the community's keys and JSON
    values, the item fingerprint; the heartbeat, signed, with the signed address inside it, and
    the base list; the ticket; and a config sample with every key, which must keep loading.
    Sync's own wire facts, the catalogue key and the heartbeat topic, were pinned already.
- **5d-5 — the custody score through `libm::log`.** *Done (#175, P5-43).* The same ranking on
  every platform, fixed before v1 freezes it; the scores' bits and one group's ranking pinned.
- **5d-6 — a data-dir format version.** *Done (#176, P5-44).* A binary refuses a data dir newer
  than it knows, and has a place for migrations; none at v1.
- **5d-7 — the group protocol version.** `protocol` in the membership state, founded at 1;
  `ProtocolSupported { max }`, which each node writes for itself; `ProtocolRaised { to }`, proposed
  by hand (`distlib group raise-protocol`) and approved by a core majority, refused unless every
  core member supports it. Shown in status and the UI. Two PRs (Ivan, P5-45):
  - **5d-7a — the log and the fold.** *Done (#177).* The two events, their rules, the member's
    `protocol_max`, and the node writing its own support at start.
  - **5d-7b — what an operator sees.** *Done (#178, P5-46).* `distlib raise-protocol`, its API
    method, the protocol in status (the group's, this node's, and which core members cannot raise
    yet), and the UI.
- **5d-8 — "upgrade required".** A node below the group's protocol, or meeting a log entry it
  cannot decode, stops following the log, keeps serving blobs and its custody, releases nothing,
  and says "upgrade required" in status, the CLI and the UI (Ivan, Q1 and Q2). Two PRs (Ivan,
  P5-47):
  - **5d-8a — stop following, and say so.** *Done (#179, P5-47).* A follower stops at a raise beyond its
    build, or when every core node that answered sent a page it cannot read. `node.status` and
    `distlib status` say why and where; every page of the UI shows a banner.
  - **5d-8b — release nothing while stopped.** *Done (#180, P5-48).* Custody keeps every copy it holds until
    the node is upgraded.
- **5d-9 — #183's two findings**, after 5c-5b and before the end-of-phase mutation pass. Both
  concern a raise past what some node's build speaks (P5-55):
  - **5d-9a — the start reads the stored protocol.** *Done (P5-55).* A node whose stored
    membership is past its build says "upgrade required" before anything reads the signal, so
    custody keeps every copy from the first moment, and it takes no seat, a voter restarted on
    an older binary included.
  - **5d-9b — a raise waits for Raft's voters.** *Done (#193, P5-56): voters only, not learners.* The leader writes a raise, or an approval of one,
    only while every voter in its Raft configuration, both halves of a joint one, speaks the new
    protocol. Node-local, like a refused proposal's pre-check; the fold is unchanged, so followers
    replay as before. A reconfiguration in flight only adds voters from the new core, which the fold
    already requires to speak the group's protocol.

### 5e — the reviews of #194 and #185 (9 PRs)

Two reviews after 5d. #194 reviewed membership over Raft and how core members are addressed: no
safety problem, two ways a node stays stuck for good, and gaps in relay addressing. #185 reviewed
the catalogue and custody: copies counted as protection that nothing obliges anyone to keep. Every
finding was checked against `ce9d3f5` and still holds. What v1 needs is below, smallest first; the
rest is carried forward (C51–C57), #185's finding 4 stays C41 and its finding 8 joins C1 (P5-57).
The two designs, 5e-6 to 5e-8 (decided) and 5e-9 (proposed), are discussed with Ivan before code.
The end-of-phase mutation pass follows 5f.

- **5e-1 — the plan.** This section, its C-items and P5-57.
- **5e-2 — a node starts as core only with Raft state (#194 finding 2).** *Done (P5-58): only when
  Raft's own membership lists the node.* A crash after the
  projection names this node core and before `prepare_promotion` commits leaves it seated with no
  Raft membership and no promotion; every clause of `may_speak_raft` then refuses the leader, for
  good. `starts_as_core` answers from the projection only when Raft has a membership; otherwise the
  node follows, and `take_the_seat` promotes it on its first pass. The stale comment in
  `core_group.rs` saying expulsions can empty the core goes too (#194 finding 4).
  **Acceptance:** a store reopened in that state starts as a follower, whose first pass prepares the
  promotion; a regression test beside `retirement_tests`.
- **5e-3 — a founder's relay (#194 finding 3).** *Done; the wait has no test, since no test runs a
  relay (C57).* `founders()` keeps a configured `relay` when only `addrs` is empty, ahead of the
  relay reached, and `found` waits, bounded (5 s, as `status --online`), for the home relay when
  relays are on, warning if none is reached, so the founding entry, and every ticket made from it,
  carries one. **Acceptance:** a configured relay with no `addrs` reaches the founding entry; with
  relays off, founding does not wait.
- **5e-4 — held means every file of the item (#185 finding 1).** *Done.* `Holdings::recheck` counts an item
  held only when its files fingerprint to its id and every content file is complete and kept. An
  item whose files are still arriving is not advertised, so it counts neither for a release nor for
  D6. **Acceptance:** an item whose second file record has not arrived is not held; it is once the
  record lands and its file is kept.
- **5e-5 — an add keeps before it publishes (#185 finding 2).** *Done (P5-59): the sweep runs when
  the API starts serving; a failed add leaves its tags to it.* The own tags first, then the
  catalogue write; a failed write releases them. At start, an own tag whose item the catalogue does
  not have is released, which cleans up an add stopped before its write. A partly written item keeps
  its tags, and adding it again completes it. **Acceptance:** an add whose catalogue write fails
  keeps nothing; an own tag with no item is released at start; a published item's files survive GC
  whenever the add stopped.
- **5e-6 — a hand-off counts only copies someone must keep (#185 finding 3; Ivan).** *Done:
  `CustodyGroup::obliged_holders`, under the settled group custody follows; the fetch queue's order
  (D7) still counts every real copy, since it only says which item goes first.* An old
  custodian lets go only when `replicas` online members other than itself hold a real copy and are
  obliged to keep it: the item's current custodians, and its creator while it is a member. Cached
  copies, emergency copies and other former custodians no longer count. Worked out locally from the
  custody ranking; nothing changes on the wire. A new custodian waiting for room (P5-26) keeps the
  old one holding, at no new cost: the old one has the copy already. **Acceptance:** new custodians
  waiting for room and cached copies elsewhere — the old custodians keep custody; once the new ones
  hold the item, they let go.
- **5e-7 — the emergency copy stays while it is needed (#185 findings 3 and 7; Ivan).** *Split in
  two (P5-60):* **5e-7a** *(done; the move follows availability changes and recomputed custody, never while
  waiting for an upgrade, and wakes custody waiting for room)* — the reason, the move to the cache,
  the counts and the golden;
  **5e-7b** *(done, P5-61: one semaphore of `max_fetches` places in `Keeper::keep`)* — emergency
  fetches within `max_fetches`. A new keep
  reason, `Emergency`, mandatory: D6's copy is kept under it, past the storage limit if need be, and
  moved to the cache once 5e-6's test passes for the item. `node.status`, `distlib status` and the
  Node page count it apart from custody. The `emergency/` tag prefix gets a golden test, with the
  other prefixes. Emergency copies take places in custody's `max_fetches` rather than a task each, so
  copies that cannot be evicted do not arrive in a burst. **Acceptance:** the emergency holder under
  storage pressure while the custodians are away keeps the copy; when they return and hold the item,
  it moves to the cache; many items due at once fetch at most `max_fetches` at a time.
- **5e-8 — eviction by copies elsewhere (#185 finding 3; Ivan).** *Done: `eviction_order` in
  `Storage`, copies counted from this node's view of availability, as the fetch queue counts them.*
  The cache goes first where the
  item has the most real copies on other online members, counted up to `replicas`; then least
  recently used; then by item id. The user's own file is the one exported, so a cached copy serves
  the group, as a source and as redundancy; past `replicas` copies an item is safe, and recency, which
  serving to others also sets, decides. It never refuses: an item short of copies goes last, not
  never. **Acceptance:** under pressure, of two cached items the one with more copies elsewhere goes
  first though used more recently; with equal copies, or both at `replicas` or more, the older one.
- **5e-9 — a voter away during a promotion catches up (#194 finding 1).** Core {A, B, C}; C offline
  while D is admitted and promoted; D later leads. C refuses D at the transport and in
  `may_speak_raft`, since its view predates D, and only the leader replicates the entry that would
  admit D: C is stuck for as long as D leads, enforcing a stale allowlist meanwhile. A node in the
  middle of its own promotion refuses a leader promoted after `Promotion::voters` was fixed, for the
  same reason. *Done (P5-63): the review's option 1.* A seated node that has accepted no `AppendEntries`
  for three election timeouts, and does not lead, fetches the signed log past what it applied from
  the voters it knows and those they point to, as a follower does, folds it onto a copy of its membership, and admits for Raft whoever that log
  names as core (`raft::catch_up`); the allowlist hook and `may_speak_raft` both read it, so
  authority stays with the signed log. **Acceptance:** the review's reproduction, restarting C on
  the same address: C catches up under leader D (`core_turnover.rs`).

### 5f — the review in #200, the availability topic's rejoin, and the flakes

#200 holds a review after #185: six findings in custody, five with a test that passes by asserting
today's behaviour. Each was checked against `ed88649` and holds. #200 is not merged, since its
tests would pin the defects on master: each fix below takes its test from #200, reversed, which
fails before the fix and passes after; #200 is closed once 5f-1 lands. Findings 4, 3 and 2 can
lose an item or count a lost copy as real, so v1 fixes them, most dangerous first; finding 1 is
fixed where it is cheap; finding 6 goes with 4, and finding 5 is carried forward. 5f-6, found on
#207's CI, goes first: it makes custody tests flake. 5f-7, the flakes, goes after 5f-2 and before
5f-3 (Ivan). The designs below are decided (Ivan). The end-of-phase mutation pass follows 5f, and 5g (P5-69).

- **5f-1 — the plan.** This section, its C-items and P5-64.
- **5f-2 — an item is its born files (#200 finding 4).** One `role: content` record more than the
  item was born with, which any member can write, and which a newer build may, makes the item not
  held on every node, since its files no longer fingerprint to its id (5e-4's check): nobody
  announces it, and a custodian lets go with no copy elsewhere. Whoever wrote the record also
  becomes one of the item's creators, which counts as an obliged copy (5e-6) and changes custody's
  slots. **Decided:** the born files are the content files one author wrote whose set fingerprints
  to the id; holding, custody's fetch, the item's size and its creators count those only. Other
  content records are shown and count for nothing. `Keeper::keep` rechecks holdings with the item
  as the catalogue has it after the fetch, not as it read it before (finding 6). **Acceptance:**
  #200's `one_more_content_record_makes_a_whole_item_not_held` and
  `one_stray_content_record_lets_a_custodian_go_without_copies_elsewhere`, reversed, the latter
  without its reindex.
  *Done (P5-66), as one file list per item instead (Ivan, on #212 and #213):* an item is the
  files it was added with, in one `item/{id}/files` key written with it and never after; a list
  that does not fingerprint to the id is left out, and no roles. An item is not one until its
  list is here; its one creator is the first list's author. Both tests reversed, against a
  stray list.
- **5f-3 — a creator's copy is checked like a custodian's (#200 finding 3).** 5e-6 counts a
  creator's copy as an obliged one, but probes and coverage judge custodians only: a creator's
  corrupted copy is never probed, still counts as real, and an old custodian can let go on it.
  **Decided:** probes and coverage judge `obliged_holders`, custodians and creators who are
  members, the set the hand-off counts. **Acceptance:** #200's
  `a_creator_that_announces_nothing_of_its_own_item_is_not_low`, reversed; a creator's copy is
  probed, and a failure of it stops it counting as real.
  *Done:* both pick from `obliged_holders`; `a_creator_that_announces_nothing_of_its_own_item_is_low`
  and `a_creators_copy_is_probed_and_a_failure_stops_it_counting_as_real`.
- **5f-4 — a pass clears only a failure of the same item (#200 finding 2).** A probe that passes on
  another item clears this node's failure of the custodian, so a copy still lost counts as real
  again. **Decided:** a pass clears the failure only when it names the item that passed. A later
  failure of another item no longer replaces the first (C61): a prober keeps one failure per
  custodian and item, a key format change with its golden test; the window still ages each out. **Acceptance:** #200's
  `a_pass_on_another_book_clears_the_failure_of_a_book_still_lost`, reversed; a failure of a second
  item leaves the first in place.
  *Done (P5-68):* a failure is `probe/{custodian}/{item_id}/{prober}`, and the prober's own failures
  are kept by custodian and item; `a_pass_on_another_book_leaves_the_failure_of_a_book_still_lost`
  covers both.
- **5f-5 — an item stays alone while its last holder is away (#200 finding 1).** A look lists an
  item only while exactly one online member holds it, so at a look with its holder offline the item
  drops out, and its delay starts over when the holder returns: a last copy on a node away once in
  every delay never gets its emergency copy. **Decided:** an item leaves the alone list only once it
  has a second copy; with its holder offline it keeps its time, and is due as soon as the holder
  is back if the delay has passed. That the time is forgotten at a restart is C60. **Acceptance:**
  #200's `a_last_holder_away_at_one_look_in_every_delay_never_makes_the_item_due` and
  `an_item_whose_last_holder_is_offline_is_not_listed_at_all`, reversed.
  *Done:* `sole_holders` lists an item only members away hold a real copy of as `Away`, and the
  emergency task keeps such an item's time without making it due;
  `a_last_holder_away_at_a_look_keeps_the_items_time` and
  `an_item_whose_last_holders_are_away_is_listed_as_away`.
- **5f-6 — the availability topic joins again while it has no neighbours (phase 4's D7(c)).**
  The topic is joined once, with the members reachable at that moment, and never again: D7(c)'s
  `join_peers` at zero neighbours was not built (4b-6, C30). A node restarted while its peers'
  addresses are stale, or not yet known, hears no heartbeat, so it cannot tell who holds what or
  who is online: on #207's macOS CI a restarted custodian never let go of a book whose new
  custodian held it, in `a_custodian_fetches_unasked_and_lets_go_only_with_copies_elsewhere`.
  *Done (P5-65).* `join_peers` alone cannot reach a peer one failed dial stranded (ground truth
  18), so while the topic has no neighbour the node dials every reachable member on the gossip ALPN
  itself, hands gossip each connection as an accepted one, which frees a stranded peer and sends
  what was queued for it, and then joins them: after 1 s, doubling to 60 s, and at once when an
  address is learned or the membership changes (`availability::rejoin`). **Acceptance:** two
  members whose log names no address strand each other at their first dials; once one learns the
  other's address, both hear each other's beats (`a_node_with_no_neighbour_joins_again_once_it_learns_an_address`).
- **5f-7 — flakes made diagnosable, then fixed (P5-67).** CI flakes on about every other PR, and a
  failed multi-node test's log cannot be read: custody, pins, the projection, API calls and
  consensus do not say which node logged a line, half the tests log nothing, and a wait that times
  out says what it wanted, not what it saw. Four PRs:
  - **5f-7a — every line names its node.** A root span `me=` on the projection's, custody's,
    consensus's and each API call's tasks, as the catalogue and availability have; debug lines
    for what holdings, a keep and the projection decide; CI logs those crates at debug; every
    test turns logging on.
  - **5f-7b — a wait says what it last saw.** One `until` helper the tests' waits share, whose
    timeout prints the last value it read. *Done:* `until` polls a read, `until_watched` a watch
    and `until_heard` an event bus; the waits left are on `ready()`, which has no value, and
    the few that measure a quiet group or count what is in flight.
  - **5f-7c — a node reads its own new item at once.** A node that has just added an item
    sometimes reads it back without its files or title (`keeping`, `pins` and `download` on
    macOS CI). *Done:* a collection pass deleted the value of an entry written while it ran:
    a pass asks the documents what they reference, then lists temporary tags, and iroh-docs
    drops a write's temporary tag as soon as its entry is in, so a write in between is in
    neither. Both flakes collect every second. Each of this node's writes now keeps its value's
    temporary tag until the next pass begins (`CatalogueProtection`); a synced value lost the
    same way is fetched again by the content sweep, but a node's own write has no other copy.
  - **5f-7d — the tests that fail again on a rerun**, each fixed, or kept with its reason recorded.
    *Done*, from CI since 2026-11-06, first attempts and master: `keeping` and `pins`'s
    eviction and `add_a_book` were 5f-7c's race; `probes`' re-check was C58 (P5-62);
    `pins`' let-go before #209, and `coverage`'s only on #212, which #213 replaced. Fixed here:
    `two_bookmarks_…` read a title the read model did not have yet, waiting only for the file
    list, which since P5-66 can arrive first, and `an_edited_title_…` read authors the same
    way. Two process tests timed out together on a macOS runner whose in-process tests took
    up to a minute: their bound is 60 s, as the in-process tests' is, and a failed process
    test prints every node's log.

### 5g — the reviews in #208, #210 and #211 (3 PRs)

Three reviews filed on 2026-11-16, each with tests that pass by asserting the defect, on the
branches `review/raft-nov15` and `review/raft-secondpass-nov16`. Each was checked against `ad0acdf`
and holds. Each fix takes its test, reversed, as 5f did (Ivan: fix all three, before the
end-of-phase mutation pass, which now follows 5g; P5-69).

- **5g-1 — a voter shutting down does not say it has retired (#208).** Shutdown empties the Raft
  seat while the endpoint still answers, and the retirement query reads an empty seat as retired:
  a voter removed while it shuts down is forgotten by the leader before it applies its removal,
  and comes back seated in the old core, which nobody replicates to. **Decided:** the seat is
  empty, taken or closed; shutdown closes it, and only an empty one, stood down because the log
  said so, answers retired. **Acceptance:** #208's two tests, reversed: the query says no during a
  shutdown; a voter removed then is still replicated to, and after a restart stands down.
  *Done:* `Seat`'s `SeatState`; `a_voter_shutting_down_does_not_say_it_has_retired` and
  `a_voter_removed_while_shutting_down_stands_down_after_a_restart`.
- **5g-2 — an old epoch's heartbeat is not taken for news (#210).** A beat is refused only when
  its epoch is the one held and its seq is no newer; any other epoch is taken as a restart, so a
  delayed beat of the epoch a restart replaced is taken again: it brings back holdings the member
  no longer has, or its goodbye takes the restarted member offline. **Decided:** a receiver
  remembers each member's last few epochs a restart replaced and refuses their beats. No wire
  change. A delayed old beat that reaches a receiver which restarted since is still taken; the
  member's next beat puts it right. **Acceptance:** #210's two tests, reversed.
  **Amended in review (Codex on #222, P5-70):** epochs are not ordered, so an old epoch heard
  after the current one replaces it, and refusing the current one for good would keep a live
  member out until a restart. A replaced epoch is refused only while the epoch that replaced it
  is still believed. **Amended again (Ivan, P5-71):** that left a stale beat's holdings shown for
  up to a TTL, and not being live yet, the wire's meaning may change: the epoch is the run's start
  time in microseconds, so epochs are ordered. A higher epoch is a restart, a lower one is refused
  while the held one is believed, and taken once it has run out (a clock that went back across a
  restart). The list of replaced epochs goes.
  *Done:* `epoch_now`, `Heard::is_superseded_by`;
  `a_lower_epoch_is_not_news_while_the_higher_one_is_believed`,
  `a_lower_epoch_is_taken_once_the_higher_one_has_run_out`,
  `the_epoch_is_the_start_time_in_microseconds`;
  `a_delayed_beat_of_a_replaced_epoch_brings_back_no_holdings`,
  `a_delayed_goodbye_of_a_replaced_epoch_keeps_the_member_online`.
- **5g-3 — a node run by a newer build before does not vote (#211).** The announcer only ever
  raises a member's `protocol_max`, so a node rolled back keeps the newer build's, and a raise the
  rolled-back voter cannot speak passes the leader's check. **Decided:** no rollback past a
  protocol this node has said it speaks: a node whose own record says more than its build stops as
  "upgrade required" (5d-8), at a start and when the record changes, so it never votes. Local, so
  not in the fold. **Acceptance:** #211's test, reversed: the rolled-back node does not vote, and
  says why.
  *Done* (P5-72): `UpgradeRequired::SpokeNewer { protocol, at }` (`"spoke_newer"` in
  `node.status`, the CLI and the UI); `follower::needs_an_upgrade` checks the group and this node's
  own record, at a start (no seat, the flag set from storage) and after each fetch while
  following. A voter already seated is checked at its next start: its own record can change under
  it only from storage older than its own announcement. `a_voter_a_newer_build_ran_before_takes_no_seat_on_restart`,
  `a_follower_whose_record_says_a_newer_protocol_stops`; 5d-7's
  `each_node_says_which_protocols_it_speaks_once_and_then_the_group_can_be_raised` now watches the
  raise on the founder, as its follower, saying 2 on this build, stops.

### 5h — the end-of-phase mutation pass (5 PRs)

`cargo mutants` on the phase's diff (`5790746..a9e3632`), in two stages and without
`mutate.py --every-block` (P5-73): all 1479 mutants on the fast lane (612 caught, 363 unviable,
4 timeouts, 312 missed), then the 316 not caught on the whole suite (227 caught, 2 of them by
a hang). **87 survive.** Run with debug info off and the mold linker (31 s a build rather than
92 s), three jobs at a time: more load makes cluster tests flaky, which kills mutants falsely.

**Accepted (30), with why:**

- *Stopping and printing* (9): `StorageUpkeep::stop`; `Drop` and `shutdown` of `Custody`,
  `Coverage` and `Probes`; `Debug` of `Probes` and of `Storage`'s inner state. They abort tasks
  or format; no test outlives them.
- *Equivalent* (12): `custody_of`'s `PARALLEL_FROM` (3) picks a sequential or parallel ranking
  of the same result; `Ranked::eq` (2) is never called, `Ord` decides; `UNHEARD_AFTER` (2) and
  `BYTES_PER_FETCH` (2) set only when the log is read and how big a page is; `shuffled`'s
  `last + 1` to `last * 1` is still a random order; `Emergency::run`'s first look earlier still
  waits the emergency delay; `suspects_among`'s `<` to `<=` decides only between two failures
  written at the same moment.
- *Log or unread signal only* (7): `publish_counts`' error log (4) and its change flag, which
  nothing waits on; `KeepError::is_unavailable`, a warn or an error; `Keeper::keep`'s fetched
  count, a debug line.
- *A re-check that narrows a race* (2): `RaftProtocol::accept`'s `Retired` and `ForgetMe`
  guards set to `true`. Each connection is checked before its next request too, and
  `forget_retired` has its own guards.

**The other 57 get tests**, one PR per group:

- **5h-1 — consensus access checks (7).** `may_query_retirement`, `forget_retired`'s joint
  configuration, `applied_voter`, the `ForgetMe` guard; what a seatless node serves from the
  memberlog. **Fixed on the way:** a learner's `ForgetMe` was answered and then the connection
  closed under the answer ("not a voter"), so an expelled node exited only on its retry, through
  the allowlist's refusal. The answer is now let arrive before the connection ends.
  *Done:* `Restored` (a node restored from given entries) in `network.rs`'s tests;
  `a_current_member_cannot_request_learner_cleanup_and_an_expelled_one_can` (now over the wire),
  `a_learner_is_not_forgotten_while_the_applied_configuration_is_joint`,
  `a_voter_whose_removal_is_not_yet_applied_still_speaks_raft`,
  `a_founding_peer_may_reach_a_node_not_yet_initialised`,
  `a_retired_voter_serves_what_it_applied_and_redirects_past_it`. A dead fallback in `accept`
  goes: with no Raft nothing past retirement is answered, whatever `applied_voter` says.
- **5h-2 — custody (13).** `enough_obliged_copies`, `copies_elsewhere`, `Emergency::copied`,
  `fetch_panicked`, `fetched`'s retry, `my_failures`, `latest_evidence`, `CustodyView::update`'s
  since, `Coverage::low`, `Custody::keeps`, the `releasing` pin count and the failed probe count
  in the status.
  *Done:* three survived on dead code, now gone: `enough_obliged_copies`' and
  `copies_elsewhere`'s `!= me` (this node is never online or a holder: its own beats are
  refused), and `CustodyView::update`'s demand check (under the view's one group, the same size
  is the same demand). `latest_failure_of` and `own_failures` are pure, beside
  `others_failures`. Tests: `the_latest_evidence_is_the_members_newest_failure_within_the_window`,
  `own_failures_are_those_this_node_recorded`, `the_low_are_exactly_the_members_whose_coverage_is_low`,
  `this_node_keeps_only_its_own_items_once_custody_is_computed`;
  `a_custodian_fails_once_its_bytes_are_not_the_book_until_its_next_pass` counts the failures and
  restarts the prober while its failure stands, so a pass after the start must still clear it;
  `a_custodian_does_not_let_go_while_the_copy_elsewhere_failed_a_probe` waits for the
  `releasing` count. **Moved to #185's findings 10–13:** `fetched`'s retry, `fetch_panicked`
  and `Emergency::copied`: finding 10 gives failed and panicked copies a retry, and its tests
  cover these.
- **5h-3 — consensus boundaries (17)**, in two PRs.
  - **5h-3a — what unit tests reach (11).** *Done:* `promotion()` stays open through a replayed
    Raft configuration from before it (`promotion_authority_…` replays one); `events_after` serves
    a page of exactly its byte budget whole; `a_view_is_announced_only_when_it_changes`
    (`CatchUp::set`, which the transport exception follows). The follow loop counts its answers
    through `Answers::answered`, `undecodable` and `refused`:
    `a_follower_is_refused_only_when_every_answer_was_a_refusal` (a redirect or a node with no
    group beside a refusal is not an expulsion), and nobody answering is not "upgrade required"
    (`Answers::progress`' `>` to `>=` stopped a follower whose core was all unreachable).
    **Accepted on a closer look (4):** `promotion()`'s `<` to `<=`, since a Raft configuration
    never shares its index with the core change that set `at`; `heard_from_leader` and the
    `AppendEntries` check that calls it, which only spare a seated voter reading the log ahead
    while its leader replicates; `follow`'s `!addresses_asked_for &&`, which only asks for the
    directory again at each poll.
  - **5h-3b — what needs a cluster (6).** `read_ahead`'s `>`, `core_of`,
    `fetch_log_from_core_nodes`' visited guard, `retire_learners`, `retired_and_expelled`,
    `propose_and_retry`'s stale guard.
    *Done:* `retired_and_expelled` reads only what is stored, so
    `only_an_expelled_former_voter_starts_retired_and_expelled` tests it alone. A cluster test
    of a restart between standing down and the cleanup was tried and is not deterministic: with
    the cleanup held back by a voter that is down, nothing can elect a leader again, and the
    restarted node's campaign deposed the one there was.
    `a_stale_protocol_support_is_refused_rather_than_sent_again`
    (rounds of a change racing a follower's proposal, until one is stale);
    `a_reading_gives_a_view_only_past_what_was_applied` (`Reading::view_past`). The visited
    guard's `member == me` goes: every candidate list leaves this node out. **Accepted (2):**
    `core_of` repeats the core each complete answer names, but for a read cut off between pages;
    `retire_learners`' `||` to `&&` only probes nodes it then cannot or need not remove: openraft
    refuses to remove a voter, and a re-promoted learner not yet seated is added again.
- **5h-4 — net (5).** `evict_once`, `is_remote_rejection` and its two arms, `serving_events`'
  closed connection.
  *Done:* `evict_once`'s survivor was `!handles.is_empty()`: a member whose connection
  survived one pass fell out of the table, so a later expulsion could not close it;
  `a_member_who_stays_admitted_keeps_its_connection` now expels it afterwards.
  `only_a_close_as_not_a_member_is_the_remotes_rejection`,
  `a_connection_closed_as_not_a_member_is_the_remotes_rejection`. `ConnectError`'s `Connect`
  arm goes: a refused dial is never the remote's. **Accepted (1):** `serving_events`'
  `ConnectionClosed` arm, which only frees the closed connection's entry; a reused id is
  overwritten when its new connection connects.
- **5h-5 — small unit tests (15).** API: `describe_core` moving a member (3), `named` with an
  empty name, `check_room_for`'s boundary; core: `GroupSettings::check`'s boundaries (2),
  `all_probe_failures`' key, `check_format`'s not-found guard; store: `replay`'s count; sync:
  `WritesSinceTheLastPass::release`; CLI: 4 printing checks.
  *Done:* `a_core_change_names_who_it_drops_and_moves_and_leaves_out_who_stays` (all four API
  description survivors); the storage test adds exactly up to the limit; the settings check
  passes a step at the cap and a settle at its limit;
  `the_probe_prefix_covers_every_probe_failure_and_nothing_else`;
  `an_unreadable_format_file_is_refused_rather_than_stamped_over`;
  `the_values_held_are_let_go_when_a_pass_begins`. The replay count goes: nothing read it,
  custody only waits for a change, so `replays` is a `watch` of `()`. The CLI's settings, pins
  and custodians become `settings_lines`, `pins_lines` and `custodians_line`, tested like
  `protocol_lines`; `founding_without_relays_does_not_wait_for_one` founds within
  `ONLINE_TIMEOUT`, which `||` for `&&` would wait out.

### 5i — #225 and #185's findings 10–13 (6 PRs)

Two reviews left after the mutation pass (P5-74). **#225:** start lets go of the files of any
item the catalogue lacks, so a catalogue lost, restored in part or written in an older layout
loses the only copy at the next collection. **#185's findings 10–13**, checked on a9e3632 and
again on master: 10 holds; 11 holds in part (5f-5 fixed the holder away; a new base list still
restarts the delay, and so does a restart, C60); 12 and 13 hold. **Decided** (Ivan, 2026-11-25):
an add's files are kept and reported rather than let go of; an export is checked before it is
handed over; scrubbing and repair stay in C41, a restart's lost delay in C60, and #225's other
findings become C62–C64.

- **5i-1 — start lets go of no add's files (#225).** `release_unpublished_own` takes an own item
  the catalogue lacks for an add that stopped before it was written; a missing catalogue entry
  does not say that. **Decided:** start lets go of nothing. It warns once, naming the own items
  the catalogue lacks, and `node.status`' storage counts them (`unpublished_own_items`). A
  stopped add costs its space until the operator acts (C64). **Acceptance:** #225's A: `docs/`
  moved away, a restart, a collection pass, a restart: the own tag and the blob stay; `docs/`
  back while stopped, `distlib download` gives the original bytes. B takes the same path.
  `keeping.rs`' interrupted add is reversed: its files stay, counted.
  **Amended (Ivan, P5-75):** iroh-docs keeps an entry's value in the blob store, so with `docs/`
  gone a collection takes the values too, and a lone node's restored `docs/` has entries without
  them (C65). A lone node's kept files export by hash as they were added; in a group, the
  catalogue comes back from a peer and the kept files serve the download.
  *Done:* `find_unpublished_own` and `UnpublishedOwn`; `node.status` re-reads only those items,
  so one the catalogue gains (an add made again: the same files are the same item) is not
  counted, and `distlib status` warns while any are. `starting_the_api_keeps_and_counts_the_files_of_an_add_that_stopped_before_it_was_written`,
  `a_lost_catalogue_does_not_cost_the_only_copy_of_an_items_files`,
  `a_lost_catalogue_comes_back_from_a_peer_and_the_kept_files_serve_it`.
- **5i-2 — an export is checked before it is handed over (C41, in part).** iroh-blobs exports by
  copying or cloning the store's file without reading it against its hash, so a flipped bit is
  downloaded with exit 0. What a node serves is safe: iroh-blobs checks each chunk against the
  outboard before sending it, and the receiver checks again. **Decided:** `Blobs::export` hashes
  the staged file with BLAKE3 (its `mmap` and `rayon` features, off the runtime) and renames it
  only if it matches; otherwise it removes it and fails, saying the stored copy is damaged.
  **Acceptance:** a byte of a kept blob's data file flipped: the download fails, says so, and
  leaves no file; an intact blob downloads as before.
  *Done:* `Blobs::export`, which `library.download` writes with, hashes the staged copy and fails
  with `NetError::DamagedCopy`; `an_export_of_a_damaged_copy_fails_and_leaves_no_file`, beside
  the intact export's tests. An emptied copy cloned on APFS (C41) fails the same way.
- **5i-3 — reachable sources first (#185 finding 13).** `ask_order` puts every known holder
  first, online or not, and the pin manager passes it both kinds. **Decided:** four tiers, each
  shuffled: online holders, the other online members, offline holders, the other offline
  members. **Acceptance:** an offline holder comes after an online member that does not hold the
  item.
  *Done:* `ask_order`'s tiers. `library.download` passes it only online holders, so an offline
  member is last there either way. `members_are_asked_online_first_and_holders_first_within_each`.
- **5i-4 — a failed copy is tried again on a clock (#185 finding 10).** A failed emergency copy
  is `Due` with no deadline, and a failed custody fetch is `Failed` until availability changes:
  in a quiet group neither is tried again. A panicked emergency copy stays `Copying` for good.
  **Decided:** both are tried again after a backoff, a minute doubling to an hour, whatever
  else happens; a change of availability still asks a failed custody fetch at once. An emergency
  copy's task id names its item, so a panic is a failure like any other. **Acceptance:** #185's:
  the first emergency attempt fails, source and destination are restored with no change of
  holdings or membership, and the copy is made within the backoff; the same for a custody fetch.
  Tests for the three mutants 5h left here: `Pins::fetched`' `wanted_again ||
  availability_changed`, `Pins::fetch_panicked`, `Emergency::copied`.
  *Done:* `retry::Failures`, counted through `Pin::{Queued, Fetching, Failed}` and
  `Alone::{Due, Copying, Failed}`; a panicked fetch or copy is found by its task and fails like
  any other (`KeepError::Panicked`). A failed emergency copy is due at its retry, not at the next
  look. Acceptance staged with the source's copy damaged, then put right:
  `a_failed_custody_fetch_is_tried_again_within_its_backoff`,
  `a_failed_emergency_copy_is_tried_again_within_its_backoff`. The mutants:
  `a_failed_fetch_waits_for_its_retry_unless_something_changed`, `a_fetch_is_found_by_its_task`
  (`fetch_panicked` is gone), and the emergency acceptance test for `Emergency::copied`.
- **5i-5 — an item keeps its time alone until it has its copies (#185 finding 11).** An item
  drops out of `AloneItems` whenever a look does not see it alone: while its holder's new base
  list is fetched (the holder's index entries are cleared meanwhile), or while its only copy has
  failed a probe. Seen alone again, it starts the delay again. **Decided:** an item keeps its
  time until a look sees it is not alone (two online real copies, or this node holds it) or it
  has left the catalogue. While no copy is known it is not due, there being nothing to fetch
  from; once one is, it is due at once if its time has run. **Acceptance:** the holder's new base
  list held back past a look, and its only copy failing a probe and then passing: neither starts
  the delay again.
- **5i-6 — the next member takes over a failing emergency copy (#185 finding 12).** The copy is
  the first member's after the custodians that is online and lacks it; if that member's copies
  keep failing (a full disk, no path to the source), every look picks it again. **Decided:**
  staggered: the member *k*-th in that order makes the copy once the item has been due for
  *k* × `EMERGENCY_FALLBACK_AFTER`, an hour. One more member starts per hour while the item stays
  alone, each working out its rank at its own look as now; the first copy kept ends it at
  everyone's next look. No wire or log change. **Acceptance:** #185's: the first member online
  but failing to keep: the second keeps a copy after the hour, and the third does not start.

---

## Carried forward — take or defer, explicitly

Every open C-number appears here once.

| # | Item | Phase 5 |
|---|---|---|
| **C1** | A full peer offer is O(N²) dials | **Unchanged** — and worth measuring at the thousands of members this phase targets. **#185 finding 8 counts it again** (each node offers every peer every ten minutes, and iroh-docs adds the peers it remembers to any list it is given, so a short list does not stay short): measure first, then a bounded, rotating fanout that still repairs a partition. |
| **C3** | The sweep reads the whole document every five seconds | **Deferred.** Still cost only. |
| **C6** | A field blinks out of the read model while its newest value is in flight | **Unchanged**, and probe failures blink too: a prober's newer failure is left out of `probe_failures()` until its content arrives, so for a moment a member can be listed with one prober fewer, or not red (seen on CI in 5c-5a, where the evidence named one prober of two). **Also on the adder's own node**: straight after `distlib add`, its `item` said `files 0` for a moment (the dry run before 5c-5b), and `catalogue().item()` read an item without its file on macOS CI (#179's `pins` flake). |
| **C7** | The read model is replayed in full at every start | **Deferred**, and relied on again: 5b-5's table fills from the replay. |
| **C8** | `added_by`, `created`, `modified_by` have nowhere to come from | **In part**: authorship, for the creator's copy and quotas — the read model's `file_authors`, shown as `added_by` (5b-1a, P5-8). `created` and `modified_by` stay open. |
| **C9** | `PENDING_EXPIRY` is one fixed count | **Closed by 5a-3a.** |
| **C13** | Gossip does not change sides when a node is promoted | **Deferred.** |
| **C15**–**C19** | Read-only admin; Windows file privacy and service; clearing a field; downloads as in-memory tasks | **Unchanged.** C15 is why expulsion with evidence is CLI and API only. |
| **C21** | Title sort | **Unchanged.** |
| **C22** | Consensus test peers cannot restart | **Closed by #132**: `Peer::restart`. |
| **C23**, **C24** | Release publishing; the expelled leader's lost answer | **Unchanged.** C23 is what lets 5a-3 change `GroupFounded` freely. |
| **C26** | Superseded base lists stay on the publisher's disk | **Closed by 5a-1.** |
| **C27** | Heartbeat traffic not measured above N = 50 | **No phase**, but thousands of members makes it pressing — and D10 now relies on heartbeats. C32 is its sharper form. |
| **C28** | The `item → members` map lives in memory | **No phase.** D6, D7 and D10 read it; they add nothing to it. |
| **C29**–**C33** | One wish comment per member; a failed gossip dial strands a peer; a write missing both paths; presence traffic above 1,000 members; iroh-docs' `AlreadySyncing` wedge | **Unchanged.** C32 matters at the new target. |

**New in phase 5:**

| # | Item | Where it goes |
|---|---|---|
| **C34** | **One `replicas` for the whole group** (D2) | **No phase.** Per-item values need a rule about who may ask the group to store an item more times. |
| **C35** | **A false `FileRecord::size` is logged, not acted on** (ground truth 4) | **No phase.** The fetching custodian knows the real size; correcting the catalogue's claim is a later decision. |
| **C36** | **Everyone's custody at once costs items × members** (ground truth 9) | **No phase** (P5-25): 5c no longer judges a member within its pledge, so nothing needs another member's whole assignment. Overflow (C38) would. |
| **C37** | **Settings are fixed at founding** (D2) | **No phase.** A `GroupSettingsChanged` with core approval, if a group needs it. |
| **C38** | **No overflow** — custody over a member's pledge leaves items a copy short (D9) | **No phase** (Ivan, P5-25). The load factor warns first, a node says when its custody is past its pledge, coverage shows a custodian that cannot keep up, and an item down to its last copy gets D6's emergency copy. |
| **C39** | **No `MemberLeaving` decommission** | **No phase.** A leaving member can lower its pledge in steps meanwhile. |
| **C40** | **Settling is per node, not a log entry** (D4) | **No phase.** Custody epochs, if lagging views ever cost real traffic. |
| **C30** | **One failed gossip dial strands that peer until it dials us** (phase 4's ground truth 18) | **Narrowed by 5f-6 (P5-65):** a node with no neighbour on the availability topic dials its members itself and hands gossip the connections, which frees them on every topic. A peer stranded while other neighbours remain still waits for it to dial us, or for the catalogue's backstop: **upstream**, n0-computer/iroh-gossip#159 or its like. |
| **C41** | **Nothing scrubs a node's own custody** for bitrot. *Seen by hand in 5c-5a*: a node whose custody file was deleted says nothing — `status` still counts it kept, it stays red, a restart does not notice, and `distlib download` of it fails with "could not write … to …", naming the destination rather than the missing blob. *And on macOS CI*: where the filesystem clones files (APFS; btrfs and XFS too), iroh-blobs exports by cloning the store's file, so a download of an emptied blob succeeds and writes an empty file, unchecked | **Phase 6**, with hardening: verify custody blobs now and then, re-fetch what is damaged, let a failed export name the store when the store is what failed, and check what an export wrote. **#185 finding 4 asks for it again; still deferred** (P5-57). **#225 again:** checking what an export wrote moves to 5i-2, done there; scrubbing and repair stay here (P5-74). |
| **C43** | **Purging the log loses its history and strands followers** — a snapshot keeps the folded state, not the signed events, and a follower behind the purge point is told `TooFarBehind` with no way back (P1-29); at thousands of members, 5,000 entries come in ordinary use | **Closed by 5c-0** (P5-23): never purged, and served a page at a time. |
| **C42** | **GC stops for good after a failed pass** — iroh-blobs' `run_gc` leaves its loop on the first error, logging it once, until the node restarts (found in 5a-1) | **5b-3b-1** *(detected, not fixed; P5-16)*: iroh-blobs 0.103.1 offers no way to run a pass, so the last pass is in `node.status` and an error is logged when passes stop. |
| **C44** | **The emergency copy fires only at the last live copy** (D6) | **A later version** (Ivan, P5-25). A group setting `emergency_at_copies`, 1 by default and at most `replicas − 1`, checked at founding: an item that has had at most that many live copies for `emergency_after_secs` gets emergency copies, one after another by the same first-after-the-custodians rule, the delay running from when it first fell to the threshold. It would make up for the overflow left out (C38). |
| **C45** | **A clock that steps back makes a node's own catalogue writes fail** — iroh-docs stamps each entry with the wall clock and refuses one older than the entry already at its key (`NewerEntryExists`), which reaches the caller as "the catalogue could not be written to", its cause dropped; seen once in a local `cargo test-all` on WSL, whose clock jumps back after a sleep (found in 5c-4) | **In part by #174**: the API names the cause (`…: A newer entry exists…`). Seen again on 2026-10-31 as two local flakes: WSL's two clock syncs stepped the clock back about 2.2 s every 30 s, and turning off `systemd-timesyncd` stopped it. **Phase 6**, with hardening: perhaps stamp no earlier than the entry being replaced. |
| **C46** | **Promotion cancelled before learner admission** — a promotion followed by demotion before replication starts can leave the node seated, with its allowlist held and persisted replay authority unexpired; restart resumes the same promotion | **Phase 6.** A retained learner normally catches up, but the never-added case needs an authenticated cancellation/recovery path (P5-33–P5-34). Timeout alone is not authority to reset acknowledged state. |
| **C47** | **A served copy is sometimes not counted as a use**, on Windows CI only: `a_node_that_downloads_a_file_serves_it_after_a_restart` waits a minute for alice's use and does not see it (3 failures; P5-39). Either alice's store stopped answering, or she served the book before her own read model had it, so the use matched no item | **Fixed in #169** (P5-39). The test's failure message, added to diagnose it, said on the next failure: alice's store answered, had no use, and held the item; bob held the book and carol did not, so alice had served it. Her read model did not have the item yet when she served it. A served blob's use is now kept by blob (`blob_last_used`) and joined to `item_files` when read. |
| **C48** | **Each leader change adds a log entry kept for ever** — openraft appends a `Blank` entry, about 100 bytes, when a new leader takes over, and the log is never purged (C43). A failed election writes none. A voter that returns while its leader backs off from it (up to 8 s, P5-52) can call an election before the leader reaches it, and the leader steps down on its higher term: one entry. A group changing leader once a minute adds about 50 MB a year, read by every node that joins (Ivan, on #188) | **Phase 6.** First count leader changes, by cause, in a long-running group; then, if it matters: leave `Blank` entries out of followers' pages and out of what is kept, since they carry no event, or avoid the term bump with PreVote, which openraft 0.9 lacks. |
| **C49** | **An add refused by the quota imports its files first** — `library.add` hashes each file into the blob store before it knows the requirement, since only new file keys count (D8), so a refused 1 GB add costs about two seconds and leaves an untagged blob for GC (found in 5c-5b4) | **Phase 6.** A check on the files' sizes before importing can refuse only what would be refused whatever is already held; the exact check stays after. |
| **C50** | **Paging through a role change is not tested** — replaying the log a page at a time publishes each intermediate membership to the role and allowlist watchers; promotion, demotion and re-admission across a page boundary, and a restart mid-replay, have no test (#154's review; no failure found) | **Phase 6.** |
| **C51** | **Pending custody fetches reserve no space** — admission adds only its own bytes, and a blob whose size has not arrived counts as nothing, so fetches admitted together can pass the storage limit (#185 finding 5) | **Phase 6.** Disk pressure, not a lost copy: a reservation per admitted fetch, released on completion, failure or cancellation. |
| **C52** | **Any availability change retries every failed custody fetch** — after the one-second gather, whether or not the changed member could serve the item, with no per-item backoff (#185 finding 6). The other side: while the group is quiet a failed fetch is never asked again, even once a holder's copy is put right (#200 finding 5) | **Phase 6.** Per-item backoff with jitter, reset early by a new holder of that item. **The quiet group's side is done in 5i-4:** a failed fetch is asked again on a backoff. |
| **C53** | **A small holdings change can resend a whole inventory** — a new base list after 384 changed ids, or ten quiet minutes after one, fetched by every observer from its publisher: about 3.2 MB per 100,000 items, to each (#185 finding 9) | **A later version**, measured first (with C27 and C32). |
| **C54** | **A removed voter that never returns stays a Raft learner** — asked every 2 s whether it has retired, and keeping its Raft-only transport exception (#194 finding 4) | **No phase.** Harmless; a way to clear it if one is ever needed. |
| **C55** | **Lookup by id only through n0** — `relay_mode = "default"` uses n0's pkarr and DNS, which publish the relay URL only; `custom` and `disabled` have no lookup at all, so self-hosted DNS or pkarr is not configurable (#194) | **A later version.** |
| **C56** | **No hostnames for core members** — `addrs` and the address in the log are socket addresses, so a dyndns name cannot be given (#194) | **A later version.** |
| **C57** | **No consensus test with a relay-addressed core member** — every consensus test runs on loopback with relays off (#194) | **Phase 6.** |
| **C58** | **A re-check at a recorded chunk passed once on macOS CI** — `check_now_probes_a_member_at_once_and_records_how_it_came_out`: the custodian's chunk 500 overwritten, the third member's failure there read by the founder, and `group.check` still said `passed` (run 43581593608, PR #196). Either the check did not go to that chunk, or the custodian served it as valid; the code points to the second. Not reproduced in 108 local Linux runs, 72 of them under load. Its sibling, `a_failure_another_member_recorded_is_rechecked_at_its_chunk`, timed out once on macOS (run 41791183932) | **Closed (P5-62).** Neither: the founder's own failure, cleared a moment before, was back, and the check went to its chunk. An iroh-docs 0.101 delete is an empty entry, and before taking a peer's entry iroh-docs asks for a newer one reading without empty entries: a peer's older copy replaced the delete (pinned in `distlib-sync/tests/iroh_docs.rs`). Every catalogue clear now writes a newer `null` value instead. |
| **C59** | **iroh-docs lets an older entry from a peer undo a delete** — the root of C58. `parents` in iroh-docs 0.101's `store/fs.rs`, which `insert_remote_entry` uses to look for a newer entry, calls `get_exact(…, false)`, leaving out the empty entries a delete writes. The one-line fix is `true` there. `distlib-sync/tests/iroh_docs.rs` reproduces it without a network | **Not filed upstream** (Ivan: an earlier issue there had no answer). Kept here for when it is worth filing; until then the catalogue clears with a newer value (P5-62), and that test fails once a fixed iroh-docs arrives. |
| **C60** | **A restart forgets how long an item has been alone** — the emergency delay is kept in memory, so a node restarted more often than the delay never finds an item due (#200 finding 1; pinned by its `a_restart_forgets_how_long_an_item_has_been_alone`) | **Phase 6.** Persist the time each item has been alone, or start it from the holdings' history. |
| **C61** | **A later failure of another item hides the first** — a prober keeps one failure per custodian (D12), so a failure of a second item replaces that of a first still lost, which counts as a real copy again (found planning 5f-4) | **Done in 5f-4** (Ivan): a failure is kept per custodian and item. |
| **C62** | **A good chunk clears a failure recorded at a bad one** — on a three-node check, a probe of another chunk of the same item cleared a custodian's failure while the recorded chunk stayed damaged (#225) | **Phase 6.** Re-check the recorded blob and chunk before clearing the evidence. |
| **C63** | **Two files with the same bytes keep one filename** — an audiobook's two tracks with identical bytes come back as one file, since a file is keyed by its hash (#225) | **Phase 6.** Let an item's files name one blob more than once. |
| **C64** | **An add stopped before it was written keeps its files** — since 5i-1, nothing at start can tell it from a catalogue lost or restored in part, so its own tags stay, counted in `node.status` (#225) | **Phase 6.** An explicit operator command, or durable evidence of the add's attempt, to let them go. |
| **C65** | **A lone node's lost catalogue loses its values too** — iroh-docs keeps entry values in the blob store, protected only while the documents name them, so with `docs/` gone the next collection takes them; a restored `docs/` then reads as entries without content (found in 5i-1). A group gets them back from a peer | **Phase 6**, with backups: back up `docs/` and the blobs together, or keep entry values under a tag of their own. |

---

## Testing and the lanes

**Fast lane:** rendezvous scoring's three properties, the weight step and `is_custodian` against
`custodians`, as property tests; the settle debounce under `tokio::time::pause`; the load factor;
`added_bytes` and `required_pledge`; tag transitions between own, custody and cache; the eviction
order; coverage from given holdings; the probe split; the failure key's write and clear; the
emergency decision on given counts.

**Slow lane:** GC keeping what it must (5a-1); five-node custody agreement and settling; pin, release
into the cache, throttling and eviction; the emergency copy; coverage; probes and their failures;
§9's acceptance.

Multi-node tests found their groups with short settings — a settle, an emergency delay and a probe
interval of seconds — so what is tested is the setting, not a test-only path. They **assert "within",
never an exact sequence**, as phase 4's do.

Every mechanism is mutation-checked. UI: Vitest first, Playwright for the happy paths only.

---

## Risks

**1. The first time distlib deletes anything.** Until now no blob has ever been removed: GC is off
(ground truth 7). Phase 5 needs it — to evict the cache and to collect what custody released — and a
GC pass deletes whatever is neither tagged nor protected. Today that would include the content of
every catalogue entry, without which the catalogue cannot be read. The fences:

- **5a-1 ships alone, first**, and its test runs a real GC pass over every kind of blob distlib
  writes. A new kind of blob in a later PR extends that test.
- **Only two things delete tags**: the pin manager its `custody/` ones, and the cache its `cache/`
  ones. `own/` tags and the documents' protection are never removed by anything in this phase.
- **`add_protected` errs towards keeping**: if the documents' hashes cannot be read, the pass is
  aborted (`ProtectOutcome::Abort`), not run without them.

**2. Terabytes moving to satisfy the protocol.** Ivan's concern, and the reason for D4–D7: custody
ignores liveness, membership changes are settled, weights move in steps, a release lands in the
cache, the emergency copy waits a day and only for a last copy, and every fetch is throttled. Each PR
in 5b is reviewed against D5's table: a new trigger of bulk traffic needs a row and a bound.

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

- the group is founded with `replicas = 2`, and a short settle and emergency delay;
- dave adds a book: it is his own, and one more member is its custodian and fetched it unasked;
- its custodian is stopped: nothing moves until the delay, then a third member fetches an
  emergency copy; the custodian back, nothing moves *(P5-32: dave's copy is then the last
  live one)*;
- erin, under-pledged, sees a red warning on an add, then is refused a much larger one, and is told
  the pledge needed both times;
- bob deletes a pinned file by hand: probes from two members fail, he is red on the Node page and in
  the suspects view, and `distlib expel bob --with-evidence` proposes with the evidence as its reason
  *(P5-32: the custodian, whoever it is, moves the file aside and back, and the red clears)*.
