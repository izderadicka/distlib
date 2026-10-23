# Phase 5 — Custodianship & quotas

Sequencing plan for §9's Phase 5. Written before any code, and revised in place if a PR proves part
of it wrong — an entry that turned out to be mistaken is more useful corrected than deleted.

---

## Context

**Status: agreed in discussion with Ivan, in progress — 5a done: 5a-1 (#134), 5a-2 (#135),
5a-3a (#136), 5a-3b (#137); 5b-1a (#138), 5b-1b (#139), 5b-2a (#141), 5b-2b (#142), 5b-2c (#143),
5b-3a (#144), 5b-3b (#145 to #147), 5b-3c (#148), 5b-4 (#149), 5b-5a (#150), 5b-5b (#151), 5b-6 (#152),
5b-7.** Four rounds of review on #133 shaped it;
what they decided is listed below, each with the facts it was decided on, and what is still a guess
is under [Open questions](#open-questions).

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
- **Two budgets** (round 2): the pledge, in the log, bounds custody; a local storage limit, larger
  than the pledge, bounds everything, and pressure on it evicts the cache.
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
  - **The pledge** is in the log: the space a member offers the group, which bounds its custody and
    which the quota rule measures (D8).
  - **The storage limit** is local, `[storage] limit_bytes` in the node's config, never shared: the
    disk space distlib may use on this machine for all three reasons together. It must be larger than
    the pledge — the difference is room for own items and the cache.
  - **Pressure** is own + custody + cache above the limit, and it evicts the cache, least recently
    used first, until the total fits.
  - **Mandatory alone over the limit** evicts the whole cache and is then reported in `node.status`
    and on the Node page; `library.add` refuses an add that would make it worse, and
    `group.pledge_set` refuses a pledge that is not below the limit.
  - **Custody over the pledge** — possible, since custody follows weight and not pledge exactly (D9)
    — pins the highest-scoring assigned items up to the pledge and leaves the rest, which then show
    a copy short. *(5b-7, P5-22: each that still fits, so one too large is passed over; only new
    fetches stop, what is held going by the usual safe release.)*
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
cache/<item>/<blob>      set by library.download and by an emergency copy; evicted under pressure
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

Twenty PRs — fourteen planned, 5a-3 and 5b-1 each split in two, 5b-2 in three, and 5c-0 and 5b-7 added in review. Each ends compiling,
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

- **5b-7 — custody over the pledge (D9).** *(done; delta P5-22)* Added in review of 5b-3a (P5-15).
  Custody cuts what this node keeps at its pledge: the assigned items by this node's score, highest
  first, each kept while it fits in what is left. What is over is not fetched, and is reported in
  `node.status` (`custody.over_pledge`), `distlib status` and the Node page.
  **Acceptance:** a node pledging room for one of two books fetches the one it scores higher, says
  the other is over its pledge, and never tags it.

### 5c — checking custody (6 PRs)

- **5c-0 — the whole log, kept (C43).** The log is never purged: `max_in_snapshot_log_to_keep` at
  `u64::MAX`, so openraft still snapshots but deletes nothing. Every signed event, approval and
  refusal stays on the core nodes as the group's audit history, and a follower can always read
  from index 0, so P1-29's `TooFarBehind` can no longer happen. Decided with Ivan after measuring:
  about 600 bytes per event on disk, so a busy group of 1,000 members (about 3,800 entries a year)
  holds about 230 MB after a hundred years; a joiner verifies the whole log once.
  **Acceptance:** a member joining after the leader has taken a snapshot reads every entry from the
  founding one onward, settings included, and the log's first index stays 1; the measurement
  repeated at 100,000 entries — size on disk, and how long a joiner takes to catch up.

- **5c-1 — coverage from heartbeats (D10).** Per member, of the items this node holds and that member
  should hold too, the share it announces; shown in the members list. *Should hold* is within that
  member's pledge (5b-7, P5-22): an item over it is not blamed.
  **Acceptance:** a custodian that holds its items shows full coverage; one whose pin manager is
  stopped falls behind once its assignments are older than `max_settle_secs`; one still within that
  time is not blamed.

- **5c-2 — probes (D11), and failures in the catalogue (D12).** Random sampling with the suspect
  share, the repair hook, failure keys written and cleared; off when the group says so. Only items
  within the probed member's pledge are probed (5b-7, P5-22).
  **Acceptance:** a custodian holding the item passes and nothing is written; one whose blob was
  deleted underneath it fails, its key appears on every node, and the item gets its emergency copy if
  that was its last real one; its next pass clears the key; an unreachable custodian is skipped; a
  group founded with `probes` off makes no probes.

- **5c-3 — the suspects view and expulsion with evidence (D12).** Red, the suspects view, check now,
  and `distlib expel --with-evidence`.
  **Acceptance:** failures from two probers turn a member red on every node; one prober's failure
  alone does not; `distlib expel --with-evidence` proposes with a reason that names the failures and
  fits `MAX_REASON`. Vitest per view.

- **5c-4 — §9's acceptance, as one test, reworded (P5-1).** Five nodes, `replicas = 3`: an expelled
  custodian's items are fetched by the next-ranked member; an item down to one holder gets an
  emergency copy after the delay; a node whose pinned blob is deleted fails probes from two peers and
  is red everywhere.

- **5c-5 — by-hand check.** [`manual-check.md`](../manual-check.md) grows within its one story, and
  what it finds is fixed before the phase is called done.

---

## Carried forward — take or defer, explicitly

Every open C-number appears here once.

| # | Item | Phase 5 |
|---|---|---|
| **C1** | A full peer offer is O(N²) dials | **Unchanged** — and worth measuring at the thousands of members this phase targets. |
| **C3** | The sweep reads the whole document every five seconds | **Deferred.** Still cost only. |
| **C6** | A field blinks out of the read model while its newest value is in flight | **Unchanged.** |
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
| **C36** | **Everyone's custody at once costs items × members** (ground truth 9) | **Comes due in 5c** (P5-22): coverage and probes judge a member within its pledge, which takes that member's whole assignment in its score order — one pass over every item per member checked. Overflow (C38) would need it too. |
| **C37** | **Settings are fixed at founding** (D2) | **No phase.** A `GroupSettingsChanged` with core approval, if a group needs it. |
| **C38** | **No overflow** — custody over a member's pledge leaves items a copy short (D9) | **No phase.** The load factor warns first, and since 5b-7 a node says what is over its own pledge (P5-22). |
| **C39** | **No `MemberLeaving` decommission** | **No phase.** A leaving member can lower its pledge in steps meanwhile. |
| **C40** | **Settling is per node, not a log entry** (D4) | **No phase.** Custody epochs, if lagging views ever cost real traffic. |
| **C41** | **Nothing scrubs a node's own custody** for bitrot | **Phase 6**, with hardening: verify custody blobs now and then, re-fetch what is damaged. |
| **C43** | **Purging the log loses its history and strands followers** — a snapshot keeps the folded state, not the signed events, and a follower behind the purge point is told `TooFarBehind` with no way back (P1-29); at thousands of members, 5,000 entries come in ordinary use | **5c-0**: never purge. |
| **C42** | **GC stops for good after a failed pass** — iroh-blobs' `run_gc` leaves its loop on the first error, logging it once, until the node restarts (found in 5a-1) | **5b-3b-1** *(detected, not fixed; P5-16)*: iroh-blobs 0.103.1 offers no way to run a pass, so the last pass is in `node.status` and an error is logged when passes stop. |

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
- bob, its custodian, is stopped: nothing moves; dave stopped too, after the delay a third member
  fetches an emergency copy; both back, nothing moves;
- erin, under-pledged, sees a red warning on an add, then is refused a much larger one, and is told
  the pledge needed both times;
- bob deletes a pinned file by hand: probes from two members fail, he is red on the Node page and in
  the suspects view, and `distlib expel bob --with-evidence` proposes with the evidence as its reason.
