# Phase 4 — Availability & community metadata

Sequencing plan for §9's Phase 4. Written before any code, and revised in place if a PR proves part
of it wrong — an entry that turned out to be mistaken is more useful corrected than deleted.

---

## Context

**Status: planned, not started.**

Phase 3 is complete and merged: everything phase 2 built is reachable from a page, downloads stream
their progress live, and the by-hand check has been run as one story from founding a group to losing
its leader. What it found was fixed in #78–#81 (P3-26 to P3-28).

Phase 4 is §9's "Availability + community metadata":

- heartbeats over iroh-gossip carrying an exact account of what each member holds, and an in-memory
  TTL availability index;
- availability badges in the library, pushed over SSE;
- ratings, reviews and bookmarks — written, projected and shown;
- wishes, end to end.

§9's acceptance is the target: *take a providing node offline → badge flips within TTL without any
replicated-state churn; two members rate the same item concurrently → both ratings visible
everywhere.* The second half is close to free — §5.3's keys were designed so that two members never
write the same key. **The first half is the phase's real work**, and its cost is stated once, under
[Risk](#risk-stated-once).

**The scale target is Ivan's, set at the review of this plan:** a group of about 100k items must be
easy, and one of 10M must work without trouble — and some nodes will hold a large share of the
library. Every size in this document is checked against it.

The phase also takes, explicitly:

- **C5** — a late joiner never learns an address it missed; the heartbeat carries it.
- **C14** — `sync.status`, whose material is the three events the pump discards.
- **C25** — on Windows, two followers whose introducer has gone meet only through the catalogue's
  timed re-offer.
- **P3-27's promise** — replace that thirty-second re-offer of every peer with "offer whoever newly
  appears".
- **"Held here"** in the library, which the phase-3 by-hand check asked for; "exported" was asked
  for too and is not taken, because an export directory is not fixed.
- **`lang`** in full-text search. Filters and sorting are a later usability phase.

**This document is sequencing** — what gets built, in what order, in which PR, and what each PR has
to demonstrate. Design deviations from [`distlib-plan.md`](../distlib-plan.md) still go into
[`plan-deltas.md`](../plan-deltas.md), in the PR that causes them. There must not be two sources of
truth for a deviation.

---

## Ground truth established before planning

Verified against the tree and against the vendored crate sources in `~/.cargo/registry` — iroh 1.0.3,
iroh-gossip 0.101.0, iroh-docs 0.101.0, iroh-blobs 0.103.0 — not against docs.rs prose. One entry
below exists because the prose and the code disagree.

1. **Nothing in this workspace speaks on a timer, on principle.**
   [`gossip.rs:122-171`](../../crates/distlib-consensus/src/gossip.rs) ("Three triggers, no timer"),
   [`follower.rs:307-313`](../../crates/distlib-consensus/src/raft/follower.rs),
   [`directory.rs:80-85`](../../crates/distlib-net/src/directory.rs) and
   [`signed_addr.rs:43-56`](../../crates/distlib-core/src/addr/signed_addr.rs) all say it, the last
   most bluntly: "Add a timestamp, a nonce, a counter … and the group talks for as long as it is up.
   That was measured, not feared." **A heartbeat is exactly that, on purpose.** It is the first
   traffic in distlib that never stops, so it gets a delta of its own and the phase's one risk
   section.

2. **Gossip drops repeats, so every heartbeat has to differ, and then it is flooded.** A message's id
   is `blake3(content)`, remembered for 90 s (`iroh-gossip plumtree.rs:31-37,326`), so a heartbeat
   identical to the last one would never be delivered — which is what its `seq` is for. A message
   that differs is relayed to the whole topic. `broadcast_neighbors` is one hop, neither relayed nor
   deduplicated, and is not what this wants.

3. **The gossip size limit is ours to set, but it is a wire-compatibility setting, not a tuning
   knob.** It defaults to 4096 B (`proto.rs:69`) and is set with `Gossip::builder().max_message_size()`
   (`net.rs:154`); [`runtime.rs:103`](../../crates/distlib/src/runtime.rs) takes the default, and that
   one `Gossip` carries every topic in the process — membership, addresses and the catalogue's live
   sync. Two properties decide what to do with it:
   - **A receiver rejects any frame above its own limit** (`net/util.rs:365`), and the disconnect that
     follows is delivered to *every* topic on the connection (`proto/state.rs:139`). Raising it means
     raising it on every node together; one node behind breaks consensus and docs gossip with the
     node ahead, not just heartbeats.
   - **The sender checks only when the frame is written** (`net/util.rs:383-386`), after
     `broadcast()` has already returned `Ok`. An oversize heartbeat is not an error anyone sees; it is
     a dropped connection.

   **It is a message limit, not a packet size.** Gossip frames are length-prefixed on QUIC streams
   (`RecvStream`, `net/util.rs:65,162`), which split a message across as many packets as it needs —
   "fits one UDP datagram" would be about 1.2 KB, smaller than today's limit.

   **So the limit is raised to 16 KiB now, as a protocol constant, while nothing has been released**
   (C23) and every node is a fresh build — and the heartbeat's size is still guarded when it is
   encoded (D2). After a release, a further raise has to reach every receiver before any sender uses
   it.

   **Every `Gossip` in the workspace has to take the same constant.** `Gossip::builder()` is called in
   [`runtime.rs:103`](../../crates/distlib/src/runtime.rs) and six times across four test files —
   [`rpc.rs:117,254`](../../crates/distlib-api/tests/rpc.rs),
   [`common/mod.rs:99`](../../crates/distlib-consensus/tests/common/mod.rs),
   [`memberlog.rs:64,306`](../../crates/distlib-consensus/tests/memberlog.rs) and
   [`converge.rs:76`](../../crates/distlib-sync/tests/converge.rs). A test node left on the default
   would reject a 16 KiB beat from a node built the real way and drop the connection — a test failure
   that looks like a network fault.

4. **A second topic on the existing `Gossip` is cheap and cannot stall anything.** Subscribing is
   `subscribe(topic, bootstrap)` (`iroh-gossip api.rs:157-167`). Unlike an iroh-docs subscriber, a
   gossip subscriber that falls behind holds nothing up. HyParView's defaults — an active view of 5, a
   passive view of 30, a shuffle every 60 s (`hyparview.rs:202-216`) — are what a heartbeat topic
   inherits.

5. **`SignedAddress` cannot carry holdings, and should not be made to.** Its signature covers
   `(member, addr, applied)` under `b"distlib.address.v1"`
   ([`signed_addr.rs:39,143-146`](../../crates/distlib-core/src/addr/signed_addr.rs)), and the reason
   it carries no clock is ground truth 1. `MemberId` is a 32-byte key that deliberately serialises as
   its 64-character hex string ([`id.rs:89-98`](../../crates/distlib-core/src/id.rs)), so postcard
   spends 65 B on it; the `SignedAddress` keeps that encoding, because its signature covers it, and
   the heartbeat's own fields use raw bytes. `Directory::learn`
   ([`directory.rs:132-203`](../../crates/distlib-net/src/directory.rs)) answers `Ok(false)` for an
   unchanged statement and keeps no TTL. **So the heartbeat is its own signed type, with the
   `SignedAddress` inside it** — which is also how it closes C5.

6. **Ordered providers come for free.** Any `Vec<EndpointId>` is a `ContentDiscovery` that is tried
   in order (`iroh-blobs downloader.rs:562-572`); only our own
   [`Blobs::fetch_with_progress`](../../crates/distlib-net/src/blobs.rs) shuffles, at `blobs.rs:157`.
   A provider that lacks the blob fails and the next one is tried. **So a download can put the online
   holders first** without any new discovery code — and a holder whose latest additions have not
   reached us yet is still tried, just later.

7. **Blob GC is off, deletion is crate-private, and awaiting `add_bytes` directly pins the blob for
   ever.**
   - `gc: None` (`iroh-blobs store/fs/options.rs:124`), and delete is `pub(crate)`
     (`api/blobs.rs:152-171`). Media added by `library.add` carries an auto tag, catalogue values
     only a temp tag, downloads none.
   - **`store.add_bytes(x).await` creates a new, uniquely named tag on every call**
     (`api/blobs.rs:677-685` → `with_tag`, `:717-726`), and a tagged blob is never collected. Publish
     something a thousand times that way and a thousand copies are kept.
     **`with_named_tag(name)`** (`:707-715`) instead points one fixed name at the new blob, which
     leaves the previous one untagged — collectable as soon as there is a collector.
   - `iroh_blobs::get::request::get_blob(conn, hash)` (`get/request.rs:112`) fetches a blob **into
     memory, verified, without storing it**.

8. **Community keys are invisible to the pipeline today.** `Key::parse`
   ([`catalogue.rs:212-229`](../../crates/distlib-core/src/catalogue.rs)) accepts `item/…` only — by
   design, its doc names phase 4's ratings as the reason — so the pump's `note`
   ([`changes.rs:177-182`](../../crates/distlib-sync/src/changes.rs)) and the replay's `item_ids`
   ([`catalogue.rs:444-459`](../../crates/distlib-sync/src/catalogue.rs)) skip them.

9. **A forged community entry would win every read we do today.** iroh-docs keeps one record per
   (namespace, author, key) and checks only that the entry is signed by its own author. Every read in
   [`catalogue.rs`](../../crates/distlib-sync/src/catalogue.rs) is an *unfiltered*
   `single_latest_per_key`, which picks the newest record for a key across all authors — so a newer
   `rating/X/bob` written by carol would hide bob's real one. **The crate disagrees with itself about
   the obvious fix:** its doc says a `single_latest_per_key` query applies the author filter *after*
   grouping (`iroh-docs store.rs:278-279`), and the fs store applies it *before*
   (`store/fs/query.rs:108-117`). A **flat** `Query::author(a).key_exact(k)` is unambiguous either
   way, and since a node's author key is its own key
   ([`catalogue.rs:194`](../../crates/distlib-sync/src/catalogue.rs)), `a` is the member id the key
   names, byte for byte.

10. **Writing a key prunes that author's longer keys beneath it** (`iroh-docs ranger.rs:551-583`,
    with the production bounds at `store/fs.rs:871-887`). Writing `bookmark/X/m` would silently delete
    every `bookmark/X/m/…` that `m` had written. **Community keys must never be a prefix of another
    key by the same author**, which holds as long as every key ends in a fixed-length id and nothing
    writes the bare prefix. Deleting is `Doc::del(author, prefix)` (`iroh-docs api.rs:356`), which
    clears only that author's entries.

11. **The pump already sees what C14 needs, and throws it away — and must stay trivial.**
    `NeighborUp`, `NeighborDown` and `SyncFinished(peer)` reach it and answer `false`
    ([`changes.rs:158-162`](../../crates/distlib-sync/src/changes.rs)). A second docs subscription to
    get them is phase 3's ground truth 3 over again: it stalls the live actor
    (`iroh-docs live.rs:950-967`). So the pump records them, synchronously, with no `.await`.

12. **The thirty-second re-offer has a second job nobody asked it to do.** Every sync round ends with
    `PendingContentReady` (`iroh-docs live.rs:600-620`), and that is the only thing that makes the
    read model re-read an item whose bytes the content sweep fetched behind the engine's back
    ([`projection.rs:37-46`](../../crates/distlib-store/src/projection.rs)). **Removing the timer
    without giving the sweep its own nudge would leave repaired items half-projected** until something
    else touched them — so both go in the same PR.

13. **A schema change today stops `distlib run`.** The read model has no version: no
    `user_version`, every table `IF NOT EXISTS`, and a failed upsert only warns. A changed tantivy
    schema makes `SearchIndex::open` fail, which is fatal
    ([`runtime.rs:153-155`](../../crates/distlib/src/runtime.rs)). The read model is replayed in full
    at every start anyway (C7), so rebuilding it on a mismatch costs nothing extra. `lang` is an SQL
    column and an API field, but not an index field ([`index.rs:84-95`](../../crates/distlib-store/src/index.rs)).

14. **The search index holds one document per item, replaced whole.** The upsert deletes by `id` and
    adds the document back ([`index.rs:337-362`](../../crates/distlib-store/src/index.rs)). A review is
    therefore folded into its item's document, and a changed review means re-projecting the item; a
    separate document per review would need a kind field, its own deletes, and would turn one item
    into several hits. Bookmarks are searched in SQL instead (D13) and leave the index alone.

15. **Nothing tracks what this node holds.** `Blobs::has` means `Complete`
    ([`blobs.rs:86-97`](../../crates/distlib-net/src/blobs.rs)); `item_files` has no index on `blob`
    ([`schema.rs:53-65`](../../crates/distlib-store/src/schema.rs)); `library.download` offers every
    member but itself, in no order ([`methods.rs:869-876`](../../crates/distlib-api/src/methods.rs));
    and `library.item` says outright "No ratings or availability" (`methods.rs:454-458`).

16. **The API's seams.** Dispatch is a string match
    ([`methods.rs:84-106`](../../crates/distlib-api/src/methods.rs)); a hit's fields come from
    `summary()` (`:1205-1216`); events are an enum in
    [`event.rs:25-71`](../../crates/distlib-core/src/event.rs), and the UI's `NodeEvent` already
    passes unknown names through; routes go through `router.svelte.ts`. The config has no durations
    yet — sizes are plain numbers, like `max_upload_bytes`
    ([`config.rs:99`](../../crates/distlib-core/src/config.rs)) — so the interval is
    `beat_interval_secs`.

17. **CI has never shown us C25's log.** On Windows the test passes, slowly, and nextest hides the
    output of a passing test. **Corrected in 4.0:** the follower harness in
    [`tests/catalogue.rs`](../../crates/distlib/tests/catalogue.rs) already initialises tracing from
    `RUST_LOG`; what was missing was a log line at each of our own steps, and a way to tell three
    in-process nodes apart in them.


18. **C25 is not about Windows, and its cause is one failed dial (found in 4.0).**
    - **iroh-gossip strands a peer after one failed dial.** A message to a peer with no connection
      leaves it `Pending { queue }`, and a dial starts only when that queue is empty
      (`iroh-gossip net.rs:695-700`). When the dial fails, hyparview drops the peer from its passive
      view and emits no `DisconnectPeer` (`proto/hyparview.rs:346-354`), which is the only thing that
      removes the entry (`net.rs:732-736`). So the queue is never empty again, and every later message
      to that peer — a `Join` included — waits for ever. It ends only if the other side dials *us*.
      Upstream knows the area (open PRs n0-computer/iroh-gossip#159, and #146/#147 closed unmerged);
      0.101.0 is the latest release.
    - **The peer state is per peer, not per topic.** One `Gossip` serves every topic (ground truth 3),
      so a dial that fails on the membership topic strands that peer for the catalogue document too.
    - **And we cause the failed dial.** `join_topic`
      ([`node.rs:1377-1382`](../../crates/distlib-consensus/src/node.rs)) bootstraps the membership
      topic with *every* member id. A follower that has just started knows only the core group's
      addresses, so its dial to every other follower fails — and two followers that start together
      strand each other. `sync_with` already filters to members with an address, for the same reason
      in different words; the membership topic does not.
    - **What the test showed.** Two followers are never gossip neighbours of each other, on any
      platform. The C25 test passes in seconds only because their first offers to each other happen
      to land just after bob's write; delayed by four seconds, the same test takes 36 s on Linux. On
      Windows in the full suite it took 41 s, and alone, with logging, 14 s — a race the slower,
      busier runner loses, not a Windows fault.
---

## The structural decision: availability is never replicated

**What "availability" means here:** whether an item's content can be fetched right now — which
members are online, and which items each of them holds locally. §5.6 already decides where it lives
("Never in replicated state. Reachability is liveness: per-observer, minute-to-minute"), and the
decision is restated because it is the rule this phase's PRs are reviewed against:

**Nothing about availability is written to the document, the read model or the Raft log.** Every
node works it out in memory from the heartbeats it hears, and forgets it when it stops. §9's
acceptance asserts exactly this — across the offline flip, the document's entry count and the log's
position do not move — and the test does it with counters, not by inspection.

**Where it lives:**

- **The pure parts in `distlib-core::availability`** — the wire format, signing, the holdings list's
  encoding, the size guard. Same reasoning, and same place, as `SignedAddress`.
- **The service in `distlib-sync::availability`** — the topic, the beat, the receiving side, the TTL
  index, the held set and the base list. `distlib-sync` already depends on consensus, net, gossip
  and blobs, and its offer loop is the main consumer of "somebody appeared".
- **Consensus gains one getter**, `own_address()`: a watch of the `SignedAddress` that
  `announce_address` already signs.
- **Rejected: `distlib-net`, where §8 puts it.** It sits below consensus, so the group id, the
  membership and this node's own address would all have to be handed down to it from above.

---

## Design decisions taken up front

**D1 — The heartbeat is its own signed type, carrying the signed address.**

```text
SignedHeartbeat
  address        SignedAddress     its signer is the heartbeat's signer
  epoch          u64               random per start
  seq            u64               +1 per beat
  interval_secs  u32               the sender's current interval
  holdings
    count        u64               items held now
    base         Option<Hash>      the last published full list, a blob (D3, D5)
    added        Vec<[u8; 32]>     held now, not in base
    removed      Vec<[u8; 32]>     in base, no longer held
  leaving        bool
  signature      over b"distlib.heartbeat.v1" || group_id || postcard(the above)
```

The group id is signed but not sent, so a heartbeat cannot be replayed into another group. There is
no separate member field — the address names the member, and the receiver checks the two signatures
agree. Item ids travel as raw bytes, not as the hex strings they are elsewhere. Apart from the delta,
the size is fixed: about 720 B with an address of up to about twenty direct addresses.

**D2 — The frame is 16 KiB, and the size is guarded when the heartbeat is encoded.**
One constant, `GOSSIP_MAX_MESSAGE`, set on every `Gossip` the workspace builds (ground truth 3). The
heartbeat's payload is capped a little under it, and the delta — `added` and `removed` together — at
12 KiB, about 380 ids. When the next change would overflow the delta, the node publishes a new base
first (D5). If a heartbeat is still over the cap — an absurd address list — the node logs an error and
sends nothing. **An oversize frame is never sent**, because the failure it causes is a dropped
connection on every topic.

**D3 — Holdings are an exact base list plus a cumulative delta — Ivan's call, at the review of this
plan.** It is what §5.6 sketches — a `holds_manifest_hash` in the heartbeat, "announced only when
changed" — with the change itself carried alongside.

- **The base** is the sorted list of every item the node held when it was published: a format byte,
  then raw 32-byte ids. Sorted only so that one set always makes the same bytes, and so the same
  hash — nothing ever searches it.
- **The delta is counted from the base, not from the previous beat.** Every heartbeat is complete on
  its own, so a receiver that missed one — gossip can lose a message, a node can be down for a minute
  — loses nothing: the next beat says the same and more. That is also why no Merkle tree or set hash
  is needed: the base's own hash says exactly which list the delta applies to.
- **A receiver** that already holds the member's `base` applies `added` and `removed`; one that does
  not fetches the base once (D5), then applies them. The one check is free:
  `len(base) + len(added) − len(removed) == count`. A mismatch is logged and the base fetched again.
  *(As built: not fetched again — the hash fixes the bytes; see 4b-4.)*
- **The receiver keeps one inverted map, `item → members holding it`,** with members as `u16` indexes
  into the current membership. That map is what the library page asks ("n online") and what a later
  "available now" filter would need. A member's base changing, or the member going away, is one pass
  over the map. *(As built: members keyed by `MemberId`, and "going away" is leaving the group, not
  going offline; see 4b-4.)*

What a base costs on the wire, once per publish and receiver:

| Held by the member | Base list |
|---|---|
| 10k | 320 KB |
| 100k | 3.2 MB |
| 1M | 32 MB |

And what the map costs each receiver in memory — about 60–100 B per distinct item held by anyone, plus
two bytes per further copy:

| Distinct items held in the group | Receiver's map |
|---|---|
| 100k | ~10 MB |
| 1M | ~100 MB |
| 10M | ~1 GB |

Up to the "100k easily" target this is nothing. **At the 10M end the map belongs in SQLite** — a
local table, still never replicated — which is C28, Ivan's call, taken when a group gets there.

Rejected at review: **a Bloom filter** in place of the list. It buys one thing, size — about 27 times
smaller — at the price of false positives, sizing arithmetic and a `providers` count that can be
wrong, and the size only matters at the 10M end. The base's format byte keeps it, or 8-byte id
prefixes, open as a compatible change if that end is ever measured and found wanting.

**D4 — Cadence, TTL and budget.**

- **The interval is configurable** — `[availability] beat_interval_secs`, default 60, with ±10%
  jitter so the group does not beat in step. A change to what this node holds triggers an extra beat,
  at most once every 10 s. Tests set the interval in code, at 1 s.
- **The budget is a constant — Ivan's call.** With N members, each beat reaches all of them, so every
  node receives N beats per interval. Each sender stretches its own interval to
  `beat_interval × ⌈N/50⌉`, which keeps what any node receives at about fifty beats a minute.
  **The interval and the TTL grow linearly with N; the traffic per node stays flat** — up to a cap.
  At the default, N = 200 beats every 4 minutes and goes offline after 12.
- **The interval is capped at 20 minutes** — reached at 1,000 members at the default. Beyond it the
  traffic per node grows with N, linearly. Linear rather than logarithmic, which would grow the
  traffic sooner; presence for groups far beyond the thousands §2 aims at is to be reworked later
  (C32). **Ivan's call**, at the review of 4b-3.
- **TTL = 3 × the sender's `interval_secs`**, read from the heartbeat and clamped to 1 s – 1 h —
  three of the capped interval, derived from it so the two clamps cannot disagree. The
  sender says how long to trust it, so two nodes with different configs, or different views of N,
  still agree. At the default that is 180 s, against §5.6's "~5 min" — a delta, **Ivan's call**.
- **A `leaving` beat on graceful shutdown** removes the member at once, sent before the endpoint closes (P4-2). A
  kill still waits out the TTL.

The receiving side, in order:

1. check the size, then decode;
2. verify the signature, and that the address inside is signed by the same member;
3. the signer is a current member, and not this node;
4. a new `epoch` is accepted as an **appearance**; within an epoch, only a higher `seq`;
5. hand the address to `Directory::learn` — which is how C5 closes;
6. stamp the entry with this node's own `Instant` — the sender's clock is never used;
7. if `base` is not the one this node holds for the member, fetch it (D5); then apply the delta
   and check the count (D3).

Expiry is one `sleep_until` on the earliest deadline. A member who leaves the allowlist is dropped at
once rather than at the TTL.

**D5 — The base list: when it is published, how it is fetched, and what it leaves behind.**

- **Published** into the node's own blob store with
  `add_bytes(..).with_named_tag("distlib/availability/base")`. In plain words: the one tag name
  `distlib/availability/base` always points at the newest list, so the previous one is no longer
  protected. Awaiting `add_bytes` directly would give every list a tag of its own and keep all of
  them for ever (ground truth 7).
- **Re-published — consolidated —** when the next change would overflow the delta, and **when the
  delta is not empty and the set has not changed for ten minutes.** *(As built: not also at start,
  and "changed" rather than "added" — see 4b-4.)* Without the last rule a
  node that once downloaded three hundred items would send a 10 KB heartbeat for the rest of its life.
  With it, a burst of downloads costs one base fetch per receiver, at its end, and an idle node's
  heartbeat is back to about 0.8 KB. Removals go into `removed` like any other change.
- **Fetched** by a receiver only when a heartbeat names a base it does not hold, with `get_blob` from
  the sender, **into memory whole, read into the map and dropped — never stored**, so a receiver
  leaves no garbage. Capped at ten million ids, 320 MB on the wire. *Whole rather than streamed —
  Ivan's call at the review of 4b-1:* at the 100k target a list is 3.2 MB, at a million 32 MB, and
  only for as long as it is read.
- **Until a member's base has arrived, it counts as unknown**, not as "holds nothing".
- **Superseded lists stay on the publisher's disk** until phase 5 brings GC, together with quotas and
  custodianship: one per download burst. **C26.**

**D6 — "Held here" is a set the node maintains, not a question it asks the blob store per row.**
An item is held when every `role: content` file is `Complete` locally — covers and other roles do not
count, as they do not count towards an item's identity. A `Holdings` set in `distlib-sync::availability`,
with `recheck(&Item)`, called by:

- the **projection**, for every item it projects — which covers replay, reindex and every document
  change, including another member adding a file this node lacks;
- **`library.add`**, after its import;
- **`library.download`**, when it finishes.

The pump is not touched. 4a-1 adds the missing index on `item_files(blob)`.

**D7 — Targeted re-offers replace the timer.** The catalogue's offer loop keeps its two existing arms
— an address learned, the membership changed — and gains three:

- **(a) an appearance** (D4): a new member, or a known one with a new epoch, is offered;
- **(b) unconfirmed offers**: a peer offered with no `SyncFinished` within 10 s is offered again,
  backing off to five minutes;
- **(c) zero neighbours**: when the document's gossip neighbour count drops to 0, every member with
  an address is offered; the availability topic calls `join_peers` the same way.

`OFFER_AGAIN` goes. The content sweep gets its own nudge (ground truth 12). **A slow backstop sweep,
every ten minutes, stays** — for the split where a group of four falls into two pairs, each of which
still has a neighbour and so triggers nothing. **Ivan's call.** Exactly which arm fixes C25 is
settled by 4.0's diagnosis, not assumed here.

**D8 — Community keys are a type of their own; `Key` stays item-only.**
A `CommunityKey` enum in `distlib-core::community`, with §5.3's key formats except one:

| Key | Value (JSON) |
|---|---|
| `rating/{item}/{member}` | `1..5` |
| `review/{item}/{member}` | a string, capped at 16 KiB |
| `bookmark/{item}/{member}/{bookmark}` | `{position, note, created_at, updated_at}` |
| `wish/{wish}/{member}` | `{kind?, title?, authors?, description?, created_at?, status, item_id?}` |
| `wish_comment/{wish}/{member}` | a string |

**Bookmarks gain a fourth segment — Ivan's call, and a delta against §5.3.** A bookmark is a
**shared pointer**, so a member may leave several on one item: `bookmark` is 16 random bytes in hex,
`position` is free text (a page, a chapter, `01:23:45`), and `note` is capped at 4 KiB. Clearing or
deleting anything is `Doc::del` on your own full key (ground truth 10).

**D9 — An entry counts only if its author is the member its key names.**
Reads are flat `Query::author(m).key_exact(k)`, or a prefix scan that keeps only entries whose author
matches the key's member segment — **never `single_latest_per_key`** (ground truth 9). Writes build
the member segment from this node's own author, never from what a caller sent. **An expelled
member's entries stop counting:** the projection filters by current membership, and a membership
change re-projects the items, wishes and bookmarks it touches.

**D10 — Wishes are resolved, not stored.**
`wish_id` is 32 random bytes in hex. The creator is whoever wrote the earliest entry carrying a
title. Fulfilment is §5.3's: the fulfiller writes their own `wish/W/{me}` with
`{status: "fulfilled", item_id}`, and the reader resolves it. One comment per member per wish,
editable, as §5.3 has it; threads would be C29. **Ivan's call.**

**D11 — Events stay ids only (phase 3's D2).**

- `availability.changed {member_id}` — a delta against §7.2's `{item_id, providers}`. One node going
  offline touches every item it holds — thousands of ids in one event — so the event names the member
  and the page refetches what
  it shows.
- `wish.changed {wish_id}`, `bookmark.changed {item_id}`, and `sync.status` with no payload.
- **Ratings and reviews need no event of their own**: the item is re-projected, and they arrive as
  `catalogue.item_changed`.

The availability service publishes with `let _ = tx.send(..)` and never awaits — phase 3's rule.

**D12 — The read model gets a version.**
`READ_MODEL_VERSION`, kept in SQLite's `PRAGMA user_version` and in a `VERSION` file beside the
index. On a mismatch, or a tantivy schema error, delete the database (with its `-wal` and `-shm`) and
the index, and let the replay that runs anyway refill them. `lang` joins the default search fields at
a low boost, 0.5, so a language code does not outrank a title.

**D13 — Bookmarks are shared pointers — Ivan's call.**
Every member sees every bookmark; **"mine" is only a filter.** The projection keeps all members'
bookmarks except an expelled member's. The item page lists an item's bookmarks — member, position,
note — with your own editable. A **Bookmarks page** lists all of them, newest first, with a search
over the note, the item's title and the member's name, and filters for "mine" and for one member.
The search is SQL `LIKE`: the number of bookmarks a group makes is small next to its catalogue, and
it keeps the search index untouched (ground truth 14). §7.1 gains `community.bookmarks`.

---

## Sub-phases

Seventeen PRs. Each ends compiling, tested, and demonstrable by hand with the CLI or a page. One PR at
a time, review before the next.

### 4.0 — diagnose C25 (1 PR, CI only)

- **4.0 — find out which step fails on Windows.** Tracing for the integration tests, gated on
  `RUST_LOG`; the Windows job runs the C25 test with `--success-output final`; and a debug line at
  each step — address learned → offer → dial result → document `NeighborUp` → `SyncFinished`.
  No fix.
  *As built:* the debug lines carry a `catalogue{me=…}` span, so each names its node. The CI step ran
  on all three platforms and was removed again in the same PR: alone, the test is fast on Windows
  too, so the log showed the fast path everywhere. Delaying alice's shutdown reproduced C25 on Linux,
  and its log named the step — ground truth 18.
  **Acceptance:** a log that names the step that does not happen — met locally, not on Windows.
  **Watch for:** it goes first because its answer arrives on CI's schedule, while other work goes on.

### 4a — foundations (3 PRs)

- **4a-1 — read-model version, `lang` in the index, `item_files(blob)` indexed (D12).**
  **Acceptance:** a phase-3 data directory opens, rebuilds and is searchable; `lang:cs` finds a Czech
  item, and so does a bare `cs`. Mutation-checked: without the version check the old directory fails
  to open; without the field the search finds nothing.
  **Watch for:** this goes before any other schema change, because every later one bumps the version
  it introduces.
  *As built (P4-1):* the SQLite half drops every table rather than deleting the file, and the index
  also starts afresh when tantivy rejects its schema although the version matched.

- **4a-2 — `sync.status` (C14).** The pump records neighbours and the last sync per peer into a
  `watch<SyncState>`, synchronously (ground truth 11); `node.status` gains a sync block; the
  `sync.status` event exists.
  **Acceptance:** two nodes show one neighbour each; stop one, the other drops to zero and the event
  fires.
  **Watch for:** found in 4.0 — the pump subscribes after the catalogue opens, so a `NeighborUp` that
  fires in between is never seen, and a node can show zero neighbours while it has one. iroh-docs has
  no call that reports current neighbours (`Doc::get_sync_peers` is the remembered peers, not live
  ones), so the record has to start before `start_sync` does — subscribed when the document opens.
  *As built (P4-2):* the document's one subscription is the catalogue's, opened before syncing, and the
  projection reads from it. Graceful shutdown turned out to leave a neighbour shown for a minute —
  iroh-gossip forgets a peer on every topic when it leaves one, and tells only that one — so
  `Runtime::shutdown` now closes the endpoint first.

- **4a-3 — C25: the membership topic stops dialling members it cannot reach.** *Revised by 4.0
  (ground truth 18).* `join_topic` bootstraps with the members that have an address — the rule
  `sync_with` already follows — and the rest join as their addresses are heard. The C25 test is made
  deterministic first: alice goes only after the two followers have offered each other, which today
  takes about 30 s to converge.
  **Acceptance:** with 4a-2's neighbour record, the two followers are catalogue neighbours of each
  other before alice goes; the C25 test then converges in seconds on all three platforms, and its
  bound is tightened to match.
  **Watch for:** D7's (b) and (c) are re-weighed after this, not assumed — they cover the other way a
  peer is stranded (C30), not this one.
  *As built:* the rule is consensus's `gossip::reachable` — a core member with its address from the
  log, anyone else once its address is heard — and the catalogue's `sync_with` now calls it too, so
  the two topics cannot drift apart again. **Nothing joins the rest later**, which the plan above
  said would: a task that added each member to the membership topic as its address arrived was
  built and dropped, because removing it failed no test — once nobody is stranded, gossip introduces
  its own peers, and the catalogue already offers each member as it is heard. The C25 test now waits
  for the two followers to be catalogue neighbours, bounded at 10 s (under `OFFER_AGAIN`'s 30 s, so
  only the first introduction can pass it), before alice goes: before the fix it failed there every
  time, after it the whole test takes about 9 s. Its final read is bounded at `SOON` rather than
  `PATIENTLY`.

### 4b — availability (7 PRs)

- **4b-1 — wire format, list encoding, size guard and the 16 KiB frame (D1–D3).** The pure parts in
  `distlib-core`, and `GOSSIP_MAX_MESSAGE` on every `Gossip` — `runtime.rs` and all five test
  harnesses (ground truth 3).
  **Acceptance**, as property tests: one set always encodes to the same bytes, whatever order it was
  built in; base and heartbeat round-trip; the encoded heartbeat never exceeds the cap for 0–64
  addresses and a full delta; a tampered body, the wrong group, or an address signed by someone else
  fails. And one real 16 KiB message crosses between two nodes without dropping the connection.
  *As built:* `distlib-core::availability` — `SignedHeartbeat` (`sign`, `encode` with the guard,
  `decode`, `open` checking both signatures), `encode_base` and `decode_base` over a list fetched
  whole (D5). `GOSSIP_MAX_MESSAGE` lives in `distlib-core`, because the
  heartbeat's own cap is derived from it, and is applied by **`distlib_net::spawn_gossip`, the one
  builder** — all seven call sites use it, so no test node can drift onto the default; the binary and
  `distlib-api`'s tests no longer depend on `iroh-gossip` at all. `HEARTBEAT_MAX` is the frame less
  512 B for gossip's own framing, `DELTA_MAX` 384 ids. The 10M-id cap on a base list is enforced but
  not tested: a list that long is 320 MB.

- **4b-2 — "held here" (D6).**
  **Acceptance:** held after an add, after a download and after a replay; not held once another
  member adds a content file this node lacks.
  *As built:* `Holdings` in `distlib-sync::availability`, owned by the catalogue
  (`Catalogue::holdings()`), in memory only. **Two callers, not three**: `library.add` writes the
  item into the document after importing its files, so the projection's recheck already covers it;
  a download is the one way content arrives without the document hearing of it, so it rechecks
  before the task says finished. The projection rechecks before it writes the row. Pinned in
  `download.rs` — the add/download/restart test gains the first three points, and a solo test adds a
  cover the node lacks (still held) and then a chapter (not held).

- **4b-3 — the heartbeat service, presence only (D4).** The topic,
  `blake3("distlib.availability.v1" || group_id)`; the TTL index; `Directory::learn`;
  `own_address()`; the config key and the budget; `leaving` before the endpoint closes, which
  `Runtime::shutdown` now does first (P4-2).
  **Acceptance:** a member is online, then gone within the TTL when aborted and at once when stopped
  cleanly; a sender's interval sets its receiver's TTL; a node that missed an address announcement
  learns it from a heartbeat (C5); `a_settled_group_stops_talking_about_addresses` still passes.
  *As built:* `Availability` in `distlib-sync::availability`, started by the runtime beside the
  catalogue; `online()` is a watch of the other members online. The receive rules are a plain table,
  `Online`, unit-tested without a network: **a member taken offline keeps its last `(epoch, seq)`**,
  so a beat older than the goodbye — gossip does not keep order — cannot bring it back for a TTL; the
  entry goes only when the member leaves the group. `MembershipNode::own_address()` is a watch of the
  statement the announcer last signed, woken only when it changes. Beats go out at once, every
  interval, when that statement changes — and **when a neighbour comes up**, which the plan did not
  say: the first beat of all goes out before any neighbour is connected, to nobody, and at the default
  interval a node would otherwise be unseen for a minute. **The goodbye waits 250 ms before the
  endpoint closes**: broadcasting only hands the beat to the gossip actor, and closing the connection
  drops what it has not written — without the wait the runtime test failed on its first run, with it
  20 of 20. `beat_interval_secs` is a `NonZeroU32`, so 0 is refused when the config is read. A beat
  states its interval in whole seconds, so the shortest TTL is 3 s, and the tests beat every 1 s and
  10 s rather than every 300 ms. **No floor between beats yet**: the extra beats here are an address
  change, already floored by the announcer, and a new neighbour; the floor comes with 4b-4's
  holdings change, the burst it is for. **Measured, five nodes beating every second for a minute on
  loopback:** a beat is about 310 B; each node receives 4 beats and about 15 gossip control messages
  per interval, about 2.3 KB — about 3.3 MB a day at the default 60 s — and the node gossip made the
  hub forwards about 5 KB per interval, about 7 MB a day. **At review (Ivan):** the stretched
  interval is capped at 20 minutes and the TTL's hour derived from it — before, above 3,000 members
  a beat's interval outran its own TTL and every member flickered offline between beats (C32 for
  what lies beyond). And `Online` keeps no copy of the group: who belongs is asked of the membership
  watch as it is when a beat arrives, and an expulsion drops the member at once.

- **4b-4 — holdings on the wire: the base list and the delta (D3, D5).**
  **Acceptance:** a receiver's map matches the sender's held set exactly — after the base, after
  additions, after a removal; a dropped heartbeat changes nothing; a base is fetched only when a
  heartbeat names a new one, counted; overflowing the delta, and ten quiet minutes with a delta, each
  publish a new base, and the named tag moves to it; the receiver's blob store is unchanged.
  **Two PRs, not one** — too big to review as one: the sender first, the receiver second.
  *As built, the sender:* `Statement` in `distlib-sync::availability` states
  `{count, base, added, removed}` for each beat, against the last base it published; nothing reads
  it yet. `Holdings` signals each change to the set — a recheck that finds what was known is none,
  or a replay would prompt a beat per item — and a change prompts a beat no sooner than ten seconds
  after the last beat (D4's floor), or at the regular beat if that is due sooner: the floor holds
  back only the extra beat, so a node whose holdings keep changing still beats on its interval
  and does not outlive its TTL (macOS CI found the floor stalling a 1 s beat). **Two departures from D5:** no base is published at start — until
  the first is, the delta is counted from an empty list, and the overflow and quiet rules publish one
  when it is needed; and the quiet rule waits for ten minutes with *no change*, not "nothing added",
  since a delta of removals alone would otherwise never be folded in. A base that cannot be published
  skips the beat rather than understating what is held. Pinned without a network — the delta both
  ways, overflow at `DELTA_MAX + 1`, ten quiet minutes and not nine, the tag moving, nothing
  published with nothing to fold in — and over one: two changes half a second apart make one beat ten
  seconds after the last, twice; 385 items make a base readable from the sender's store under the
  tag, and a removal after it is counted from it. Not pinned over the network: the quiet rule's
  wiring, which would take ten minutes.
  *As built, the receiver:* `Holders` in `distlib-sync::availability` keeps the inverted map,
  and `Availability::holders(item)` and `knows_holdings_of(member)` read it. Each beat's change
  *replaces* the last one rather than adding to it: the previous change is undone and the new one
  applied, so an item added and lost again before the next base is in neither list and is not
  kept. Only the previous change is stored per member, not a copy of the base. A base is fetched,
  and taken into the map, by a task of its own, never in the listening loop; one fetch per member
  at a time, with
  `get_verified_size` first so a list over `BASE_MAX` is refused before it is read. When it arrives,
  the newest beat's change is applied on top, and a base no beat names any more is dropped.
  **Three departures from D3/D5:**
  - members are keyed by `MemberId`, not `u16` indexes — a few megabytes at the target sizes
    (KISS; numbers decide, with C28);
  - a statement or a list that does not add up is **not** fetched again: the hash fixes the bytes,
    so the same base would fail the same way on every beat. The member stays unknown until a beat
    names another base. Only a failure to reach the member is retried, by the next beat;
  - what a member holds is kept while it is offline, and dropped only when it leaves the group,
    so a member that comes back with the same base costs no fetch.
  Pinned without a network: the change replacing the last, a missed beat, a new base, a stale
  base, retry and no retry, the count check, duplicates, members apart. Over one: a receiver sees
  exactly what a member holds after its base, after an addition, after a base item and an added
  item are lost; the base is fetched once (counted on the sender) until a new one is named; the
  receiver's store has no blob and no tag; an expelled member's holdings are forgotten. Not
  pinned: the size cap, which would take a 320 MB blob.

- **4b-5 — availability in the API and CLI.** Hits gain `held` and `providers` — an exact count of
  online holders, `null` while any online member's base is still unknown;
  `library.item` gains `availability`; `library.download` orders its providers — online holders,
  then other online members, then the rest, each tier shuffled; `availability.changed`.
  **Acceptance — §9's first half:** stop the provider and `providers` drops within the TTL, while the
  document's entry count and the log's position stay where they were; a download with one offline
  provider does not wait out a dial timeout on it.
  **Two PRs** — Ivan's call: the read side first, the provider order second.
  *As built, the read side:* hits carry `held` and `providers`; `library.item`'s `availability` is
  `{held, providers, holders, unknown}` — the online holders, and the online members whose holdings
  are why `providers` is `null` (Ivan's call). `availability.changed {member_id}` is said when a
  member comes online or goes, when a beat says something new about what it holds, and when its
  base arrives — not for a beat that repeats the last. The CLI's `search` and `item` print it in
  one phrase. Acceptance: bob stops beating without a word, and alice's `providers` drops within
  the TTL while her catalogue's item ids, the item itself and the log's position stay as they were.
  The document's raw entry count is not exposed, so the item and its ids stand in for it.
  *As built, the provider order:* `Blobs::fetch` tries providers in the order given — no longer
  shuffled inside it — and `library.download` gives them online holders first, then the other
  online members, then the rest, each tier shuffled (ground truth 6: any `Vec` is a
  `ContentDiscovery` tried in order). The downloader's connect timeout is one second
  (`iroh-util`'s pool default), so an offline member asked first costs a second per file.
  Acceptance: carol is a member and stopped, bob online holds a four-file item, and alice's
  download finishes in under that second; with each file's providers shuffled as before, it
  failed 3 runs in 3.

- **4b-6 — `OFFER_AGAIN` goes (D7 a, the sweep's nudge, the backstop).**
  **Acceptance:** a quiet group makes no `start_sync` call in two minutes, counted; an item repaired
  by the sweep is re-projected with no timer; the C25 bound from 4a-3 still holds.
  **Watch for:** ground truth 12 — the nudge and the removal are one PR, never two.
  *As built:* the timer is `BACKSTOP`, ten minutes, and the content sweep calls
  `News::content_landed` — the same `content_arrived` flag `ContentReady` sets — for every hash
  it fetches. **D7 (a) is not built (Ivan's call)**, and (b) and (c) neither, as 4a-3 said to
  re-weigh: no test needs an appearance offer. A restarted member offers its own peers when it
  starts, and the catalogue offers each member as its address is heard; and a partition loses
  the availability topic's neighbours along with the catalogue's, so no appearance is heard to
  act on. The quiet window is 45 s, not two minutes (Ivan's call) — still past the old 30 s.
  Acceptance: three nodes, settled, finish no sync round for 45 s
  (`a_settled_group_stops_syncing`; fails with the timer back at 30 s); bob, restarted without
  his content or read model, projects the item whole within 20 s
  (`an_item_whose_content_the_sweep_fetched_is_projected_whole`; fails without the nudge); the
  C25 test still converges, in 5–8 s.

- **4b-7 — availability in the UI.** A library column — held here / n online / none online /
  unknown — the same on the item page, and neighbours and last sync on the Node page.
  **Acceptance:** Vitest, mutation-checked; the badge changes on `availability.changed` without a
  reload.
  *As built:* one phrase everywhere, in the CLI's words — `held here, 2 online`, `1 online`,
  `none online`, `online holders unknown` — from one `availability()` in `format.ts`. The library
  and item pages reload on any `availability.changed`, since it names a member, not an item. The
  item page shows the phrase only, not who the holders are (Ivan's call). The Node page's members
  table gains two columns, Neighbour and Last sync (a failed round says so), and reloads on
  `sync.status`. Eleven mutations, every one caught by Vitest; the e2e browse path also checks
  the phrase against a real node's answer, since the Vitest fixtures are written by hand.

### 4c — community (7 PRs)

Needs only 4a-1, so it can interleave with 4b.

- **4c-1 — keys, reads, writes and the author check (D8, D9).** The pump routes community keys: an
  item's ratings and reviews mark the item dirty; bookmarks and wishes go to `Batch.bookmarks` and
  `Batch.wishes`.
  **Acceptance:** a forged `rating/X/bob` written by carol is ignored on every node; two concurrent
  ratings of one item both survive.
  *As built, in two PRs (Ivan's call): ratings and reviews first, end to end.* 4c-1a:
  - `CommunityKey` (rating and review) in `distlib-core::community`, with `Rating` (1–5) and
    `Review` (at most 16 KiB of UTF-8). Both are checked when read as well as when made, so a raw put
    of anything else is not one.
  - **A key counts only in the spelling it is written in.** Ids parse from more than one spelling
    (hex in either case), and a second spelling would be a second key by the same member, passing
    the author check: a second rating.
  - `Catalogue::rate`, `review`, `ratings` and `reviews`. Writes name this node's own member; reads
    are a flat prefix scan keeping each entry whose author is the member its key names.
  - The pump marks an item dirty for its ratings and reviews.
  - Acceptance, on three founders: carol forges bob's rating and alice's, and every node counts
    only bob's real one; two ratings made at once and a review all reach every node.

  4c-1b is bookmarks, and 4c-1c wishes (Ivan's call: two PRs). `Batch.bookmarks` and `Batch.wishes`
  move to 4c-2, where the projection first reads them.

  *As built, 4c-1b:*
  - **The value is `{position, note, created_at}`, with no `updated_at` (Ivan's call).** "Updated" is
    the entry's own timestamp, as an item's `last_modified` is; a stored field would be a second
    answer able to disagree. `created_at` is stored, since an edit rewrites the entry. Times are
    microseconds since the epoch.
  - **Caps:** the note is 4 KiB, as planned, and the position 256 bytes, since it names a page, a
    chapter or a time, not prose.
  - **Ids:** `BookmarkId` is 16 bytes of lowercase hex.
  - **Catalogue:** `bookmark(item, id, &Bookmark)` writes. `bookmarks(item)` reads with the same
    author check as ratings, returning `ReadBookmark {member, id, bookmark, last_modified}` sorted
    by member, then id. Sorted explicitly, because the query's own order is not that.
  - **The pump does not mark an item dirty for a bookmark**, since a bookmark is not part of the
    item it points into.
  - **Acceptance, on three founders:** two of bob's bookmarks on one item both count on every
    node, and one carol forges in his name counts on none. An edited bookmark keeps its
    `created_at` and gets a later `last_modified` everywhere.

  *As built, 4c-1c:*
  - **Keys:** `wish/{wish}/{member}` and `wish_comment/{wish}/{member}`, with `WishId` (32 bytes
    of lowercase hex). One `random_id!` macro makes it and `BookmarkId`.
  - **A wish entry is `Wish(WishFields)`** — `kind?` (an `ItemKind`, as on an item; added at
    review), `title?`, `authors?`, `description?`, `created_at?` and `WishStatus`. `WishStatus`
    is `Open | Fulfilled { item_id }` (Ivan's call), so an item exists exactly when the entry says
    fulfilled. In JSON it is the plan's `status`, with `item_id` beside it.
  - **Caps (Ivan's call):** a whole entry is capped at 16 KiB of JSON, one rule rather than one per
    field; a comment at 4 KiB. `Comment` and `Review` come from one `capped_text!` macro.
  - **Catalogue:** `wish(id, &Wish)` and `wish_comment(id, &Comment)` write under this node's own
    member. `wishes(id)` and `wish_comments(id)` read by member with the author check. Resolving who
    created a wish and whether it is fulfilled (D10) is 4c-4's.
  - **`CommunityKey::item()` is gone**, since a wish has no item. The pump marks an item dirty for
    ratings and reviews only.
  - **Acceptance, on three founders:** alice wishes, bob comments, carol fulfils — and forges a
    comment in alice's name. Every node holds the two entries and bob's comment only.

- **4c-2 — the projection (a version bump).** Tables for ratings, reviews, bookmarks (everyone's),
  wish entries and comments; expelled members filtered out. **Reviews are not searched** (P4-4,
  Ivan's call).
  **Acceptance:** an expelled member's rating and bookmarks disappear; a second replay changes
  nothing.
  *As built, in two PRs (Ivan's call): ratings and reviews first, as 4c-1 was.* 4c-2a:
  - **`ReadItem` carries the item's ratings and reviews**, read with the author check, so a re-read
    of the item is when they are projected. They do not make an item: one nobody has written an
    entry of is still `None`, however many members rated it. `waiting_for_content` covers them.
  - **Tables `ratings (item, member, rating)` and `reviews (item, member, review)`**, keyed by item
    and member, cleared and rewritten with the item. `StoredItem` carries both; `Store::item` and
    `items` fill them, the reads that leave `files` empty leave them empty. Version 2.
  - **Expelled members are filtered when projecting (Ivan's call)**, not on read: the projection
    keeps only current members' ratings and reviews, and **replays everything when the set of
    members changes** — not on every membership change, since pledges, proposals and approvals
    change it too. One look at the log gives both the `members` table and the filter.
  - **Acceptance, on three nodes:** bob and carol rate and review an item; alice expels bob, and
    her read model keeps carol's alone; a rating of an item nobody wrote makes no item; a reindex
    afterwards changes nothing. Mutation-checked, except the content-waiting path for ratings,
    which no harness here can arrange deterministically.

- **4c-3 — ratings, reviews and bookmarks in the API and CLI.** `community.rate`,
  `community.review`, `community.bookmark` (create, edit or delete your own) and
  `community.bookmarks {q?, member?, item_id?}`; the CLI's `rate`, `review`, `bookmark` and
  `bookmarks`; `library.item` gains `ratings {average, count, mine}`, `reviews` and `bookmarks`.
  **Acceptance — §9's second half:** two members rate one item at once, and both ratings show on all
  three nodes. And: two bookmarks by one member on one item both survive, and another member finds
  one by a word from its note.

- **4c-4 — wishes in the API and CLI (D10).** `community.wish_create`, `wish_list`, `wish_comment`,
  `wish_fulfill`, as §7.1 names them.
  **Acceptance:** A creates a wish, B comments, C fulfils it with an item; A sees it fulfilled and
  linked to the item.

- **4c-5 — community on the item page** — ratings, reviews, bookmarks.
  **Acceptance:** Vitest; one Playwright test that rates, reviews and bookmarks.

- **4c-6 — the Wishes page** — route, nav, create, a picker to fulfil with, comments.
  **Acceptance:** Vitest; one Playwright test.
  **Watch for:** C20 — a new page is when the CSP's inline styles are revisited.

- **4c-7 — the Bookmarks page** — route, nav, search, the "mine" and member filters.
  **Acceptance:** Vitest; one Playwright test.

**Order:** 4.0 first; 4a-1 before any schema change; 4a-2 before 4a-3 and 4b-6; 4b-1 and 4b-2
before 4b-3, then 4b-4; 4b-3 before 4b-6. 4c needs only 4a-1.

---

## Carried forward from Phase 3 — take or defer, explicitly

Every open C-number appears here once.

| # | Item | Phase 4 |
|---|---|---|
| **C1** | A full peer offer is O(N²) dials | **Narrowed.** A full offer happens only on a membership change and the ten-minute backstop; a member whose address is heard is offered alone (4b-6). |
| **C3** | The sweep reads the whole document every five seconds | **Deferred.** Still cost only. |
| **C5** | Nothing asks again when a member cannot be resolved | **Closed by 4b-3.** Every heartbeat carries the sender's signed address. |
| **C6** | A field blinks out of the read model while its newest value is in flight | **Mitigated for community rows**: an old row is kept while its new value is in flight. Items unchanged. |
| **C7** | The read model is replayed in full at every start | **Deferred**, and now relied on: D12's rebuild costs nothing because of it. |
| **C8** | `added_by`, `created`, `modified_by` have nowhere to come from | **Deferred again.** Nothing in §9's phase-4 list wants it. |
| **C9** | `PENDING_EXPIRY` is one fixed count | **Phase 5.** |
| **C13** | Gossip does not change sides when a node is promoted | **Deferred.** |
| **C14** | `sync.status` does not exist | **Closed by 4a-2.** |
| **C15**–**C19** | Read-only admin; Windows file privacy and service; clearing a field; downloads as in-memory tasks | **Unchanged.** |
| **C20** | The CSP allows `style-src 'unsafe-inline'` | **Revisited in 4c-6**, the first new page. |
| **C21**–**C24** | Title sort; consensus test peers that cannot restart; release publishing; the expelled leader's lost answer | **Unchanged.** |
| **C25** | On Windows, two followers meet only through the timed re-offer | **Closed by 4a-3.** Not Windows-specific: a dial on the membership topic to a member with no address strands that peer in iroh-gossip for every topic (ground truth 18). |

**New in phase 4:**

| # | Item | Where it goes |
|---|---|---|
| **C26** | **Superseded base lists stay on the publisher's disk** (D5), one per download burst | **Phase 5**, with GC, quotas and custodianship. |
| **C27** | **Heartbeat traffic has not been measured above N = 50** | **No phase.** Measured on five nodes here; a larger group is the trigger. |
| **C28** | **The `item → members` map lives in memory** (D3) — about 1 GB per node at 10M distinct items | **No phase.** Moves to a local SQLite table when a group reaches the 10M end — Ivan's call. Never replicated either way. |
| **C29** | **One wish comment per member** (D10) | **No phase.** Threads, when somebody asks. |
| **C30** | **One failed gossip dial strands that peer until it dials us** (ground truth 18) — an offline member, say, is never dialled again by gossip once it is back | **Upstream**, n0-computer/iroh-gossip#159 or its like. Meanwhile a returning member dials us, which clears it; if it did not, the only retry left is the catalogue's ten-minute backstop offering it again — D7's appearance, unconfirmed-offer and zero-neighbour arms were not built (4b-6). |
| **C31** | **A write made as two nodes connect can miss both paths to the other** — broadcast before the gossip neighbour is up, and after the running sync round compared the two sides; iroh-docs drops the sync it would start for the new neighbour because a round is already running, and only a `SyncReport` queues one (`engine/state.rs:195-206`). Found from the macOS CI log of `what_one_member_writes_the_other_reads`, write and neighbour 0.1 ms apart; repaired only by `OFFER_AGAIN`, which 4b-6 removes | **Worked around** after 4a-3: the pump asks for one more round with a neighbour whose round began before it came up (`Pump::began_before_neighbour`). Upstream: `NewNeighbor` should queue a resync the way `SyncReport` does — an issue to open. |
| **C32** | **Presence traffic grows with N above 1,000 members** — the beat interval is capped at 20 minutes (D4), so each node receives N beats per 20 minutes; at 10,000 about 400 MB a day, and full broadcast cannot serve a group of a million at any interval | **No phase — Ivan's call, KISS for now.** A larger group is the trigger: presence by sampling or aggregation rather than every member's beat reaching every other. |
| **C33** | **iroh-docs wedges a pair of peers on `AlreadySyncing`** — a connect refused because the remote is still *accepting* the round before is ignored (`engine/live.rs:498`, which assumes the remote is dialling us), so the local round stays marked running and every later round between the two is refused, both ways, until a restart. Found from Linux CI on #94: `read_model`'s restarted node never caught up. Our own trigger was the C31 workaround's round asked for the moment the last one ended | **Worked around**: that round now waits a second (`SETTLE`); 60 of 60 stress runs against one failure in seven to twenty. Other offers can still coincide with the end of a round, so **upstream** is the fix — filed as [n0-computer/iroh-docs#121](https://github.com/n0-computer/iroh-docs/issues/121), with a test in their own suite that fails on their `main` (branch `test/already-syncing-wedge` on izderadicka/iroh-docs). |

---

## Testing and the lanes

**Fast lane:** canonical list encoding and the size guard, as property tests; applying a delta to a
base, including a missed beat; consolidation on overflow and after ten quiet minutes, under
`tokio::time::pause`; signing round trips and tampering; the epoch/seq rule; the TTL index under `tokio::time::pause`, including a
sender-declared interval; community keys and the author check; the read-model rebuild on a version
mismatch.

**Slow lane:** the online/offline flip; C5; a 16 KiB message between two nodes; bases fetched on
change only; the quiet-group counters;
C25; both halves of §9's acceptance; a forged and an expelled entry; provider order on download.

Heartbeat tests run at 1 s intervals with a TTL of 3 s, and **assert "within",
never an exact sequence** — the same rule as phase 3's SSE tests, for the same reason.

Every mechanism is mutation-checked. UI: Vitest first, Playwright for the happy paths only.

---

## Risk, stated once

**The first traffic that never stops.** Ground truth 1 is a principle this workspace measured, and a
heartbeat breaks it deliberately: every member sends one per interval for as long as it is up, and
every other member receives it.

A beat is about 0.8 KB while its sender is idle, and up to 16 KB only while the sender has a delta —
during a download burst and for at most ten quiet minutes after it (D5). Each node receives N beats
per interval and forwards roughly as many again. At the default 60 s, what each node receives:

| Members | All idle, per day | Every member mid-burst, per hour |
|---|---|---|
| 5 | ~6 MB | ~5 MB |
| 50 | ~58 MB | ~48 MB |
| 50 to 1,000 | the N = 50 figure — D4's budget stretches the interval instead | the same |
| more than 1,000 | N / 1,000 × the N = 50 figure — the interval is capped (D4, C32) | the same |

A base costs its own size times N once per consolidation — once per download burst, or every ~380
changes during a long one. A node holding 100k items and 50 members: about 160 MB sent, per burst.

The fences, each a rule PRs are reviewed against:

- **the size guard at encode** (D2), property-tested — an oversize frame is a dropped connection on
  every topic;
- **jitter**, and **a floor between beats**, so a burst of changes is one beat, not many;
- **TTL-only state** — nothing to clean up, nothing replicated;
- **a measured rate** on a five-node run in 4b-3, recorded in its delta.

The rest are bounded and named: superseded bases (C26), forged entries (D9), and a receiver's memory
at the 10M end (D3's table, C28).

---

## Verification

Per PR, before review:

- `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` — clean.
- `cargo test-all` — green.
- Any test asserting a new mechanism gets a **mutation check**: delete the line the test is about,
  confirm the test fails, restore.
- For UI PRs: `npm run check`, `npm test` and `npm run e2e`.

At the end of the phase, [`manual-check.md`](../manual-check.md) grows within its one story:

- dave's library shows what is **held here**;
- **a new step after downloads:** bob's book shows "1 online"; Ctrl-C on bob flips it at once,
  `kill -9` within the TTL, and a restart flips it back;
- **in the edit step:** alice and dave rate Dune at the same moment, both ratings show on both pages,
  and a word from dave's review finds it;
- dave leaves two bookmarks with notes on Lectures, and alice finds one by its note on the Bookmarks
  page;
- **a wishes step, before erin leaves:** erin wishes for a book, dave comments, bob fulfils it;
- **when the leader goes down:** the Node page shows the neighbour count drop.
