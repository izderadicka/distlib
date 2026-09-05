# distlib — Distributed Community Media Library

Design & implementation plan. Intended as the working spec for development with Claude Code.

---

## 1. Vision

A peer-to-peer (p2p) media library — ebooks, audiobooks, videos — for **closed groups whose members trust each other** (friends, clubs, co-ops). Each member runs `distlib`, a single Rust binary with a browser UI. The group collectively maintains a long-lived library that reflects community interest, with no central server and no operator who can be shut down or acquired.

**This is not BitTorrent.** BitTorrent optimizes transient distribution of popular content to strangers. distlib optimizes *long-term preservation* of a curated collection inside a bounded, trusted community — including the obscure items nobody is currently reading or viewing.

### Design principles

1. **No single party can lock members in or shut the group down.** Identities are member-owned keypairs. Relay/DNS infrastructure is swappable config.
2. **Consistency by need, not by default.** Only membership requires consensus. Everything else is CRDT (Conflict-free Replicated Data Type — replicated state that merges deterministically without coordination) or ephemeral gossip.
3. **Convergence ≠ correctness is designed around, not ignored.** Scarce decisions (membership, pledges) go through consensus; everything else stays available offline.
4. **Commitment proportional to consumption.** Members pledge storage; the system enforces it.

### Non-goals (v1)

- Byzantine fault tolerance. Raft's crash-fault model is accepted; misbehavior is handled socially via auditable expulsion, backed by storage challenges.
- Anonymity / metadata privacy. Relays see who talks to whom.
- Erasing data from expelled members' disks. Expulsion prevents *future* participation only.
- Encryption at rest / group key epochs. Deferred; revisit before any deployment beyond friends.
- Semantic deduplication (Work/Edition/File model). Phase 2 — but the schema leaves room for it (see §6.2).
- Transcoding, streaming optimization, media playback beyond what the browser does natively.

---

## 2. System model & trust assumptions

- Group size: up to ~1,000s of members. Catalogue: millions of items. Data: hundreds of TB aggregate.
- Members are semi-trusted: assumed not to attack the protocol, but free-riding and negligence (pledging storage and not providing it) are expected and must be detected.
- Peers churn freely. A small **core group** (5–7 stable, well-connected nodes) is expected to have good uptime.
- Platforms: Linux, Windows, macOS. Container-friendly (single binary, one data dir, config via file + env vars).

### Threat model (v1)

| Threat | Mitigation |
|---|---|
| Outsider joins/reads library | All connections require NodeId on the membership allowlist; iroh QUIC is mutually authenticated by ed25519 keys |
| Expelled member keeps participating | Connection refused at accept (allowlist check); no further sync/blobs served |
| Member pledges storage, stores nothing | Random challenge-response proofs (§5.6); failures accumulate → expulsion proposal |
| Sybil (one person, many identities) | Invitation-only admission through core-group consensus |
| Impersonation | Identity *is* the ed25519 key (iroh NodeId); nothing to steal server-side |
| Relay operator snooping | Sees only encrypted QUIC + traffic metadata; relays self-hostable |

---

## 3. Architecture overview

Four state layers, by consistency requirement:

```
┌─────────────────────────────────────────────────────────────┐
│  Layer 1: MEMBERSHIP LOG          — Raft (core group only)  │
│  members, expulsions, storage pledges. Small, append-only.  │
│  All other layers derive authority from this log.           │
├─────────────────────────────────────────────────────────────┤
│  Layer 2: CATALOGUE               — CRDT, permanent         │
│  item identity + descriptive metadata. Grow-only.           │
├─────────────────────────────────────────────────────────────┤
│  Layer 3: COMMUNITY METADATA      — CRDT, per-author keys   │
│  ratings, reviews, bookmarks, wishes. (item,member)-keyed   │
│  → writers never conflict, no merge logic needed.           │
├─────────────────────────────────────────────────────────────┤
│  Layer 4: AVAILABILITY            — gossip, ephemeral       │
│  who is online, who holds what. TTL'd, in-memory only,      │
│  NEVER written to replicated state.                         │
└─────────────────────────────────────────────────────────────┘
         │ sync: iroh-docs (layers 2,3) + openraft (layer 1)
         │ transport: iroh QUIC, gossip: iroh-gossip
         │ content: iroh-blobs (BLAKE3 content-addressed)
         ▼
┌─────────────────────────────────────────────────────────────┐
│  LOCAL READ MODEL: SQLite projection + tantivy FTS index    │
│  All queries served locally. Replicated layers are          │
│  write-path + sync only.                                    │
└─────────────────────────────────────────────────────────────┘
         ▼
┌─────────────────────────────────────────────────────────────┐
│  API: JSON-RPC 2.0 over HTTP + SSE event stream             │
│  UI: Svelte 5, embedded in binary via rust-embed            │
└─────────────────────────────────────────────────────────────┘
```

### Technology choices (settled)

| Concern | Choice | Rationale |
|---|---|---|
| Transport / identity | `iroh` (QUIC, ed25519 NodeIds, hole-punching via relays) | Proven, prior experience (simple-p2p) |
| Content transfer | `iroh-blobs` (BLAKE3 content addressing) | Exact dedup for free; scales KB→TB |
| Metadata sync | `iroh-docs` **as sync engine only** (range-based set reconciliation) | Reimplementing reconciliation = weeks of subtle work. Its `(namespace, author, key)` (Last Write Wins) is wrapped, not exposed |
| Broadcast | `iroh-gossip` (HyParView + PlumTree epidemic broadcast) | Availability heartbeats, event notification |
| Consensus | `openraft` | Maintained, storage/network left to us (network = iroh connections) |
| Read model | SQLite (`rusqlite` or `sqlx` - if async is needed) | Projection target; all queries local |
| Search | `tantivy` | Full-text over millions of items is unremarkable for it |
| Local KV (iroh-docs backing) | `redb` if appropriate - can choose different if async important | What iroh-docs uses |
| API | JSON-RPC 2.0 + SSE (Server-Sent Events) | SSE proven in mbs4 |
| UI | Svelte 5 (runes), embedded via `rust-embed` | Single-binary deployment |

**Version pinning policy:** the iroh family moves fast (0.10x). Pin exact versions in workspace `Cargo.toml`; upgrade deliberately, per-milestone, never mid-feature.

---

## 4. Identity, membership, consensus

### 4.1 Identity

- A member = an ed25519 keypair. Public key = iroh NodeId = member ID. Generated on first run, stored in the data dir (file perms 0600).
- Human-readable display name is metadata in the membership log, not identity.
- v1: one node per member. (Multi-device per member = phase 2; schema keeps `member_id` distinct from `node_id` to allow it later, even though v1 sets them equal.)

### 4.2 Membership log (the only Raft state)

Append-only log of signed events, committed by the core group via openraft:

```rust
enum MembershipEvent {
    GroupFounded { founders: Vec<MemberRecord>, group_id: GroupId },
    MemberAdded  { member: MemberRecord, invited_by: MemberId },
    MemberExpelled { member: MemberId, reason: String, proposed_by: MemberId },
    PledgeChanged { member: MemberId, pledge_bytes: u64 },
    CoreGroupChanged { core: Vec<NodeId> },   // uses openraft membership change
}

struct MemberRecord {
    member_id: MemberId,      // = NodeId in v1
    node_id: NodeId,
    display_name: String,
    pledge_bytes: u64,        // storage commitment — lives HERE, not in gossip,
                              // because custodian assignment (§5.5) depends on
                              // all peers agreeing on identical weights
    joined_at: Timestamp,
    last_changed: Timestamp
}
```

- **Core group** (3–7 nodes) are the only Raft voters. All other members are *followers of the log*: they fetch it, verify the core's signatures, apply it. They do not vote.
- Every non-core member holds the full log locally and derives from it: the connection allowlist, the pledge table, the custodian weights.
- Log distribution: committed entries announced via gossip; peers fetch missing suffix from any core node (or any up-to-date peer) over a dedicated ALPN (Application-Layer Protocol Negotiation — QUIC's mechanism for multiplexing protocols on one endpoint).

### 4.3 Admission

1. Invitee generates keypair out-of-band, sends NodeId + name to an existing member.
2. That member submits `ProposeAdd` to any core node.
3. v1 policy: any core node commits it (proposer is recorded — auditability over ceremony). Group-specific policies (N-of-M core approval) = config, phase 2.
4. New member receives a **join ticket** (group ID + core node addresses + relay config), connects, fetches the log, starts syncing.

### 4.4 Expulsion

1. Any member submits `ProposeExpel` with reason to a core node.
2. v1 policy: requires acknowledgment by a configurable quorum of core nodes (default: majority) before commit. Committed within seconds — "fair but efficient" = signed, logged, auditable, fast; not a group-wide vote.
3. On apply, every peer: drops the member from the allowlist, closes open connections to that NodeId, stops serving them sync/blobs.
4. No rekeying in v1 (no encryption at rest). Expelled member keeps what they already downloaded — this is by design and documented.

### 4.5 The Raft implementation

- `openraft` with: log + state machine persisted in redb; network layer = iroh connections with a small bincode/postcard RPC (dedicated ALPN).
- Snapshots: trivial (state = the full membership table; it's small).
- Core group membership changes use openraft's joint-consensus mechanism, recorded as `CoreGroupChanged` so non-core followers know where to fetch from.

---

## 5. Data layers

### 5.1 iroh-docs usage pattern (wrap, don't expose)

Three namespaces (iroh-docs replicas), all writable by any member, synced with all connected peers:

| Namespace | Content | Write pattern |
|---|---|---|
| `catalogue` | item records, field-level keys | any member, field-level LWW |
| `community` | ratings/reviews/bookmarks/wishes | strictly per-author keys → conflict-free |
| *(reserved)* `works` | phase-2 semantic dedup equivalences | — |

Rules:

- Small values (< ~1 KB): stored as the iroh-docs entry's blob but **immediately projected into SQLite**; reads never touch docs.
- Access to iroh-docs goes through one module (`distlib-sync`); the rest of the codebase sees typed records and a projection stream, so if iroh-docs churns or is outgrown, one layer is replaced.
- **Sync authorization:** iroh-docs capability tickets are NOT the security boundary. The membership allowlist at connection-accept is. Namespace secrets are distributed via the join flow but treated as bearer tokens of convenience, not access control.

### 5.2 Catalogue schema (namespace `catalogue`)
 
Field-level keys so concurrent metadata improvements by different members don't clobber whole records:
 
```
item/{item_id}/type          → "ebook" | "audiobook" | "video"
item/{item_id}/title         → string
item/{item_id}/authors       → JSON array
item/{item_id}/genres        → JSON array
item/{item_id}/series        → JSON {name, index} | null
item/{item_id}/year          → int | null
item/{item_id}/lang          → BCP-47 tag
item/{item_id}/description   → string
item/{item_id}/added_by      → MemberId
item/{item_id}/replicas      → int (target replica count, default 3)
 
item/{item_id}/file/{blob_hash} → JSON {role, format, size, filename,
                                        seq?, disc?, title?, duration?}
item/{item_id}/created      → string - last modification date and time - ISO format                                        
item/{item_id}/last_modified      → string - last modification date and time - ISO format
item/{item_id}/modified_by      → MemberId                                        
```
 
**Per-file keys, not a `files` array.** An item is frequently a *set* — a 40-chapter audiobook, a multi-part video, an EPUB plus its PDF. A single `files` key under last-writer-wins loses data: two members concurrently adding chapters 1–20 and 21–40 would clobber each other and chapters would vanish. Keying by `blob_hash` makes distinct files distinct keys, so concurrent additions are conflict-free by construction (same trick as §5.3). `seq` gives playback order for chapterized media; `disc` handles boxed sets. `role` is `content` | `cover` | `subtitle` | `metadata` | `other`.
 
**Item identity — fingerprint over the content set:**
 
```
item_id = BLAKE3( "distlib.item.v1" || n || sorted[ len(h_i) || h_i ] )
```
 
where `h_i` are the blob hashes of the `role: content` files only, sorted lexicographically, each length-prefixed, with count `n` included.
 
- **Sorted** → order-independent, so filename conventions don't affect identity. **Length-prefixed + counted** → concatenation is unambiguous. **Domain-separated tag** → an item_id can never collide with a blob hash.
- **Only `role: content` files count** — adding cover art or subtitles must not change the item's identity.
- **Computed at creation, then frozen** as `birth_fingerprint`. Adding a missing chapter or a second format later does *not* change `item_id`: custodianship (§5.5), ratings, reviews and bookmarks all key off it.
- Property retained: two members who independently add the byte-identical file set converge on the same item with zero coordination. Two *different* rips of the same work still produce different ids — that is semantic dedup (§6.2), not identity.
Consequence for the API: `library.download` operates on an item plus an optional file filter, and progress aggregates across the set.
 
- Per-field conflict resolution: last-writer-wins by iroh-docs timestamp. Acceptable: fields are independently improvable facts, and any member can re-edit. Full edit history is retained by docs anyway.
- Catalogue is **grow-only** in v1. No item deletion (tombstone semantics deferred). "Wrong" items are fixed by editing metadata.

### 5.3 Community metadata schema (namespace `community`)

Everything keyed by author → zero conflicts, zero merge logic, zero tombstone problems (an author overwrites or clears only their own key):

```
rating/{item_id}/{member_id}      → 1..5
review/{item_id}/{member_id}      → markdown string
bookmark/{item_id}/{member_id}    → JSON {position, note, updated_at}
wish/{wish_id}/{member_id}        → JSON {title, authors?, description, created_at, status}
wish_comment/{wish_id}/{member_id}→ string
```

Wishes: any member creates one; anyone who can obtain the media adds the item to the catalogue and sets the wish's `status: fulfilled` with the `item_id`. Wish fulfillment linking is per-author too (the fulfiller writes their own key; UI resolves).

### 5.4 Read model (SQLite + tantivy)

- A projection task consumes the iroh-docs event stream (plus membership log events) and upserts into SQLite tables: `items`, `item_files`, `ratings`, `reviews`, `bookmarks`, `wishes`, `members`, `custodianships`.
- tantivy indexes `title, authors, description, genres, series, reviews` with per-field boosts; index rebuilt incrementally from the same projection stream; full rebuild command exists (`distlib admin reindex`).
- All RPC queries hit SQLite/tantivy only. Cold-start = full docs replay; projection is idempotent.

### 5.5 Custodianship & quotas (the anti-BitTorrent core)

**Rules:**

- Minimum pledge to join: configurable, default 100 GB.
- Commitment proportional to consumption: required pledge ≥ max(minimum, 2 × bytes of content you've added to the catalogue). Enforced at `PledgeChanged`/add-item time (soft-block adding when under-pledged).
- Every downloaded item is automatically provided (seeded) while it remains on disk — download implies serve.

**Assignment — weighted rendezvous hashing (HRW):** for item `i` and member `m` with pledge `w_m`:

```
score(i, m) = - w_m / ln( h(i, m) )      where h ∈ (0,1) from BLAKE3(item_id || member_id)
```

Custodians of item `i` = top `replicas` members by score. Properties: every peer computes identical custodian sets from the membership log alone (zero coordination); membership/pledge changes reshuffle minimally; capacity-proportional load.

- Custodians **pin**: fetch and retain the item regardless of personal interest, up to their pledge. If assignments exceed a member's pledge, lowest-score assignments overflow to next-ranked member (deterministic, computed by everyone identically).
- A `custodian` background task continuously diffs (computed assignments) vs (local pins) and fetches/releases accordingly, rate-limited.
- When a member is expelled or lowers pledge, reassignment is automatic — next-ranked members' custodian tasks notice and fetch.

**Concentration risk & mitigation (weight cap).**
 
Unbounded weights let one large member attract a majority of replica slots: a member holding share *s* of total weight is custodian for roughly *3s* of all items at `replicas=3`. Their departure then removes one replica from nearly every item at once — exactly the correlated failure replication exists to prevent — and triggers a library-wide re-replication storm.
 
Note the trigger is narrower than it looks: assignment derives from the **membership log, not the availability index**, so a large member merely going offline changes nobody's custodian set. Only a logged event (expulsion, leave, pledge change) reshuffles. That makes the common case staged rather than sudden.
 
Four mitigations:
 
1. **Effective-weight cap.** `w_eff(m) = min(pledge(m), cap)`. `cap` is group config stored **in the Raft log** as an absolute byte value — never derived from group totals, otherwise every join changes every weight and reshuffles everything. Choose so no member exceeds ~`1/(2 × replicas)` of total effective weight (≈15 % at `replicas=3`); then no single departure touches more than about half the items, and none drops below 2 live replicas.
2. **Excess pledge is used, not wasted.** Capacity above the cap serves *opportunistic* custody: overflow from under-pledged assigned custodians (§ overflow rule above), plus voluntary extra copies of at-risk items. Held and served, but not load-bearing for the target replica count.
3. **Decommission state.** A `MemberLeaving` event ramps `w_eff` to zero over a configured window (default: days). Custodian tasks migrate gradually while the departing member is still serving. Expulsion remains abrupt by design — it is the rare, adversarial case.
4. **Prioritized, throttled recovery.** On any reshuffle, fetch order is by *current live replica count ascending* (items down to 1 copy first, items at 2 last), not item order, with a per-peer concurrent-fetch limit. Live counts come from the availability index (§5.6), so this is cheap. Standard practice in Ceph/Cassandra.
There is also a non-technical reason for the cap: a member holding half the library **is** the landlord the project exists to avoid. Concentration of storage is concentration of power, regardless of intent.


**Verification — storage challenges:**

- Periodically (default: each member challenges each of its *own items'* custodians weekly, randomized), send: `Challenge {blob_hash, byte_range}` → expect BLAKE3 of that range within timeout.
- Unforgeable without the data; cheap for honest nodes (one read + hash).
- Results recorded locally and gossiped as claims (`challenge_result` gossip messages, signed). Persistent failures across multiple challengers surface in UI as evidence → any member attaches it to a `ProposeExpel`. v1 keeps the *decision* human + core-group; the *evidence* is automated.

### 5.6 Availability layer (ephemeral — the "reachable items" answer)

- Never in replicated state. Reachability is liveness: per-observer, minute-to-minute. Writing it into a CRDT = unbounded tombstone garbage that's stale anyway.
- Each peer gossips a signed heartbeat every ~60 s: `{node_id, seq, holds_manifest_hash}` where the manifest (compact set of held item IDs — a Bloom-filter or roaring-bitmap digest, exact set fetchable on demand) is announced only when changed.
- Peers maintain an in-memory availability index with TTL (~5 min). UI marks items: `online (n providers)` / `custodians offline` / `unknown`.
- **Search flow:** query → tantivy → results returned immediately → availability resolved asynchronously (SSE pushes badge updates). Probing beyond the gossip index targets only the item's computed custodians (≤ `replicas` probes), not all holders.

### 5.7 Transfer

- `iroh-blobs` for all content. Provider selection: prefer online custodians, fall back to any online holder from the availability index. (Multi-source swarming beyond what iroh-blobs gives = phase 2; not a v1 goal since this isn't a distribution race.)
- Download → verify (BLAKE3 is inherent to the transfer) → register as holder → next heartbeat announces it.

---

## 6. Deduplication

### 6.1 v1 — exact
 
- Byte-identical files: same BLAKE3 → same blob, network-wide. Free via iroh-blobs.
- Same-set-different-item: prevented by the content-set fingerprint `item_id` (§5.2) — independently adding the identical set of content files converges on one item.
- Add-time assist: before creating an item, client computes the fingerprint and checks the catalogue; on a hit, warns and redirects to the existing item (offering to contribute any files it is missing rather than creating a second item).
- **Near-duplicate sets** (overlapping but not identical — e.g. the same 40 chapters plus a bonus track, or a different chapter split): different fingerprints by definition, so detect rather than prevent. Compute Jaccard similarity of the content-file hash sets against existing items; above ~0.8, flag in the UI as a merge candidate. **Never merge automatically** — route through the two-person merge rule (§6.2). Candidate scan runs on add and as a background sweep; it is a hint, not an action.


### 6.2 Phase 2 — semantic (schema reserved now)

- Work → Edition → File hierarchy (FRBR — Functional Requirements for Bibliographic Records: the library-science model where a *Work* is the abstract creation, an *Edition* a concrete publication, a *File* a digital manifestation).
- `works` namespace holds member-asserted equivalences (union-find style merges). Merges require confirmation by a second member (merging is CRDT-friendly; un-merging is not — hence the two-person rule).
- v1 only reserves the namespace and keeps `item_id` stable so items can later hang off Editions.

---

## 7. API & UI

### 7.1 JSON-RPC 2.0 over HTTP (axum), localhost by default

Method groups (sketch — Claude Code should flesh out request/response types):

```
library.search        {query, filters{type,genre,lang,author,series}, page}
library.item          {item_id} → full record incl. ratings summary, files, availability
library.add           {file_path | upload, metadata} → item_id
library.download      {item_id, file_index?} → task_id
library.edit_metadata {item_id, fields{...}}

community.rate / review / bookmark   {item_id, ...}
community.wish_create / wish_list / wish_comment / wish_fulfill

group.members / group.propose_add {node_id, name} / group.propose_expel {member_id, reason, evidence?}
group.pledge_set {bytes}

node.status           → identity, core group, sync state, storage usage vs pledge, custodianships
node.challenges       → recent challenge results (mine and gossiped)
admin.reindex / admin.gc
```

### 7.2 SSE event stream (`GET /events`)

Event types: `download.progress`, `availability.changed {item_id, providers}`, `catalogue.item_added`, `catalogue.item_changed`, `membership.changed`, `wish.changed`, `challenge.result`, `custodian.assignment_changed`, `sync.status`.

### 7.3 UI (Svelte 5)

- Pages: Search/Browse (with async availability badges), Item detail (metadata, files, ratings/reviews, download), Wishes, Members & group admin, My node (storage, pledge, custodianships, challenges, sync health).
- Svelte 5 runes; SSE-driven stores; no SSR — static build embedded via `rust-embed`, served by the same axum server.
- Deliberately minimal; functional over pretty. (This part is vibe-coded; keep component structure boring and flat.)
- Auth to local API: bearer token generated at first run, printed/stored in data dir; UI served only with token (protects the localhost port in shared-machine/container scenarios) - possible to switch off via program option.

---

## 8. Workspace layout

```
distlib/
├── Cargo.toml                  # workspace, pinned versions
├── crates/
│   ├── distlib-core/           # domain types, item/member records, ids, config
│   ├── distlib-consensus/      # openraft integration: membership log, storage (redb),
│   │                           #   iroh network layer, log-follower for non-core nodes
│   ├── distlib-sync/           # iroh-docs wrapper: namespaces, typed records,
│   │                           #   projection event stream. ONLY crate touching iroh-docs
│   ├── distlib-store/          # SQLite read model + tantivy index + projections
│   ├── distlib-net/            # iroh endpoint setup, ALPNs, allowlist enforcement,
│   │                           #   gossip (heartbeats, challenge results), blob provider
│   ├── distlib-custodian/      # rendezvous computation, pin manager, challenges
│   ├── distlib-api/            # axum: JSON-RPC + SSE + embedded UI
│   └── distlib/         # binary: wires everything, CLI (clap): run/init/invite/status
├── ui/                         # Svelte 5 app → built into distlib-api via rust-embed
├── docker/                     # Dockerfile (scratch/distroless + single binary), compose example
└── tests/                      # integration: multi-node in-process clusters
```

Config: single TOML file + env overrides (`DISTLIB_*`). Data dir: `~/.local/share/distlib` (platform-appropriate via `directories` crate) or `--data-dir`; everything (keys, redb, SQLite, tantivy, blobs) under it — container = mount one volume.

---

## 9. Implementation plan

Each phase ends runnable and demo-able. Suggested Claude Code session granularity = one checklist block.

### Phase 0 — Skeleton & transport (foundation)
- [x] Workspace, CI (fmt, clippy, test), pinned deps
- [x] `distlib-core`: ids (MemberId, ItemId, GroupId), config loading, error types
- [x] `distlib-net`: iroh endpoint with persistent keypair; ALPN registry; **allowlist hook at connection accept** (allowlist source stubbed as static config for now)
- [x] `distlib-daemon`: `init` (keygen), `run`, `status` (prints NodeId)
- [x] Two nodes connect (direct + via relay), ping over custom ALPN
- **Acceptance:** two containers on separate networks establish a connection and exchange a message; unknown NodeId is refused.

### Phase 1 — Membership log (Raft core)
- [x] `distlib-consensus`: openraft type config; redb log+state storage; iroh RPC network
- [x] MembershipEvent state machine; allowlist now derived from committed log
- [x] Log-follower mode for non-core nodes (fetch + verify + apply, gossip-notified)
- [x] `GroupFounded` bootstrap flow: `distlib init-group --core <ids...>`; join tickets
- [x] `propose_add` / `propose_expel` / `pledge_set` RPCs end-to-end
- **Acceptance:** 3-core-node cluster + 2 follower nodes; add a member → it can connect; expel it → open connection drops, reconnect refused; kill one core node → group still admits members.

### Phase 2 — Catalogue & library basics
- [ ] `distlib-sync`: namespaces, typed catalogue records, field-level keys, projection stream
- [ ] `distlib-store`: SQLite schema + projections; tantivy index; search API (internal)
- [ ] `distlib-net`: iroh-blobs store wired in; add-file flow (hash → blob → catalogue item)
- [ ] `library.add`, `library.search`, `library.item`, `library.download` via CLI first
- [ ] Exact-dup guard on add
- **Acceptance:** node A adds 3 ebooks; node B (fresh join) syncs catalogue, searches by author, downloads a file, and after restart still serves it.

### Phase 3 — API + UI
- [ ] `distlib-api`: axum, JSON-RPC methods from §7.1 (library.* + node.status), SSE stream, bearer token
- [ ] Svelte 5 UI: search/browse, item detail, downloads with progress (SSE), add-item form
- [ ] rust-embed integration; single-binary check on Linux/Win/macOS; Dockerfile
- **Acceptance:** everything from Phase 2 done through the browser; download progress streams live.

### Phase 4 — Availability + community metadata
- [ ] Heartbeats + holds-manifest over iroh-gossip; in-memory TTL availability index
- [ ] Async availability badges in search results (SSE `availability.changed`)
- [ ] `community` namespace: ratings, reviews, bookmarks; projections; UI
- [ ] Wishes end-to-end (create, list, comment, fulfill)
- **Acceptance:** take a providing node offline → badge flips within TTL without any replicated-state churn; two members rate the same item concurrently → both ratings visible everywhere.

### Phase 5 — Custodianship & quotas
- [ ] Weighted rendezvous computation from membership log (property-tested: determinism, minimal reshuffle, capacity proportionality)
- [ ] Pin manager: assignment diff → fetch/release, pledge-capped with deterministic overflow
- [ ] Pledge enforcement on add (2× rule)
- [ ] Challenge protocol (issue, respond, verify, gossip results); evidence view in UI; attach to expel proposal
- **Acceptance (the money demo):** 5 nodes, item with `replicas=3`; kill a custodian → next-ranked node auto-fetches within one cycle; a node that deletes its pinned data fails challenges from 2 peers and shows up red in the members view.

### Phase 6 — Hardening & release
- [ ] Multi-node integration test harness (in-process cluster; also compose-based chaos: partitions, restarts)
- [ ] Cold-start replay & `admin.reindex`; backup/restore of data dir documented
- [ ] Rate limits (gossip, challenges, blob serving), disk-full handling, graceful shutdown
- [ ] Docs: README, group-founding guide, self-hosted relay/DNS guide
- **Acceptance:** 10-node compose cluster survives scripted churn (random restarts + one partition) for 1 h with catalogue convergence verified by hash.

---

## 10. Testing strategy

- **Property tests** (proptest): rendezvous determinism & minimal-reshuffle; projection idempotency (replay N times = replay once); catalogue field-LWW convergence under shuffled delivery.
- **Simulation:** in-process multi-node harness (real iroh over localhost) as the default integration substrate — fast enough for CI.
- **Consensus:** openraft's own guarantees assumed; our tests target *our* storage + network layer (crash-recovery of redb log, follower catch-up) not Raft itself.
- **Chaos (phase 6):** compose + `pumba`/`tc`-style partitions.

---

## 11. Open questions (decide during implementation, don't block on them)

1. Heartbeat manifest encoding: Bloom filter vs roaring bitmap vs plain sorted-varint set — pick after measuring real catalogue sizes.
2. Challenge cadence & failure thresholds (defaults above are guesses; make them config).
3. iroh-docs entry pruning: docs retains history; decide a compaction story if metadata churn makes redb files grow noticeably (measure first).
4. Under-pledged custodian overflow: current rule is deterministic skip-to-next; alternative is proportional scaling of all pledges. Revisit with real numbers.
5. Multi-device members, encryption-at-rest + epochs, semantic dedup, group-configurable admission policies → phase 2+ backlog.

---

## 12. Reference pointers for implementation

- iroh: https://github.com/n0-computer/iroh — endpoint, Router, ALPN pattern
- iroh-docs: https://github.com/n0-computer/iroh-docs — study `Docs`/`Engine` setup with blobs+gossip; range-based set reconciliation paper (Aljoscha Meyer) for internals
- iroh-blobs, iroh-gossip: same org; gossip = HyParView + PlumTree papers
- openraft: https://github.com/databendlabs/openraft — implement `RaftLogStorage`, `RaftStateMachine`, `RaftNetwork`
- Weighted rendezvous hashing: Thaler & Ravishankar; the `-w/ln(h)` scoring is the standard logarithmic method
- Prior art worth skimming for the p2p patterns: `sendme`, `dumbpipe` (n0 examples), Ivan's `simple-p2p`
