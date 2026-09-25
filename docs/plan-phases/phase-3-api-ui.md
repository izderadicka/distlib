# Phase 3 — API & UI

Sequencing plan for §9's Phase 3. Written before any code, and revised in place if a PR proves part
of it wrong — an entry that turned out to be mistaken is more useful corrected than deleted.

---

## Context

Phase 2 is complete and merged: the catalogue converges across the group, every node answers queries
from its own SQLite + tantivy projection, files move as content-addressed blobs, and all of it is
reachable from the CLI. The thirteen items phase 2 carried out of itself were triaged into C1–C13,
two of which — C4 and C10 — were closed on the way out.

Phase 3 is §9's "API + UI". §9's acceptance criterion is the target: *everything from Phase 2 done
through the browser; download progress streams live.* The first half is reach — every flow the CLI
can drive has to be drivable from a page. The second half is the one that costs work: nothing in
this workspace can currently name a piece of work in progress, let alone report on it.

**It is also the first phase where a non-Rust toolchain enters the repo.** npm, a lockfile, a Node
version and a build step that `cargo build` must not depend on. That is the phase's one real risk
and it is stated once, below.

**This document is sequencing** — what gets built, in what order, in which PR, and what each PR has
to demonstrate. Design deviations from [`distlib-plan.md`](../distlib-plan.md) still go into
[`plan-deltas.md`](../plan-deltas.md), in the PR that causes them. There must not be two sources of
truth for a deviation.

---

## Ground truth established before planning

Verified against the tree and against the vendored crate sources in `~/.cargo/registry`, not against
docs.rs prose.

1. **`axum::response::sse` is ungated in the pinned `axum 0.8.9`** — `pub mod sse;`
   (`src/response/mod.rs:7`), with `Sse::keep_alive` (`sse.rs:74`) and
   `KeepAlive::interval` (`sse.rs:517,534`). Server-sent events need no feature change and no new
   dependency.

2. **Download progress already exists, and we throw it away.** `Downloader::download`
   (`iroh-blobs-0.103.0/src/api/downloader.rs:404`) is **not** an `async fn`: it returns a
   `DownloadProgress`, which implements `IntoFuture`. [`blobs.rs:127`](../../crates/distlib-net/src/blobs.rs)
   `.await`s it, which takes the `IntoFuture` path and discards every event. The other path is
   `DownloadProgress::stream()`, yielding `DownloadProgressItem`. Three properties of that stream
   shape the work rather than decorate it:
   - **`Progress(u64)` is a cumulative byte offset with no denominator.** The total has to come from
     our own `FileRecord::size`.
   - **It is not monotonic.** A provider failover restarts the offset, so a naive bar goes backwards.
   - **An already-held blob emits nothing at all**, and `complete()` is private — its contract is
     three rules that have to be reimplemented: `Error(e)` fails, `DownloadError` fails, and
     **exhausting the stream with neither is the only success signal there is**.

3. **A slow document subscriber stalls this node's sync.** `Subscribers::send`
   (`iroh-docs-0.101.0/src/engine/live.rs:957`) awaits *every* subscriber through
   `futures_buffered::join_all`; the channels are `async_channel::bounded(256)`
   (`engine.rs:37,212`); and a full channel **waits** — the removal branch fires on `is_err()`, i.e.
   the receiver being gone, never on the channel being full. The call sits inside the live actor's
   `run_inner` select loop. **Blast radius: this node's live actor**, which drives every namespace on
   the engine. Other nodes go on syncing; this one falls behind and stops answering sync attempts.
   **We are not exposed today** — the only subscriber is the pump at
   [`changes.rs:126`](../../crates/distlib-sync/src/changes.rs), which takes a mutex, notes an id and
   releases it. Phase 3 is the first phase with a reason to add a second one, which is why this is
   ground truth and not a footnote.

4. **`MembershipNode::subscribe()` is ready as it stands** —
   [`node.rs:602`](../../crates/distlib-consensus/src/node.rs) hands out a
   `tokio::sync::watch::Receiver<MembershipState>`: multi-consumer, conflating, and already used by
   `distlib run` to log changes. Membership events need no new plumbing in consensus.

5. **`item_added` and `item_changed` are not derivable from what the store returns today.** The
   public `Store::upsert_item` ([`store.rs:102`](../../crates/distlib-store/src/store.rs)) returns
   `Result<()>`, and the free function it calls (`store.rs:261`) is
   `INSERT … ON CONFLICT(id) DO UPDATE SET …`, whose row count is dropped on the floor. Telling the
   two apart wants a `SELECT 1 … WHERE id = ?1` **inside the transaction that is already open** —
   not a second round trip, and not a guess from whether the projection had seen the id before.

6. **Nothing in the workspace can name a piece of work in progress.** No task id, no registry, no
   broadcast channel anywhere under `crates/`. **What that costs is the phase acceptance itself:**
   §7.1 wants `library.download` to answer with a `task_id` and §7.2 wants `download.progress` *for
   that id*, and neither has anywhere to live. A progress event with nothing to attach it to is
   unusable, and a second download started while the first is running is indistinguishable from it.
   So the registry is not a nicety that could be deferred to phase 4; it is what makes "download
   progress streams live" expressible at all, and [3a-5](#3a--api-and-events-7-prs) builds it.

   **`ReindexHandle` is not a base to build on.**
   [`projection.rs:76`](../../crates/distlib-store/src/projection.rs) is
   `mpsc::Sender<oneshot::Sender<()>>`: queue depth one, reply type `()`, one operation in flight, no
   id, no progress, and no way to ask what is happening. One idea carries over and nothing else —
   that the API holds a cheap clone-able handle while the task owns its own lifetime — along with its
   test double ([`rpc.rs:39`](../../crates/distlib-api/tests/rpc.rs)) as the precedent for keeping
   such a handle mockable.

7. **There is nothing to call for browse.** `SearchIndex::search`
   ([`index.rs:246`](../../crates/distlib-store/src/index.rs)) goes through tantivy's `QueryParser`,
   which has no match-all, and `Store::items` (`store.rs:174`) is unpaged — every row, ordered by id.
   The paging primitive exists upstream: `TopDocs::and_offset` (`tantivy-0.26.2`).

8. **`library.add` takes `files: Vec<PathBuf>` and nothing else**
   ([`methods.rs:1215`](../../crates/distlib-api/src/methods.rs)), and a browser cannot supply a real
   path — `<input type=file>` yields `C:\fakepath\name.ext` by specification. §7.1 already sketches
   `{file_path | upload, metadata}`, so a second door for uploaded bytes honours the design rather
   than deviating from it.

9. **`tower-http 0.6.11` is already in [`Cargo.lock`](../../Cargo.lock)** transitively, so the
   upload route's body-limit layer costs no new crate. axum's `DefaultBodyLimit` is 2 MB and has to
   be lifted on that one route.

10. **The API has one route and two response formats.** `POST /rpc` answers JSON-RPC, but an auth
    failure returns **plain text with a 401** ([`lib.rs:101`](../../crates/distlib-api/src/lib.rs)),
    and a malformed body does likewise. The comment there gives the reasoning — the request never
    got as far as being a call — which is sound about *status* and wrong about *body*: every caller
    needs two parsers and has to guess which it got before reading. Settled by [D9](#design-decisions-taken-up-front).

11. **`rust-embed` is not a dependency, not in the lockfile and not vendored**, and there is no
    `ui/` anywhere in the tree. Its behaviour when the embedded folder is missing **cannot be
    verified offline**, which is why [D7](#design-decisions-taken-up-front) does not rely on it.

12. **The CI matrix is a one-element list** — `os: [ubuntu-latest]`
    ([`ci.yml:38`](../../.github/workflows/ci.yml)) — and
    [`private_file.rs`](../../crates/distlib-core/src/private_file.rs)'s hardening is `#[cfg(unix)]`.
    So `api.token` is probably unprotected on Windows and nothing reports it. P0-6 has now been
    deferred twice; phase 3 is the phase that ships a binary to people.

13. **The only §7.1 gap phase 3 owns is `library.edit_metadata`.** `community.*` and `wish.*` are
    phase 4, `node.challenges` is phase 5. Ten of §7.1's twenty methods exist; where the built ones
    differ from the sketch, P2-21 and P2-24 already record why.

14. **The CLI falls back to the database when the node is down**
    ([`commands.rs:1030`](../../crates/distlib/src/commands.rs)) — it asks `node.status` and reads
    the files only when nothing answers. **A browser has no such fallback.** "Is the node running"
    becomes a UI state with no CLI equivalent, and it is the one piece of the UI that cannot be
    copied from an existing command.

---

## The structural decision: where events come from

There are two ways to feed `GET /events`, and the choice is structural rather than stylistic.

The first is a second `doc.subscribe()`. It compiles today — `Api` holds a `Catalogue` and
`Catalogue::changes()` is `pub` — and it is wrong for ground truth 3. A browser tab that has been
backgrounded, or a client on a slow link, is the textbook slow consumer, and a slow consumer on that
channel does not get dropped: it holds the live actor, and with it every namespace this node syncs.
A page left open on a laptop lid would be able to stop this node syncing.

The second is a `tokio::sync::broadcast` tap fed **by the projection**. It is lossy by design — a
receiver that falls behind gets `Lagged(n)` and skips ahead, which is exactly the right behaviour for
a stream whose consumers are browser tabs — and the projection is the one place that knows both what
changed and what the new row says.

**Decision: the projection publishes; nothing else subscribes to the document.** This is a rule PRs
are reviewed against, on phase 2's precedent of naming a mitigation as a reviewable rule rather than
a preference. The rule has a second half that is easy to lose: **the publish is
`let _ = tx.send(event);` — synchronous, lossy, never awaited.** Replacing it with a bounded `mpsc`
"so that no event is dropped" reintroduces exactly the stall this decision avoids, one layer up, and
it would look like an improvement while doing it.

One nuance, so that nobody has to re-derive it: `broadcast::send` *is* synchronous and lossy, so a
tap placed on the pump itself would technically be safe too. The reason the pump stays trivial is
that its safety would then depend on nobody ever adding an `.await` to it.

---

## Design decisions taken up front

**D1 — Events are published by the projection, never by a second document subscription.**
As above. It is the phase's structural rule and the reason the event bus is built in 3a-1 rather
than grown out of whatever the first page happens to need.

**D2 — Every event carries ids, never values. The UI's only reaction is "refetch this id".**
The same rule [`changes.rs`](../../crates/distlib-sync/src/changes.rs) already states for its
`Batch`. Three things fall out of it for free: a new event type can never break an existing page,
`Lagged` is handled by the same refetch path as everything else rather than by special-case
recovery, and a page can be built before the events that drive it exist. The cost is one extra round
trip per change, against a local loopback API.

**D3 — One auth mechanism, and the static assets are not behind it.**
A bearer header on `/rpc`, `/events` and `/upload`; the assets served open; the token handed to the
page in the URL fragment. `distlib run` prints `http://127.0.0.1:11280/#token=…`, the SPA reads
`location.hash`, stores it in `sessionStorage` and strips it from the address bar. **Fragments are
never sent to the server**, so the token is not in an access log, a `Referer` header or a proxy
trace — which is exactly what a query parameter would get wrong. A cookie was rejected for a
different reason: it would be a second authentication path, and a credential the browser attaches
automatically turns `POST /rpc` into a CSRF target that then needs its own defence.

**The header choice rules out `EventSource`**, and that is a consequence rather than an aside: the
browser's own SSE client cannot set request headers, so the UI reads `/events` with `fetch()` and a
`ReadableStream`, parsing frames itself. Reaching for `EventSource` in 3b-1 leads straight back to a
token in the query string, which is the thing this decision rejects.

This is a delta against §7.3's "served only with token", and the trade is stated rather than buried:
an unauthenticated caller can fetch the HTML and the JavaScript bundle. They are the same bytes for
every group and contain nothing about this node; every byte that *is* about this node sits behind the
header. Alternative rejected explicitly: gating the assets means the browser cannot set a header on
its first navigation, which forces either a cookie or a token in the path — both worse than what
they would be protecting.

**D4 — `library.download` becomes asynchronous and answers `{task_id, files}`.**
This departs from [`methods.rs:565`](../../crates/distlib-api/src/methods.rs)'s own prediction that
turning it into a task later would be "an addition rather than a change: the answer gains a field".
That was written before there was a browser. axum drops a handler's future when the client
disconnects, so a page reload part-way through a synchronous download would silently kill it — and
"silently" is the operative word, since the bytes already written stay on disk.

It stays cheap because **all three validations stay synchronous, before the spawn**: the destination
is a directory, no filename collides, no filename is anything but plain. Every existing refusal test
is therefore untouched, and only the success path changes shape. Record the departure as a delta.

**D5 — A browser adds files by raw streaming upload, not multipart.**
`POST /upload?filename=…` streams the body to a staging directory **inside the data dir** — the same
filesystem as the blob store, already covered by its permissions, which `/tmp` is not — and then
calls the same `Catalogue::add_file` the CLI path calls. `library.add` gains an `uploads` field
beside `files`, with exactly one of the two non-empty. One deduplication implementation, two doors.

Multipart was rejected: it pulls in `multer` and buys nothing for a single file whose name is
already in the query string. The costs are real and stated: the first non-JSON-RPC route in the API,
a size cap that has to be chosen, staged-file cleanup on both the success and failure paths, and
**the bytes are written twice** — once to staging, once into the blob store.

**D6 — `sync.status` is deferred to phase 4.**
Its material is the three events [`changes.rs:160`](../../crates/distlib-sync/src/changes.rs)
deliberately discards — `NeighborUp`, `NeighborDown`, `SyncFinished` — and reaching them means
touching the pump, widening `Batch`, or adding the second subscription D1 forbids. What replaces it
costs nothing: the catalogue tap says when the read model changed, `node.status` covers identity and
group, and **the SSE connection is itself the liveness signal** — it drops when the node does, so
reconnect-with-backoff gives the UI live-versus-stale for free. That also answers ground truth 14.

**D7 — `cargo build` must work on a machine with no npm.**
A committed `ui/dist/` holding a placeholder page that says, in plain text, that this binary was
built without the UI and how to build it. CI and the release job overwrite it with the real bundle.
Two alternatives rejected: a cargo feature (splits the test matrix and makes the default build the
broken one), and committing the real `dist` (a build artefact in every review diff, going stale
silently whenever somebody forgets).

**D8 — Group admin is read-only in the UI this phase.**
§7.3's page list includes group administration, but §9's phase-3 checklist names search/browse, item
detail, downloads and add-item, and the acceptance criterion is the library flow. One click that
expels a member needs a confirmation surface — and a story for what happens when the proposal is
pending approval — that this phase should not be designing in passing. Members and node state are
shown; nothing on a page can change them. Stated as a decision so that §7.3's page list does not
make it silently.

**D9 — Every response from every route is JSON, with no exceptions.**
A client that has to parse a body two ways depending on which failure it hit is a client that will
get it wrong once and then hide the real error. The auth and malformed-body paths return a JSON-RPC
error object — `rpc.rs`'s existing `Error`, which already serialises — instead of plain text, and
`/upload` answers JSON on both its paths too.

**The HTTP status stays exactly as it is.** 401 is still 401, because a browser and a proxy both read
it, and JSON-RPC's own error code is carried in the body beside it; the two are not in competition,
which is what ground truth 10's comment was half-right about. Existing clients are unaffected on the
success path, and `ClientError::Unauthorised`
([`client.rs:92`](../../crates/distlib-api/src/client.rs)) already keys off the status rather than
the body. Landed in 3a-1, where the auth check is being extracted into middleware anyway, so it is a
few lines rather than a refactor of its own. Worth a delta: §7 says nothing about error transport, so
this fills a gap rather than contradicting one.

---

## Sub-phases

Twelve PRs, plus C12, which is a test with no phase of its own (below). Each ends compiling, tested, and — from 3a-1 on — demonstrable by hand with `curl` or
the CLI. One PR at a time, review before the next.

### 3a — API and events (7 PRs)

Rust only. Every step is verifiable without a browser, which is the point of doing all of it before
any of 3b.

- **3a-0 — widen the CI matrix.** Alone, and first. P0-6 has now been deferred twice, phase 2 added
  three on-disk stores, and ground truth 12 says the token file is probably unprotected on Windows.
  Budget generously: "whatever it breaks" is the deliverable, and finding it here is far cheaper
  than finding it while also debugging rust-embed and a Dockerfile.
  **Acceptance:** CI green on Linux, macOS and Windows; the token file is either given equivalent
  protection on Windows or the gap is recorded as a delta with the reason.

- **3a-1 — the event bus and `GET /events`.** An `Event` enum in `distlib-core`; a
  `broadcast::Sender<Event>` **created by the binary** at
  [`commands.rs:129`](../../crates/distlib/src/commands.rs) and handed to both `Projection::start`
  and `Api` — not created by the projection, because download events come from `distlib-api` and a
  bus owned by one producer is a bus the other has to reach through it. The auth check moves into
  shared middleware; the route is `Sse` with a keep-alive; and the one producer this PR wires is an
  adapter over `MembershipNode::subscribe()`. **Build the bus now rather than a watch-to-SSE
  adapter** — a route shaped around a conflating `watch` would have to be reshaped the moment a
  second producer arrives. **D9 lands here**, while the auth check is being moved anyway.
  **Acceptance:** connect with the token, add a member, read a `membership.changed` frame; connect
  without the token and get a 401 whose body parses as a JSON-RPC error *before any frame arrives* —
  and the same for `/rpc`, so that no caller needs a second parser.
  **Watch for:** `Lagged(n)` is not the stream ending — map it to a `resync` event the page handles
  with the same refetch as everything else; capacity ~256. Do not pull in `tokio-stream` for
  `BroadcastStream`: `Lagged` has to be matched explicitly anyway, so a short `unfold` over the
  receiver keeps the handling where a reviewer can see it. Never put a compression layer in front of
  `/events` — it buffers a stream into lumps, which is the one thing a live stream must not do. And
  **this is where the `Api` struct-literal tax is paid or refused**: eight `pub` fields across six
  construction sites (one in production, five in tests), and this phase adds a bus, a task registry
  and a staging directory. Bundle them behind one struct here, or decide explicitly to keep paying.

- **3a-2 — the catalogue tap.** The projection publishes after it commits; `upsert_item` reports
  created-versus-updated (ground truth 5); `catalogue.item_added` and `catalogue.item_changed` reach
  the stream.
  **Acceptance:** an item written on A produces `item_added` for a subscriber on B; a second write to
  the same item produces `item_changed`; and a subscriber that stops reading falls behind rather than
  stalling the projection — the claim D1 exists to protect, so it is asserted rather than argued.
  **Watch for:** two ordering rules that are correctness rather than polish. **Emit after the
  commit**, not per item — `project()` commits tantivy once at the end of a batch, so an event fired
  inside the loop sends a page to `library.search` before the item is findable. And **`replay` must
  emit nothing**: it runs on every start, not only a cold one (C7), so emitting would push every
  subscriber straight into `Lagged` at startup. A rebuild of the read model is not news.

- **3a-3 — `library.edit_metadata`.** The one §7.1 method phase 3 owns (ground truth 13).
  **Acceptance:** a title edited on A converges to B, `item_changed` arrives, and `library.item`
  shows the new value on both.

- **3a-4 — progress-aware fetch.** `Blobs::fetch_with_progress` beside the existing `fetch`,
  reimplementing the terminal contract of ground truth 2 — because `complete()` is private, the three
  rules are ours to get right, and a fetch that reports success on a stream that ended in an error
  would be worse than no progress at all.
  **Acceptance:** a real two-node fetch reports increasing offsets that reach the blob's size; an
  unreachable provider reaches a failed terminal state rather than hanging or reporting success.
  **Watch for:** progress is not monotonic, so the *client* clamps to a high-water mark. A bar that
  jumps backwards on a provider failover is the library being honest, not a bug to fix in the stream.

- **3a-5 — download as a task.** A task registry in `distlib-api/src/tasks.rs` — not in
  `distlib-store`, whose identity is "these tables are a function of the document", and not a new
  crate for one map. In memory, capped, and pruned, because a registry that grows for the life of the
  process is a leak with a nice name. **On connect, the stream replays a snapshot of in-flight tasks
  before going live**, so a page reload keeps its progress bar rather than watching a download it can
  no longer see. Adds `library.task`, the `download.progress` / `download.finished` /
  `download.failed` events, and a wait path in `distlib download` — the cheapest real client that
  exercises the whole task lifecycle with no browser in the room.
  **Acceptance:** `distlib download` still blocks and still reports the same files (D4 changes the
  method, not the command); a subscriber sees progress climb to the total and **exactly one**
  terminal event.

- **3a-6 — browse and paging.** `library.list {offset, limit}`, `offset` on `library.search`, and a
  paged `Store::items`. No `filters` object — P2-21 stands, and a browse page does not need one.
  **Acceptance:** 250 items read in three pages with no overlap and no gap, and the same at an offset
  past the end.

### 3b — UI (4 PRs)

Svelte 5, as §7.3 specifies. §7.3 says minimal and flat, and "boring" is the instruction rather than
a hedge.

- **3b-1 — the shell.** Vite + Svelte under `crates/distlib-api/ui/`, `rust-embed` over `ui/dist`,
  an SPA fallback route, the `#token=` handoff, a typed RPC client, an SSE client with reconnect and
  backoff, and a read-only members / node page. This is the highest-leverage PR of the phase: until
  it exists, every other UI PR is unverifiable end to end.
  **Acceptance:** `distlib run` prints a URL; opening it shows this node's id, its group and its
  members, updating live when a member is added from another node's CLI.
  **Watch for:** **no CORS layer, anywhere.** The Vite dev proxy is server-side, so the browser only
  ever sees one origin — said here explicitly so that nobody adds a layer that then has to be
  secured. And clippy runs `--all-features -D warnings`, so an `#[allow]` on the embed module
  pre-empts a lint against generated code landing at the worst possible moment.

- **3b-2 — search, browse and item detail.** Including `last_modified`, which the projection already
  has; `added_by` and `created` do not exist to show (C8).
  **Acceptance:** a catalogue written on another node is searchable, browsable and readable in the
  browser.

- **3b-3 — download with progress.**
  **Acceptance:** the second half of §9's criterion — a download started in the browser streams its
  progress live, and survives a page reload.

- **3b-4 — add item and edit metadata.** The upload door of D5, and `library.edit_metadata` from a
  form.
  **Acceptance:** a file chosen in a file picker becomes an item that another node can see and
  download.

### 3c — packaging (1 PR)

- **3c-1 — release and Docker.** A CI job that builds the UI and embeds it, a Dockerfile, release
  artefacts for the three operating systems 3a-0 made green, and a fix to
  [`lib.rs`](../../crates/distlib-api/src/lib.rs)'s doc comment, which currently promises TLS is
  "phase 3's" — the honest sentence is a reverse proxy, and the Dockerfile is where binding to
  something other than loopback stops being hypothetical.
  **Acceptance:** a downloaded binary with no adjacent files serves the real UI on each OS, and
  `docker run` does the same.
  **Watch for:** the reverse-proxy note has to mention response buffering. Most proxies buffer a
  proxied response by default, which turns a live event stream into delayed lumps, so whoever puts
  one in front of `distlib` needs buffering off for `/events` — `proxy_buffering off` in nginx, and
  the equivalent elsewhere. That belongs in the deployment note rather than in the Rust: emitting
  nginx's own `X-Accel-Buffering` header would be a guess about which proxy somebody chose, and it
  does nothing for the loopback and dev-proxy cases that are the only ones this phase ships.

### C12 — no PR slot

**C12 is not given a slot of its own.** It is a test of about fifty lines — promote a node, demote
it, promote it again — blocked on nothing and related to nothing else here. Land it whenever the
queue is free, in whichever sub-phase it happens to fall between.

---

## Carried forward from Phase 2 — take or defer, explicitly

Every C-number appears here, so that the phase-2 triage has a successor rather than going quiet.

| # | Item | Phase 3 |
|---|---|---|
| **C1** | A full peer offer is O(N²) dials | **Deferred.** Waiting on a measurement from a group larger than anything phase 3 stands up. Unchanged by an API. |
| **C2** | Content nobody will serve is asked for for ever | **Closed** in phase 2 (P2-18). |
| **C3** | The sweep reads the whole document every five seconds | **Deferred.** Cost only, and dwarfed by what a round costs when anything *is* missing. Wants a catalogue big enough to measure on. |
| **C4** | A restarted core node cannot refill its own directory | **Closed** on the way out of phase 2. |
| **C5** | Nothing asks again when a member cannot be resolved | **Deferred to phase 4**, with the decision the row was waiting on taken here: **one method noticing on the node's behalf is enough for now.** `library.download` detects a failed fetch and refreshes the directory once; generalising that across three layers which disagree about what "cannot be resolved" means is work that wants a reason, and §5.6's availability index — phase 4 — is what gives it one. |
| **C6** | A field blinks out of the read model while its newest value is in flight | **Deferred**, as phase 2 decided. Phase 3 *surfaces* it — a title that blinks is now something a person watches happen — but the first step is still the counter, not the fix, and D2 softens it: a page refetches on the next event either way. |
| **C7** | The read model is replayed in full at every start | **Deferred.** Unchanged, and now load-bearing in one new place: 3a-2's rule that `replay` emits nothing exists because this replay runs on every start. |
| **C8** | `added_by`, `created`, `modified_by` have nowhere to come from | **Deferred to phase 4.** The phase-2 row named phase 3 as the trigger — "revisit when a phase actually wants to show who added something" — and the answer, taken here, is that the item page shows `last_modified`, which is already projected. Adding the other three is a change to the *catalogue* (new entries, and last-writer-wins on a field two members set differently), not to the read model, and it is not what §9's acceptance asks for. |
| **C9** | `PENDING_EXPIRY` is one fixed count | **Deferred to phase 5**, where §5.5's policy-event machinery lands. |
| **C10** | The fast lane is not fast | **Closed** on the way out of phase 2. |
| **C11** | A demoted core node keeps running as a voter | **Closed** in phase 2 (MEM-04). |
| **C12** | A node cannot be promoted, demoted and promoted again | **Taken**, without a PR slot of its own — see [C12 — no PR slot](#c12--no-pr-slot). |
| **C13** | Gossip does not change sides when a node is promoted | **Deferred.** Nothing blocks it and nothing wants it; unreachable by anything the code can currently produce. |

---

## Testing and the lanes

The split holds: `slow-tests` on by default, `cargo test-fast` during development, `cargo test-all`
before a task is done. C10 closed with the fast lane at 6.7 s and it should stay in that shape.

**Fast lane**: the event enum's serialisation, the `Lagged`-to-`resync` mapping, paging arithmetic,
created-versus-updated in a single-node store, and the task registry's cap and pruning — all
single-node, none timer-bound.

**Slow lane**: anything with two nodes, every blob transfer, and the whole of 3a-4 and 3a-5's real
fetches.

**SSE is tested with the hyper-util client already in
[`rpc.rs`](../../crates/distlib-api/tests/rpc.rs)**, reading frames through
`http_body_util::BodyExt::frame()`, with every read wrapped in `tokio::time::timeout` — and
**never asserting an exact event sequence.** The catalogue's re-offer timer and the membership watch
fire on their own schedules, so the assertion is always "an event matching X arrives within N
seconds", never "the next event is X". A test that pins a sequence here is a flake with a date on it.

**The JS build is not a cargo test.** It gets its own CI job, and a UI test beyond "it builds" is out
of scope for this phase — said plainly so that its absence is a decision rather than an oversight.

---

## Risk, stated once

**The npm supply chain.** §7.3 specifies Svelte, so this phase adds a dependency tree nobody in this
workspace audits, running with the privileges of whoever builds a release. There is no version of
this phase that avoids it short of hand-writing the UI, and that trade was already made in the
design.

So: take it, and fence it. The mitigation is a rule PRs are reviewed against, in the same way §5.1's
"wrap, don't expose" was phase 2's:

- a committed lockfile and a pinned Node version, so two builds of one commit agree;
- `npm ci`, never `npm install`, in CI and in the release job;
- no dependency added without a reason recorded in the PR that adds it;
- and **the UI build kept out of the path that produces a `cargo test` result** (D7), so a compromised
  or merely broken package cannot decide whether the Rust suite passes.

---

## Verification

Per PR, before review:

- `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` — clean.
- `cargo test-all` — green. `cargo test-fast` unchanged in shape; if a PR moves it noticeably, say so
  in the PR body.
- Any test asserting a new mechanism gets a **mutation check**: delete the line the test is about,
  confirm the test fails, restore.

At the end of the phase:

- §9's Phase 3 acceptance driven through the browser: everything from phase 2 done from a page, with
  download progress streaming live.
- The same criteria added to [`manual-check.md`](../manual-check.md) as a by-hand §11 — because the
  by-hand run is what found P1-39, P1-40 and P1-41 in phase 1, and three more mistakes in phase 2
  that no test noticed.
