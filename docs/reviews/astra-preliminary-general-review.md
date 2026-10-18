# distlib: concept, architecture, and code organization review

**Reviewed snapshot:** [`93bcae7993edf08945e616b20af2016d34d33b68`](https://github.com/izderadicka/distlib/tree/93bcae7993edf08945e616b20af2016d34d33b68), `master`, 17 October 2026.  
**Reviewer:** GPT-6 Astra.  
**Scope:** general design and implementation organization, with concrete briefs for subsequent technical reviews. No repository changes were made.

## Overall assessment

**The project has a coherent architecture worth continuing.** Its strongest choice is to give different kinds of state different consistency guarantees: membership and group policy are ordered through Raft; catalogue and community records converge through iroh-docs; presence and holdings are observer-local; SQLite and Tantivy are disposable projections. The implementation substantially follows that separation rather than merely describing it.

This is also more developed than the README's phase summary suggests. At this snapshot, phase 4 functionality exists, and phase 5 has implemented blob retention foundations, group settings, weighted custody computation, creator attribution, and the service that follows custody assignments. Automatic custody fetching, budget enforcement, and verification remain later work. **The preservation promise is therefore still being built:** calculating who should keep an item does not yet make those copies exist.

The main risks are at the boundaries between otherwise sensible components: membership recovery after log compaction; the gap between a committed decision and all its effects; catalogue changes becoming durable blobs and trustworthy custody inputs; and projection failures becoming visible, recoverable application states. I would prioritize those over a large restructuring or a change of technology.

The engineering discipline is a strength: pure decision logic, meaningful domain types, real multi-node tests, dependency pinning, explicit design deltas, and documented findings from manual use. The largest maintainability weakness is the cost of reconstructing the *current* design from the original plan, several phase plans, long delta tables, and historical comments.

## 1. What the system actually promises

The useful product concept is a **community-owned, replicated collection with an eventual preservation policy**, rather than simply peer-to-peer file transfer. Exact content identity, member-owned keys, a searchable local catalogue, and automatic serving of retained downloads support that concept well.

The trust model is an appropriate simplification for friends and clubs. It accepts crash-fault consensus and social handling of misconduct; it does not attempt to tolerate malicious Raft voters. Signatures establish authorship, and the endpoint allowlist controls direct participation, but neither turns Raft into Byzantine consensus nor erases data already received by an expelled member. Those are deliberate boundaries, not omissions to “fix” with a cryptographic redesign. See the [original vision and threat model][design].

Two product statements need more precise operational wording:

- **“No central server” still requires a sufficiently available core quorum.** Catalogue activity can continue between members when the core is unavailable, but membership changes need Raft. A two-founder quickstart is useful for a demonstration; both voters are needed for its majority. Explain the distinction between a demo topology and a group intended to survive one voter going offline.
- **“No administrator” means no single central operator, not equal authority for every member.** The current fold requires one core approval for an ordinary admission or non-core expulsion, a majority for voter-changing decisions, and no external approval for a member's own pledge. That is a reasonable governance policy, but users should see this current policy directly rather than infer it from the original plan's different expulsion description. The [actual approval rules][membership] are the authority.

A third distinction matters before phase 5 is presented to users: **assigned, retained, announced, reachable, and verified are different facts.** A custodian assignment is an obligation; a keeping tag protects local bytes from GC; a heartbeat is a signed claim; a provider count is this observer's current view; a successful fetch or probe provides stronger evidence. The code already separates many of these concepts. Preserve that separation in API fields and UI language.

## 2. Architecture and domain model

The nine-crate workspace has sensible boundaries. It is not suffering from an excess of crates.

| Area | Current responsibility | Assessment |
|---|---|---|
| `distlib-core` | IDs, records, configuration, community types, group settings, keeping reasons, pure custody scoring | A useful shared vocabulary. Pure custody computation fits here; it does not need its own framework. |
| `distlib-net` | Endpoint, transport, address knowledge, connection pooling, membership hooks, media transfer | Correct home for a security check that must cover third-party protocol handlers. |
| `distlib-consensus` | Signed membership events, deterministic fold, persistent Raft integration, non-voting log following, voter reconciliation | The most sensitive subsystem; pure policy and asynchronous enactment are visibly separated. |
| `distlib-sync` | iroh-docs integration, typed catalogue/community access, event pump, metadata repair, availability | Contains the difficult adapter behavior. Some internal subdivision would help, while retaining this crate boundary. |
| `distlib-store` | SQLite projection, Tantivy index, replay and local queries | A rebuildable read model is the right default at this stage. Its failure contract needs strengthening. |
| `distlib-custody` | Settled membership view, whole-catalogue and incremental custody computation | An appropriate new service boundary. It currently computes assignments; it does not yet maintain physical replicas. |
| `distlib-api` | JSON-RPC, SSE, uploads, downloads and their task state, application operations | Practical today; increasingly contains application orchestration as well as HTTP adaptation. |
| `distlib-ui` | Embedded assets and the Svelte application | Flat components and a small client layer suit the product. |
| `distlib` | CLI and runtime composition/lifecycle | Correct place to assemble the endpoint, stores, services, and shutdown order. |

The [runtime][runtime] makes the essential sharing explicit: one endpoint, one gossip swarm, and one blob store used for both document values and media. That last point is important: a downloaded file becomes serveable because it lands in the same store the blob protocol serves. The composition code also explicitly accounts for startup subscription order and shutdown order. Those details are substantive architecture, not merely plumbing.

The primary flows are understandable:

1. A membership proposal is signed, committed, and checked by the pure membership fold. Subscribers derive the allowlist, membership views, and desired core configuration; a separate reconciliation loop makes Raft's voter set follow that desired state.
2. A catalogue or community write enters iroh-docs. A single subscription is drained into coalesced invalidations. The projection rereads affected records, updates SQLite and the search index, then tells local clients what changed.
3. A download fetches verified blobs, retains the store's copy, and exports a separate user-owned copy. Holdings feed the heartbeat service, whose received statements produce local availability.
4. Custody combines **membership/settings and catalogue-derived item facts**. It waits for local membership settling before recomputing, and reacts to changed items and completed replays.

The domain choices generally support these flows:

- Public keys are validated member identities; opaque identifiers are distinct types. Avoiding a fabricated default `MemberId` is a particularly useful boundary.
- An item is identified by its initial content-hash set, with field-level and per-file entries. This supports independent metadata edits and exact duplicate convergence without requiring semantic deduplication first.
- Community ownership is checked against the signed document author, not merely a member ID embedded in a key. Per-author keys are a good fit for ratings, reviews, and bookmarks.
- The current implementation uses **one group document with prefixes**, not the original plan's separate catalogue/community namespaces. That is a reasonable simplification because the audience is presently the same.
- Creator attribution is now derived from file-key authors, while custody counts an active creator's copy toward the replica target. This removes a contradictory free-form `added_by` field, but introduces a preservation assumption that deserves a focused review below.

Relevant implementations: [catalogue schema][schema], [document adapter and item reads][catalogue], [community author validation][community-read], [pure custody scoring][scoring].

## 3. What is already strong

### Authority is enforced at the correct layers

The allowlist is attached to the endpoint, with outgoing and post-handshake checks and eviction of existing connections. This covers iroh-docs, blobs, and gossip as well as distlib's own protocols. Keeping it there is essential. Separating voter-only Raft RPCs from member-facing proposals/log retrieval is another good boundary; being a group member should not imply being allowed to inject Raft traffic. See [endpoint enforcement][hooks] and [memberlog protocol separation][memberlog].

The membership fold owns signature validation and policy decisions, rather than trusting the API to have checked them. Rejected committed events do not bring every node down; they leave membership unchanged while the storage layer advances appropriately. Ordered collections and explicit stale-proposal checks make the intended deterministic behavior reviewable.

### Most derived state can repair itself

Rereading one complete logical item after an invalidation is easier to reason about than maintaining a second incremental event-sourced model. Startup replay, explicit reindexing, versioned read-model rebuilding, and custody's response to replay completion all reduce the amount of irreplaceable local state.

The code also recognizes that a document entry and its value can arrive separately. The repair sweep and content-arrival nudges address actual upstream behavior, not an idealized replication API. This adapter work is important evidence that the project has progressed beyond a happy-path prototype. See [projection behavior][projection] and [sync repair/backstop][sync-repair].

### Client events are designed as invalidations

The lossy broadcast bus does not let a slow browser stall replication. A lagging watcher receives `resync`; the browser reconnects with a refetch; a small reloader coalesces bursts. This is a good fit for a local control UI and should remain simpler than a durable client event log. The remaining issue is defining exactly when a derived view is ready to refetch, rather than abandoning this design. See [SSE][events] and [browser event handling][ui-events].

### Tests exercise real behavior

The suite includes real local node clusters, endpoint refusal and eviction, Raft storage conformance, process-level CLI scenarios, projection/restart behavior, and browser tests against running nodes. The new GC integration test checks that a real collection pass preserves imported media, downloads, and document values. The custody integration test checks agreement and settling across five nodes. These are materially better evidence than mocks alone. See [membership acceptance][acceptance], [GC acceptance][keeping-test], and [custody acceptance][custody-test].

The exact snapshot's [CI run](https://github.com/izderadicka/distlib/actions/runs/39022992372) succeeded: formatting/Clippy, UI checks/tests/build, browser E2E, and Rust tests plus doctests on Linux, macOS, and Windows. This is evidence of a healthy baseline, not proof that the distributed-state boundaries below are correct under every failure.

## 4. Concerns and recommendations, in priority order

Here, **confirmed** means directly supported by this snapshot's source or documents. **Audit question** means a scenario requiring a focused test or deeper proof; it is not a reproduced defect. Priorities describe review/release importance, not security severity.

### A. Membership recovery must survive the lifetime of the group — high priority

**Confirmed:** non-voting followers receive `TooFarBehind` when their cursor precedes the retained log. There is no follower snapshot-transfer path; the follow loop enters a stranded outcome. This is explicitly deferred as P1-29. It is distinct from Raft voter snapshot installation, which exists and has tests. See [memberlog fetch][memberlog-fetch], [follower recovery][follower], and [the recorded deferral][deltas].

This is a conceptual mismatch with a long-lived library whose members may remain offline for months. The limit is driven by membership-log history, not catalogue size. Once all reachable sources have purged the required prefix, a newly joining follower with cursor zero has the same bootstrap problem as an old follower. The current diagnostic's suggestion to delete the data directory and rejoin does not supply the missing prefix; it also puts valuable identity and local data at risk.

**Recommendation:** treat a supported recovery path as a release prerequisite for persistent groups. Specify the follower snapshot/checkpoint trust model, group identity binding, monotonicity, and suffix catch-up before implementing it. Recovery should preserve the member's identity and retained media. Until then, document the limitation and avoid presenting a reset as a general remedy. This is targeted completion of the existing architecture, not a reason to replace Raft.

### B. Finish the preservation contract before adding more custody mechanisms — high priority

**Confirmed:** `KeepReason` currently distinguishes `Own` and `Cache`; the runtime enables GC with document protection; `distlib-custody` computes a view. The planned pin manager, budgets, eviction, emergency copies, and probes are not all implemented at this snapshot. The [phase 5 plan][phase5] correctly describes these as subsequent work.

The difficult invariant is no longer just “every node computes the same ranking.” It is **“a counted replica corresponds to a complete retained item, and a transition never releases a needed copy prematurely.”** Several inputs are eventually consistent or locally timed:

- Any file-key author can make a node a creator in the read model; that fact is not itself proof of retaining every current file.
- An item's file set can evolve after identity is frozen. A creator's initial import and the item's current complete file set can differ.
- Holdings currently concern complete content files, while custody sizing includes all item files. That distinction is reasonable, but should be explicit when “replica” is reported.
- Nodes receive membership changes at different times, settle locally, and adopt their initial membership immediately on startup. Temporary assignment disagreement is therefore expected.
- Available copies are observer-local claims; assignment does not change merely because a member goes offline.

**Audit questions:** what retains a creator's newly added files, when may an old custodian release its tags, and what happens if a node restarts during settling? What happens after partial import/tag failure, disk-full, or a concurrent GC pass? How do several reasons retain a shared blob independently?

**Recommendation:** write a short transition/invariant table before the pin manager lands, then test those transitions across the catalogue, tags, GC, projection, and custody service. Keep the accepted simplifications—no deterministic overflow and no logged custody epochs for this phase—unless evidence shows they cannot satisfy the chosen safety rule. Do not quietly promise simultaneous assignments or continuous target replication when the design provides eventual repair.

The use of platform `f64::ln` is an explicitly accepted near-tie determinism tradeoff in P5-4. Preserve that qualification in claims about identical rankings; a later scoring review should decide whether portable arithmetic is warranted and include endpoint/rounding cases. It is not the highest-risk part of preservation today. See [scoring][scoring] and [local settling][settling].

### C. Make projection degradation and completion truthful — high priority for hardening

**Confirmed:** `project_items` can log an indexing or index-commit error while still returning events derived from successful SQLite writes. `replay()` logs failures and returns `()`, and `admin.reindex` completion uses a `oneshot<()>`. A caller can therefore receive completion without learning that some records or the search index failed to rebuild. The source's “index included” event-ordering explanation is stronger than the error path guarantees. See [projection write/error paths][projection-errors].

Cross-store atomicity is not required here. SQLite and Tantivy are rebuildable views, and retaining useful reads during a partial failure is sensible. The missing contract is what remains stale, how it is retried, and how the operator learns that repair is incomplete.

**Recommendation:** introduce a small projection status/result: replay in progress, completed with failures, last successful completion, and pending retry/repair work. Return a meaningful reindex outcome and retain failed IDs or schedule another repair pass. Separate “the underlying document accepted the write” from “all read views reflect it.” Prefer this modest mechanism over distributed transactions or a general job framework.

**Related audit question:** membership changes cause a full filtered-community replay, but replay emits no item/bookmark/wish invalidations. The API publishes `membership.changed` independently of projection completion, and `ItemPage` handles it by reloading member names, not the item record. This is a source-supported stale-view scenario, not a browser reproduction from this review. Test an already-open item page while a review author is expelled and verify the resulting record refresh. A projection-completed invalidation is a stronger fix than having every page race the membership publisher. See [membership replay][projection] and [ItemPage][item-page].

### D. Separate the modest deployment envelope from the scale ambition — medium-high priority

The architecture can plausibly serve small groups well. Claims about thousands of members and millions of items need a workload definition and measurements of the *whole node*, not only custody hashing.

**Confirmed costs include:** full read-model replay on startup and member-set changes; periodic scans for missing document content; a full peer offer on membership changes and the ten-minute backstop; an in-memory item-to-holders map; and fetching complete holdings bases. These are deliberate choices. Phase 4 already records the map's approximate 1 GB cost at 10 million distinct items, the 320 MB maximum base payload, and larger-group presence/traffic limitations. Those numbers are the project's estimates, not measurements performed in this review. See [phase 4 costs and carry-forwards][phase4], [holder index][holders], and [repair scheduling][sync-repair].

**Recommendation:** define an initial supported envelope—members, items, files per item, community churn, RAM, and expected restart time—and benchmark it end to end. Include mostly complete steady state, cold join, restart, a member expulsion, a large holdings change, and a partition healing. Measure replay time, disk I/O, memory peaks, traffic per node, and UI latency. Keep the existing scan/replay approaches until the measurements identify the first constraint. Moving the availability index to SQLite, adding checkpoints, or batching projection work should follow that evidence, not precede it.

### E. Document one current architecture and one compatibility boundary — medium priority now, high before release

**Confirmed:** the README says phase 4 is planned while describing completed phase 4 features. The phase 5 plan says phase 4 and the refactor series are complete; the refactor document's opening still says “not done yet.” Some code comments similarly describe old limitations alongside newer implementations. The original plan is intentionally immutable, so it cannot be a standalone current specification.

The delta log is valuable decision history. The problem is making every reader mentally apply that history to understand current behavior. It is especially risky when original statements about namespaces, approval thresholds, replicas, history retention, or custody overflow are read as present guarantees.

**Recommendation:** add a concise current-design index containing state ownership, trust boundaries, current policy, implemented phase status, and known release blockers. Link into the original plan and delta records for rationale. Maintain an explicit precedence rule: current contracts and code describe behavior; phase plans distinguish completed from future work; the original plan records intent. Update status summaries as part of the same PR that completes a phase.

Before persistent groups are encouraged, also define which formats may break together: membership events/snapshots, derived document identity and keys, tags, local read-model versions, and RPC/UI shapes. Unknown catalogue keys are deliberately tolerated, but that does not establish general mixed-version compatibility for signed events or custody policy. Record the supported upgrade/rollback procedure, including backups. A pre-release policy of rebuilding data is acceptable only while clearly communicated and operationally consistent with distributed peers.

## 5. Code organization: targeted improvements

The crate dependency direction is broadly right. I would keep it and make a few local improvements when those files next change.

**Split large files by responsibility, while keeping simple dispatch together.** At this snapshot `methods.rs` and `commands.rs` each exceed 2,000 lines, `consensus/node.rs` and `sync/catalogue.rs` exceed 1,500, and `store.rs` is about 1,000. These sizes include extensive comments and are not defect counts. The existing refactor series has already improved function-level structure. The next useful step is file-level navigation: library operations, community operations, group operations; catalogue replication/repair versus typed record access; and consensus role/lifecycle coordination versus proposal handling. Keep exhaustive method/event matches together where they act as readable tables. See the [completed refactor work][refactor].

**Name the application boundary before adding a second caller.** The API currently performs important use cases such as importing, deduplicating, tagging, and downloading. That is practical while the CLI calls the same API, but the custody service will also need retention and fetch behavior. Reuse low-level transfer/retention operations or extract a small use-case module when necessary; do not make the custody service call JSON-RPC or duplicate the user-download path. No new “service layer” crate is required merely to satisfy a diagram.

**Keep domain invariants close to the code; move archaeology elsewhere.** Many comments explain a real failure mode and should stay. Others recount several superseded implementations or make timing/coverage claims that can drift. A short invariant, the consequence of violating it, and a named regression test usually help more than repeated historical essays. Link to the delta record for the rest. This matters particularly in membership and shutdown code, where a stale comment can misdirect a careful reviewer.

**Strengthen the Rust/TypeScript contract with a small check first.** Rust responses are often assembled with `json!`, while `rpc.ts` declares their shapes manually. The client typing is useful, but it does not prove the server returns those shapes. Add contract fixtures or a few integration checks for evolving responses and event payloads. Consider generated types later if drift becomes recurrent; a schema-generation infrastructure project is not presently necessary.

**Keep deliberate simplicity.** One document, field-level keys, whole-item projection, plain Svelte components, and derived custody are good choices for this project. Semantic Work/Edition modelling, multi-device identity, generalized plugin abstractions, and a new event framework would all widen the correctness surface before the current preservation path is finished.

## 6. Briefs for the subsequent technical reviews

These reviews should follow user-visible flows and failure boundaries, not just crate boundaries. Start with a pinned snapshot, identify the invariant under review, and separate reproduced failures from unresolved questions. Each brief below is small enough to become a focused review or a short series of PRs.

| Order | Review and principal files | Invariants and scenarios | Expected output |
|---|---|---|---|
| **1 — high** | **Membership authority, reconfiguration, and recovery.** `consensus/state.rs`, `signed.rs`, `raft/{state_machine,log_store,memberlog,follower,core_group,seat}.rs`, `node.rs`; `net/hooks.rs`. | Rejected events leave state unchanged; followers never accept the wrong group or regress; a committed voter change eventually reaches Raft; promotion/demotion survives a crash at each boundary; expulsion closes all protocol connections. Exercise lost quorum, leader self-removal, stale approvals, re-admission, repeated role changes, purged-log join and long-offline return. Review what evidence a non-voter relies on for commitment. | A current policy/state-transition table, a recovery design for non-voters, and a short list of reproducible failures or proof obligations. |
| **2 — high, before pin-manager completion** | **Physical preservation and custody transitions.** `core/{custody,keeping,group_settings}.rs`, `custody/{service,settling,view}.rs`, `net/blobs.rs`, `sync/catalogue.rs`, `store/store.rs`, `api/{methods,tasks}.rs`, `runtime.rs`. | A counted replica is complete; independent keep reasons cannot release each other's data; imported/downloaded bytes survive GC and restart; creator attribution and replica counting agree; settlement changes never justify unsafe early release. Exercise shared blobs, multiple creators, later file additions, partial writes, disk-full, restart during transfer/settling, creator departure, under-capacity groups, and GC races. | A retention state machine and integration tests around the chosen handoff policy; distinguish assignment from actual protection. |
| **3 — high** | **Convergence, projection, and invalidation.** `sync/{catalogue,changes}.rs`, `core/{catalogue,community}.rs`, `store/{projection,store,index,schema}.rs`, `api/events.rs`, UI event consumers. | Equivalent replicated state yields equivalent read views; author ownership cannot be spoofed by key text; late content repairs partial records; deleted community values do not reappear incorrectly; errors remain repairable; watchers refetch only when the relevant view is ready. Exercise out-of-order entry/value arrival, concurrent field edits, clock skew, expel/re-admit, repeated replay, index-commit failure, failed SQLite writes, event lag and reconnect. | A failure/retry contract, projection status semantics, and regressions for stale or divergent views. |
| **4 — medium-high** | **Availability and transfer under churn.** `sync/availability/{heartbeat,holders,online,statement}.rs`, `core/availability.rs`, `net/{directory,addresses,connections,blobs}.rs`, `api/{availability,tasks}.rs`. | Unknown is distinct from zero holders; old bases/deltas cannot overwrite a newer accepted state; restart epochs and delayed heartbeats have defined semantics; expelled members do not count; temporary failures do not permanently strand a peer; cancellation and shutdown release resources. Exercise partitions, delayed messages, moving addresses, large manifests, dishonest/inconsistent claims within the stated threat boundary, stalled providers, and dependency-workaround timing. | A liveness/recovery scenario matrix, resource bounds, and any narrowly reproducible failures. |
| **5 — medium-high before release** | **Local API, files, UI, and packaging.** `api/{lib,rpc,uploads,tasks,methods}.rs`, `core/{private_file,identity,token,ticket}.rs`, UI `lib/*`, download/add pages, Dockerfile and release workflow. | Token possession has an explicitly documented scope; private files remain private on supported platforms; upload/temp-file lifetimes are bounded; exports cannot clobber unrelated files; incomplete requests do not report success; UI state survives reconnect and renders untrusted text safely. Exercise filename/path edge cases, symlinks/races, interrupted exports, wrong tokens, stale tabs, browser reload during download, and proxy deployment. | Focused security findings, API contract checks, and a verified install/upgrade/backup story. |
| **6 — medium, after baseline correctness** | **Whole-node scaling and operational resilience.** Runtime, replay/index paths, metadata sweep, availability map/base fetch, custody recompute, task registries. | Stated scale fits stated hardware; background maintenance cannot starve requests; repair remains bounded under churn; errors and stuck services are visible. Exercise the workload matrix from section 4D and deliberate slow/full disks. | Reproducible measurements and only the optimizations those measurements justify. |

The first two reviews can inform each other: membership recovery changes what a restarting node knows, and custody determines what that node is safe to release. They should not be treated as isolated algorithm audits.

## 7. Scope, evidence, and limits

I read the original design and current delta/phase material, the engineering standards and refactor plan, the workspace layout, and representative code across all nine crates. The detailed source reading covered runtime assembly/shutdown, membership policy and reconciliation, follower/log serving, endpoint enforcement, catalogue reads/writes and repair, projection, availability state, custody computation and settling, API use cases, browser RPC/events/item handling, and representative acceptance/GC/custody tests.

This is **not** a complete line-by-line audit, a dependency-source audit, a security certification, or a performance validation. The review did not run an adversarial cluster, reproduce the UI invalidation scenario, or execute local Rust tests; this environment has no Rust toolchain. The successful exact-commit CI run supplies baseline verification. Timing and scale figures quoted from the repository remain the project's measurements or estimates.

The most useful next step is a focused membership/recovery review and a preservation-invariant review before the remaining custody mechanics accumulate. The architecture can support those changes without a broad rewrite.

[design]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/docs/distlib-plan.md
[membership]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/crates/distlib-consensus/src/state.rs#L181-L355
[runtime]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/crates/distlib/src/runtime.rs
[schema]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/crates/distlib-core/src/catalogue.rs
[catalogue]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/crates/distlib-sync/src/catalogue.rs#L1-L210
[community-read]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/crates/distlib-sync/src/catalogue.rs#L850-L916
[scoring]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/crates/distlib-core/src/custody.rs
[hooks]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/crates/distlib-net/src/hooks.rs
[memberlog]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/crates/distlib-consensus/src/raft/memberlog.rs#L1-L20
[memberlog-fetch]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/crates/distlib-consensus/src/raft/memberlog.rs#L335-L367
[follower]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/crates/distlib-consensus/src/raft/follower.rs#L349-L424
[deltas]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/docs/plan-deltas.md
[projection]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/crates/distlib-store/src/projection.rs
[projection-errors]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/crates/distlib-store/src/projection.rs#L333-L514
[sync-repair]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/crates/distlib-sync/src/catalogue.rs#L1124-L1565
[events]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/crates/distlib-api/src/events.rs
[ui-events]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/crates/distlib-ui/web/src/lib/events.ts
[item-page]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/crates/distlib-ui/web/src/ItemPage.svelte#L29-L75
[acceptance]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/crates/distlib-consensus/tests/acceptance.rs
[keeping-test]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/crates/distlib/tests/keeping.rs
[custody-test]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/crates/distlib/tests/custody.rs
[phase5]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/docs/plan-phases/phase-5-custodianship-quotas.md
[settling]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/crates/distlib-custody/src/settling.rs
[phase4]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/docs/plan-phases/phase-4-availability-community.md
[holders]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/crates/distlib-sync/src/availability/holders.rs#L242-L315
[refactor]: https://github.com/izderadicka/distlib/blob/93bcae7993edf08945e616b20af2016d34d33b68/docs/plan-refactor-complexity.md
