# Raft, membership, and pledge correctness review

Reviewed commit `935e82fecf6d0a761a373840faf39e6d2382a435`, 2026-10-23. Evidence branch: `review/raft-membership-pledges`. No production implementation changes made.

The main risks are in the integration around OpenRaft: translating application membership into voting membership, changing roles, and serving the log to non-voters. The storage primitives and deterministic membership fold have substantial coverage, but that does not establish correctness of these transitions.

P1 below means a correctness or availability issue to fix before relying on persistent groups. P2 means a narrower policy or recovery defect. Source references are relative to the repository root. “Reproduced” describes a targeted component test; it does not imply a live distributed reproduction.

## 1. P1 — Retiring voters before the Raft reconfiguration completes can destroy quorum

**Source:** `crates/distlib-consensus/src/node.rs:1266`, `:1277`; `crates/distlib-consensus/src/raft/core_group.rs:218`, `:378`.

`stand_down` reacts to the application projection dropping this node, immediately stops reconciliation, vacates the seat, and shuts down Raft. It never waits for the final uniform OpenRaft membership to commit. Expulsion also changes the connection allowlist at the application-event boundary.

The application event is only the intent to reconfigure. Committing it does not mean the old voters are no longer needed. A 3-to-1 reduction can shut down two voters while the actual Raft configuration still needs two votes. Replacing a singleton voter A with an admitted member B is even clearer: A stops, while B has not yet been caught up and promoted. `next_change` also removes before adding learners, so a disjoint replacement first attempts to remove every old voter, which OpenRaft refuses. A newly seated B trusts the desired core, which excludes A, creating another obstacle to that handoff.

**Recommendation:** represent desired and effective voting membership separately. Add and catch up replacement learners while the old quorum is intact; complete joint consensus and the final uniform configuration before retiring old Raft participants. Keep the narrowly required consensus transport available during this transition, including when application access is revoked. Restart must follow the same rules.

**Evidence:** source-backed. A two-node singleton-replacement probe was written, but socket binding was blocked by the execution sandbox, so the live scenario was not run.

## 2. P1 — An interrupted promotion can remain in joint consensus indefinitely

**Source:** `crates/distlib-consensus/src/raft/core_group.rs:268`, `:323`, `:349`.

The reconciler reduces OpenRaft membership to `voter_ids()`, the union of the joint configuration's voter sets. Consider old `{A,B,C}` and new `{A,B,C,D}`. If the leader fails after committing the joint entry but before committing the uniform entry, the union already equals the desired application core. `next_change` returns no change, and the new leader reports convergence without finishing the transition.

OpenRaft 0.9.25 explicitly documents that `change_membership` can leave a joint configuration in this failure window; its second write is performed by the awaiting caller. The old quorum requirement can therefore survive indefinitely, reducing availability relative to the advertised new configuration.

**Recommendation:** inspect the joint configuration itself, and require a committed uniform configuration matching the desired voters before reporting convergence. Add a deterministic recovery test seeded with an interrupted joint configuration.

**Evidence:** source-backed, including the pinned OpenRaft implementation; no live leader-failure reproduction.

## 3. P1 — A fetch racing with log compaction can silently lose membership events

**Source:** `crates/distlib-consensus/src/raft/memberlog.rs:344`, `:346`, `:359`; `crates/distlib-consensus/src/raft/log_store.rs:164`; `crates/distlib-consensus/src/raft/follower.rs:392`.

The serving node checks the purge watermark and then reads the requested events using separate redb read transactions. A purge can commit between those reads. The first check says the requested prefix exists; the second returns only a suffix, or nothing, but the response still advertises the original `up_to`.

The follower advances its durable cursor to `up_to` anyway. It permanently skips the missing events. Later events can then be rejected as stale because `changed_at` no longer matches, while the follower continues advancing its cursor and enforcing an obsolete membership or pledge table.

**Recommendation:** read the retention watermark and the requested log range from one consistent database read transaction. Return either a complete range under that snapshot or an explicit snapshot-required response.

**Evidence:** reproduced the allowed interleaving with a real stored founding entry: check watermark, purge entry, read range successfully as empty. This is a deterministic storage-level demonstration, not a timed network-race test.

## 4. P1 — Promotion loses the durable information needed to recover after a crash

**Source:** `crates/distlib-consensus/src/node.rs:240`, `:585`, `:1379`; `crates/distlib-consensus/src/raft/state_machine.rs:310`.

Promotion stores its trusted core only in memory, then durably clears the membership, log, snapshot, and follow cursor. The promotion index used to ignore historical configurations is also only local task state.

A crash immediately after reset leaves no durable indication that this node was being promoted or which current core it trusted. Startup selects its role using the now-empty projection and bootstrap configuration. A normal joiner whose configuration does not name itself starts as an ordinary follower. If the prefix has already been purged, it cannot retrieve the promotion event and cannot resume the Raft snapshot path that was supposed to recover it. During partial catch-up, restart can similarly see an older projection that does not yet name the node as core.

**Recommendation:** persist the transition phase, trusted source identities, group identity, and promotion boundary atomically with the reset, or preserve the authoritative application projection while rebuilding the Raft storage. Exercise crashes before reset, after reset, after snapshot installation, and before the promotion entry is reapplied.

**Evidence:** source-backed crash-window analysis, not process-kill testing.

## 5. P1 — An in-flight snapshot builder can resurrect a snapshot deleted by promotion

**Source:** `crates/distlib-consensus/src/raft/state_machine.rs:327`, `:663`.

Reset deletes the existing snapshot, but a previously created `SnapshotBuilder` still owns the old state and database. When it finishes after reset, its only guard is whether a *newer snapshot* is already stored. An empty slot passes that guard, so the builder writes the pre-reset snapshot back beside an empty or partially rebuilt state machine. Fresh builders below that old index can subsequently be prevented from replacing it.

This is relevant to production lifecycle behavior: OpenRaft 0.9.25's state-machine worker spawns snapshot builds as detached tasks; `Raft::shutdown` does not join those snapshot tasks.

**Recommendation:** bind builders to a durable storage generation and reject writes from older generations within the write transaction. Alternatively, explicitly drain every old writer before reset, including detached snapshot work. Clearing a key alone is insufficient.

**Evidence:** reproduced: capture a builder at index 200, reset, finish the old builder; `last_applied` is zero but the current snapshot is again at 200. The existing reset regression checks an already-stored snapshot, not an unfinished builder.

## 6. P1 — Compaction permanently strands ordinary followers and new joiners

**Source:** `crates/distlib-consensus/src/raft/follower.rs:407`; `crates/distlib-consensus/src/raft/memberlog.rs:346`.

`TooFarBehind` has no recovery path for non-voters. A long-offline member, or a new member starting at cursor zero after compaction, cannot reconstruct membership. This is an existing documented deferral, but it remains a correctness/operability limitation for long-lived groups.

The loop also stops at the first `TooFarBehind`, claiming every source purges at the same watermark. Local snapshot/purge progress need not be identical, so another voter may still retain the needed prefix. The diagnostic suggesting deletion of the data directory is not a remedy: it resets the cursor further and cannot restore purged history.

**Recommendation:** support authenticated, group-bound follower snapshot/checkpoint transfer followed by suffix replay; preserve identity and retained content. In the meantime, try remaining sources and remove the misleading deletion advice. Retention policy alone is a mitigation, not recovery.

**Evidence:** explicit source behavior and documented limitation; not a new distributed reproduction.

## 7. P2 — Restart ignores the persisted core when choosing follower sources

**Source:** `crates/distlib-consensus/src/node.rs:585`; `crates/distlib-consensus/src/raft/follower.rs:362`.

Startup always initializes `Sources` from the supplied bootstrap configuration, even when the persisted membership already contains a newer core. The allowlist/address-book task does not replace the source candidate identities. A follower that learned a replacement core, then restarts after all its original ticket's core nodes are gone, keeps trying the obsolete nodes despite knowing the current core on disk.

**Recommendation:** seed sources from persisted membership when founded; use bootstrap configuration only when authoritative membership is unavailable. Test a restart after complete core turnover with the original bootstrap nodes offline.

**Evidence:** source-backed.

## 8. P2 — Admission can assign somebody else's initial pledge

**Source:** `crates/distlib-consensus/src/state.rs:693`.

Preserving an existing member's pledge fixes the rename/update case, but a new or re-admitted member uses `member.pledge_bytes` from `MemberAdded`. A core member can commit an admission assigning another member `u64::MAX` without any signature from that member. That changes custody weights and minimum-pledge download eligibility even though `PledgeChanged` is explicitly self-only. The normal API constructs zero, but the replicated state machine does not enforce that rule. Founding records similarly accept supplied pledges.

**Recommendation:** enforce zero initial pledges in the fold, or require a separately authenticated pledge from each owner. Do not rely on one client constructor for a distributed ownership invariant.

**Evidence:** reproduced using `MembershipState::apply` with an admission signed only by the founder.

## Other weak points and explicit limits

- **Expiry semantics disagree with execution.** `state.rs:232` sweeps only after dispatching a successful event. A proposal at index 2 with expiry 128 can still be approved at index 1000 if intervening entries were rejected or Raft-only entries. Reproduced. Either validate expiry before approval while preserving rejection atomicity, or use an explicit successful-change counter if expiry is intended to count accepted membership changes. Current comments promise a log-index boundary that is not enforced.
- **Consumption-proportional pledges are client policy.** `distlib-api/src/methods.rs:435` checks the required pledge against the local catalogue projection, while `consensus/state.rs:257` enforces only ownership and the group minimum. A custom client can directly submit a pledge above the minimum but below half its content requirement. Concurrent adds/pledge reductions also lack a common reservation/serialization point and read a lagging projection. This is consistent with some recorded tradeoffs, but the claim that consensus enforces proportional contribution would be too strong. Enforcing it cannot simply read each voter's local catalogue during apply: that would make the fold nondeterministic.
- **Signatures establish attribution, not Raft commitment.** Followers trust one authenticated source's event ordering, indices, completeness, and applied watermark. `SignedEvent` does not bind the group ID or committed log index, and a signature is not a quorum certificate. This is a trust limitation under the stated crash-fault model, not a demand to implement Byzantine consensus. Document it accurately; bind group identity if cross-group replay isolation is intended.
- **Global stale checking is coarse.** Every successful proposal or approval advances `changed_at`, so unrelated pledge updates conflict. `node.rs:911` retries once with the same event against a refreshed view. Reusing an absolute `CoreGroupChanged` map can overwrite an intervening address update; refreshing the version does not merge the user's original intent. Rebuild such operations against current state or return a conflict for review.

## What is working well

The fold verifies signatures, enforces self-only pledge changes and a shared minimum, deduplicates approvals, checks current approver eligibility, and preserves state on rejected events. Ordered collections support deterministic replay. Log votes and appends wait for redb commit; purge updates its watermark in the same transaction. State-machine application and snapshot installation persist their related keys together. These are useful foundations; the critical missing guarantees are around transitions and concurrent readers/writers.

## Validation and follow-up tests

- **96 existing tests passed:** membership, state machine, log store, previous proof-issue regressions, and OpenRaft storage conformance (`cargo nextest run --offline -p distlib-consensus --test membership --test state_machine --test log_store --test proof_issues --test conformance --no-fail-fast`).
- **Four evidence probes passed by demonstrating current defects:** arbitrary admission pledge, approval beyond the documented expiry boundary, old builder after reset, and fetch/purge interleaving.
- **Live networking was not validated.** The full workspace run stopped after local socket creation returned `Operation not permitted` (14 passed, 3 failed due to environment restrictions, 602 not run). The singleton replacement probe was blocked for the same reason. A network-enabled retry did not execute because the permission request was aborted. These failures are not evidence of product failures.
- Runnable evidence is committed in [`crates/distlib-consensus/tests/review_probes.rs`](../../crates/distlib-consensus/tests/review_probes.rs). Run `cargo nextest run -p distlib-consensus --test review_probes`. These four tests deliberately assert the presence of the reviewed defects; passing is evidence of the defect, not a correctness guarantee. Reverse the assertions when implementing fixes. The unvalidated live-node probe is excluded from this branch.
- The evidence branch changes only this report and the four component probes. No production sources were changed.

Prioritize a persistent reconfiguration lifecycle and atomic follower log serving. Then add deterministic tests for joint-configuration recovery, disjoint replacement, retirement of an old majority, restart at each promotion boundary, and snapshot completion across reset. Ordinary 3-to-2 demotion and uninterrupted promotion tests do not cover those cases.
