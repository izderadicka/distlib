# Raft membership and pledges: revised review

Reviewed `master` at `47616a1b46e6f93a6f45fb1e77bce5577067f8fc` (PR #155), 2026-10-24. This replaces the earlier review of `935e82f`. Findings below were reassessed from implementation and pinned OpenRaft source, not project design documentation. Evidence branch: `review/raft-membership-pledges`.

## Retention change verified; obsolete findings removed

`crates/distlib-consensus/src/node.rs:131` now sets `max_in_snapshot_log_to_keep: u64::MAX`. In OpenRaft 0.9.25, `LogHandler::calc_purge_upto` subtracts that retention count with saturation, so ordinary snapshot completion no longer schedules prefix purging. Snapshot creation remains enabled. Raft still needs to truncate conflicting uncommitted suffixes; retaining committed history does not require disabling that operation.

The fetch path now pages the retained history: `LogStore::events_after` bounds the event count and encoded bytes, `MemberlogProtocol::fetch` advances only to the last returned event when the page is cut short, and the follower immediately requests another page when `more` is true. I checked those paths and the pinned OpenRaft replication-selection implementation rather than relying on the new comments.

Accordingly, the old **finding 3** (fetch racing with automatic compaction) and **finding 6** (ordinary followers permanently stranded by compaction) are removed from the active review. The purge-interleaving probe is removed from this branch. The old **finding 4**, whose permanent-failure example depended on purged history after a promotion crash, is also withdrawn as a standalone P1 finding: with retained history and reachable sources, replay can recover. Loss of in-memory transition context remains a test obligation, not proof of permanent failure. The source-discovery problem below is separate and still applies.

This review assumes groups using the current retention policy. It does not reopen incomplete-history findings as hypothetical future compaction requirements. Original finding numbers are retained for cross-reference.

## Remaining findings

### 1. P1 — Voters retire before their Raft removal is committed

**Code:** `crates/distlib-consensus/src/node.rs:1273`, `:1284`; `crates/distlib-consensus/src/raft/core_group.rs:218`, `:378`.

`stand_down` still reacts to the application membership excluding this node, stops reconciliation, and shuts down its Raft without checking that the final uniform OpenRaft configuration has committed. The intent event can commit while the old voter set still governs subsequent writes. Retiring an old majority at this point can prevent the membership change itself from committing.

The reconciler also removes departed voters before creating replacement learners. For an accepted singleton replacement `{A} -> {B}`, its first requested change is `RemoveVoters({A})`, before B has been added or caught up. OpenRaft cannot accept an empty voter set, and A's role task independently retires it. Neither issue depends on log retention.

**Evidence:** the new unit probe `review_disjoint_replacement_removes_every_old_voter_before_learning` calls the actual `next_change` helper and confirms that exact removal decision. The complete live quorum-loss scenario remains source-backed, not a network reproduction.

**Fix direction:** separate desired application membership from effective Raft membership; add/catch up replacements first, finish joint and uniform configurations, then retire removed participants. Preserve the consensus connectivity needed for that transition, including across restart.

### 2. P1 — Interrupted joint promotion is mistaken for convergence

**Code:** `crates/distlib-consensus/src/raft/core_group.rs:268`, `:323`, `:349`.

`current_voters` reduces a configuration to `voter_ids()`, the union of its voting sets. For joint `{A,B,C}` / `{A,B,C,D}`, that union already equals desired `{A,B,C,D}`. The reconciler therefore returns `Converged` even though the old quorum requirement remains. A leader change between the two writes in OpenRaft's `change_membership` leaves exactly this recoverable intermediate state, but the new leader does not request its completion.

**Evidence:** `review_interrupted_joint_promotion_is_mistaken_for_convergence` constructs an actual two-set OpenRaft membership, passes it through the actual `current_voters` and `next_change` helpers, and confirms no change is requested. It is a deterministic reconciliation test; it does not simulate a leader crash. OpenRaft's two-write implementation was rechecked.

**Fix direction:** make reconciliation aware of joint versus uniform configuration and committed versus merely effective configuration. Require a completed uniform membership before declaring convergence.

### 5. P2 — A snapshot builder from before promotion reset can republish old state

**Code:** `crates/distlib-consensus/src/raft/state_machine.rs:310`, `:663`.

Promotion reset deletes the stored snapshot, but a previously created builder still holds its captured state and database. If it finishes after reset, the guard only compares against any snapshot currently stored. An empty slot permits the old snapshot to be written again. OpenRaft's state-machine worker spawns snapshot builders independently; shutting down the Raft core does not join those tasks.

**Evidence:** `pre_reset_snapshot_builder_can_restore_the_deleted_snapshot` still reproduces the inconsistency on current master: capture a builder at index 200, reset, complete it; applied position is zero while the stored snapshot is at 200.

**Revised severity:** downgraded from P1 to P2. Automatic snapshot creation still makes the lifecycle race relevant, but full retained history changes its operational impact. This review no longer claims that the race causes missing-history recovery failure or that snapshot creation must be blocked forever. Once replay catches up, a sufficiently new snapshot can replace the old one.

**Fix direction:** reject builders from an old storage generation inside the transaction, or reliably drain all old writers before reset. Add a regression asserting that a pre-reset builder cannot repopulate the new generation's snapshot slot.

### 7. P2 — Restart uses obsolete bootstrap sources despite knowing the current core

**Code:** `crates/distlib-consensus/src/node.rs:592`; `crates/distlib-consensus/src/raft/follower.rs:346`.

Startup still builds `Sources` from the supplied bootstrap configuration even when persisted membership contains a newer core. `fetch_log_from_core_nodes` only tries those candidate identities. The address-book/allowlist task does not replace the source list with the persisted current core.

A follower can therefore know the current core on disk, restart with its original ticket/configuration after those bootstrap voters have departed, and fail to contact any source. A complete log on the current voters does not help when the follower never asks them. A promotion reset also loses in-memory source/trust context, but that is not counted as a second independently proven failure.

**Evidence:** rechecked startup and candidate-selection code; no live restart reproduction.

**Fix direction:** initialize sources from the persisted current membership when available, falling back to bootstrap configuration before founding. Test restart after core turnover with obsolete bootstrap peers offline, and test crashes at promotion boundaries with reachable current sources.

### 8. P2 — Admission can assign another member's initial pledge

**Code:** `crates/distlib-consensus/src/state.rs:693`.

The existing-member case preserves the old pledge, but a new or re-admitted member receives `member.pledge_bytes` supplied by the admitting proposer. A founder can assign another member `u64::MAX` without that member signing a pledge. This changes custody weight and minimum-pledge eligibility despite `PledgeChanged` being self-only. The normal admission API supplies zero, but the replicated fold does not enforce zero. Founding records likewise accept supplied pledge values.

**Evidence:** `admission_accepts_a_pledge_never_signed_by_its_owner` still passes on current master by demonstrating this behavior.

**Fix direction:** require zero initial pledge in the fold, or require an authenticated pledge from its owner. This is a protocol/API invariant mismatch; it does not imply Byzantine tolerance from Raft.

## Other checks

- **Expiry boundary remains inconsistent.** `state.rs:232` checks expiry after dispatching a successful event. A proposal at index 2 with expiry 128 can still be approved at index 1000 if intervening entries were rejected or Raft-only entries. The retained probe demonstrates this. Decide whether expiry counts log indices or accepted membership changes, then enforce that definition consistently; an after-dispatch sweep does not enforce the apparent log-index deadline.
- **Proportional pledge enforcement remains API-local.** `distlib-api/src/methods.rs:435` checks the consumption-based requirement against the local catalogue projection; the replicated fold enforces pledge ownership and the group minimum. Concurrent adds/reductions have no common reservation point. Treat this as the enforcement boundary, not as a Raft guarantee. Reading each voter's independently lagging catalogue during apply would make the state machine nondeterministic.
- **Stale retries of absolute core maps remain risky.** `node.rs:918` retries the same event against a fresher `changed_at`. An absolute `CoreGroupChanged` map can therefore restore an address changed in between. Rebuild such intent against the refreshed state or return a conflict.
- **Signature trust boundary is unchanged.** Event signatures establish who proposed an event. Non-voters still trust the authenticated serving core for committed ordering and completeness; signatures are not quorum certificates. This is an architectural limit, not a newly asserted Byzantine-consensus defect.
- **Pagination deserves lifecycle tests.** The event/count/byte cursor logic is covered by current tests. Replaying historical membership page by page also publishes intermediate memberships to role and allowlist watchers. Exercise promotion/demotion/re-admission across page boundaries and restart during replay. I have not established an additional live failure from that interaction and do not count it as a confirmed finding.

## Evidence and validation

The branch incorporates current master and changes only this report and test code. It adds no production behavior changes.

Five evidence probes assert the **presence** of current defects; passing demonstrates the observed behavior, not correctness. Reverse the relevant expectations when fixing the implementation:

| Probe | Location | Evidence |
| --- | --- | --- |
| `review_disjoint_replacement_removes_every_old_voter_before_learning` | `src/raft/core_group.rs`, existing `#[cfg(test)]` module | Actual reconciler chooses removal before learning |
| `review_interrupted_joint_promotion_is_mistaken_for_convergence` | same test module | Actual reconciler ignores unfinished joint membership |
| `pre_reset_snapshot_builder_can_restore_the_deleted_snapshot` | `tests/review_probes.rs` | Old builder republishes snapshot after reset |
| `admission_accepts_a_pledge_never_signed_by_its_owner` | `tests/review_probes.rs` | Fold accepts another member's initial pledge |
| `expired_proposal_can_be_approved_after_a_gap_of_rejected_or_raft_entries` | `tests/review_probes.rs` | Approval bypasses the apparent log-index expiry boundary |

Run the evidence only:

```sh
cargo nextest run -p distlib-consensus --lib -E 'test(review_)'
cargo nextest run -p distlib-consensus --test review_probes
```

Validation on the current source plus these probes:

- **119 tests passed**, including all five probes, membership properties, storage/snapshot tests, new log-pagination tests, and OpenRaft storage conformance:
  `cargo nextest run --offline -p distlib-consensus --lib --test review_probes --test membership --test state_machine --test log_store --test proof_issues --test conformance --no-fail-fast`.
- `cargo fmt --all --check` passed. `cargo clippy --offline -p distlib-consensus --tests -- -D warnings` passed without warnings, including both added unit probes.
- Live node/whole-log networking tests were not rerun in this review environment; previous attempts could not bind sockets. The no-purge policy is verified from current implementation and pinned OpenRaft source; master also includes the live `whole_log` acceptance test. Its presence is not represented as a successful run here.

Prioritize the two remaining reconfiguration P1 findings. Full-history retention addresses the compaction concerns; it does not alter when voters may safely retire or how an interrupted joint configuration must be completed.
