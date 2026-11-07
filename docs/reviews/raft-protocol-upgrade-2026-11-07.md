# Raft, membership and pledge review — 7 November 2026

Reviewed master **8a7827fb9f2b139e0f006667df07219568b0619f** against the previously reviewed PR #160 head **673e4c853dde5c9f35c541a9106ea3ba0eb98e44**. The local review baseline has the exact master tree **18edfef1a3c6654d0493bae64711c44dc8942368**. Conclusions are based on implementation and tests, not design documentation.

Two correctness gaps remain in the new protocol-upgrade implementation. Both concern transitions to a protocol newer than this binary supports. `PROTOCOL_MAX` is currently 1; the evidence simulates an upgraded participant advertising protocol 2, as existing upgrade tests do. This is not evidence of a mixed-version production failure already happening today.

Evidence branch: [`review/raft-protocol-upgrade-nov7`](https://github.com/izderadicka/distlib/tree/review/raft-protocol-upgrade-nov7). The [test commit](https://github.com/izderadicka/distlib/commit/8c568adfb201123933711d6ab49f6e5a00e804b3) adds only the two reproduction tests on top of the reviewed master.

## 1. High: a protocol raise can overtake removal of an unsupported Raft voter

**Locations:** `crates/distlib-consensus/src/state.rs:344`, `crates/distlib-consensus/src/raft/state_machine.rs:569`, and the asynchronous reconciliation in `crates/distlib-consensus/src/raft/core_group.rs`.

`ProtocolRaised` checks support among `MembershipState::core()`. That is the signed *desired* core, which changes before Raft finishes changing its actual voters. Keeping departing voters alive until durable removal is correct and was an essential part of PR #160, but the new protocol check does not account for that interval.

A valid ordering is:

1. Actual Raft voters are A and B. A supports protocol 1; upgraded B advertises protocol 2.
2. A and B approve the signed core change to `{B}`.
3. Before the reconciler commits Raft's removal configurations, B submits `ProtocolRaised { to: 2 }`.
4. The raise succeeds: the desired core contains only B. A is nevertheless still an actual voter and still required for the two-voter quorum.
5. The Raft state-machine apply path also accepts subsequent membership events on this protocol-1 build; unlike `apply_followed`, it has no unsupported-protocol boundary.

**Impact:** the implementation's requirement that voters understand the active protocol is not maintained during reconfiguration. Once protocol 2 changes event semantics or encoding, a departing protocol-1 voter can apply subsequent entries using old rules, fail to decode them, or obstruct completion of removal. The evidence establishes the invalid ordering and continued application, not actual data loss or a live mixed-binary Raft failure.

**Evidence:** `proof_raise_can_overtake_removal_of_an_unsupported_raft_voter` in `crates/distlib-consensus/tests/review_nov7.rs` applies signed events through the real state-machine store. It checks that the raise succeeds while A remains in the applied Raft membership, A has not advertised protocol 2, and a later event still applies. The test constructs the legal log ordering directly; it does not control a live reconciler race.

**Suggested fix:** serialize protocol activation with Raft reconfiguration. Before activating a version, ensure every voter still needed by the effective configuration, including either side of joint consensus, supports it. One approach is to defer the raise until removal has durably converged and prevent another incompatible transition from overtaking it. Do not add a decision based only on local Raft metrics to the shared membership fold: follower replay must remain deterministic. Add explicit handling for a core node restarting on an older binary as well. A blanket early shutdown of departing voters would reintroduce the quorum-loss problem fixed in PR #160.

## 2. Medium: restart temporarily clears a durable upgrade requirement

**Locations:** `crates/distlib-consensus/src/node.rs:697`, `crates/distlib-consensus/src/raft/follower.rs:182`, and `crates/distlib-custody/src/pins.rs:488`. Runtime wiring is in `crates/distlib/src/runtime.rs:590`.

Startup opens the persisted membership, then initializes `upgrade_required` to `None` unconditionally. The follower checks the persisted protocol only **after** awaiting a network fetch. A follower that stopped at a protocol-2 boundary therefore restarts reporting no upgrade requirement even though its own database already proves that this binary is too old. An unreachable source extends the interval until its timeout; trying additional sources can extend it further.

**Impact:** status temporarily reports the wrong state. More seriously, the same watch is the guard used by `PinManager::release_if_safe` to retain copies while the node cannot interpret the group's current state. During the restart interval that guard is absent, allowing otherwise-eligible releases based on the frozen membership view. Such releases move data into an evictable cache. The evidence proves the absent guard, not an end-to-end blob loss.

**Evidence:** `proof_restart_temporarily_clears_a_durable_upgrade_requirement` in `crates/distlib-consensus/tests/review_nov7.rs` uses real local peers. A follower observes protocol 2 and reports `UpgradeRequired`; its source is stopped and the follower restarts with the same database. The test checks that protocol 2 remains persisted but the upgrade signal is `None` immediately after restart and 200 ms later, then eventually becomes `Some` after the fetch finishes.

**Suggested fix:** seed the upgrade watch from durable membership before returning the node to consumers or starting custody work, and check the persisted protocol before the first fetch. Consider making the live transition into an unsupported protocol atomic with the publication used by custody as well. Preserve the boundary index so a compatible binary can resume correctly.

## Other reviewed areas

- Log purging remains disabled: `raft_config` sets `max_in_snapshot_log_to_keep` to `u64::MAX`. No finding here depends on a purged or incomplete log.
- The prior durable-retirement, retained-learner, promotion-authority and retained-prefix protections remain in place. The production core-group reconciler and log store are unchanged from the reviewed baseline.
- Admission and founding now start pledges at zero. Profile updates preserve an existing member's pledge; re-admission resets it. Pledge ownership and minimum checks remain in the signed fold.
- Proposal expiry is checked when approving and when deciding whether a pending proposal occupies its subject, including after refused log entries.
- A stale `CoreGroupChanged` is no longer automatically re-signed against a newer membership.
- Snapshot builders now carry a reset generation and check it inside the database write transaction, preventing a builder captured before reset from restoring the old snapshot afterward.
- The previously deferred promotion-cancellation case (C46: promoted, then removed before learner admission) is not addressed by these changes. It remains a known limitation, not a newly established regression from this review.

## Verification and scope

The evidence tests intentionally assert the observed defects, so **passing means the defect is reproduced**, not that the implementation is correct. Production files were not modified.

Commands:

```sh
cargo check -p distlib-consensus --all-features --tests --offline
cargo test -p distlib-consensus --all-features --test review_nov7 --offline -- --nocapture
cargo test-all --offline --no-fail-fast
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --offline -- -D warnings
```

Network permission is enabled for commands that run socket-using tests.

- Consensus test compilation: passed.
- Both evidence tests: passed, reproducing both defects (6.32 seconds total). In the restart case the two upgrade-required log messages were approximately 3.05 seconds apart, spanning source shutdown and follower restart.
- Formatting: passed.
- Clippy across the workspace, all targets and all features, with warnings denied: passed.
- Upstream [CI for the reviewed master](https://github.com/izderadicka/distlib/actions/runs/42358595771): successful. This does not include the new evidence tests.
- Full local Rust workspace suite: **754 passed, 0 failed, 1 skipped**, in 205.784 seconds after compilation. This includes both evidence tests. The skipped test is the explicitly ignored 100,000-entry measurement. The full-log-after-snapshot test passed and asserted that nothing was purged.
