# Follow-up implementation review — 15 November 2026

Reviewed master **ed88649a1cf34b9822667dbf54de02cfab0f5a40**, tree **a123891d15936b2d2e36d062ab0fd87b7cac5b9a**, against the previous review at **8a7827fb9f2b139e0f006667df07219568b0619f**. Scope: Raft membership, voter retirement and recovery, protocol upgrades, pledges, and the related custody changes. Conclusions come from implementation and tests, not design documents. This is not a complete UI review.

**One new correctness regression is reproduced. Log purging remains disabled; this finding does not depend on missing log entries.**

## P2: shutdown falsely acknowledges durable Raft retirement

Introduced by **88a0516801a754e95dc7cf5200e914cb50b62d48** (PR #188).

Relevant implementation:

- [`node.rs:1314`](https://github.com/izderadicka/distlib/blob/ed88649a1cf34b9822667dbf54de02cfab0f5a40/crates/distlib-consensus/src/node.rs#L1314): `stop_consensus()` calls `seat.vacate()` before shutting down Raft.
- [`network.rs:392`](https://github.com/izderadicka/distlib/blob/ed88649a1cf34b9822667dbf54de02cfab0f5a40/crates/distlib-consensus/src/raft/network.rs#L392): `Retired` replies with `seat.raft().is_none()`.
- [`core_group.rs:494`](https://github.com/izderadicka/distlib/blob/ed88649a1cf34b9822667dbf54de02cfab0f5a40/crates/distlib-consensus/src/raft/core_group.rs#L494): the surviving leader treats that reply as authorization to forget a retained learner.
- [`node.rs:343`](https://github.com/izderadicka/distlib/blob/ed88649a1cf34b9822667dbf54de02cfab0f5a40/crates/distlib-consensus/src/node.rs#L343): restart correctly seats a node still listed in its persisted Raft voter configuration, which exposes the stale state left by the false acknowledgement.

### Trigger and consequence

`Runtime::shutdown()` intentionally stops consensus while its endpoint and protocol handlers remain alive. Emptying the seat therefore no longer means that the node applied a durable, uniform removal configuration. It can mean only that the process is stopping.

A concrete sequence is:

1. A, B and C are Raft voters.
2. C starts shutting down. Its consensus stops, but its endpoint still answers RPCs.
3. A and B approve and commit C's removal. Their quorum is enough; C cannot apply the removal because its consensus has stopped.
4. The leader asks C whether it retired. C returns `true` solely because shutdown emptied its seat.
5. The leader removes C's retained learner entry and stops replicating to it.
6. C restarts from its unchanged database, which still says it votes with A and B. It takes a Raft seat, but neither survivor replicates the removal to it anymore.

The new #206 read-ahead task does not recover this situation: it updates temporary Raft authorization, deliberately leaving the application state and Raft log untouched. The survivors already were authorized; the missing replication relationship is the problem. C remains a stale voter instead of becoming a follower and keeping its membership current.

**Impact:** a graceful shutdown overlapping removal can strand the removed node in stale membership indefinitely without further intervention. The reproduction does not show conflicting commits or loss of quorum in the surviving A/B cluster; the confirmed failure is retirement/restart liveness and stale membership on C.

### Executable evidence

`crates/distlib-consensus/tests/review_nov15.rs` contains two tests, using the real public node API and real local RPCs:

- `proof_shutdown_acknowledges_retirement_without_durable_voter_removal`: a durable voter answers `Retired = false`, then answers `true` immediately after `stop_consensus()`, without any change to its voter configuration.
- `proof_shutdown_during_removal_is_forgotten_then_restarts_as_a_stale_voter`: drives the complete three-node sequence, verifies both survivors forgot C, then restarts C at the same address. It remains seated with the old core after 13 seconds, more than two read-ahead intervals.

The second test deliberately holds the shutdown interval open to make the interleaving deterministic. It uses the same split shutdown API that runtime uses; it neither changes production code nor fabricates a retirement response. The tests assert the observed defect, so **passing reproduces the bug**. Both passed in the targeted run (13.611 seconds).

```sh
cargo test-all --offline -E 'binary(review_nov15)'
```

### Suggested correction

Represent stopping consensus separately from completing voter retirement. An idempotent shutdown must not create a successful retirement acknowledgement. Either retain an explicit stopped/retired distinction, or derive the acknowledgement from the node's durable applied removal state and promotion boundary, using its own identity. Keep the router available as required by the existing shutdown order, but answer `false` or refuse the query while durable retirement has not happened.

Keep the retained learner until the returning node can receive and apply its uniform removal. Do not solve this by standing a voter down as soon as the desired application core excludes it; that would regress the quorum protection from PR #160. Preserve the two shutdown-window tests as regressions, with their assertions changed to require no false acknowledgement and successful catch-up after restart.

## Previous findings and other reviewed changes

- **#183 restart guard:** addressed. The upgrade watch is initialized from persisted protocol state before the node is exposed, and an incompatible core node does not take a seat on restart. Existing restart regressions pass locally.
- **#183 protocol raise:** the ordinary local and forwarded paths now check the leader's effective voters, including both sides of a joint configuration. The added rejection tests pass. The fold remains deterministic. A possible election/preflight race was investigated; the particular controlled schedule tested was rejected by the existing retry check, so this review does **not** claim a reproduced bypass or reopen #183 on that basis. The check remains outside Raft's serialized write path and is an area for further hardening.
- **#195 promotion restart:** the startup decision now distinguishes followed promotion intent from actual Raft state. Its regression passes.
- **#206 stale-voter recovery:** the read-ahead recovery test passes, and the code uses signed membership events to authorize newer core peers without directly altering the Raft log. This recovery is distinct from the missing replication relationship in the confirmed shutdown bug.
- **Pledges and membership fold:** no new regression identified. The signed fold itself is unchanged from the previous reviewed master; zero-on-admission, owner-only pledge changes and pending-proposal expiry remain in place.
- **Retention:** `max_in_snapshot_log_to_keep` remains `u64::MAX`. The full-log-after-snapshot test passes and checks that no log was purged.
- **Custody:** reviewed the complete-item fingerprint check, keeping uploads before publication, counting obliged holders for hand-off, emergency keep reasons, shared fetch slots, and eviction ordering. No additional production defect is established here, but the test instability below remains unresolved.
- The previously deferred promotion-cancellation case C46 was not established as fixed by these changes.

## Validation and unresolved test failures

- First full local run, before adding evidence: **780 passed, 1 failed, 1 skipped**, out of 781 tests. `a_custodian_fetches_unasked_and_lets_go_only_with_copies_elsewhere` timed out waiting 60 seconds for the old custody tag to be released (`pins.rs:143`).
- That exact test passed on an isolated rerun (36.863 seconds). This establishes intermittent behavior, not a root cause or a fix.
- Both new retirement evidence tests passed in a targeted run.
- Clippy with all workspace targets/features and `-D warnings`: passed.
- The exact reviewed master's [CI run](https://github.com/izderadicka/distlib/actions/runs/43744856722) is **red**. Linux failed `an_emergency_copy_stays_past_the_limit_until_the_custodian_holds_the_item`, waiting 60 seconds for `Emergency = false`. macOS failed the custodianship acceptance test because it measured an emergency copy at 9.763 seconds against its 10-second minimum. Windows, formatting/Clippy and UI jobs passed. These failures are different from the new retirement reproduction; their root causes are not established by this review.
- Final full local run with the two evidence tests: **783 passed, 0 failed, 1 skipped** in 232.478 seconds. Both new tests reproduced the shutdown bug again. The earlier local hand-off timeout and both tests that failed in upstream CI passed in this run; their intermittent failures remain recorded above. The skipped test is the explicitly ignored 100,000-entry measurement.
- Final formatting check: passed.

Socket-using test commands ran with network permission enabled. Production source files were not modified. The evidence and this report are on [`review/raft-nov15`](https://github.com/izderadicka/distlib/tree/review/raft-nov15).
