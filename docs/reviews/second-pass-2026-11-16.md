# Second-pass implementation review — 2026-11-16

## Scope and baseline

Reviewed implementation at `ed88649a1cf34b9822667dbf54de02cfab0f5a40`. GitHub master at the time of this pass was `a22ac6ff9febc8c85b26b3ec911ca33ecfbd0b0f`; the intervening changes affect only two planning documents, so the production code is identical. Conclusions below come from code and executable evidence, not design documentation.

This pass found two additional defects, distinct from retirement issue [#208](https://github.com/izderadicka/distlib/issues/208). Neither finding requires log truncation or purging. These are newly verified findings, not claims that the latest merge introduced them. Existing findings tracked in #185 and #200 are not counted as new findings here.

The branch adds three tests and this report; it makes no production fix. **Passing proof tests assert the defective behavior.**

## 1. P2 — Delayed heartbeats from an old process incarnation overwrite the restarted member's state

### Cause

`crates/distlib-sync/src/availability/online.rs:88` rejects an older sequence number only when the incoming epoch equals the currently remembered epoch. Every different epoch is accepted and replaces the remembered epoch. After accepting a restart, the receiver therefore accepts messages from the previously observed, superseded incarnation again.

The signed receive path at `crates/distlib-sync/src/availability/heartbeat.rs:630` verifies the signatures and group, calls this check, then updates holdings at line 650. A valid signature establishes authorship; it does not establish that this process incarnation is still current.

### Reproduction and observed result

The tests use the production signed heartbeat receiver:

1. Receive epoch 10, sequence 1, advertising an item.
2. Receive epoch 20, sequence 1, advertising no items after restart. The old holding is correctly removed.
3. Receive a previously unseen, delayed epoch 10, sequence 2 heartbeat advertising the old item.
4. The receiver puts the member back into the item's real-holder set and considers it online.

A second test delivers a delayed epoch-10 goodbye after the epoch-20 restart heartbeat. The receiver marks the live, restarted member offline.

These are distinct signed messages, not duplicate deliveries of the same gossip payload. Message deduplication does not reject a previously unseen delayed message. The tests control delivery order at the signed receiver; they do not reproduce the ordering through a live multi-hop gossip network.

Evidence: `crates/distlib-sync/src/availability/review_nov16.rs`:

- `proof_a_delayed_old_epoch_restores_holdings_removed_after_restart`
- `proof_an_old_goodbye_takes_a_restarted_member_offline`

### Impact and limits

This affects the current protocol. Availability can regress from a newer truthful empty report to an obsolete positive report, or incorrectly mark a restarted member offline. Subsequent fresh heartbeats can repair the view, but incorrect decisions can be made in the interval.

Custody's `enough_obliged_copies` in `crates/distlib-custody/src/pins.rs:715` counts members that are both online and reported as real holders. An obsolete positive report can therefore contribute to the copy count used to release a local copy. This downstream risk follows from the code; the tests do **not** demonstrate an end-to-end release or data loss.

### Suggested solution

Define ordering for process incarnations and reject superseded incarnations before updating liveness, TTL or holdings. Remembering retired epochs addresses the demonstrated return to an already observed old incarnation; a complete design also needs to handle receiver restarts and delayed incarnations the receiver has not previously seen. A persisted monotonic incarnation or equivalent authenticated ordering is stronger than treating every unfamiliar random epoch as newer. Current epochs are random `u64` values: numeric comparison of those values is not a valid fix.

Regression tests should expect both delayed messages above to be ignored while allowing valid messages from a genuinely newer incarnation.

## 2. P2 — Rolling back a voter leaves its newer build's capability in the log, allowing an unsupported protocol raise

### Cause

`announce_protocol_support` at `crates/distlib-consensus/src/node.rs:162` proposes an update only when the persisted member record advertises less than the running build. It never corrects a larger advertisement left by a newer build.

Startup eligibility at `node.rs:326` and the upgrade check at `crates/distlib-consensus/src/raft/follower.rs:261` compare the binary's support with the **group's current protocol**, not its own persisted capability advertisement. An older binary can therefore resume voting when the group has not yet raised its protocol.

The leader's actual-voter preflight, `crates/distlib-consensus/src/raft/core_group.rs:434`, relies on those persisted capability advertisements. A stale advertisement passes the check even though the running voter no longer supports that protocol.

### Reproduction and observed result

`crates/distlib-consensus/tests/review_nov16_protocol.rs` runs a real peer and restarts it using the same durable database:

1. Found a group at protocol 1.
2. Use the production capability announcer to advertise protocol 2, simulating the previous newer build. Keep the group at protocol 1.
3. Restart the current protocol-1 binary from that store. It seats itself as a voter and reports no upgrade requirement.
4. Run its normal protocol-1 announcer. The stored advertisement remains protocol 2.
5. Propose a raise to protocol 2 through the ordinary proposal path. It succeeds; the protocol-1 process remains seated and still reports no upgrade requirement.

Test: `proof_a_rolled_back_voter_keeps_its_newer_builds_capability_and_accepts_a_raise`.

### Impact and limits

This is a latent upgrade/rollback defect. The current binary supports protocol 1, so a deployment containing only honest current binaries does not normally advertise protocol 2. The test simulates the earlier newer build's advertisement; it does not run an actual protocol-2 executable or exercise future protocol-2 semantics.

The scenario also requires the older binary to be able to read the persisted store. Under those conditions, the protocol activation check no longer establishes that all participating voters support the activated protocol. This differs from the already-group-at-protocol-2 startup case: here the group is still at protocol 1 when the rollback starts.

### Suggested solution

Choose and enforce a rollback policy. A conservative policy refuses voter startup when this member's persisted capability exceeds the running binary's maximum, and prevents the role-management path from reseating it. Alternatively, capability withdrawal needs a safe protocol that prevents voting or activation based on a stale advertisement until the correction is committed.

Keep local binary support checks out of the deterministic membership fold. The runtime should enforce participation eligibility; every replica must continue to derive the same fold result from the same log.

## Reproduction commands

From the review checkout, with the Rust toolchain configured:

```sh
cargo nextest run --workspace --all-features --offline -E 'test(review_nov16)'
cargo test-all --offline -E 'binary(review_nov16_protocol)'
```

The first command runs the two pure signed-receiver tests. The second runs the peer restart test and needs local socket access.

## Validation

- All three new proof tests passed in targeted runs.
- `cargo clippy --workspace --all-targets --all-features --offline -- -D warnings` passed.
- `cargo fmt --all -- --check` and `git diff --check` passed.
- Full `cargo test-all --offline --no-fail-fast`, with socket access enabled: 786 tests ran, 785 passed, 1 failed, 1 skipped. All three new proofs passed. The existing `a_download_does_not_wait_on_an_offline_member` failed its one-second elapsed-time assertion at 1.092859816 seconds; it passed an isolated rerun. This does not establish whether the full-run failure was scheduling load or an unwanted dial. It is not counted as a third verified defect.
- A second full-suite invocation was cancelled before execution; there is no clean second full-suite result to report.

The checkout includes the two previous retirement proof tests from local commit `6bad81c`. Those tests are separate from the three new proofs above.
