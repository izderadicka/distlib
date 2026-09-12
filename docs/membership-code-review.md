# Membership Implementation Code Review

**Project:** `distlib`
**Scope:** Deep inspection of the membership implementation (`distlib-consensus`, `distlib-net`, `distlib-core`, `distlib-api`, CLI commands)
**Date:** September 2026

---

## Executive Summary

As of Phase 2.3-2, the `distlib` membership layer is functional and backed by comprehensive automated test suites (`acceptance.rs`, `membership.rs`, `node.rs`, etc.). The design guarantees—such as mutual authentication via ed25519 keys, allowlist enforcement at the endpoint level, signed log entry attribution, and Raft consensus—are cleanly structured and implemented.

This code review identified **6 findings** across the consensus state machine, storage layer, node lifecycle, and API methods:
- **2 High Severity Findings**: Logic flaws in pending proposal management and snapshot persistence during role transitions.
- **2 Medium Severity Findings**: Unexpected state side-effects on member re-admission and incomplete runtime demotion cleanup.
- **2 Low Severity / Quality Findings**: Unbounded API input fields and hardcoded time/log expiry thresholds.

Executable proof of concept tests demonstrating the findings have been added in `crates/distlib-consensus/tests/proof_issues.rs` without modifying any production source code.

---

## Findings Overview

| ID | Title | Severity | Location | Proof Test |
|---|---|---|---|---|
| **MEM-01** | Core Address Update Silently Erases Unrelated Pending Core Proposals | **High** | `distlib-consensus::state` | `proof_core_address_update_erases_pending_core_proposals` |
| **MEM-02** | `reset_for_promotion` Retains Stale Snapshot Record in Database | **High** | `distlib-consensus::raft::state_machine` | `proof_reset_for_promotion_leaves_stale_snapshot` |
| **MEM-03** | Member Re-admission via `MemberAdded` Overwrites Storage Pledge | **Medium** | `distlib-consensus::state` | `proof_readmitting_member_resets_pledge_bytes` |
| **MEM-04** | Demoted Core Member Remains in Raft Engine Loop Without Transitioning to Follower | **Medium** | `distlib-consensus::raft::core_group` / `node` | N/A (runtime task lifecycle) |
| **MEM-05** | Pending Proposal Expiry Uses Log Entry Distance Rather Than Real Time | **Low** | `distlib-consensus::state` | N/A (design limitation) |
| **MEM-06** | Unbounded String Lengths in API Proposals (`display_name`, `reason`) | **Low** | `distlib-api::methods` | N/A (validation gap) |

---

## Detailed Findings

### MEM-01: Core Address Update Silently Erases Unrelated Pending Core Proposals

- **Severity:** High
- **Component:** `distlib-consensus::state` (`MembershipState::enact`)
- **Impact:** An operator updating a core node's IP/port via `distlib core set` inadvertently cancels in-flight core group governance proposals (e.g., a pending promotion or demotion waiting for majority approval).

#### Root Cause Analysis
In `MembershipState::enact`, when any `MembershipEvent` is enacted, stale pending proposals are pruned:
```rust
let about = subject(event);
self.pending.retain(|_, pending| {
    let pending = subject(&pending.event);
    if pending == about {
        return false;
    }
    !(moved_the_voters && pending == Some(Subject::CoreGroup))
});
```
Here, `subject(event)` returns `Some(Subject::CoreGroup)` for **any** `CoreGroupChanged` event—whether it changes the voter set (e.g. promoting or demoting a member) or merely updates a socket address (`core set`).
Because `subject(event)` matches `Subject::CoreGroup`, enacting an address update matches `pending == about` for ALL pending core proposals, dropping them from `self.pending`.

#### Proof of Concept
A test in `crates/distlib-consensus/tests/proof_issues.rs` (`proof_core_address_update_erases_pending_core_proposals`):
1. Alice proposes promoting Dave to core (`CoreGroupChanged`). Dave's promotion is placed in `pending` awaiting majority approval.
2. Carol updates her own socket address (`CoreGroupChanged`).
3. Dave's pending promotion proposal is deleted from `pending`.

#### Recommendation
Refine the subject classification or pruning logic in `enact()` so that simple socket address updates (`Readdress`) do not match pending voter membership proposals (`Subject::CoreGroup`), or only prune pending core proposals if the actual voter set (`core.keys()`) changes.

---

### MEM-02: `reset_for_promotion` Retains Stale Snapshot Record in Database

- **Severity:** High
- **Component:** `distlib-consensus::raft::state_machine` (`StateMachineStore::reset_for_promotion`)
- **Impact:** A promoted node serves stale pre-promotion snapshots to catch-up peers via `get_current_snapshot()`. Furthermore, attempts to create new snapshots after promotion fail to persist because `build_snapshot()` compares against the stale snapshot watermark on disk.

#### Root Cause Analysis
When a follower is promoted to a voter in `StateMachineStore::reset_for_promotion()`, the applied state machine state is reset to `Applied::default()` and saved to redb under the `APPLIED` key in table `SM`:
```rust
pub async fn reset_for_promotion(&self) -> StorageResult<()> {
    let encoded = {
        let mut applied = self.lock();
        *applied = Applied::default();
        encode(&*applied, ErrorSubject::StateMachine)?
    };

    write_key(&self.inner.db, SM, APPLIED, encoded, ErrorSubject::StateMachine).await?;
    self.announce(&MembershipState::new());
    Ok(())
}
```
However, the `SNAPSHOT` key in table `SM` is **not** cleared or deleted.
Consequently:
1. `get_current_snapshot()` reads `read_key(db, SM, SNAPSHOT, ...)`, which returns the pre-promotion snapshot.
2. When `build_snapshot()` executes later at a lower log index (e.g., index 1 after re-founding or catching up), it checks:
   ```rust
   let newer_exists = stored.meta.last_log_id > ours;
   if newer_exists { return Ok(()); }
   ```
   Because `stored.meta.last_log_id` (e.g. index 10) is greater than `ours` (index 1), `build_snapshot()` aborts persisting the new snapshot.

#### Proof of Concept
A test in `crates/distlib-consensus/tests/proof_issues.rs` (`proof_reset_for_promotion_leaves_stale_snapshot`):
1. A node applies 10 entries and creates a snapshot at index 10.
2. `reset_for_promotion()` is called.
3. `get_current_snapshot()` returns the old snapshot at index 10.
4. Applying entry 1 and building a new snapshot fails to overwrite index 10 on disk.

#### Recommendation
In `reset_for_promotion()`, explicitly delete the `SNAPSHOT` key from table `SM` in redb within the same transaction that resets `APPLIED`.

---

### MEM-03: Member Re-admission via `MemberAdded` Overwrites Storage Pledge

- **Severity:** Medium
- **Component:** `distlib-consensus::state` (`MembershipState::apply_to_founded_group`)
- **Impact:** Re-admitting a member or updating their display name via `distlib admit` / `group.propose_add` wipes out their previously recorded storage pledge (`pledge_bytes`), setting it back to `0`.

#### Root Cause Analysis
In `apply_to_founded_group()`:
```rust
MembershipEvent::MemberAdded { member } => {
    self.members.insert(member.member_id, member.clone());
    Ok(())
}
```
`MemberAdded` carries a `MemberRecord { member_id, display_name, pledge_bytes }`.
When `distlib admit <id> --name <new_name>` or API `group.propose_add` is called, `pledge_bytes` is hardcoded to `0` (since admission does not specify pledge).
When `self.members.insert` executes, it completely replaces the existing `MemberRecord` for that member ID, resetting `pledge_bytes` from whatever value the member had previously set (e.g. 500 GB) to `0`.

#### Proof of Concept
A test in `crates/distlib-consensus/tests/proof_issues.rs` (`proof_readmitting_member_resets_pledge_bytes`):
1. Member Bob is admitted.
2. Bob sets his pledge to 500 GB (`PledgeChanged`).
3. Admin re-admits Bob or updates his display name via `MemberAdded`.
4. Bob's `pledge_bytes` drops to 0.

#### Recommendation
In `apply_to_founded_group()` for `MemberAdded`, if `self.members` already contains `member.member_id`, preserve the existing `pledge_bytes`:
```rust
if let Some(existing) = self.members.get_mut(&member.member_id) {
    existing.display_name = member.display_name.clone();
} else {
    self.members.insert(member.member_id, member.clone());
}
```

---

### MEM-04: Demoted Core Member Remains in Raft Engine Loop Without Transitioning to Follower

- **Severity:** Medium
- **Component:** `distlib-consensus::raft::core_group` and `distlib-consensus::node`
- **Impact:** When a core voter is demoted by the group via `CoreGroupChanged` or `MemberExpelled`, the leader's reconciliation loop removes the voter from openraft. However, on the **demoted node itself**, the process continues running its `openraft::Raft` engine in its seat and does not spawn a follower loop.

#### Root Cause Analysis
In Phase 2.3-2, promotion was implemented (`take_the_seat` transitions a follower to a voter).
Demotion in-place on a running node was documented as a known gap (see `docs/plan-phases/phase-2-catalogue.md` "Carried out of Phase 2"):
- When `MembershipState` indicates a node is no longer core, its `enact` loop on the leader removes it from openraft.
- But on the demoted node itself, `seat.is_taken()` remains `true`. The demoted node retains its `Raft` instance and never spawns `follower::follow`.
- The demoted node becomes isolated from log updates because it is no longer replicated to by the leader, and it is not polling over `memberlog` as a follower.

#### Recommendation
Implement the demotion counterpart to `take_the_seat`:
1. Observe `MembershipState::is_core(&self.id)` on the running node.
2. If `is_core` transitions from `true` to `false`, shut down the node's `Raft` engine, clear the `Seat`, and spawn `follower::follow`.

---

### MEM-05: Pending Proposal Expiry Uses Log Entry Distance Rather Than Real Time

- **Severity:** Low
- **Component:** `distlib-consensus::state` (`PENDING_EXPIRY = 128`)
- **Impact:** Pending proposals expire after `128` committed membership log entries. In quiet groups where membership changes are rare, a proposal can wait indefinitely. In high-velocity groups, 128 log entries might pass quickly during automated batch operations, expiring a proposal earlier than expected.

#### Root Cause Analysis
As documented in `state.rs`, log entry distance was selected because system clocks are unverified across nodes. However, using a single fixed constant `128` across all groups leads to asymmetric behavior depending on group velocity.

#### Recommendation
Keep log-index distance for determinism, but make `PENDING_EXPIRY` a group policy parameter stored in the membership state (or log policy event), or introduce a time-backed pruning mechanism when applying snapshot timestamps.

---

### MEM-06: Unbounded String Lengths in API Proposals

- **Severity:** Low
- **Component:** `distlib-api::methods` (`ProposeAdd`, `ProposeExpel`)
- **Impact:** An authenticated API caller (holding the local `api.token`) can submit arbitrarily large strings for `display_name` or `reason` (e.g. megabytes of text), which are then postcard-encoded, signed, replicated through Raft, and stored permanently in `redb`.

#### Root Cause Analysis
Structs `ProposeAdd` and `ProposeExpel` deserialize JSON params without checking string lengths:
```rust
struct ProposeAdd {
    member: MemberId,
    #[serde(default)]
    name: Option<String>,
}
```
No validation is performed on `name.len()` or `reason.len()` before constructing `MembershipEvent` and passing it to `node.propose()`.

#### Recommendation
Add strict length limits (e.g. `display_name` ≤ 128 chars, `reason` ≤ 1024 chars) in `propose_add` and `propose_expel` in `distlib-api::methods`.

---

## Verification & Proof Tests

All proof tests are located in `crates/distlib-consensus/tests/proof_issues.rs` and can be executed via:

```sh
cargo test --test proof_issues
```

Test results:
```text
running 3 tests
test proof_core_address_update_erases_pending_core_proposals ... ok
test proof_reset_for_promotion_leaves_stale_snapshot ... ok
test proof_readmitting_member_resets_pledge_bytes ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.25s
```

All existing workspace unit and integration tests continue to pass without regression (`cargo test --workspace`).
