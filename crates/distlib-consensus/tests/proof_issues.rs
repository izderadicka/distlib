//! Proof of concept tests demonstrating edge cases and potential issues
//! discovered during the membership implementation review.
//!
//! These tests DO NOT modify any functional source code, serving as verified proofs.

#![allow(clippy::unwrap_used)]

use distlib_consensus::{
    ConsensusError, LogStore, MemberRecord, MembershipEvent, MembershipState, SignedEvent,
    StateMachineStore, Timestamp, TypeConfig,
};
use distlib_core::{MemberId, NodeAddr, RawMemberId};
use iroh::SecretKey;
use openraft::{
    CommittedLeaderId, Entry, EntryPayload, LogId, RaftSnapshotBuilder, storage::RaftStateMachine,
};
use redb::Database;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tempfile::TempDir;

struct Signer {
    secret: SecretKey,
    id: MemberId,
}

impl Signer {
    fn generate() -> Self {
        let secret = SecretKey::generate();
        let id = MemberId::from(secret.public());
        Self { secret, id }
    }

    fn record(&self, name: &str) -> MemberRecord {
        MemberRecord {
            member_id: self.id,
            display_name: name.to_owned(),
            pledge_bytes: 0,
        }
    }

    fn founder(&self, name: &str) -> (MemberRecord, NodeAddr) {
        (self.record(name), self.addr(0))
    }

    fn addr(&self, port: u16) -> NodeAddr {
        let octet = self.id.as_bytes()[0];
        NodeAddr::default().with_direct(SocketAddr::from((
            Ipv4Addr::new(127, 0, 0, octet),
            11204 + port,
        )))
    }

    fn sign(&self, event: MembershipEvent, changed_at: u64) -> SignedEvent {
        SignedEvent::sign(&self.secret, event, Timestamp::from_millis(1), changed_at).unwrap()
    }
}

fn propose(
    state: &mut MembershipState,
    who: &Signer,
    event: MembershipEvent,
) -> Result<(), ConsensusError> {
    let seen = state.changed_at();
    let signed = who.sign(event, seen);
    state.apply(seen + 1, &signed)
}

fn founded_by(n: usize) -> (MembershipState, Vec<Signer>) {
    let signers: Vec<Signer> = (0..n).map(|_| Signer::generate()).collect();
    let founders = signers
        .iter()
        .enumerate()
        .map(|(index, signer)| signer.founder(&format!("founder-{index}")))
        .collect();
    let mut state = MembershipState::new();
    propose(
        &mut state,
        &signers[0],
        MembershipEvent::found(founders, Timestamp::from_millis(1)).unwrap(),
    )
    .unwrap();
    (state, signers)
}

fn core_group(signers: &[&Signer]) -> Vec<(MemberId, NodeAddr)> {
    signers
        .iter()
        .map(|signer| (signer.id, signer.addr(0)))
        .collect()
}

fn log_id(index: u64) -> LogId<RawMemberId> {
    LogId::new(CommittedLeaderId::new(1, RawMemberId::default()), index)
}

fn entry(index: u64, event: SignedEvent) -> Entry<TypeConfig> {
    Entry {
        log_id: log_id(index),
        payload: EntryPayload::Normal(event),
    }
}

fn founding_entry(founder: &Signer) -> Entry<TypeConfig> {
    entry(
        1,
        founder.sign(
            MembershipEvent::found(
                vec![(founder.record("founder"), NodeAddr::default())],
                Timestamp::from_millis(1),
            )
            .unwrap(),
            0,
        ),
    )
}

/// PROOF 1: Updating a core node's address silently erases unrelated pending core group proposals.
///
/// When an operator updates a core node's IP/port via `CoreGroupChanged` (which is an address set),
/// `MembershipState::enact` matches `pending == Subject::CoreGroup` and removes ALL pending core group
/// proposals—even those waiting for majority approval (such as a pending promotion or demotion)!
#[test]
fn proof_core_address_update_erases_pending_core_proposals() {
    let (mut state, founders) = founded_by(3);
    let (a, b, c) = (&founders[0], &founders[1], &founders[2]);
    let dave = Signer::generate();

    // Admit Dave
    propose(
        &mut state,
        a,
        MembershipEvent::MemberAdded {
            member: dave.record("dave"),
        },
    )
    .unwrap();

    // Alice proposes promoting Dave to core (CoreGroupChanged). Needs 2 of 3 approvals.
    propose(
        &mut state,
        a,
        MembershipEvent::CoreGroupChanged {
            core: core_group(&[a, b, c, &dave]),
        },
    )
    .unwrap();

    let pending_count_before = state.pending().count();
    assert_eq!(
        pending_count_before, 1,
        "Dave's promotion should be waiting in pending"
    );

    // Now, Carol updates her own address (CoreGroupChanged with new address for Carol).
    let mut updated_core = core_group(&[a, b, c]);
    updated_core[2].1 = c.addr(5); // New port for Carol
    propose(
        &mut state,
        c,
        MembershipEvent::CoreGroupChanged { core: updated_core },
    )
    .unwrap();

    // Check pending proposals:
    let pending_count_after = state.pending().count();
    assert_eq!(
        pending_count_after, 0,
        "BUG DEMONSTRATED: Dave's promotion proposal was silently erased by Carol's address update!"
    );
}

/// PROOF 2: `reset_for_promotion` leaves a stale `SNAPSHOT` record in redb, causing stale snapshot
/// reads and preventing new snapshot creation when `last_applied` is lower than the old snapshot's index.
#[tokio::test]
async fn proof_reset_for_promotion_leaves_stale_snapshot() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path().join("sm.redb")).unwrap());

    let _log = LogStore::from_database(Arc::clone(&db)).unwrap();
    let mut sm = StateMachineStore::from_database(db).unwrap();

    let alice = Signer::generate();
    // Build state up to index 10
    for idx in 1..=10 {
        sm.apply(vec![entry(
            idx,
            alice.sign(
                MembershipEvent::PledgeChanged {
                    member: alice.id,
                    pledge_bytes: idx * 100,
                },
                idx - 1,
            ),
        )])
        .await
        .unwrap();
    }

    // Build and save a snapshot at index 10
    let snapshot10 = sm
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    let snap10_id = snapshot10.meta.snapshot_id.clone();

    // Verify snapshot is present in current_snapshot
    let current_before = sm.get_current_snapshot().await.unwrap().unwrap();
    assert_eq!(current_before.meta.snapshot_id, snap10_id);

    // Now call reset_for_promotion (as happens when a follower is promoted)
    sm.reset_for_promotion().await.unwrap();

    // Verify state machine state was reset:
    assert!(
        sm.membership().is_empty(),
        "Membership state should be reset"
    );

    // BUG PART A: get_current_snapshot still returns the OLD pre-promotion snapshot at index 10!
    let current_after_reset = sm.get_current_snapshot().await.unwrap();
    assert!(
        current_after_reset.is_some(),
        "BUG DEMONSTRATED: get_current_snapshot returns stale snapshot (index 10) from before promotion!"
    );
    assert_eq!(
        current_after_reset.unwrap().meta.snapshot_id,
        snap10_id,
        "BUG DEMONSTRATED: Returned snapshot is the old pre-promotion snapshot!"
    );

    // BUG PART B: Attempting to build a new snapshot while applied index < 10 (e.g. index 1)
    // fails to persist because `newer_exists` compares against the stale snapshot at index 10!
    let bob = Signer::generate();
    sm.apply(vec![founding_entry(&bob)]).await.unwrap(); // New founding at index 1

    let mut builder = sm.get_snapshot_builder().await;
    let _new_snapshot = builder.build_snapshot().await.unwrap();

    // Verify that current_snapshot STILL returns snap10_id because the new snapshot at index 1 was blocked by index 10
    let current_after_build = sm.get_current_snapshot().await.unwrap().unwrap();
    assert_eq!(
        current_after_build.meta.snapshot_id, snap10_id,
        "BUG DEMONSTRATED: New snapshot at index 1 was NOT persisted because old snapshot at index 10 blocked it!"
    );
}

/// PROOF 3: Re-admitting an existing member via MemberAdded silently resets their pledge_bytes to 0.
#[test]
fn proof_readmitting_member_resets_pledge_bytes() {
    let (mut state, signers) = founded_by(1);
    let alice = &signers[0];
    let bob = Signer::generate();

    // Admit Bob
    propose(
        &mut state,
        alice,
        MembershipEvent::MemberAdded {
            member: bob.record("bob"),
        },
    )
    .unwrap();

    // Bob sets his storage pledge to 500 GB (500_000_000_000 bytes)
    propose(
        &mut state,
        &bob,
        MembershipEvent::PledgeChanged {
            member: bob.id,
            pledge_bytes: 500_000_000_000,
        },
    )
    .unwrap();

    assert_eq!(state.member(&bob.id).unwrap().pledge_bytes, 500_000_000_000);

    // Alice updates Bob's display name or re-admits Bob via MemberAdded (e.g. through propose_add API)
    propose(
        &mut state,
        alice,
        MembershipEvent::MemberAdded {
            member: MemberRecord {
                member_id: bob.id,
                display_name: "bob_updated".to_owned(),
                pledge_bytes: 0, // Default in MemberAdded / propose_add
            },
        },
    )
    .unwrap();

    // Check Bob's pledge:
    assert_eq!(
        state.member(&bob.id).unwrap().pledge_bytes,
        0,
        "EDGE CASE DEMONSTRATED: Re-adding Bob with MemberAdded wiped his pledge back to 0!"
    );
}
