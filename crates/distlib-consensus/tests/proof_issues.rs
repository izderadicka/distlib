//! The findings of the membership review, each one now a regression test.
//!
//! Started life as `proof_issues.rs` on the review branch, where every test
//! here asserted that the bug was present and all of them passed. They are
//! kept in the same file, under the same finding ids, so that a finding maps
//! to a test without reading a diff — what changed is which way round each
//! assertion runs.
//!
//! | Finding | What was wrong | Test |
//! |---|---|---|
//! | MEM-01 | an address-only core change erased every pending core proposal | `mem_01_*` |
//! | MEM-02 | `reset_for_promotion` left the pre-promotion snapshot behind | `mem_02_*` |
//! | MEM-03 | `MemberAdded` on an existing member wiped their pledge | `mem_03_*` |
//! | MEM-04 | a demoted core node kept its seat and never followed | `crates/distlib/tests/founding.rs` |
//! | MEM-06 | `display_name` and `reason` were unbounded | `mem_06_*` |
//!
//! MEM-04 needs two real nodes and a leader to demote one of them, so it lives
//! with the other end-to-end membership tests rather than here. MEM-05 is
//! known and deferred.

#![allow(clippy::unwrap_used)]

use distlib_consensus::{
    ConsensusError, LogStore, MAX_DISPLAY_NAME, MAX_REASON, MemberRecord, MembershipEvent,
    MembershipState, SignedEvent, StateMachineStore, Timestamp, TypeConfig,
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

/// MEM-01: an address-only core change used to erase *every* pending core
/// proposal, this one included, because `enact` treated "the core group" as a
/// single question.
///
/// Dave's promotion does still go — it names Carol at the address she has just
/// left, so approving it later would put that address back, which under
/// `relay_mode = "disabled"` is how a core node becomes permanently
/// unreachable (P1-23). What changed is that it goes *because it is stale*
/// rather than because anything touched the core group at all, it is logged
/// rather than silent, and it can be remade at once against the map that now
/// holds.
#[test]
fn mem_01_a_pending_core_map_that_would_revert_an_address_is_dropped_and_can_be_remade() {
    let (mut state, founders) = founded_by(3);
    let (a, b, c) = (&founders[0], &founders[1], &founders[2]);
    let dave = Signer::generate();

    propose(
        &mut state,
        a,
        MembershipEvent::MemberAdded {
            member: dave.record("dave"),
        },
    )
    .unwrap();

    // Alice proposes promoting Dave. Changing who votes takes 2 of 3, so it waits.
    propose(
        &mut state,
        a,
        MembershipEvent::CoreGroupChanged {
            core: core_group(&[a, b, c, &dave]),
        },
    )
    .unwrap();
    assert_eq!(state.pending().count(), 1);

    // Carol moves house. An address is not a voter change, so one core
    // member's say-so is enough and it takes effect immediately.
    let moved = c.addr(5);
    let mut updated = core_group(&[a, b, c]);
    updated[2].1 = moved.clone();
    propose(
        &mut state,
        c,
        MembershipEvent::CoreGroupChanged { core: updated },
    )
    .unwrap();
    assert_eq!(state.core().get(&c.id), Some(&moved));

    // Dave's map named Carol where she no longer is, so it is gone. Stated as
    // the property rather than as a count, because the count is not what
    // matters: what must not exist is an approvable map that would put the
    // dead address back. Narrowing this rule to "prune only when the voter
    // *ids* move" — the shape the review recommended — leaves exactly that.
    assert!(
        !state.pending().any(|(_, waiting)| matches!(
            waiting.event(),
            MembershipEvent::CoreGroupChanged { core }
                if core.iter().any(|(id, at)| *id == c.id && *at != moved)
        )),
        "no approvable proposal may put Carol's old address back"
    );
    assert_eq!(state.pending().count(), 0);

    // And nothing was lost that cannot be had again: the same promotion,
    // composed against the map that now holds, waits for its approvals and
    // carries Carol's new address with it.
    let mut wanted = core_group(&[a, b, c, &dave]);
    wanted[2].1 = moved.clone();
    propose(
        &mut state,
        a,
        MembershipEvent::CoreGroupChanged {
            core: wanted.clone(),
        },
    )
    .unwrap();
    let (proposal, _) = state.pending().next().expect("waiting for approvals");
    propose(&mut state, b, MembershipEvent::Approved { proposal }).unwrap();

    assert!(state.is_core(&dave.id), "Dave should be voting now");
    assert_eq!(
        state.core().get(&c.id),
        Some(&moved),
        "and Carol must still be where she moved to"
    );
}

/// MEM-01, the half the fix is actually for: a pending change that does *not*
/// disagree with what just happened keeps its approvals.
///
/// Demoting Carol and moving Carol are compatible — the map omits her either
/// way, so approving it cannot revert the move, it makes it moot. Before the
/// fix this proposal was dropped along with everything else, discarding
/// approvals nobody had withdrawn and leaving the group to start the
/// governance over.
#[test]
fn mem_01_an_address_change_leaves_a_pending_demotion_standing() {
    let (mut state, founders) = founded_by(3);
    let (a, b, c) = (&founders[0], &founders[1], &founders[2]);

    // Alice proposes dropping Carol from the core group: 2 of 3, so it waits.
    propose(
        &mut state,
        a,
        MembershipEvent::CoreGroupChanged {
            core: core_group(&[a, b]),
        },
    )
    .unwrap();
    let (proposal, _) = state.pending().next().expect("waiting for approvals");

    // Carol moves house while the vote on her seat is in progress.
    let mut updated = core_group(&[a, b, c]);
    updated[2].1 = c.addr(5);
    propose(
        &mut state,
        c,
        MembershipEvent::CoreGroupChanged { core: updated },
    )
    .unwrap();

    assert_eq!(
        state.pending().count(),
        1,
        "the demotion says nothing about where Carol is, so the move cannot have answered it"
    );

    // Bob's approval finishes what Alice started, on the proposal Alice made.
    propose(&mut state, b, MembershipEvent::Approved { proposal }).unwrap();
    assert!(!state.is_core(&c.id), "Carol should have been demoted");
    assert_eq!(state.core().len(), 2);
}

/// MEM-01: a pending core map that still names a member the group has since
/// expelled is stale by the same rule, arriving through a different door.
///
/// The mirror of the test above: absence never counts against a map, but
/// *presence* does whenever the change moved that member — and an expelled
/// voter has been moved out of the core group by an event that is not a
/// `CoreGroupChanged` at all.
#[test]
fn mem_01_a_pending_map_naming_an_expelled_voter_is_dropped() {
    let (mut state, founders) = founded_by(3);
    let (a, b, c) = (&founders[0], &founders[1], &founders[2]);
    let dave = Signer::generate();

    propose(
        &mut state,
        a,
        MembershipEvent::MemberAdded {
            member: dave.record("dave"),
        },
    )
    .unwrap();

    // Alice proposes promoting Dave, keeping everyone else where they are.
    // Changing who votes takes 2 of 3, so it waits.
    propose(
        &mut state,
        a,
        MembershipEvent::CoreGroupChanged {
            core: core_group(&[a, b, c, &dave]),
        },
    )
    .unwrap();
    assert_eq!(state.pending().count(), 1);

    // Carol is expelled before anyone gets to Dave's promotion.
    propose(
        &mut state,
        a,
        MembershipEvent::MemberExpelled {
            member: c.id,
            reason: "left the group".to_owned(),
        },
    )
    .unwrap();
    let expulsion = state.changed_at();
    propose(
        &mut state,
        b,
        MembershipEvent::Approved {
            proposal: expulsion,
        },
    )
    .unwrap();

    assert!(state.member(&c.id).is_none(), "Carol should be gone");
    assert!(!state.is_core(&c.id));
    assert_eq!(
        state.pending().count(),
        0,
        "Dave's map still names Carol as a voter, and she is no longer a member"
    );
}

/// MEM-02: `reset_for_promotion` clears the snapshot it is resetting past.
///
/// Two failures, and the second is the worse one. A stale snapshot is handed
/// to a leader as this node's picture of a group it is no longer in; and
/// because `build_snapshot` refuses to persist anything older than what is
/// stored, every snapshot the node built afterwards was dropped and its log
/// would never compact again.
#[tokio::test]
async fn mem_02_reset_for_promotion_clears_the_stale_snapshot() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path().join("sm.redb")).unwrap());

    let _log = LogStore::from_database(Arc::clone(&db)).unwrap();
    let mut sm = StateMachineStore::from_database(db).unwrap();

    let alice = Signer::generate();
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

    let at_ten = sm
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap()
        .meta
        .snapshot_id;
    assert_eq!(
        sm.get_current_snapshot()
            .await
            .unwrap()
            .unwrap()
            .meta
            .snapshot_id,
        at_ten
    );

    sm.reset_for_promotion().await.unwrap();
    assert!(sm.membership().is_empty());

    assert!(
        sm.get_current_snapshot().await.unwrap().is_none(),
        "a snapshot of the group this node has just left is not this node's state"
    );

    // And the node can snapshot again from its new, lower watermark.
    let bob = Signer::generate();
    sm.apply(vec![founding_entry(&bob)]).await.unwrap();
    let at_one = sm
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap()
        .meta
        .snapshot_id;
    assert_eq!(
        sm.get_current_snapshot()
            .await
            .unwrap()
            .unwrap()
            .meta
            .snapshot_id,
        at_one,
        "nothing older is left to block it, so the log can compact again"
    );
}

/// MEM-04, the part the end-to-end test cannot see: a demoted node's follow
/// cursor starts from what its Raft had applied.
///
/// Tested here rather than against two running nodes because the difference
/// is invisible from outside — a cursor left at zero re-fetches the whole log
/// on the first poll, the fold refuses every replayed event, and the node
/// converges on the same membership either way. What it costs is the transfer
/// and a refusal logged per entry, every time a node is demoted.
#[tokio::test]
async fn mem_04_a_demoted_node_follows_on_from_what_it_applied() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("sm.redb");

    let alice = Signer::generate();
    {
        let mut sm = StateMachineStore::open(&path).unwrap();
        sm.apply(vec![founding_entry(&alice)]).await.unwrap();
        for index in 2..=10 {
            sm.apply(vec![entry(
                index,
                alice.sign(
                    MembershipEvent::PledgeChanged {
                        member: alice.id,
                        pledge_bytes: index * 100,
                    },
                    index - 1,
                ),
            )])
            .await
            .unwrap();
        }
        assert_eq!(sm.last_applied_index(), 10);
        assert_eq!(
            sm.followed_upto(),
            0,
            "a voter never follows, so its cursor has never moved"
        );

        sm.resume_following().await.unwrap();
        assert_eq!(sm.followed_upto(), 10);
    }

    // And it is on disk, so a restart between standing down and the first
    // poll does not put the node back to re-fetching the log.
    let reopened = StateMachineStore::open(&path).unwrap();
    assert_eq!(reopened.followed_upto(), 10);
}

/// MEM-03: `MemberAdded` naming a member who is already one keeps their pledge.
///
/// The pledge is the one field of the record its own member owns —
/// `PledgeChanged` is self-only precisely so nobody sets somebody else's — and
/// `MemberAdded` always carries the zero `propose_add` fills in. Correcting a
/// display name used to retract the storage promise along with it.
#[test]
fn mem_03_readmitting_a_member_keeps_their_pledge() {
    let (mut state, signers) = founded_by(1);
    let alice = &signers[0];
    let bob = Signer::generate();

    propose(
        &mut state,
        alice,
        MembershipEvent::MemberAdded {
            member: bob.record("bob"),
        },
    )
    .unwrap();
    propose(
        &mut state,
        &bob,
        MembershipEvent::PledgeChanged {
            member: bob.id,
            pledge_bytes: 500_000_000_000,
        },
    )
    .unwrap();

    propose(
        &mut state,
        alice,
        MembershipEvent::MemberAdded {
            member: MemberRecord {
                member_id: bob.id,
                display_name: "bob_updated".to_owned(),
                pledge_bytes: 0,
            },
        },
    )
    .unwrap();

    let bob_now = state.member(&bob.id).unwrap();
    assert_eq!(
        bob_now.pledge_bytes, 500_000_000_000,
        "correcting a name must not retract a storage promise"
    );
    assert_eq!(
        bob_now.display_name, "bob_updated",
        "and the correction itself must still land"
    );
}

/// MEM-03, the other half: re-admitting somebody who was *expelled* starts
/// them at zero, because there is no record left to carry anything over from.
#[test]
fn mem_03_readmitting_an_expelled_member_starts_their_pledge_at_zero() {
    let (mut state, signers) = founded_by(1);
    let alice = &signers[0];
    let bob = Signer::generate();

    propose(
        &mut state,
        alice,
        MembershipEvent::MemberAdded {
            member: bob.record("bob"),
        },
    )
    .unwrap();
    propose(
        &mut state,
        &bob,
        MembershipEvent::PledgeChanged {
            member: bob.id,
            pledge_bytes: 500_000_000_000,
        },
    )
    .unwrap();
    propose(
        &mut state,
        alice,
        MembershipEvent::MemberExpelled {
            member: bob.id,
            reason: "left".to_owned(),
        },
    )
    .unwrap();
    propose(
        &mut state,
        alice,
        MembershipEvent::MemberAdded {
            member: bob.record("bob"),
        },
    )
    .unwrap();

    assert_eq!(state.member(&bob.id).unwrap().pledge_bytes, 0);
}

/// MEM-06: free text on a proposal is bounded.
///
/// Checked where a proposal enters the log rather than in the fold — see
/// [`MembershipEvent::within_limits`] — so this is the rule itself; that both
/// doors call it is `an_over_long_name_is_refused_before_it_is_signed` in
/// `crates/distlib-consensus/tests/node.rs`.
#[test]
fn mem_06_free_text_on_a_proposal_is_bounded() {
    let bob = Signer::generate();
    let long = "x".repeat(MAX_DISPLAY_NAME + 1);

    let mut record = bob.record(&long);
    let refused = MembershipEvent::MemberAdded {
        member: record.clone(),
    }
    .within_limits()
    .unwrap_err();
    assert!(
        matches!(refused, ConsensusError::TooLong { ref field, limit, .. }
            if field == "display_name" && limit == MAX_DISPLAY_NAME),
        "unexpected refusal: {refused}"
    );

    // Founding takes the same names from configuration, by the same door.
    assert!(
        MembershipEvent::found(
            vec![(record.clone(), NodeAddr::default())],
            Timestamp::from_millis(1)
        )
        .unwrap()
        .within_limits()
        .is_err()
    );

    // Exactly at the limit is fine; bytes, not characters.
    record.display_name = "x".repeat(MAX_DISPLAY_NAME);
    assert!(
        MembershipEvent::MemberAdded {
            member: record.clone()
        }
        .within_limits()
        .is_ok()
    );
    record.display_name = "é".repeat(MAX_DISPLAY_NAME);
    assert!(
        MembershipEvent::MemberAdded { member: record }
            .within_limits()
            .is_err()
    );

    let refused = MembershipEvent::MemberExpelled {
        member: bob.id,
        reason: "x".repeat(MAX_REASON + 1),
    }
    .within_limits()
    .unwrap_err();
    assert!(
        matches!(refused, ConsensusError::TooLong { ref field, .. } if field == "reason"),
        "unexpected refusal: {refused}"
    );

    // Nothing free-text on the rest, and they must not start refusing.
    assert!(
        MembershipEvent::PledgeChanged {
            member: bob.id,
            pledge_bytes: 1,
        }
        .within_limits()
        .is_ok()
    );
}
