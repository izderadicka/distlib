//! The rules the membership log enforces, and the properties its projection holds.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use distlib_consensus::{
    ConsensusError, MemberRecord, MembershipEvent, MembershipState, SignedEvent, Timestamp,
};
use distlib_core::MemberId;
use iroh::SecretKey;
use proptest::prelude::*;

/// A member we can sign as.
struct Signer {
    secret: SecretKey,
    id: MemberId,
}

impl Signer {
    fn generate() -> Self {
        Self::from_secret(SecretKey::generate())
    }

    /// A signer with a fixed key, so a scenario replayed twice uses the same
    /// members and the two runs are actually comparable.
    fn seeded(seed: u8) -> Self {
        Self::from_secret(SecretKey::from_bytes(&[seed.wrapping_add(1); 32]))
    }

    fn from_secret(secret: SecretKey) -> Self {
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

    fn sign(&self, event: MembershipEvent) -> SignedEvent {
        SignedEvent::sign(&self.secret, event, Timestamp::from_millis(1)).unwrap()
    }
}

/// A founded group with `alice` as its single founder.
fn founded() -> (MembershipState, Signer) {
    let alice = Signer::generate();
    let mut state = MembershipState::new();
    state
        .apply(&alice.sign(MembershipEvent::found(
            vec![alice.record("alice")],
            Timestamp::from_millis(1),
        )))
        .unwrap();
    (state, alice)
}

fn allowlist(state: &MembershipState) -> Vec<MemberId> {
    state.allowlist().collect()
}

// --- founding ---------------------------------------------------------------

#[test]
fn founding_seeds_members_and_core() {
    let (state, alice) = founded();

    assert!(state.group_id().is_some());
    assert_eq!(allowlist(&state), vec![alice.id]);
    assert!(
        state.core().contains(&alice.id),
        "founders are the initial voters"
    );
}

#[test]
fn a_group_is_founded_only_once() {
    let (mut state, alice) = founded();

    let again = alice.sign(MembershipEvent::found(
        vec![alice.record("alice")],
        Timestamp::from_millis(2),
    ));

    assert_eq!(state.apply(&again), Err(ConsensusError::AlreadyFounded));
}

#[test]
fn events_before_founding_are_refused() {
    let alice = Signer::generate();
    let mut state = MembershipState::new();

    let event = alice.sign(MembershipEvent::MemberAdded {
        member: alice.record("alice"),
    });

    assert_eq!(state.apply(&event), Err(ConsensusError::NotFounded));
}

#[test]
fn a_founder_must_be_in_their_own_founding_set() {
    // Otherwise they create a group they are not in, and can never propose to it.
    let alice = Signer::generate();
    let bob = Signer::generate();
    let mut state = MembershipState::new();

    let event = alice.sign(MembershipEvent::found(
        vec![bob.record("bob")],
        Timestamp::from_millis(1),
    ));

    assert_eq!(
        state.apply(&event),
        Err(ConsensusError::FounderNotIncluded { proposer: alice.id })
    );
}

#[test]
fn the_group_id_does_not_depend_on_founder_order() {
    let alice = Signer::generate();
    let bob = Signer::generate();
    let at = Timestamp::from_millis(7);

    let one = MembershipEvent::found(vec![alice.record("a"), bob.record("b")], at);
    let other = MembershipEvent::found(vec![bob.record("b"), alice.record("a")], at);

    let (
        MembershipEvent::GroupFounded {
            group_id: first, ..
        },
        MembershipEvent::GroupFounded {
            group_id: second, ..
        },
    ) = (&one, &other)
    else {
        panic!("found() must produce GroupFounded");
    };
    assert_eq!(first, second, "founders are sorted before hashing");
}

// --- who may propose --------------------------------------------------------

#[test]
fn a_non_member_cannot_propose() {
    // The check that keeps the log closed.
    let (mut state, _alice) = founded();
    let outsider = Signer::generate();

    let event = outsider.sign(MembershipEvent::MemberAdded {
        member: outsider.record("outsider"),
    });

    assert_eq!(
        state.apply(&event),
        Err(ConsensusError::ProposerNotAMember {
            proposer: outsider.id
        }),
        "an outsider must not be able to add themselves"
    );
}

#[test]
fn an_expelled_member_can_no_longer_propose() {
    let (mut state, alice) = founded();
    let bob = Signer::generate();
    state
        .apply(&alice.sign(MembershipEvent::MemberAdded {
            member: bob.record("bob"),
        }))
        .unwrap();
    state
        .apply(&alice.sign(MembershipEvent::MemberExpelled {
            member: bob.id,
            reason: "left".to_owned(),
        }))
        .unwrap();

    let event = bob.sign(MembershipEvent::MemberExpelled {
        member: alice.id,
        reason: "revenge".to_owned(),
    });

    assert_eq!(
        state.apply(&event),
        Err(ConsensusError::ProposerNotAMember { proposer: bob.id })
    );
}

// --- signatures -------------------------------------------------------------

#[test]
fn a_signature_from_the_wrong_key_is_refused() {
    let (mut state, alice) = founded();
    let mallory = Signer::generate();

    // Mallory signs an event, then it is re-attributed to Alice on the wire —
    // what a compromised core node would try. `SignedEvent`'s fields are
    // private, so the only way to forge one is to rewrite its encoding, which
    // is exactly the capability a hostile peer has.
    //
    // The event names only Alice, so Mallory's id occurs once (as proposer) and
    // the splice cannot hit the wrong field.
    let forged = mallory.sign(MembershipEvent::PledgeChanged {
        member: alice.id,
        pledge_bytes: 99,
    });
    let mut bytes = postcard::to_stdvec(&forged).unwrap();
    let alice_id = postcard::to_stdvec(&alice.id).unwrap();
    let mallory_id = postcard::to_stdvec(&mallory.id).unwrap();
    assert_eq!(
        bytes
            .windows(mallory_id.len())
            .filter(|window| *window == mallory_id.as_slice())
            .count(),
        1,
        "the proposer id must appear exactly once for this splice to be meaningful"
    );
    let at = bytes
        .windows(mallory_id.len())
        .position(|window| window == mallory_id.as_slice())
        .expect("the proposer id must appear in the encoding");
    bytes.splice(at..at + mallory_id.len(), alice_id);

    let tampered: SignedEvent = postcard::from_bytes(&bytes).unwrap();
    assert!(
        matches!(
            state.apply(&tampered),
            Err(ConsensusError::BadSignature { .. })
        ),
        "an event re-attributed to another member must not verify"
    );
}

// --- expulsion --------------------------------------------------------------

#[test]
fn expulsion_removes_from_the_allowlist_and_the_core() {
    let (mut state, alice) = founded();
    let bob = Signer::generate();
    state
        .apply(&alice.sign(MembershipEvent::MemberAdded {
            member: bob.record("bob"),
        }))
        .unwrap();
    state
        .apply(&alice.sign(MembershipEvent::CoreGroupChanged {
            core: vec![alice.id, bob.id],
        }))
        .unwrap();
    assert!(state.core().contains(&bob.id));

    state
        .apply(&alice.sign(MembershipEvent::MemberExpelled {
            member: bob.id,
            reason: "inactive".to_owned(),
        }))
        .unwrap();

    assert!(!allowlist(&state).contains(&bob.id));
    assert!(
        !state.core().contains(&bob.id),
        "a non-member must not remain a voter; raft would wait on a vote that cannot come"
    );
}

#[test]
fn an_expelled_member_can_be_re_admitted() {
    let (mut state, alice) = founded();
    let bob = Signer::generate();

    for event in [
        MembershipEvent::MemberAdded {
            member: bob.record("bob"),
        },
        MembershipEvent::MemberExpelled {
            member: bob.id,
            reason: "mistake".to_owned(),
        },
        MembershipEvent::MemberAdded {
            member: bob.record("bob again"),
        },
    ] {
        state.apply(&alice.sign(event)).unwrap();
    }

    assert!(state.is_member(&bob.id), "the latest event wins");
    assert_eq!(state.member(&bob.id).unwrap().display_name, "bob again");
}

#[test]
fn expelling_a_non_member_is_refused() {
    let (mut state, alice) = founded();
    let stranger = Signer::generate();

    let event = alice.sign(MembershipEvent::MemberExpelled {
        member: stranger.id,
        reason: "who?".to_owned(),
    });

    assert_eq!(
        state.apply(&event),
        Err(ConsensusError::UnknownMember {
            member: stranger.id
        })
    );
}

// --- core group -------------------------------------------------------------

#[test]
fn the_core_group_must_be_members() {
    let (mut state, alice) = founded();
    let outsider = Signer::generate();

    let event = alice.sign(MembershipEvent::CoreGroupChanged {
        core: vec![alice.id, outsider.id],
    });

    assert_eq!(state.apply(&event), Err(ConsensusError::InvalidCoreGroup));
}

#[test]
fn the_core_group_cannot_be_emptied() {
    // A group with no voters can never commit anything again, including the
    // event that would restore its voters.
    let (mut state, alice) = founded();

    let event = alice.sign(MembershipEvent::CoreGroupChanged { core: vec![] });

    assert_eq!(state.apply(&event), Err(ConsensusError::InvalidCoreGroup));
}

// --- pledges ----------------------------------------------------------------

#[test]
fn a_pledge_change_updates_the_record() {
    let (mut state, alice) = founded();

    state
        .apply(&alice.sign(MembershipEvent::PledgeChanged {
            member: alice.id,
            pledge_bytes: 42,
        }))
        .unwrap();

    assert_eq!(state.member(&alice.id).unwrap().pledge_bytes, 42);
}

// --- properties -------------------------------------------------------------

/// A founded group plus a sequence of events over a small pool of members.
///
/// Generated as *indices* into the pool so the events refer to each other
/// coherently; keys are made once, since generating them is the slow part.
fn scenario() -> impl Strategy<Value = (usize, Vec<(usize, usize, u64)>)> {
    (
        1usize..4,
        prop::collection::vec((0usize..4, 0usize..4, 0u64..3), 0..12),
    )
}

proptest! {
    // Every case signs a dozen events, and ed25519 signing dominates the
    // runtime. The reachable state space here is small — four members, three
    // kinds of operation — so fewer cases lose very little.
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Folding the same events always reaches the same *encoding* — the property
    /// the whole design rests on, since every node folds the log independently
    /// and Phase 1a will snapshot the result and compare it across nodes.
    ///
    /// Compared as bytes rather than by `PartialEq`: map equality ignores
    /// iteration order, so it would happily pass if the state were switched to a
    /// `HashMap` and the snapshot encoding started differing between nodes.
    #[test]
    fn folding_is_deterministic((founders, ops) in scenario()) {
        let first = postcard::to_stdvec(&fold(founders, &ops)).unwrap();
        let second = postcard::to_stdvec(&fold(founders, &ops)).unwrap();
        prop_assert_eq!(first, second);
    }

    /// Applying a prefix and then the rest equals applying the whole run, so
    /// `apply` carries no state between calls and a node that catches up in
    /// chunks lands where a node that replayed everything at once does.
    #[test]
    fn applying_in_chunks_matches_applying_at_once((founders, ops) in scenario()) {
        let split = ops.len() / 2;
        prop_assert_eq!(fold(founders, &ops), fold_in_two(founders, &ops, split));
    }

    /// Nobody outside the membership is ever in the derived allowlist. This is
    /// the security property the transport layer depends on.
    #[test]
    fn the_allowlist_never_exceeds_the_membership((founders, ops) in scenario()) {
        let state = fold(founders, &ops);
        for id in state.allowlist() {
            prop_assert!(state.is_member(&id));
        }
    }

    /// Voters are always members. Raft cannot wait on a vote from someone who is
    /// no longer allowed to connect.
    #[test]
    fn the_core_group_is_always_a_subset_of_the_membership((founders, ops) in scenario()) {
        let state = fold(founders, &ops);
        for id in state.core() {
            prop_assert!(state.is_member(id));
        }
    }
}

/// The fixed cast a generated scenario draws from. Seeded rather than random so
/// two runs of the same scenario involve the same members.
fn pool() -> Vec<Signer> {
    (0..4).map(Signer::seeded).collect()
}

/// Runs a generated scenario, ignoring events the rules refuse — the point is
/// the state that results, not which operations happened to be legal.
fn fold(founders: usize, ops: &[(usize, usize, u64)]) -> MembershipState {
    let pool = pool();
    let mut state = MembershipState::new();
    apply_scenario(&mut state, &pool, founders, ops, 0..ops.len());
    state
}

fn fold_in_two(founders: usize, ops: &[(usize, usize, u64)], split: usize) -> MembershipState {
    let pool = pool();
    let mut state = MembershipState::new();
    apply_scenario(&mut state, &pool, founders, ops, 0..split);
    apply_scenario(&mut state, &pool, founders, ops, split..ops.len());
    state
}

fn apply_scenario(
    state: &mut MembershipState,
    pool: &[Signer],
    founders: usize,
    ops: &[(usize, usize, u64)],
    range: std::ops::Range<usize>,
) {
    if range.start == 0 {
        let records = pool[..founders]
            .iter()
            .map(|s| s.record("founder"))
            .collect();
        let _ =
            state.apply(&pool[0].sign(MembershipEvent::found(records, Timestamp::from_millis(1))));
    }
    for &(actor, subject, kind) in &ops[range] {
        let (actor, subject) = (&pool[actor % pool.len()], &pool[subject % pool.len()]);
        let event = match kind {
            0 => MembershipEvent::MemberAdded {
                member: subject.record("member"),
            },
            1 => MembershipEvent::MemberExpelled {
                member: subject.id,
                reason: "generated".to_owned(),
            },
            _ => MembershipEvent::CoreGroupChanged {
                core: vec![actor.id, subject.id],
            },
        };
        // Refused events are expected and are exactly what the rules are for.
        let _ = state.apply(&actor.sign(event));
    }
}
