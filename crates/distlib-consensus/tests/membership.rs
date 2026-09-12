//! The rules the membership log enforces, and the properties its projection holds.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use distlib_consensus::{
    ConsensusError, MemberRecord, MembershipEvent, MembershipState, PENDING_EXPIRY, SignedEvent,
    Timestamp,
};
use std::net::{Ipv4Addr, SocketAddr};

use distlib_core::{GroupId, MemberId, Namespace, NamespaceSecret, NodeAddr};
use iroh::SecretKey;
// The property tests, and only they, are generated.
#[cfg(feature = "slow-tests")]
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
    ///
    /// Only the property tests replay anything, so this goes with them.
    #[cfg(feature = "slow-tests")]
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

    /// The founder entry for this signer: its record, and where it is reachable.
    ///
    /// A distinct address per signer, derived from the id, because the point of
    /// the log carrying addresses is that they are not interchangeable — a test
    /// that gave every founder the same one could not tell a lost address from
    /// a kept one.
    fn founder(&self, name: &str) -> (MemberRecord, NodeAddr) {
        (self.record(name), self.addr(0))
    }

    /// A recognisable address for this signer, `port` distinguishing successive
    /// addresses of the same one.
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

/// `who` proposes `event` against the membership as it stands, applied as the
/// next log entry.
///
/// The index only has to be monotonic here — nothing in this file is a real
/// Raft log — but it must move, because it is what the next proposal is
/// checked against.
fn propose(
    state: &mut MembershipState,
    who: &Signer,
    event: MembershipEvent,
) -> Result<(), ConsensusError> {
    let seen = state.changed_at();
    let signed = who.sign(event, seen);
    state.apply(seen + 1, &signed)
}

/// Applies an already-signed event as the next entry.
///
/// For the cases that build one by hand — a tampered signature, a deliberately
/// stale `changed_at` — where [`propose`] would sign a correct one.
fn apply_next(state: &mut MembershipState, signed: &SignedEvent) -> Result<(), ConsensusError> {
    let index = state.changed_at() + 1;
    state.apply(index, signed)
}

/// `who` approves the proposal made at log index `proposal`.
fn approve(state: &mut MembershipState, who: &Signer, proposal: u64) -> Result<(), ConsensusError> {
    propose(state, who, MembershipEvent::Approved { proposal })
}

/// The index of the one proposal waiting for approvals.
///
/// Panics if there is not exactly one, because every test here that asks has
/// made exactly one and the alternative is asserting against whichever the map
/// happened to yield first.
fn the_pending_one(state: &MembershipState) -> u64 {
    let pending: Vec<u64> = state.pending().map(|(index, _)| index).collect();
    assert_eq!(pending.len(), 1, "expected exactly one pending proposal");
    pending[0]
}

/// A founded group of `n` founders, all of whom therefore vote.
///
/// Founding rather than admitting, for two reasons: promotion is refused until
/// a node can start voting without restarting (see `the_core_group_cannot_grow_yet`),
/// and the size of the core group is what sets every threshold in §4.4, so a
/// test that needs three voters has to start with three.
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

/// A founded group with `alice` and `bob` as founders, so both already vote.
fn founded_by_two() -> (MembershipState, Signer, Signer) {
    let (state, mut signers) = founded_by(2);
    let bob = signers.pop().expect("two founders");
    let alice = signers.pop().expect("two founders");
    (state, alice, bob)
}

/// A founded group with `alice` as its single founder.
fn founded() -> (MembershipState, Signer) {
    let alice = Signer::generate();
    let mut state = MembershipState::new();
    propose(
        &mut state,
        &alice,
        MembershipEvent::found(vec![alice.founder("alice")], Timestamp::from_millis(1)).unwrap(),
    )
    .unwrap();
    (state, alice)
}

/// The core group as `CoreGroupChanged` wants it: each member at its own address.
fn core_group(signers: &[&Signer]) -> Vec<(MemberId, NodeAddr)> {
    signers
        .iter()
        .map(|signer| (signer.id, signer.addr(0)))
        .collect()
}

/// Just the ids of the current voters, for the assertions that only care who.
fn voters(state: &MembershipState) -> Vec<MemberId> {
    state.core().keys().copied().collect()
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
    assert!(state.is_core(&alice.id), "founders are the initial voters");
}

#[test]
fn a_group_is_founded_only_once() {
    let (mut state, alice) = founded();

    let again = alice.sign(
        MembershipEvent::found(vec![alice.founder("alice")], Timestamp::from_millis(2)).unwrap(),
        state.changed_at(),
    );

    assert_eq!(
        apply_next(&mut state, &again),
        Err(ConsensusError::AlreadyFounded)
    );
}

#[test]
fn events_before_founding_are_refused() {
    let alice = Signer::generate();
    let mut state = MembershipState::new();

    let event = alice.sign(
        MembershipEvent::MemberAdded {
            member: alice.record("alice"),
        },
        state.changed_at(),
    );

    assert_eq!(
        apply_next(&mut state, &event),
        Err(ConsensusError::NotFounded)
    );
}

#[test]
fn a_founder_must_be_in_their_own_founding_set() {
    // Otherwise they create a group they are not in, and can never propose to it.
    let alice = Signer::generate();
    let bob = Signer::generate();
    let mut state = MembershipState::new();

    let event = alice.sign(
        MembershipEvent::found(vec![bob.founder("bob")], Timestamp::from_millis(1)).unwrap(),
        state.changed_at(),
    );

    assert_eq!(
        apply_next(&mut state, &event),
        Err(ConsensusError::FounderNotIncluded { proposer: alice.id })
    );
}

#[test]
fn a_repeated_founder_is_refused_when_building_the_event() {
    let alice = Signer::generate();

    let built = MembershipEvent::found(
        vec![alice.founder("alice"), alice.founder("alice again")],
        Timestamp::from_millis(1),
    );

    assert_eq!(
        built.unwrap_err(),
        ConsensusError::DuplicateFounder { member: alice.id },
        "the group id is derived from the founder list, so it is only well \
         defined for a set"
    );
}

#[test]
fn a_repeated_founder_is_refused_on_apply() {
    // Hand-built rather than via `found`, standing in for an event arriving
    // from a node whose constructor we did not run. Without this check the
    // founders would collapse into the member map and leave the group id
    // describing a larger set than the group actually has.
    let alice = Signer::generate();
    let bob = Signer::generate();
    let mut state = MembershipState::new();

    let event = alice.sign(
        MembershipEvent::GroupFounded {
            group_id: GroupId::from_bytes([7; 32]),
            founders: vec![
                alice.founder("alice"),
                bob.founder("bob"),
                alice.founder("dup"),
            ],
        },
        state.changed_at(),
    );

    assert_eq!(
        apply_next(&mut state, &event),
        Err(ConsensusError::DuplicateFounder { member: alice.id })
    );
}

#[test]
fn a_group_cannot_be_founded_empty() {
    assert_eq!(
        MembershipEvent::found(Vec::new(), Timestamp::from_millis(1)).unwrap_err(),
        ConsensusError::NoFounders
    );
}

#[test]
fn the_group_id_derivation_is_fixed() {
    // A golden value, because this is a wire fact: two nodes computing it
    // differently would disagree about which group they are in, and nothing
    // would say so. It pins the tag, the count, the timestamp encoding and the
    // sort together — the sort in particular, which is over `MemberId` and has
    // to stay its key bytes' own order.
    let founders: Vec<(MemberRecord, NodeAddr)> = [3u8, 1, 2]
        .into_iter()
        .map(|seed| {
            let signer = Signer::from_secret(SecretKey::from_bytes(&[seed; 32]));
            signer.founder("founder")
        })
        .collect();

    let MembershipEvent::GroupFounded { group_id, .. } =
        MembershipEvent::found(founders, Timestamp::from_millis(1)).unwrap()
    else {
        panic!("found() builds a GroupFounded");
    };

    assert_eq!(
        group_id.to_string(),
        "5dfd2ec7199b5fa93b50ff9e6eb68e981ac46e2c2624000601eaccabb8e840c1"
    );
}

#[test]
fn the_group_id_does_not_depend_on_founder_order() {
    let alice = Signer::generate();
    let bob = Signer::generate();
    let at = Timestamp::from_millis(7);

    let one = MembershipEvent::found(vec![alice.founder("a"), bob.founder("b")], at).unwrap();
    let other = MembershipEvent::found(vec![bob.founder("b"), alice.founder("a")], at).unwrap();

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

    let event = outsider.sign(
        MembershipEvent::MemberAdded {
            member: outsider.record("outsider"),
        },
        state.changed_at(),
    );

    assert_eq!(
        apply_next(&mut state, &event),
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
    propose(
        &mut state,
        &alice,
        MembershipEvent::MemberAdded {
            member: bob.record("bob"),
        },
    )
    .unwrap();
    propose(
        &mut state,
        &alice,
        MembershipEvent::MemberExpelled {
            member: bob.id,
            reason: "left".to_owned(),
        },
    )
    .unwrap();

    let event = bob.sign(
        MembershipEvent::MemberExpelled {
            member: alice.id,
            reason: "revenge".to_owned(),
        },
        state.changed_at(),
    );

    assert_eq!(
        apply_next(&mut state, &event),
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
    let forged = mallory.sign(
        MembershipEvent::PledgeChanged {
            member: alice.id,
            pledge_bytes: 99,
        },
        state.changed_at(),
    );
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
            apply_next(&mut state, &tampered),
            Err(ConsensusError::BadSignature { .. })
        ),
        "an event re-attributed to another member must not verify"
    );
}

// --- expulsion --------------------------------------------------------------

#[test]
fn expulsion_removes_from_the_allowlist_and_the_core() {
    // Three founders, because removing a voter takes a majority of the voters
    // and the one being removed does not get a say: with two, one approval is
    // all there could ever be and a majority is two.
    let (mut state, founders) = founded_by(3);
    let (alice, bob, carol) = (&founders[0], &founders[1], &founders[2]);
    assert!(state.is_core(&carol.id));

    propose(
        &mut state,
        alice,
        MembershipEvent::MemberExpelled {
            member: carol.id,
            reason: "inactive".to_owned(),
        },
    )
    .unwrap();
    let proposal = the_pending_one(&state);
    approve(&mut state, bob, proposal).unwrap();

    assert!(!allowlist(&state).contains(&carol.id));
    assert!(
        !state.is_core(&carol.id),
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
        propose(&mut state, &alice, event).unwrap();
    }

    assert!(state.is_member(&bob.id), "the latest event wins");
    assert_eq!(state.member(&bob.id).unwrap().display_name, "bob again");
}

#[test]
fn expelling_a_non_member_is_refused() {
    let (mut state, alice) = founded();
    let stranger = Signer::generate();

    let event = alice.sign(
        MembershipEvent::MemberExpelled {
            member: stranger.id,
            reason: "who?".to_owned(),
        },
        state.changed_at(),
    );

    assert_eq!(
        apply_next(&mut state, &event),
        Err(ConsensusError::UnknownMember {
            member: stranger.id
        })
    );
}

// --- core group -------------------------------------------------------------

#[test]
fn founding_records_where_each_founder_is() {
    let alice = Signer::generate();
    let bob = Signer::generate();
    let mut state = MembershipState::new();
    propose(
        &mut state,
        &alice,
        MembershipEvent::found(
            vec![alice.founder("alice"), bob.founder("bob")],
            Timestamp::from_millis(1),
        )
        .unwrap(),
    )
    .unwrap();

    assert_eq!(
        state.core().get(&alice.id),
        Some(&alice.addr(0)),
        "the log is where a node with no log yet learns to dial the core group"
    );
    assert_eq!(state.core().get(&bob.id), Some(&bob.addr(0)));
}

#[test]
fn a_voter_can_be_given_a_new_address() {
    // The failure this exists for: with `relay_mode = "disabled"` the recorded
    // socket is the only way to reach a core node, so one that changes IP or
    // port is out of its own group permanently (P1-23). Submitting a changed
    // address is the same event as adding or removing a voter.
    let (mut state, alice) = founded();
    assert_eq!(state.core().get(&alice.id), Some(&alice.addr(0)));

    propose(
        &mut state,
        &alice,
        MembershipEvent::CoreGroupChanged {
            core: vec![(alice.id, alice.addr(1))],
        },
    )
    .unwrap();

    assert_eq!(
        state.core().get(&alice.id),
        Some(&alice.addr(1)),
        "the group must be able to say a core node moved"
    );
    assert_eq!(voters(&state), vec![alice.id], "and only its address moved");
}

#[test]
fn the_core_group_can_grow() {
    // Until 2.3-2 this was refused outright with `PromotionUnsupported`: a node
    // served `distlib/raft/0` only if it had started as a voter, so a promoted
    // one would have counted toward quorum and never answered. Both halves of
    // that have moved — every node serves the protocol now, and the
    // reconciliation loop adds a new voter as a learner before promoting it —
    // so the fold's job here is the same as for any other core-group change:
    // decide whether the map is valid, and record it.
    let (mut state, alice) = founded();
    let bob = Signer::generate();
    propose(
        &mut state,
        &alice,
        MembershipEvent::MemberAdded {
            member: bob.record("bob"),
        },
    )
    .unwrap();

    // A majority of one is one, so alice's own proposal decides it.
    propose(
        &mut state,
        &alice,
        MembershipEvent::CoreGroupChanged {
            core: core_group(&[&alice, &bob]),
        },
    )
    .unwrap();

    let mut grown = voters(&state);
    grown.sort();
    let mut both = vec![alice.id, bob.id];
    both.sort();
    assert_eq!(grown, both, "the projection names the new voter");
}

#[test]
fn a_voter_named_twice_is_refused_rather_than_folded() {
    // Two addresses for one member has no single answer, and collecting into a
    // map would silently keep whichever came last.
    let (mut state, alice) = founded();

    let refused = propose(
        &mut state,
        &alice,
        MembershipEvent::CoreGroupChanged {
            core: vec![(alice.id, alice.addr(1)), (alice.id, alice.addr(2))],
        },
    );

    assert_eq!(refused, Err(ConsensusError::InvalidCoreGroup));
    assert_eq!(
        state.core().get(&alice.id),
        Some(&alice.addr(0)),
        "a refused event must leave the address it failed to change"
    );
}

#[test]
fn the_core_group_must_be_members() {
    let (mut state, alice) = founded();
    let outsider = Signer::generate();

    let event = alice.sign(
        MembershipEvent::CoreGroupChanged {
            core: core_group(&[&alice, &outsider]),
        },
        state.changed_at(),
    );

    assert_eq!(
        apply_next(&mut state, &event),
        Err(ConsensusError::InvalidCoreGroup)
    );
}

#[test]
fn the_core_group_cannot_be_emptied() {
    // A group with no voters can never commit anything again, including the
    // event that would restore its voters.
    let (mut state, alice) = founded();

    let event = alice.sign(
        MembershipEvent::CoreGroupChanged { core: vec![] },
        state.changed_at(),
    );

    assert_eq!(
        apply_next(&mut state, &event),
        Err(ConsensusError::InvalidCoreGroup)
    );
}

// --- approvals (§4.4 step 2) ------------------------------------------------
//
// One rule throughout: changing who votes takes a majority of voters, and
// everything else takes one.

#[test]
fn a_core_member_admitting_somebody_is_a_single_step() {
    // What must not change. An admission takes one approval, a core proposer's
    // own proposal is that approval, so `distlib admit` run against a core node
    // still takes effect at once — the existing acceptance test and the
    // by-hand runbook both depend on it.
    let (mut state, alice) = founded();
    let bob = Signer::generate();

    propose(
        &mut state,
        &alice,
        MembershipEvent::MemberAdded {
            member: bob.record("bob"),
        },
    )
    .unwrap();

    assert!(state.is_member(&bob.id));
    assert_eq!(state.pending().count(), 0, "nothing was left waiting");
}

#[test]
fn an_admission_proposed_by_a_follower_waits_for_a_core_member() {
    // §4.4 step 1 lets any member submit; step 2 gives the decision to the core
    // group. This is the only thing that changes for an ordinary admission.
    let (mut state, alice) = founded();
    let bob = Signer::generate();
    let carol = Signer::generate();
    propose(
        &mut state,
        &alice,
        MembershipEvent::MemberAdded {
            member: bob.record("bob"),
        },
    )
    .unwrap();
    assert!(!state.is_core(&bob.id), "bob joined after founding");

    propose(
        &mut state,
        &bob,
        MembershipEvent::MemberAdded {
            member: carol.record("carol"),
        },
    )
    .unwrap();
    assert!(
        !state.is_member(&carol.id),
        "a follower's proposal must not admit anybody by itself"
    );

    let proposal = the_pending_one(&state);
    approve(&mut state, &alice, proposal).unwrap();
    assert!(state.is_member(&carol.id));
    assert_eq!(
        state.pending().count(),
        0,
        "a decided proposal stops pending"
    );
}

#[test]
fn expelling_a_core_member_takes_a_majority_of_the_core() {
    // The rule this sub-phase exists for. Before it, one signed proposal from
    // any member at all removed a voter — and since a removed voter really does
    // leave openraft's voter set now, a handful of them shrank the group's
    // ability to commit anything.
    let (mut state, founders) = founded_by(3);
    let (alice, bob, carol) = (&founders[0], &founders[1], &founders[2]);

    propose(
        &mut state,
        alice,
        MembershipEvent::MemberExpelled {
            member: carol.id,
            reason: "mistyped, perhaps".to_owned(),
        },
    )
    .unwrap();
    assert!(
        state.is_core(&carol.id),
        "one core member's word must not remove a voter"
    );
    assert_eq!(voters(&state).len(), 3);

    let proposal = the_pending_one(&state);
    approve(&mut state, bob, proposal).unwrap();
    assert!(!state.is_member(&carol.id));
}

#[test]
fn expelling_a_follower_takes_one_core_member() {
    // The other side of the same rule: only *voters* are expensive to remove.
    // Somebody who does not vote can be shown the door by any core member, and
    // that is the common case.
    let (mut state, founders) = founded_by(3);
    let (alice, bob) = (&founders[0], &founders[1]);
    let newcomer = Signer::generate();
    propose(
        &mut state,
        alice,
        MembershipEvent::MemberAdded {
            member: newcomer.record("newcomer"),
        },
    )
    .unwrap();

    propose(
        &mut state,
        bob,
        MembershipEvent::MemberExpelled {
            member: newcomer.id,
            reason: "not a voter".to_owned(),
        },
    )
    .unwrap();

    assert!(!state.is_member(&newcomer.id), "one core member is enough");
    assert_eq!(state.pending().count(), 0);
}

#[test]
fn giving_a_voter_a_new_address_takes_one_core_member() {
    // A core node that moved has not changed who votes, and it is usually
    // urgent: under `relay_mode = "disabled"` the group cannot reach it until
    // this commits. Making it wait for a quorum would be ceremony charged at
    // the worst moment.
    let (mut state, founders) = founded_by(3);
    let moved: Vec<(MemberId, NodeAddr)> = founders
        .iter()
        .enumerate()
        .map(|(index, signer)| (signer.id, signer.addr(u16::from(index == 2))))
        .collect();

    propose(
        &mut state,
        &founders[0],
        MembershipEvent::CoreGroupChanged { core: moved },
    )
    .unwrap();

    assert_eq!(
        state.core().get(&founders[2].id),
        Some(&founders[2].addr(1))
    );
    assert_eq!(state.pending().count(), 0);
}

#[test]
fn the_member_being_expelled_cannot_approve_it() {
    // The threshold is a majority of the core group *including* them, so their
    // own vote would let a core of four remove one of its own on the agreement
    // of two other people rather than three.
    let (mut state, founders) = founded_by(3);
    let (alice, carol) = (&founders[0], &founders[2]);
    propose(
        &mut state,
        alice,
        MembershipEvent::MemberExpelled {
            member: carol.id,
            reason: "asked to leave".to_owned(),
        },
    )
    .unwrap();

    let proposal = the_pending_one(&state);
    assert_eq!(
        approve(&mut state, carol, proposal),
        Err(ConsensusError::SelfApproval { member: carol.id })
    );
    assert!(state.is_core(&carol.id), "the refused approval did nothing");
}

#[test]
fn a_follower_cannot_approve() {
    // Submitting is open to every member; deciding is not (§4.4 step 2).
    let (mut state, founders) = founded_by(3);
    let bystander = Signer::generate();
    propose(
        &mut state,
        &founders[0],
        MembershipEvent::MemberAdded {
            member: bystander.record("bystander"),
        },
    )
    .unwrap();
    propose(
        &mut state,
        &founders[0],
        MembershipEvent::MemberExpelled {
            member: founders[2].id,
            reason: "pending".to_owned(),
        },
    )
    .unwrap();

    let proposal = the_pending_one(&state);
    assert_eq!(
        approve(&mut state, &bystander, proposal),
        Err(ConsensusError::ApproverNotCore {
            approver: bystander.id
        })
    );
    assert!(state.is_core(&founders[2].id));
}

#[test]
fn approving_something_that_is_not_pending_is_refused() {
    // Covers both "never proposed" and "already decided": approvals do not
    // accumulate against a proposal that has taken effect.
    let (mut state, founders) = founded_by(3);
    let (alice, bob, carol) = (&founders[0], &founders[1], &founders[2]);

    assert_eq!(
        approve(&mut state, alice, 7),
        Err(ConsensusError::UnknownProposal { proposal: 7 })
    );

    propose(
        &mut state,
        alice,
        MembershipEvent::MemberExpelled {
            member: carol.id,
            reason: "decided".to_owned(),
        },
    )
    .unwrap();
    let proposal = the_pending_one(&state);
    approve(&mut state, bob, proposal).unwrap();

    assert_eq!(
        approve(&mut state, bob, proposal),
        Err(ConsensusError::UnknownProposal { proposal })
    );
}

#[test]
fn the_last_voter_cannot_be_expelled() {
    // A group with no voters can never commit anything again — including the
    // event that would restore its voters — and unlike `CoreGroupChanged`,
    // which refuses an empty core group outright, expulsion used to walk
    // straight past that guard.
    //
    // What refuses it now is the threshold, not a special case: removing a
    // voter takes a majority of the core, the only core member is the one being
    // removed, and they cannot approve their own expulsion. So it can be
    // proposed and never decided. Deleting any *one* of those three rules is
    // enough to re-open the hole, which is why this asserts the outcome rather
    // than an error.
    let (mut state, alice) = founded();
    let bob = Signer::generate();
    propose(
        &mut state,
        &alice,
        MembershipEvent::MemberAdded {
            member: bob.record("bob"),
        },
    )
    .unwrap();

    for proposer in [&alice, &bob] {
        propose(
            &mut state,
            proposer,
            MembershipEvent::MemberExpelled {
                member: alice.id,
                reason: "the whole core group".to_owned(),
            },
        )
        .unwrap();
        let proposal = the_pending_one(&state);
        assert_eq!(
            approve(&mut state, &alice, proposal),
            Err(ConsensusError::SelfApproval { member: alice.id }),
            "the only core member is the one being expelled"
        );
        // Clear it, so the next round's `the_pending_one` is unambiguous.
        propose(
            &mut state,
            &alice,
            MembershipEvent::MemberAdded {
                member: alice.record("alice"),
            },
        )
        .unwrap();
    }

    assert_eq!(voters(&state), vec![alice.id], "the group can still commit");
}

#[test]
fn an_approval_from_a_member_since_demoted_no_longer_counts() {
    // The threshold is measured against the core group as it stands when an
    // approval lands, and so are the approvals themselves. Counting one from
    // somebody who has left the core group would let a shrinking core carry
    // decisions on the word of people who are no longer voters.
    let (mut state, founders) = founded_by(5);
    let (a, b, c, d, e) = (
        &founders[0],
        &founders[1],
        &founders[2],
        &founders[3],
        &founders[4],
    );

    // Expelling `e` takes three of five. `a` proposes it, `b` agrees: two.
    propose(
        &mut state,
        a,
        MembershipEvent::MemberExpelled {
            member: e.id,
            reason: "under discussion".to_owned(),
        },
    )
    .unwrap();
    let expulsion = the_pending_one(&state);
    approve(&mut state, b, expulsion).unwrap();

    // Meanwhile `b` is demoted, which also takes three of five.
    propose(
        &mut state,
        a,
        MembershipEvent::CoreGroupChanged {
            core: core_group(&[a, c, d, e]),
        },
    )
    .unwrap();
    let demotion = state
        .pending()
        .map(|(index, _)| index)
        .find(|index| *index != expulsion)
        .expect("the demotion is pending too");
    approve(&mut state, c, demotion).unwrap();
    approve(&mut state, d, demotion).unwrap();
    assert!(!state.is_core(&b.id), "b has been demoted");

    // The expulsion now needs three of four, and holds approvals from `a` and
    // `b` — but `b` is not a voter any more, so only `a`'s counts.
    approve(&mut state, c, expulsion).unwrap();
    assert!(
        state.is_member(&e.id),
        "two current voters are not a majority of four"
    );

    approve(&mut state, d, expulsion).unwrap();
    assert!(!state.is_member(&e.id), "three are");
}

#[test]
fn the_approvals_reported_are_the_ones_that_count() {
    // What anything showing an operator "1 of 3" has to show. Reporting the raw
    // set beside a threshold computed from the current core group produces
    // sentences like "2 of 3, still waiting" when only one of the two is still
    // a voter — or, once the core shrinks further, "5 of 2", which reads as a
    // broken group rather than as approvals from people who have left it.
    let (mut state, founders) = founded_by(5);
    let (a, b, c, d, e) = (
        &founders[0],
        &founders[1],
        &founders[2],
        &founders[3],
        &founders[4],
    );

    propose(
        &mut state,
        a,
        MembershipEvent::MemberExpelled {
            member: e.id,
            reason: "under discussion".to_owned(),
        },
    )
    .unwrap();
    let expulsion = the_pending_one(&state);
    approve(&mut state, b, expulsion).unwrap();

    let held = |state: &MembershipState| {
        let (_, proposal) = state
            .pending()
            .find(|(index, _)| *index == expulsion)
            .expect("still pending");
        (
            proposal.approvals().count(),
            state.approvals_counting(proposal).count(),
        )
    };
    assert_eq!(held(&state), (2, 2), "both approvers are voters");

    // Expel `b`, who is one of them. Their approval stays on the record and
    // stops counting.
    propose(
        &mut state,
        a,
        MembershipEvent::MemberExpelled {
            member: b.id,
            reason: "overtaken".to_owned(),
        },
    )
    .unwrap();
    let departure = state
        .pending()
        .map(|(index, _)| index)
        .find(|index| *index != expulsion)
        .expect("pending too");
    approve(&mut state, c, departure).unwrap();
    approve(&mut state, d, departure).unwrap();
    assert!(!state.is_member(&b.id));

    assert_eq!(
        held(&state),
        (2, 1),
        "the record keeps both; only one of them is still a voter"
    );
}

#[test]
fn a_pending_proposal_about_a_member_is_dropped_when_their_membership_changes() {
    // A pending expulsion of somebody who has since been expelled has nothing
    // left to do, and one of somebody since re-admitted was decided against a
    // group they were not in. Either way it must not sit there waiting to be
    // approved into effect against a question that has moved.
    let (mut state, founders) = founded_by(3);
    let alice = &founders[0];
    let (bob, carol) = (Signer::generate(), Signer::generate());
    for newcomer in [&bob, &carol] {
        propose(
            &mut state,
            alice,
            MembershipEvent::MemberAdded {
                member: newcomer.record("newcomer"),
            },
        )
        .unwrap();
    }

    // A follower proposes expelling another follower: one approval short.
    propose(
        &mut state,
        &carol,
        MembershipEvent::MemberExpelled {
            member: bob.id,
            reason: "proposed by a follower".to_owned(),
        },
    )
    .unwrap();
    assert_eq!(state.pending().count(), 1);

    // A core member expels bob directly, which takes effect at once.
    propose(
        &mut state,
        alice,
        MembershipEvent::MemberExpelled {
            member: bob.id,
            reason: "decided the other way".to_owned(),
        },
    )
    .unwrap();

    assert!(!state.is_member(&bob.id));
    assert_eq!(
        state.pending().count(),
        0,
        "a proposal about a member whose membership just changed is stale"
    );
}

#[test]
fn an_approval_can_be_repeated_to_re_examine_a_proposal() {
    // A proposal can become sufficient without anybody approving it: expelling
    // a voter takes a majority of the core, so a core group that shrinks lowers
    // the threshold under a proposal already sitting there. Nothing re-examines
    // a pending proposal on its own — deciding several at once on one
    // membership change would be a surprising cascade — so an approver saying
    // so again is what makes it reachable.
    let (mut state, founders) = founded_by(5);
    let (a, b, c, e) = (&founders[0], &founders[1], &founders[2], &founders[4]);

    // Three of five to expel `e`; `a` and `b` agree, which is two.
    propose(
        &mut state,
        a,
        MembershipEvent::MemberExpelled {
            member: e.id,
            reason: "under discussion".to_owned(),
        },
    )
    .unwrap();
    let expulsion = the_pending_one(&state);
    approve(&mut state, b, expulsion).unwrap();

    // `d` stands down, so three of five becomes three of four... and then `c`
    // stands down too, leaving `a`, `b` and `e`: a majority is two, which the
    // proposal already has.
    for core in [core_group(&[a, b, c, e]), core_group(&[a, b, e])] {
        propose(&mut state, a, MembershipEvent::CoreGroupChanged { core }).unwrap();
        let demotion = state
            .pending()
            .map(|(index, _)| index)
            .find(|index| *index != expulsion)
            .expect("the demotion is pending");
        for approver in [b, c, e] {
            if state.pending().any(|(index, _)| index == demotion) {
                approve(&mut state, approver, demotion).unwrap();
            }
        }
    }
    assert_eq!(
        voters(&state).len(),
        3,
        "the core group has shrunk to three"
    );
    assert!(
        state.is_member(&e.id),
        "nothing re-examined the proposal on its own"
    );

    approve(&mut state, b, expulsion).unwrap();
    assert!(
        !state.is_member(&e.id),
        "an approver saying so again is what re-examines it"
    );
}

#[test]
fn a_last_approval_that_cannot_be_applied_leaves_the_proposal_as_it_was() {
    // `apply` promises that a refused entry changes nothing, and the last
    // approval is the one place that is not free: the rules are re-checked and
    // the change attempted in the same step, so a change that fails must not
    // leave the approval that triggered it on the record. Otherwise a proposal
    // that can never be applied quietly accumulates agreement, and a later
    // reader cannot tell how many people actually said yes to something that
    // happened.
    //
    // A core group naming somebody who is not a member is the case to use,
    // because it fails at apply time for a reason nothing between the proposal
    // and the last approval can clear. `InvalidCoreGroup` is about the map
    // itself, and the only thing that would make it valid — admitting the
    // stranger — is a change to a different subject, so the prune in `enact`
    // does not reach it either. The races that used to serve here are now
    // swept as stale before they can fail, which is that prune's whole point;
    // promotion served here until 2.3-2 made it succeed.
    let (mut state, founders) = founded_by(3);
    let (a, b, c) = (&founders[0], &founders[1], &founders[2]);
    let stranger = Signer::generate();

    // Changing the core group moves the voters, so it takes two of the three.
    propose(
        &mut state,
        a,
        MembershipEvent::CoreGroupChanged {
            core: core_group(&[a, b, c, &stranger]),
        },
    )
    .unwrap();
    let doomed = the_pending_one(&state);

    assert_eq!(
        approve(&mut state, b, doomed),
        Err(ConsensusError::InvalidCoreGroup)
    );

    let (index, proposal) = state.pending().next().expect("still pending");
    assert_eq!(index, doomed, "a refused change is not a decided one");
    assert_eq!(
        proposal.approvals().collect::<Vec<_>>(),
        vec![a.id],
        "the approval that could not be applied is not recorded"
    );
    assert_eq!(voters(&state).len(), 3, "and nothing moved");
}

#[test]
fn a_proposer_can_take_back_their_own_proposal() {
    let (mut state, founders) = founded_by(3);
    let (alice, carol) = (&founders[0], &founders[2]);

    propose(
        &mut state,
        alice,
        MembershipEvent::MemberExpelled {
            member: carol.id,
            reason: "on reflection, no".to_owned(),
        },
    )
    .unwrap();
    let proposal = the_pending_one(&state);

    propose(&mut state, alice, MembershipEvent::Withdrawn { proposal }).unwrap();

    assert_eq!(state.pending().count(), 0);
    assert!(state.is_core(&carol.id), "and nothing was decided");
}

#[test]
fn only_the_proposer_may_withdraw() {
    // The rule that keeps withdrawal from being a veto. A core member who could
    // withdraw anybody's proposal could stop a decision the rest of the core
    // group was reaching — which is exactly what the thresholds exist to
    // prevent, arriving by another door.
    let (mut state, founders) = founded_by(3);
    let (alice, bob, carol) = (&founders[0], &founders[1], &founders[2]);

    propose(
        &mut state,
        alice,
        MembershipEvent::MemberExpelled {
            member: carol.id,
            reason: "a majority may want this".to_owned(),
        },
    )
    .unwrap();
    let proposal = the_pending_one(&state);

    for other in [bob, carol] {
        assert_eq!(
            propose(&mut state, other, MembershipEvent::Withdrawn { proposal }),
            Err(ConsensusError::NotTheProposer {
                member: other.id,
                proposal,
            }),
            "a core member is not thereby the proposer"
        );
    }

    // And the decision the withdrawal would have blocked still happens.
    approve(&mut state, bob, proposal).unwrap();
    assert!(!state.is_member(&carol.id));
}

#[test]
fn withdrawing_something_that_is_not_pending_is_refused() {
    let (mut state, alice) = founded();

    assert_eq!(
        propose(
            &mut state,
            &alice,
            MembershipEvent::Withdrawn { proposal: 7 }
        ),
        Err(ConsensusError::UnknownProposal { proposal: 7 })
    );
}

#[test]
fn a_second_proposal_about_the_same_member_is_refused_naming_the_first() {
    // Two proposals about one subject split the approvals they need and neither
    // reaches its threshold. The shape that really bites is one member asking
    // over and over, spreading them thinner with every attempt.
    let (mut state, founders) = founded_by(3);
    let (alice, carol) = (&founders[0], &founders[2]);

    propose(
        &mut state,
        alice,
        MembershipEvent::MemberExpelled {
            member: carol.id,
            reason: "first".to_owned(),
        },
    )
    .unwrap();
    let waiting = the_pending_one(&state);

    assert_eq!(
        propose(
            &mut state,
            &founders[1],
            MembershipEvent::MemberExpelled {
                member: carol.id,
                reason: "asking again".to_owned(),
            },
        ),
        Err(ConsensusError::AlreadyPending { proposal: waiting }),
        "the answer is to approve the one already waiting"
    );
    assert_eq!(state.pending().count(), 1);
}

#[test]
fn the_core_group_is_a_subject_of_its_own() {
    // The case nothing could conflict with and nothing could clear before this:
    // `CoreGroupChanged` had no subject, so a member who kept asking to change
    // the core group made a fresh pending entry every time, for ever.
    let (mut state, founders) = founded_by(4);
    let (a, b, c, d) = (&founders[0], &founders[1], &founders[2], &founders[3]);

    propose(
        &mut state,
        a,
        MembershipEvent::CoreGroupChanged {
            core: core_group(&[a, b, c]),
        },
    )
    .unwrap();
    let waiting = the_pending_one(&state);

    assert_eq!(
        propose(
            &mut state,
            b,
            MembershipEvent::CoreGroupChanged {
                core: core_group(&[a, b, d]),
            },
        ),
        Err(ConsensusError::AlreadyPending { proposal: waiting }),
        "one core group, one proposal about it at a time"
    );
}

#[test]
fn deciding_the_core_group_clears_a_proposal_composed_against_the_old_one() {
    // `CoreGroupChanged` carries the whole desired core group rather than a
    // delta (P1-23), so a pending one was composed against a map that has since
    // moved: approving it later would not add to the change just made, it would
    // silently revert it.
    let (mut state, founders) = founded_by(3);
    let (a, b, c) = (&founders[0], &founders[1], &founders[2]);

    // `c` proposes dropping `b`, and it waits: two of three are needed.
    propose(
        &mut state,
        c,
        MembershipEvent::CoreGroupChanged {
            core: core_group(&[a, c]),
        },
    )
    .unwrap();
    assert_eq!(state.pending().count(), 1);

    // Meanwhile the group expels `c`, which is also a change to the voters and
    // also takes two — so it is decided by the other two.
    propose(
        &mut state,
        a,
        MembershipEvent::MemberExpelled {
            member: c.id,
            reason: "overtaken".to_owned(),
        },
    )
    .unwrap();
    let expulsion = state
        .pending()
        .find(|(_, proposal)| matches!(proposal.event(), MembershipEvent::MemberExpelled { .. }))
        .map(|(index, _)| index)
        .expect("the expulsion is pending");
    approve(&mut state, b, expulsion).unwrap();

    assert!(!state.is_member(&c.id));
    assert_eq!(
        state.pending().count(),
        0,
        "a core group proposed against the old map must not survive the new one"
    );
}

#[test]
fn a_change_that_takes_effect_at_once_is_not_refused_as_a_duplicate() {
    // The rule governs proposals that *wait*. An immediate change has no
    // approvals to split, so a core member deciding something is never blocked
    // by somebody else having proposed it — their decision simply settles it,
    // and the prune clears what was waiting.
    let (mut state, founders) = founded_by(3);
    let alice = &founders[0];
    let (bob, carol) = (Signer::generate(), Signer::generate());
    for newcomer in [&bob, &carol] {
        propose(
            &mut state,
            alice,
            MembershipEvent::MemberAdded {
                member: newcomer.record("newcomer"),
            },
        )
        .unwrap();
    }

    propose(
        &mut state,
        &carol,
        MembershipEvent::MemberExpelled {
            member: bob.id,
            reason: "proposed by a follower".to_owned(),
        },
    )
    .unwrap();
    assert_eq!(state.pending().count(), 1);

    propose(
        &mut state,
        alice,
        MembershipEvent::MemberExpelled {
            member: bob.id,
            reason: "decided by a core member".to_owned(),
        },
    )
    .unwrap();

    assert!(!state.is_member(&bob.id));
    assert_eq!(state.pending().count(), 0);
}

/// Advances the log without touching any subject, until `changed_at` reaches
/// `upto`.
///
/// Pledges are the filler because they are the one event that needs no
/// approvals and is about nothing: they apply at once, move the index, and
/// cannot conflict with or clear what is waiting.
fn advance_to(state: &mut MembershipState, who: &Signer, upto: u64) {
    let mut pledge = 0;
    while state.changed_at() < upto {
        pledge += 1;
        propose(
            state,
            who,
            MembershipEvent::PledgeChanged {
                member: who.id,
                pledge_bytes: pledge,
            },
        )
        .unwrap();
    }
    assert_eq!(state.changed_at(), upto, "the filler must land exactly");
}

#[test]
fn a_proposal_can_be_approved_on_the_last_index_before_it_expires() {
    // The boundary itself rather than somewhere near it. An off-by-one in a
    // rule that silently deletes governance state does not look like an
    // off-by-one — it looks like an approval that did nothing.
    let (mut state, founders) = founded_by(3);
    let (alice, bob, carol) = (&founders[0], &founders[1], &founders[2]);

    propose(
        &mut state,
        alice,
        MembershipEvent::MemberExpelled {
            member: carol.id,
            reason: "decided at the last moment".to_owned(),
        },
    )
    .unwrap();
    let proposal = the_pending_one(&state);

    // The sweep runs after the dispatch, so an approval landing exactly
    // `PENDING_EXPIRY` entries later is still acted on.
    advance_to(&mut state, alice, proposal + PENDING_EXPIRY - 1);
    assert_eq!(
        state.expires_after(proposal),
        0,
        "the next change sweeps it, so this is the last chance"
    );

    approve(&mut state, bob, proposal).unwrap();
    assert!(!state.is_member(&carol.id), "the approval still counted");
}

#[test]
fn a_proposal_stops_waiting_once_the_log_has_moved_past_it() {
    // One entry later than the test above, and the proposal is gone. This is
    // what bounds the map: `MembershipState` is re-encoded into redb on every
    // apply, so a proposal nobody will ever decide is write amplification on
    // the hot path for the life of the group.
    let (mut state, founders) = founded_by(3);
    let (alice, bob, carol) = (&founders[0], &founders[1], &founders[2]);

    propose(
        &mut state,
        alice,
        MembershipEvent::MemberExpelled {
            member: carol.id,
            reason: "nobody got round to it".to_owned(),
        },
    )
    .unwrap();
    let proposal = the_pending_one(&state);

    advance_to(&mut state, alice, proposal + PENDING_EXPIRY);
    assert_eq!(state.pending().count(), 0, "swept");

    assert_eq!(
        approve(&mut state, bob, proposal),
        Err(ConsensusError::UnknownProposal { proposal }),
        "and an approval arriving late is told so rather than silently ignored"
    );
    assert!(
        state.is_member(&carol.id),
        "an expiry is not a decision either way"
    );
}

#[test]
fn a_subject_is_free_again_once_its_proposal_has_expired() {
    // The two rules have to fit together: one proposal per subject would be a
    // slot nothing could reuse if expiry did not eventually clear it. This is
    // what makes refusing a duplicate safe when the proposer has gone away.
    let (mut state, founders) = founded_by(3);
    let (alice, carol) = (&founders[0], &founders[2]);
    let expel = |reason: &str| MembershipEvent::MemberExpelled {
        member: carol.id,
        reason: reason.to_owned(),
    };

    propose(&mut state, alice, expel("abandoned")).unwrap();
    let proposal = the_pending_one(&state);
    assert!(matches!(
        propose(&mut state, alice, expel("too soon")),
        Err(ConsensusError::AlreadyPending { .. })
    ));

    advance_to(&mut state, alice, proposal + PENDING_EXPIRY);

    propose(&mut state, alice, expel("asking afresh")).unwrap();
    assert_eq!(state.pending().count(), 1, "the subject is free again");
}

// --- pledges ----------------------------------------------------------------

#[test]
fn a_pledge_change_updates_the_record() {
    let (mut state, alice) = founded();

    propose(
        &mut state,
        &alice,
        MembershipEvent::PledgeChanged {
            member: alice.id,
            pledge_bytes: 42,
        },
    )
    .unwrap();

    assert_eq!(state.member(&alice.id).unwrap().pledge_bytes, 42);
}

#[test]
fn a_member_cannot_set_another_members_pledge() {
    // A pledge is a promise about the proposer's own storage, and §5.5 makes
    // custodian assignment depend on it. If anyone could rewrite anyone else's,
    // one member could move everybody's data.
    let (mut state, alice) = founded();
    let bob = Signer::generate();
    propose(
        &mut state,
        &alice,
        MembershipEvent::MemberAdded {
            member: bob.record("bob"),
        },
    )
    .unwrap();

    let refused = propose(
        &mut state,
        &alice,
        MembershipEvent::PledgeChanged {
            member: bob.id,
            pledge_bytes: 1 << 40,
        },
    );

    assert_eq!(
        refused,
        Err(ConsensusError::PledgeNotOwn {
            proposer: alice.id,
            member: bob.id,
        })
    );
    assert_eq!(
        state.member(&bob.id).unwrap().pledge_bytes,
        0,
        "the refused event must leave the record alone"
    );
}

#[test]
fn a_non_core_member_cannot_change_the_core_group() {
    // The core group is the set of Raft voters. A member outside it rewriting
    // the set could remove every voter but themselves.
    let (mut state, alice) = founded();
    let bob = Signer::generate();
    propose(
        &mut state,
        &alice,
        MembershipEvent::MemberAdded {
            member: bob.record("bob"),
        },
    )
    .unwrap();
    assert!(!state.is_core(&bob.id), "bob joined after founding");

    let refused = propose(
        &mut state,
        &bob,
        MembershipEvent::CoreGroupChanged {
            core: core_group(&[&bob]),
        },
    );

    assert_eq!(
        refused,
        Err(ConsensusError::NotCoreMember { proposer: bob.id })
    );
    assert_eq!(
        voters(&state),
        vec![alice.id],
        "the voter set must be untouched"
    );
}

#[test]
fn a_core_member_can_change_the_core_group() {
    // The other half of the rule: the check must not refuse everybody.
    let (mut state, alice, bob) = founded_by_two();
    propose(
        &mut state,
        &alice,
        MembershipEvent::CoreGroupChanged {
            core: core_group(&[&alice]),
        },
    )
    .unwrap();
    // Demotion moves the voter set, so it takes a majority of it — and unlike
    // an expulsion, the member concerned may agree to their own. Somebody
    // standing down from the core group is resigning, not being removed.
    let proposal = the_pending_one(&state);
    approve(&mut state, &bob, proposal).unwrap();

    assert!(!state.is_core(&bob.id), "a core member may drop another");
    assert_eq!(voters(&state), vec![alice.id]);
}

#[test]
fn a_proposal_against_a_superseded_membership_is_refused() {
    // What makes a stale view announce itself. Without this a node behind on
    // the log proposes against a group that has already moved on, and finds out
    // only from whatever the change happened to do to its proposal.
    let (mut state, alice) = founded();
    let bob = Signer::generate();

    // Alice reads the membership here...
    let seen = state.changed_at();

    // ...and somebody else changes it before her proposal lands.
    propose(
        &mut state,
        &alice,
        MembershipEvent::MemberAdded {
            member: bob.record("bob"),
        },
    )
    .unwrap();

    let stale = alice.sign(
        MembershipEvent::MemberExpelled {
            member: bob.id,
            reason: "decided before bob existed".to_owned(),
        },
        seen,
    );

    assert_eq!(
        apply_next(&mut state, &stale),
        Err(ConsensusError::StaleProposal {
            seen,
            current: state.changed_at(),
        })
    );
    assert!(
        state.is_member(&bob.id),
        "the refused event changed nothing"
    );
}

#[test]
fn founding_is_proposed_against_the_empty_membership() {
    // The uniform case of the rule above: before anything is applied there is
    // nothing to have seen, so a founder proposes against zero and the check
    // needs no exception for founding.
    let alice = Signer::generate();
    let mut state = MembershipState::new();
    let founding =
        MembershipEvent::found(vec![alice.founder("alice")], Timestamp::from_millis(1)).unwrap();

    let invented = alice.sign(founding.clone(), 9);
    assert_eq!(
        apply_next(&mut state, &invented),
        Err(ConsensusError::StaleProposal {
            seen: 9,
            current: 0
        }),
        "a founder cannot claim to have seen a membership that never existed"
    );

    propose(&mut state, &alice, founding).unwrap();
    assert!(state.group_id().is_some());
}

// --- namespaces -------------------------------------------------------------

/// The catalogue's key reaches every member the way membership does, and the
/// same rule decides who may hand it out.
#[test]
fn a_core_member_creates_a_namespace_and_everybody_has_the_key() {
    let (mut state, alice) = founded();
    let secret = NamespaceSecret::generate().unwrap();

    assert_eq!(state.namespace(Namespace::Catalogue), None, "none to start");
    propose(
        &mut state,
        &alice,
        MembershipEvent::NamespaceCreated {
            kind: Namespace::Catalogue,
            secret: secret.clone(),
        },
    )
    .unwrap();

    assert_eq!(state.namespace(Namespace::Catalogue), Some(&secret));
    assert_eq!(
        state.pending().count(),
        0,
        "a core member's own approval is the only one this needs"
    );
}

/// A namespace is a thing the group agrees about, so the core group decides
/// it — the [`MembershipEvent::CoreGroupChanged`] rule, for the same reason.
#[test]
fn a_follower_cannot_create_a_namespace() {
    let (mut state, alice) = founded();
    let bob = Signer::generate();
    propose(
        &mut state,
        &alice,
        MembershipEvent::MemberAdded {
            member: bob.record("bob"),
        },
    )
    .unwrap();
    assert!(state.is_member(&bob.id) && !state.is_core(&bob.id));

    let refused = propose(
        &mut state,
        &bob,
        MembershipEvent::NamespaceCreated {
            kind: Namespace::Catalogue,
            secret: NamespaceSecret::generate().unwrap(),
        },
    )
    .unwrap_err();

    assert_eq!(
        refused,
        ConsensusError::NamespaceNotCore { proposer: bob.id }
    );
    assert_eq!(state.namespace(Namespace::Catalogue), None);
}

/// The first key wins. A second would not replace the namespace so much as
/// abandon it: everything written under the first would still exist and no
/// longer be anybody's catalogue.
#[test]
fn a_namespace_is_created_once_and_keeps_its_first_key() {
    let (mut state, alice) = founded();
    let first = NamespaceSecret::generate().unwrap();
    propose(
        &mut state,
        &alice,
        MembershipEvent::NamespaceCreated {
            kind: Namespace::Catalogue,
            secret: first.clone(),
        },
    )
    .unwrap();

    let refused = propose(
        &mut state,
        &alice,
        MembershipEvent::NamespaceCreated {
            kind: Namespace::Catalogue,
            secret: NamespaceSecret::generate().unwrap(),
        },
    )
    .unwrap_err();

    assert_eq!(
        refused,
        ConsensusError::NamespaceExists {
            kind: Namespace::Catalogue
        }
    );
    assert_eq!(
        state.namespace(Namespace::Catalogue),
        Some(&first),
        "the key the group has been using is the one it keeps"
    );
}

/// The realistic way a secret escapes is a log line, and events are what get
/// logged: a refused entry, a dropped proposal, a test failure all print one.
#[test]
fn an_event_carrying_a_secret_does_not_print_it() {
    let secret = NamespaceSecret::generate().unwrap();
    let event = MembershipEvent::NamespaceCreated {
        kind: Namespace::Catalogue,
        secret: secret.clone(),
    };

    let shown = format!("{event:?}");
    assert!(shown.contains("<redacted>"), "{shown}");
    assert!(
        !shown.contains(&format!("{:?}", secret.expose())),
        "the bytes themselves must not appear: {shown}"
    );
}

/// The fold may not invent anything, and a secret is the sharpest case: a
/// state machine that generated one would leave every node holding a
/// different key to a namespace they all believe they share.
#[test]
fn folding_one_entry_twice_reaches_the_same_key() {
    let (mut state, alice) = founded();
    let mut elsewhere = state.clone();

    let signed = alice.sign(
        MembershipEvent::NamespaceCreated {
            kind: Namespace::Catalogue,
            secret: NamespaceSecret::generate().unwrap(),
        },
        state.changed_at(),
    );
    apply_next(&mut state, &signed).unwrap();
    apply_next(&mut elsewhere, &signed).unwrap();

    assert_eq!(
        state.namespace(Namespace::Catalogue),
        elsewhere.namespace(Namespace::Catalogue),
        "two nodes folding the same entry hold the same key"
    );
    assert!(state.namespace(Namespace::Catalogue).is_some());
}

// --- properties -------------------------------------------------------------
//
// Six seconds, which is the whole of this binary's runtime: sixty-four cases
// across five properties, each signing a dozen events. Everything from here to
// the end of the file is behind `slow-tests`, helpers included — they exist
// only to run these — so `--no-default-features` leaves this file at
// milliseconds. See the feature in Cargo.toml.

/// A founded group plus a sequence of events over a small pool of members.
///
/// Generated as *indices* into the pool so the events refer to each other
/// coherently; keys are made once, since generating them is the slow part.
#[cfg(feature = "slow-tests")]
fn scenario() -> impl Strategy<Value = (usize, Vec<(usize, usize, u64)>)> {
    (
        1usize..4,
        prop::collection::vec((0usize..4, 0usize..4, 0u64..5), 0..12),
    )
}

#[cfg(feature = "slow-tests")]
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
        for id in state.core().keys() {
            prop_assert!(state.is_member(id));
        }
    }

    /// Nothing waiting is ever a duplicate, and there is never more waiting
    /// than there are things to wait about.
    ///
    /// The general form of one-proposal-per-subject, which the cases above can
    /// only check one at a time. The bound is the number of distinct subjects a
    /// generated scenario can produce — four members and the core group — and
    /// it is what makes the map's size a property of the *group* rather than of
    /// how many times somebody has asked.
    #[test]
    fn nothing_waits_twice_about_the_same_thing((founders, ops) in scenario()) {
        let state = fold(founders, &ops);
        let mut subjects: Vec<String> = state
            .pending()
            .map(|(_, proposal)| match proposal.event() {
                MembershipEvent::MemberAdded { member } => member.member_id.to_string(),
                MembershipEvent::MemberExpelled { member, .. } => member.to_string(),
                // One key for every core group, not one per *proposed* core
                // group — otherwise two proposals about it would look like two
                // subjects and the property would pass on the very case it
                // exists to catch.
                MembershipEvent::CoreGroupChanged { .. } => "the core group".to_owned(),
                other => panic!("{other:?} does not wait for approvals"),
            })
            .collect();
        let before = subjects.len();
        subjects.sort();
        subjects.dedup();
        prop_assert_eq!(before, subjects.len(), "two proposals about one subject");
        prop_assert!(before <= 5, "four members and the core group");
    }

    /// A founded group always has somebody who can vote. Not a rule of its own
    /// but the sum of three — the majority a voter's removal takes, the
    /// exclusion of the member being removed from it, and `CoreGroupChanged`
    /// refusing an empty core group — and the thing all three exist to keep
    /// true, since a group with no voters can never commit anything again,
    /// including the event that would restore its voters.
    #[test]
    fn a_founded_group_is_never_left_without_a_voter((founders, ops) in scenario()) {
        let state = fold(founders, &ops);
        prop_assert_eq!(state.group_id().is_some(), !state.core().is_empty());
    }
}

/// The fixed cast a generated scenario draws from. Seeded rather than random so
/// two runs of the same scenario involve the same members.
#[cfg(feature = "slow-tests")]
fn pool() -> Vec<Signer> {
    (0..4).map(Signer::seeded).collect()
}

/// Runs a generated scenario, ignoring events the rules refuse — the point is
/// the state that results, not which operations happened to be legal.
#[cfg(feature = "slow-tests")]
fn fold(founders: usize, ops: &[(usize, usize, u64)]) -> MembershipState {
    let pool = pool();
    let mut state = MembershipState::new();
    apply_scenario(&mut state, &pool, founders, ops, 0..ops.len());
    state
}

#[cfg(feature = "slow-tests")]
fn fold_in_two(founders: usize, ops: &[(usize, usize, u64)], split: usize) -> MembershipState {
    let pool = pool();
    let mut state = MembershipState::new();
    apply_scenario(&mut state, &pool, founders, ops, 0..split);
    apply_scenario(&mut state, &pool, founders, ops, split..ops.len());
    state
}

#[cfg(feature = "slow-tests")]
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
            .map(|s| s.founder("founder"))
            .collect();
        let signed = pool[0].sign(
            MembershipEvent::found(records, Timestamp::from_millis(1)).unwrap(),
            state.changed_at(),
        );
        let _ = apply_next(state, &signed);
    }
    for &(actor, choice, kind) in &ops[range] {
        let (actor, subject) = (&pool[actor % pool.len()], &pool[choice % pool.len()]);
        let event = match kind {
            0 => MembershipEvent::MemberAdded {
                member: subject.record("member"),
            },
            1 => MembershipEvent::MemberExpelled {
                member: subject.id,
                reason: "generated".to_owned(),
            },
            2 => MembershipEvent::CoreGroupChanged {
                core: core_group(&[actor, subject]),
            },
            _ => {
                // Approvals and withdrawals are generated too, or the scenarios
                // would stop exercising anything past the point a proposal
                // starts waiting — which, now that most of them do, is nearly
                // everything. Chosen by position rather than at random so a
                // replayed run picks the same one and
                // `applying_in_chunks_matches_applying_at_once` still means
                // something.
                let Some((proposal, _)) =
                    state.pending().nth(choice % state.pending().count().max(1))
                else {
                    continue;
                };
                if kind == 3 {
                    MembershipEvent::Approved { proposal }
                } else {
                    MembershipEvent::Withdrawn { proposal }
                }
            }
        };
        // Refused events are expected and are exactly what the rules are for.
        let _ = propose(state, actor, event);
    }
}
