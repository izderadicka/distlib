//! What the directory takes, what it refuses, and what it forgets.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::net::SocketAddr;

use distlib_core::{NodeAddr, SignedAddress};
use distlib_net::{Directory, NetError};
use iroh::SecretKey;

fn somewhere(port: u16) -> NodeAddr {
    NodeAddr {
        relay: None,
        direct: [SocketAddr::from(([127, 0, 0, 1], port))]
            .into_iter()
            .collect(),
    }
}

#[test]
fn a_member_is_taken_at_its_word() {
    let key = SecretKey::generate();
    let directory = Directory::default();

    let taken = directory
        .learn(&SignedAddress::sign(&key, somewhere(5001), 4).unwrap())
        .unwrap();

    assert!(taken);
    assert_eq!(
        directory.position_of(distlib_core::MemberId::from(key.public())),
        Some(4)
    );
}

/// **Only the latest statement is wanted, and "latest" is the log position.**
///
/// Not a clock: positions from one member are compared only with each other.
/// See [`SignedAddress::applied`] for why a wall clock would be worse.
#[test]
fn a_statement_from_further_back_does_not_win() {
    let key = SecretKey::generate();
    let directory = Directory::default();

    directory
        .learn(&SignedAddress::sign(&key, somewhere(5002), 9).unwrap())
        .unwrap();
    let taken = directory
        .learn(&SignedAddress::sign(&key, somewhere(5003), 2).unwrap())
        .unwrap();

    assert!(!taken, "an older log position must not replace a newer one");
    assert_eq!(
        directory.position_of(distlib_core::MemberId::from(key.public())),
        Some(9)
    );
}

/// And the same position is taken, because within one stretch of unchanging
/// membership every statement carries the same one.
///
/// The deliberate limit of the rule: it stops a stale *epoch*, not a stale
/// message inside one. A node that restarts on a quiet group must be able to
/// announce its new address, and this is what lets it.
#[test]
fn the_same_position_is_still_heard() {
    let key = SecretKey::generate();
    let directory = Directory::default();

    directory
        .learn(&SignedAddress::sign(&key, somewhere(5004), 7).unwrap())
        .unwrap();
    let taken = directory
        .learn(&SignedAddress::sign(&key, somewhere(5005), 7).unwrap())
        .unwrap();

    assert!(
        taken,
        "a node that restarts on a quiet group must still be heard"
    );
}

/// Saying again what we already hold is not news.
///
/// The commonest statement on the topic, and the one that must cost nothing: a
/// member re-announces because it heard of somebody new, and everyone who
/// already knew where it is hears that too. Taking it as news would wake the
/// catalogue into dialling every peer it has, and would give this node a reason
/// to announce in turn — which is a reason for its neighbours to announce, and
/// so on, for as long as the group is up.
///
/// The statement is still *accepted* — it has to be, since an equal position
/// ties rather than losing. Accepted and "something changed" are different
/// answers, and this is the case that separates them.
#[test]
fn hearing_again_what_we_already_hold_changes_nothing() {
    let key = SecretKey::generate();
    let directory = Directory::default();
    let member = distlib_core::MemberId::from(key.public());

    directory
        .learn(&SignedAddress::sign(&key, somewhere(5012), 4).unwrap())
        .unwrap();
    let changed = directory
        .learn(&SignedAddress::sign(&key, somewhere(5012), 4).unwrap())
        .unwrap();

    assert!(!changed, "an address we already hold is not news");
    assert_eq!(
        directory.address_of(member),
        Some(somewhere(5012)),
        "and the answer is unchanged, not erased on the way to saying so"
    );
}

/// ...and neither is saying it again from further along the log.
///
/// A member whose group has moved on re-announces the same address at a higher
/// position. Nothing about where it is has changed, so nothing wakes — but the
/// position must still be recorded, because it is what the next statement is
/// judged against, and a stale one would make that statement look older than it
/// is.
#[test]
fn a_later_position_alone_changes_nothing_but_is_still_recorded() {
    let key = SecretKey::generate();
    let directory = Directory::default();
    let member = distlib_core::MemberId::from(key.public());

    directory
        .learn(&SignedAddress::sign(&key, somewhere(5013), 4).unwrap())
        .unwrap();
    let changed = directory
        .learn(&SignedAddress::sign(&key, somewhere(5013), 9).unwrap())
        .unwrap();

    assert!(
        !changed,
        "the same place, said later, is still the same place"
    );
    assert_eq!(
        directory.position_of(member),
        Some(9),
        "but the position has to advance, or the next statement is judged \
         against a stale one"
    );
}

/// Positions are never compared between members.
#[test]
fn one_member_cannot_drown_out_another() {
    let ahead = SecretKey::generate();
    let behind = SecretKey::generate();
    let directory = Directory::default();

    directory
        .learn(&SignedAddress::sign(&ahead, somewhere(5006), 500).unwrap())
        .unwrap();
    let taken = directory
        .learn(&SignedAddress::sign(&behind, somewhere(5007), 1).unwrap())
        .unwrap();

    assert!(
        taken,
        "a member far behind in the log is still the authority on where it is"
    );
}

/// An unverifiable statement is refused rather than quietly dropped.
#[test]
fn an_address_nobody_signed_is_refused() {
    let alice = SecretKey::generate();
    let bob = SecretKey::generate();
    let directory = Directory::default();

    // Bob's real statement, re-encoded with alice's id in front of it. postcard
    // writes fields in order, so the bytes decode as a `SignedAddress` about
    // alice carrying bob's signature.
    let by_bob = SignedAddress::sign(&bob, somewhere(5008), 1).unwrap();
    let encoded = postcard::to_stdvec(&by_bob).unwrap();
    let alice_id = distlib_core::MemberId::from(alice.public());
    let mut forged = postcard::to_stdvec(&alice_id).unwrap();
    forged.extend_from_slice(&encoded[postcard::to_stdvec(&by_bob.member()).unwrap().len()..]);

    let claimed: SignedAddress = postcard::from_bytes(&forged).unwrap();
    match directory.learn(&claimed) {
        Err(NetError::BadAddress { member, .. }) => assert_eq!(member, alice_id),
        other => panic!("an unsigned claim must be refused; got {other:?}"),
    }
    assert_eq!(directory.position_of(alice_id), None);
}

/// An empty address must not retire a good one.
#[test]
fn saying_nothing_does_not_erase_something() {
    let key = SecretKey::generate();
    let directory = Directory::default();

    directory
        .learn(&SignedAddress::sign(&key, somewhere(5009), 1).unwrap())
        .unwrap();
    let taken = directory
        .learn(&SignedAddress::sign(&key, NodeAddr::default(), 2).unwrap())
        .unwrap();

    assert!(!taken);
    assert_eq!(
        directory.position_of(distlib_core::MemberId::from(key.public())),
        Some(1),
        "an empty announcement means `find me some other way`, not `forget me`"
    );
}

/// **A member that moves leaves nothing behind.**
///
/// The rule that separates this from [`distlib_net::AddressBook`], and the one
/// that is invisible from outside without asking: `MemoryLookup` *merges*
/// addresses, so a laptop hopping between a home network, an office and a
/// phone tether would accumulate every address it had ever had and hand iroh
/// all of them to race on every dial, for the life of the process.
#[test]
fn a_member_that_moves_leaves_nothing_behind() {
    let key = SecretKey::generate();
    let member = distlib_core::MemberId::from(key.public());
    let directory = Directory::default();

    directory
        .learn(&SignedAddress::sign(&key, somewhere(5010), 1).unwrap())
        .unwrap();
    directory
        .learn(&SignedAddress::sign(&key, somewhere(5011), 2).unwrap())
        .unwrap();

    assert_eq!(
        directory.address_of(member),
        Some(somewhere(5011)),
        "the latest statement replaces the last, rather than joining it"
    );
}

/// What a core node hands over when it answers for the group: the members' own
/// statements, not a summary of them.
#[test]
fn everything_hands_over_the_statements_themselves() {
    let alice = SecretKey::generate();
    let bob = SecretKey::generate();
    let directory = Directory::default();

    let by_alice = SignedAddress::sign(&alice, somewhere(5101), 3).unwrap();
    let by_bob = SignedAddress::sign(&bob, somewhere(5102), 7).unwrap();
    assert!(directory.learn(&by_alice).unwrap());
    assert!(directory.learn(&by_bob).unwrap());

    // Byte-identical to what was learned. This is the property the relay rests
    // on: a node that received these can check the signatures itself, so it is
    // believing alice and bob rather than whoever passed them along.
    let mut held = directory.everything();
    held.sort_by_key(|signed| signed.member().to_string());
    let mut expected = vec![by_alice, by_bob];
    expected.sort_by_key(|signed| signed.member().to_string());
    assert_eq!(held, expected);
}

/// One member, one statement — the latest. A directory that grew with every
/// announcement would make the answer above O(messages) instead of O(members).
#[test]
fn everything_holds_one_statement_per_member() {
    let alice = SecretKey::generate();
    let directory = Directory::default();

    for (port, position) in [(5103, 1), (5104, 2), (5105, 3)] {
        assert!(
            directory
                .learn(&SignedAddress::sign(&alice, somewhere(port), position).unwrap())
                .unwrap()
        );
    }

    let held = directory.everything();
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].applied(), 3);
    assert_eq!(held[0].addr().unwrap(), &somewhere(5105));
}
