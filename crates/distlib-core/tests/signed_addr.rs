//! What a member may say about where it is, and what it may not.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::net::SocketAddr;

use distlib_core::{CoreError, MemberId, NodeAddr, SignedAddress};
use iroh::{SecretKey, Signature};
use serde::Serialize;

fn somewhere(port: u16) -> NodeAddr {
    NodeAddr {
        relay: None,
        direct: [SocketAddr::from(([127, 0, 0, 1], port))]
            .into_iter()
            .collect(),
    }
}

/// The same shape as `SignedAddress`, for forging one.
///
/// postcard encodes a struct as its fields in order and nothing else — no
/// names, no type tag — so this serialises to bytes that decode as the real
/// type. That is exactly what an attacker on the gossip topic can do, which
/// makes it the right way to test the check rather than a trick.
#[derive(Serialize)]
struct AsItGoesOnTheWire<'a> {
    member: &'a MemberId,
    addr: &'a NodeAddr,
    applied: u64,
    signature: &'a Signature,
}

#[test]
fn a_member_can_say_where_it_is() {
    let key = SecretKey::generate();
    let signed = SignedAddress::sign(&key, somewhere(4001), 7).unwrap();

    assert_eq!(signed.member(), MemberId::from(key.public()));
    assert_eq!(signed.applied(), 7);
    assert_eq!(signed.addr().unwrap(), &somewhere(4001));
}

/// And it survives the encoding it actually travels in.
#[test]
fn it_survives_the_wire() {
    let key = SecretKey::generate();
    let signed = SignedAddress::sign(&key, somewhere(4002), 3).unwrap();

    let encoded = postcard::to_stdvec(&signed).unwrap();
    let decoded: SignedAddress = postcard::from_bytes(&encoded).unwrap();

    assert_eq!(decoded.addr().unwrap(), &somewhere(4002));
}

/// **The claim the signature exists for.** Gossip relays these, so the node
/// that hands one over is almost never the node it is about — which is why
/// `delivered_from` cannot be used to attribute one, and why this check is the
/// only thing standing between a member and pointing the group's dials
/// wherever it likes.
#[test]
fn nobody_can_move_a_member_somewhere_it_did_not_say() {
    let key = SecretKey::generate();
    let honest = SignedAddress::sign(&key, somewhere(4003), 1).unwrap();

    // The signature and the member as they really are; the address changed.
    let forged = postcard::to_stdvec(&AsItGoesOnTheWire {
        member: &honest.member(),
        addr: &somewhere(9999),
        applied: honest.applied(),
        signature: &signature_of(&honest),
    })
    .unwrap();

    let tampered: SignedAddress = postcard::from_bytes(&forged).unwrap();
    match tampered.addr() {
        Err(CoreError::BadAddressSignature { member }) => {
            assert_eq!(
                member,
                honest.member(),
                "the error should name who it was about"
            );
        }
        other => panic!("a moved address must not verify; got {other:?}"),
    }
}

/// One member cannot sign for another.
#[test]
fn nobody_can_speak_for_somebody_else() {
    let alice = SecretKey::generate();
    let bob = SecretKey::generate();
    let by_alice = SignedAddress::sign(&alice, somewhere(4004), 1).unwrap();

    // Alice's signature and address, attributed to bob.
    let forged = postcard::to_stdvec(&AsItGoesOnTheWire {
        member: &MemberId::from(bob.public()),
        addr: &somewhere(4004),
        applied: 1,
        signature: &signature_of(&by_alice),
    })
    .unwrap();

    let tampered: SignedAddress = postcard::from_bytes(&forged).unwrap();
    assert!(
        matches!(tampered.addr(), Err(CoreError::BadAddressSignature { .. })),
        "a statement attributed to somebody who did not make it must not verify"
    );
}

/// Changing the log position changes the pre-image, so freshness cannot be
/// rewritten in transit to make a stale address look current.
#[test]
fn the_log_position_is_signed_too() {
    let key = SecretKey::generate();
    let honest = SignedAddress::sign(&key, somewhere(4005), 2).unwrap();

    let forged = postcard::to_stdvec(&AsItGoesOnTheWire {
        member: &honest.member(),
        addr: &somewhere(4005),
        applied: 9_999,
        signature: &signature_of(&honest),
    })
    .unwrap();

    let tampered: SignedAddress = postcard::from_bytes(&forged).unwrap();
    assert!(
        matches!(tampered.addr(), Err(CoreError::BadAddressSignature { .. })),
        "a rewritten log position must not verify"
    );
}

/// The signature, read back off the wire form.
///
/// `SignedAddress` does not expose it — nothing in production needs it — so a
/// test that wants to re-use one takes it from the encoding, which is where an
/// attacker would get it too.
fn signature_of(signed: &SignedAddress) -> Signature {
    let encoded = postcard::to_stdvec(signed).unwrap();
    let (_rest, signature) = split_signature(&encoded);
    signature
}

/// postcard writes fields in order, so the signature is the last 64 bytes.
fn split_signature(encoded: &[u8]) -> (&[u8], Signature) {
    let (head, tail) = encoded.split_at(encoded.len() - 64);
    let bytes: [u8; 64] = tail.try_into().expect("just split off 64 bytes");
    (head, Signature::from_bytes(&bytes))
}
