//! The heartbeat on the wire, and the list of held items it points at (4b-1).

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::net::{SocketAddr, SocketAddrV6};

use distlib_core::{
    ContentHash, CoreError, GroupId, Heartbeat, Holdings, ItemId, NodeAddr, SignedAddress,
    SignedHeartbeat,
    availability::{BaseDecoder, DELTA_MAX, HEARTBEAT_MAX, encode_base},
};
use iroh::{SecretKey, Signature};
use proptest::prelude::*;
use serde::Serialize;

const GROUP: GroupId = GroupId::from_bytes([7; 32]);

fn ids(raw: &[[u8; 32]]) -> Vec<ItemId> {
    raw.iter().copied().map(ItemId::from_bytes).collect()
}

/// A heartbeat from `key`, at an address of `direct` sockets.
fn heartbeat(key: &SecretKey, direct: Vec<SocketAddr>, holdings: Holdings) -> Heartbeat {
    let addr = NodeAddr {
        relay: Some("https://relay.example.org./".to_owned()),
        direct: direct.into_iter().collect(),
    };
    Heartbeat {
        address: SignedAddress::sign(key, addr, u64::MAX).unwrap(),
        epoch: u64::MAX,
        seq: u64::MAX,
        interval_secs: u32::MAX,
        holdings,
        leaving: false,
    }
}

fn decoded(chunks: &[&[u8]]) -> Result<(Vec<ItemId>, u64), CoreError> {
    let mut decoder = BaseDecoder::default();
    let mut read = Vec::new();
    for chunk in chunks {
        decoder.feed(chunk, |id| read.push(id))?;
    }
    Ok((read, decoder.finish()?))
}

proptest! {
    /// The base's hash names the list, so one set has to make one list —
    /// however it was gathered, and with any repeats.
    #[test]
    fn one_set_always_encodes_to_the_same_bytes(
        raw in prop::collection::vec(any::<[u8; 32]>(), 0..200)
            .prop_flat_map(|raw| (Just(raw.clone()), Just(raw).prop_shuffle())),
    ) {
        let (raw, shuffled) = raw;
        let mut repeated = shuffled.clone();
        repeated.extend(shuffled.iter().take(5));
        prop_assert_eq!(encode_base(ids(&raw)), encode_base(ids(&repeated)));
    }

    /// And it reads back as that set, however the transfer cut it up.
    #[test]
    fn a_base_list_reads_back_in_chunks_of_any_size(
        raw in prop::collection::vec(any::<[u8; 32]>(), 0..200),
        cuts in prop::collection::vec(1usize..100, 1..50),
    ) {
        let bytes = encode_base(ids(&raw));
        let mut chunks = Vec::new();
        let mut rest = bytes.as_slice();
        for cut in cuts.iter().cycle() {
            if rest.is_empty() {
                break;
            }
            let (chunk, after) = rest.split_at((*cut).min(rest.len()));
            chunks.push(chunk);
            rest = after;
        }

        let mut expected = ids(&raw);
        expected.sort_unstable();
        expected.dedup();
        let (read, count) = decoded(&chunks).unwrap();
        prop_assert_eq!(count, expected.len() as u64);
        prop_assert_eq!(read, expected);
    }

    #[test]
    fn a_heartbeat_round_trips(
        added in prop::collection::vec(any::<[u8; 32]>(), 0..20),
        removed in prop::collection::vec(any::<[u8; 32]>(), 0..20),
        base in proptest::option::of(any::<[u8; 32]>()),
        count in any::<u64>(),
        leaving in any::<bool>(),
    ) {
        let key = SecretKey::generate();
        let mut beat = heartbeat(
            &key,
            vec![SocketAddr::from(([127, 0, 0, 1], 4000))],
            Holdings {
                count,
                base: base.map(ContentHash::from_bytes),
                added: ids(&added),
                removed: ids(&removed),
            },
        );
        beat.leaving = leaving;

        let signed = SignedHeartbeat::sign(&key, &GROUP, beat.clone()).unwrap();
        let read = SignedHeartbeat::decode(&signed.encode().unwrap()).unwrap();
        prop_assert_eq!(read.open(&GROUP).unwrap(), &beat);
    }

    /// **D2's guarantee**: with a full delta and an address of up to 64
    /// sockets, every number at its widest, a heartbeat still fits — so the
    /// guard at encode is for the absurd, not the ordinary.
    #[test]
    fn a_heartbeat_with_a_full_delta_fits_the_frame(
        sockets in prop::collection::vec(any::<SocketAddrV6>(), 0..=64),
        split in 0..=DELTA_MAX,
    ) {
        let key = SecretKey::generate();
        let delta: Vec<ItemId> = (0..DELTA_MAX)
            .map(|n| ItemId::from_bytes([(n % 251) as u8; 32]))
            .collect();
        let (added, removed) = delta.split_at(split);
        let beat = heartbeat(
            &key,
            sockets.into_iter().map(SocketAddr::V6).collect(),
            Holdings {
                count: u64::MAX,
                base: Some(ContentHash::from_bytes([0xff; 32])),
                added: added.to_vec(),
                removed: removed.to_vec(),
            },
        );

        let encoded = SignedHeartbeat::sign(&key, &GROUP, beat).unwrap().encode().unwrap();
        prop_assert!(encoded.len() <= HEARTBEAT_MAX, "{} bytes", encoded.len());
    }
}

/// An absurd address is refused at encode, never sent: what an oversize frame
/// costs is the connection, on every topic.
#[test]
fn an_oversize_heartbeat_is_refused_rather_than_sent() {
    let key = SecretKey::generate();
    let sockets = (0..1_000u16)
        .map(|port| SocketAddr::from(([0xfe80, 0, 0, 0, 0, 0, 0, port], port)))
        .collect();
    let signed =
        SignedHeartbeat::sign(&key, &GROUP, heartbeat(&key, sockets, Holdings::default())).unwrap();

    assert!(matches!(
        signed.encode(),
        Err(CoreError::HeartbeatTooLarge { .. })
    ));
    assert!(matches!(
        SignedHeartbeat::decode(&vec![0; HEARTBEAT_MAX + 1]),
        Err(CoreError::HeartbeatTooLarge { .. })
    ));
}

/// The same shape as `SignedHeartbeat`, for forging one — postcard writes a
/// struct as its fields in order, so this is what an attacker on the topic can
/// send. The same device `signed_addr.rs` uses.
#[derive(Serialize)]
struct AsItGoesOnTheWire<'a> {
    heartbeat: &'a Heartbeat,
    signature: &'a Signature,
}

/// Re-signs nothing: puts `heartbeat` on the wire under somebody's signature.
fn forged(heartbeat: &Heartbeat, signature: &Signature) -> SignedHeartbeat {
    let bytes = postcard::to_stdvec(&AsItGoesOnTheWire {
        heartbeat,
        signature,
    })
    .unwrap();
    SignedHeartbeat::decode(&bytes).unwrap()
}

fn signature_of(signed: &SignedHeartbeat) -> Signature {
    #[derive(serde::Deserialize)]
    struct Parts {
        _heartbeat: Heartbeat,
        signature: Signature,
    }
    let parts: Parts = postcard::from_bytes(&signed.encode().unwrap()).unwrap();
    parts.signature
}

#[test]
fn a_heartbeat_changed_after_signing_is_refused() {
    let key = SecretKey::generate();
    let beat = heartbeat(&key, Vec::new(), Holdings::default());
    let signed = SignedHeartbeat::sign(&key, &GROUP, beat.clone()).unwrap();
    assert!(forged(&beat, &signature_of(&signed)).open(&GROUP).is_ok());

    let mut claimed_more = beat;
    claimed_more.holdings.count += 1;
    assert!(matches!(
        forged(&claimed_more, &signature_of(&signed)).open(&GROUP),
        Err(CoreError::BadHeartbeatSignature { .. })
    ));
}

/// The group is signed and never sent, so a beat cannot be replayed into
/// another group.
#[test]
fn a_heartbeat_for_another_group_is_refused() {
    let key = SecretKey::generate();
    let signed = SignedHeartbeat::sign(
        &key,
        &GROUP,
        heartbeat(&key, Vec::new(), Holdings::default()),
    )
    .unwrap();

    assert!(matches!(
        signed.open(&GroupId::from_bytes([8; 32])),
        Err(CoreError::BadHeartbeatSignature { .. })
    ));
}

/// A heartbeat speaks for whoever its address names, so it must be signed by
/// them: lifting a member's address into one's own beat claims to be them.
#[test]
fn a_heartbeat_carrying_somebody_elses_address_is_refused() {
    let mine = SecretKey::generate();
    let theirs = SecretKey::generate();
    let their_address = heartbeat(&theirs, Vec::new(), Holdings::default()).address;

    let mut beat = heartbeat(&mine, Vec::new(), Holdings::default());
    let signed_by_me = SignedHeartbeat::sign(&mine, &GROUP, beat.clone()).unwrap();
    beat.address = their_address;

    assert!(matches!(
        SignedHeartbeat::sign(&mine, &GROUP, beat.clone()),
        Err(CoreError::BadHeartbeatSignature { .. })
    ));
    assert!(matches!(
        forged(&beat, &signature_of(&signed_by_me)).open(&GROUP),
        Err(CoreError::BadHeartbeatSignature { .. })
    ));
}

#[test]
fn a_base_list_that_is_not_one_is_refused() {
    let one = [0x11; 32];
    let list = encode_base(ids(&[one]));

    let mut other_format = list.clone();
    other_format[0] = 2;
    for (bytes, why) in [
        (&[][..], "empty"),
        (&other_format[..], "another format"),
        (&list[..list.len() - 1], "cut short"),
    ] {
        assert!(
            matches!(decoded(&[bytes]), Err(CoreError::BadBaseList { .. })),
            "{why}"
        );
    }
}

/// Both signatures are checked, not only the outer one: the address goes on to
/// the directory, so a member's beat must not carry an address statement that
/// does not stand on its own — even one it signed around.
#[test]
fn a_heartbeat_whose_address_was_doctored_is_refused() {
    #[derive(Serialize, serde::Deserialize)]
    struct AddressParts {
        member: distlib_core::MemberId,
        addr: NodeAddr,
        applied: u64,
        signature: Signature,
    }
    let key = SecretKey::generate();
    let mut beat = heartbeat(&key, Vec::new(), Holdings::default());
    let mut parts: AddressParts =
        postcard::from_bytes(&postcard::to_stdvec(&beat.address).unwrap()).unwrap();
    parts.applied -= 1;
    beat.address = postcard::from_bytes(&postcard::to_stdvec(&parts).unwrap()).unwrap();

    let signed = SignedHeartbeat::sign(&key, &GROUP, beat).unwrap();
    assert!(matches!(
        signed.open(&GROUP),
        Err(CoreError::BadAddressSignature { .. })
    ));
}
