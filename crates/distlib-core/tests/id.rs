//! Item fingerprinting and identifier round-trips.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::str::FromStr;

use distlib_core::{GroupId, ItemId, MemberId};
use proptest::prelude::*;

fn hash(seed: u8) -> [u8; 32] {
    [seed; 32]
}

#[test]
fn fingerprint_ignores_order() {
    let set = [hash(3), hash(1), hash(2)];
    let reversed = [hash(2), hash(1), hash(3)];

    assert_eq!(
        ItemId::from_content_hashes(&set),
        ItemId::from_content_hashes(&reversed),
        "two members adding the same files in different order must converge"
    );
}

#[test]
fn fingerprint_distinguishes_different_sets() {
    assert_ne!(
        ItemId::from_content_hashes(&[hash(1), hash(2)]),
        ItemId::from_content_hashes(&[hash(1), hash(3)]),
    );
}

#[test]
fn fingerprint_counts_repeats() {
    // The count is part of the pre-image, so a repeated hash is not the same
    // input as a single one even though the sorted bytes would concatenate the
    // same way without it.
    assert_ne!(
        ItemId::from_content_hashes(&[hash(1)]),
        ItemId::from_content_hashes(&[hash(1), hash(1)]),
    );
}

#[test]
fn fingerprint_is_domain_separated_from_blob_hashes() {
    let blob = hash(7);
    let item = ItemId::from_content_hashes(&[blob]);

    assert_ne!(
        item.as_bytes(),
        &blob,
        "an item id must never equal one of its own blob hashes"
    );
    assert_ne!(
        item.as_bytes(),
        blake3::hash(&blob).as_bytes(),
        "an item id must not be a bare hash of its content"
    );
}

#[test]
fn adding_a_file_changes_identity_but_the_id_is_frozen_by_the_caller() {
    // Establishes the property the catalogue depends on: the fingerprint is a
    // function of the content set, so a *different* set is a different id. The
    // "frozen at creation" rule lives in the caller, not here.
    let original = ItemId::from_content_hashes(&[hash(1), hash(2)]);
    let extended = ItemId::from_content_hashes(&[hash(1), hash(2), hash(3)]);

    assert_ne!(original, extended);
}

proptest! {
    #[test]
    fn any_permutation_yields_the_same_id(mut hashes in prop::collection::vec(any::<[u8; 32]>(), 1..12)) {
        let expected = ItemId::from_content_hashes(&hashes);
        hashes.reverse();
        prop_assert_eq!(ItemId::from_content_hashes(&hashes), expected);
        hashes.sort_unstable();
        prop_assert_eq!(ItemId::from_content_hashes(&hashes), expected);
    }

    #[test]
    fn distinct_sets_yield_distinct_ids(a in prop::collection::hash_set(any::<[u8; 32]>(), 1..8),
                                        b in prop::collection::hash_set(any::<[u8; 32]>(), 1..8)) {
        let left: Vec<_> = a.iter().copied().collect();
        let right: Vec<_> = b.iter().copied().collect();
        prop_assert_eq!(
            ItemId::from_content_hashes(&left) == ItemId::from_content_hashes(&right),
            a == b
        );
    }

    #[test]
    fn item_id_survives_a_string_round_trip(bytes in any::<[u8; 32]>()) {
        let id = ItemId::from_bytes(bytes);
        prop_assert_eq!(ItemId::from_str(&id.to_string()).unwrap(), id);
    }

    #[test]
    fn group_id_survives_a_string_round_trip(bytes in any::<[u8; 32]>()) {
        let id = GroupId::from_bytes(bytes);
        prop_assert_eq!(GroupId::from_str(&id.to_string()).unwrap(), id);
    }
}

#[test]
fn member_id_survives_a_string_round_trip() {
    let member = MemberId::from(iroh::SecretKey::generate().public());

    assert_eq!(MemberId::from_str(&member.to_string()).unwrap(), member);
}

#[test]
fn member_id_rejects_rubbish() {
    assert!(MemberId::from_str("not-a-key").is_err());
}

#[test]
fn member_id_is_the_endpoint_id() {
    let secret = iroh::SecretKey::generate();
    let member = MemberId::from(secret.public());

    assert_eq!(member.endpoint_id(), secret.public());
}

// --- RawMemberId ------------------------------------------------------------

#[test]
fn a_raw_member_id_round_trips_through_a_real_member() {
    let member = MemberId::from(iroh::SecretKey::generate().public());

    let raw = distlib_core::RawMemberId::from(member);

    assert_eq!(MemberId::try_from(raw).unwrap(), member);
    assert_eq!(raw.to_string(), member.to_string(), "both render as hex");
}

#[test]
fn the_default_raw_member_id_is_all_zeros_and_unusable_as_an_identity() {
    // Worth pinning precisely, because the obvious assumption is wrong: the
    // all-zeros value IS a valid curve point and converts to a MemberId
    // without complaint. What makes it harmless is that it is a low-order
    // point with no usable secret key, so nothing can ever sign as it — and
    // every membership event is admitted only against its proposer\'s
    // signature. The sentinel therefore cannot propose its way into a log.
    let sentinel = distlib_core::RawMemberId::default();

    assert_eq!(sentinel.as_bytes(), &[0u8; 32]);
    assert!(MemberId::try_from(sentinel).is_ok());
}

#[test]
fn a_raw_member_id_survives_a_string_round_trip() {
    let raw = distlib_core::RawMemberId::from_bytes([9; 32]);

    assert_eq!(
        distlib_core::RawMemberId::from_str(&raw.to_string()).unwrap(),
        raw
    );
}

#[test]
fn raw_bytes_that_are_not_a_key_are_refused() {
    // Unvalidated in, validated out — arbitrary bytes are a legal RawMemberId
    // and an illegal MemberId, which is the point of having both. Around half
    // of all 32-byte values fail to decompress to a curve point; this is one.
    let raw = distlib_core::RawMemberId::from_bytes([2; 32]);

    assert!(
        MemberId::try_from(raw).is_err(),
        "conversion has to actually validate, not just reinterpret bytes"
    );
}
