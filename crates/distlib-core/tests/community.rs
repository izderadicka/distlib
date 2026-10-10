//! What members say about items: the keys they say it in, and the values.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use distlib_core::{CommunityKey, ItemId, Key, MemberId, REVIEW_MAX_BYTES, Rating, Review};
use iroh::SecretKey;

fn member() -> MemberId {
    MemberId::from(SecretKey::generate().public())
}

/// An id whose hex has letters in it, so it has another spelling.
const ITEM: ItemId = ItemId::from_bytes([0xab; 32]);

#[test]
fn a_key_reads_back_as_itself() {
    let bob = member();
    for key in [
        CommunityKey::Rating {
            item: ITEM,
            member: bob,
        },
        CommunityKey::Review {
            item: ITEM,
            member: bob,
        },
    ] {
        let encoded = key.encode();
        assert_eq!(
            CommunityKey::parse(encoded.as_bytes()),
            Some(key),
            "{encoded}"
        );
        assert_eq!((key.item(), key.member()), (ITEM, bob));
    }
}

#[test]
fn a_key_is_named_by_its_kind_the_item_and_the_member() {
    let bob = member();
    assert_eq!(
        CommunityKey::Rating {
            item: ITEM,
            member: bob
        }
        .encode(),
        format!("rating/{ITEM}/{bob}")
    );
    assert_eq!(
        CommunityKey::Review {
            item: ITEM,
            member: bob
        }
        .encode(),
        format!("review/{ITEM}/{bob}")
    );
}

/// A second spelling of one member's key would be a second rating by them,
/// and it would pass the author check.
#[test]
fn a_key_counts_only_in_the_spelling_it_is_written_in() {
    let bob = member();
    let item = ITEM.to_string().to_uppercase();
    let who = bob.to_string().to_uppercase();
    assert_eq!(
        item.parse::<ItemId>().ok(),
        Some(ITEM),
        "the id itself parses"
    );
    assert_eq!(
        CommunityKey::parse(format!("rating/{item}/{bob}").as_bytes()),
        None
    );
    if who.parse::<MemberId>().is_ok() {
        assert_eq!(
            CommunityKey::parse(format!("rating/{ITEM}/{who}").as_bytes()),
            None
        );
    }
}

#[test]
fn anything_else_is_somebody_elses_key() {
    let bob = member();
    for key in [
        format!("rating/{ITEM}/{bob}/extra"),
        format!("rating/{ITEM}"),
        format!("rating/{ITEM}/"),
        format!("bookmark/{ITEM}/{bob}"),
        format!("item/{ITEM}/title"),
        format!("rating/{bob}/{ITEM}"),
    ] {
        assert_eq!(CommunityKey::parse(key.as_bytes()), None, "{key}");
    }
    // And the other way round: an item's keys never read as a rating's.
    let rating = CommunityKey::Rating {
        item: ITEM,
        member: bob,
    }
    .encode();
    assert_eq!(Key::parse(rating.as_bytes()), None);
}

#[test]
fn a_prefix_covers_one_kind_of_key_about_one_item() {
    let bob = member();
    let rating = CommunityKey::Rating {
        item: ITEM,
        member: bob,
    }
    .encode();
    let review = CommunityKey::Review {
        item: ITEM,
        member: bob,
    }
    .encode();
    let other = ItemId::from_bytes([8; 32]);

    assert!(rating.starts_with(&CommunityKey::ratings_of(ITEM)));
    assert!(review.starts_with(&CommunityKey::reviews_of(ITEM)));
    assert!(!review.starts_with(&CommunityKey::ratings_of(ITEM)));
    assert!(!rating.starts_with(&CommunityKey::ratings_of(other)));
}

#[test]
fn a_rating_is_one_to_five() {
    for value in 1..=5 {
        let rating = Rating::try_from(value).unwrap();
        assert_eq!(rating.get(), value);
        assert_eq!(Rating::decode(&rating.encode()), Some(rating));
    }
    for value in [0, 6, u8::MAX] {
        assert!(Rating::try_from(value).is_err(), "{value}");
    }
}

#[test]
fn a_rating_is_written_as_a_json_number() {
    assert_eq!(Rating::try_from(4).unwrap().encode(), b"4");
}

/// Whatever is in the document is read as a rating only if it is one: a raw
/// put, or another build, can leave anything there.
#[test]
fn a_value_that_is_not_a_rating_does_not_read_as_one() {
    for value in [&b"0"[..], b"6", b"\"4\"", b"4.5", b"", b"-1"] {
        assert_eq!(
            Rating::decode(value),
            None,
            "{}",
            String::from_utf8_lossy(value)
        );
    }
}

#[test]
fn a_review_is_at_most_sixteen_kibibytes_of_utf8() {
    let longest = Review::try_from("a".repeat(REVIEW_MAX_BYTES)).unwrap();
    assert_eq!(Review::decode(&longest.encode()), Some(longest));
    assert!(Review::try_from("a".repeat(REVIEW_MAX_BYTES + 1)).is_err());

    // Bytes, not characters: "č" is two.
    assert!(Review::try_from("č".repeat(REVIEW_MAX_BYTES / 2)).is_ok());
    assert!(Review::try_from("č".repeat(REVIEW_MAX_BYTES / 2 + 1)).is_err());
}

#[test]
fn a_value_that_is_not_a_review_does_not_read_as_one() {
    let too_long = serde_json::to_vec(&"a".repeat(REVIEW_MAX_BYTES + 1)).unwrap();
    for value in [&too_long[..], b"4", b"", b"{}"] {
        assert_eq!(Review::decode(value), None);
    }
    let review = Review::try_from("Válka s mloky — satire.".to_owned()).unwrap();
    assert_eq!(review.as_str(), "Válka s mloky — satire.");
    assert_eq!(Review::decode(&review.encode()), Some(review));
}
