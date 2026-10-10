//! What members say about items: the keys they say it in, and the values.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use distlib_core::{
    Bookmark, BookmarkId, COMMENT_MAX_BYTES, Comment, CommunityKey, ItemId, ItemKind, Key,
    MemberId, NOTE_MAX_BYTES, POSITION_MAX_BYTES, REVIEW_MAX_BYTES, Rating, Review, WISH_MAX_BYTES,
    Wish, WishFields, WishId, WishStatus,
};
use iroh::SecretKey;
use serde_json::json;

fn member() -> MemberId {
    MemberId::from(SecretKey::generate().public())
}

/// An id whose hex has letters in it, so it has another spelling.
const ITEM: ItemId = ItemId::from_bytes([0xab; 32]);
const A: BookmarkId = BookmarkId::from_bytes([0xa1; 16]);
const B: BookmarkId = BookmarkId::from_bytes([0xb2; 16]);
const WISH: WishId = WishId::from_bytes([0xcd; 32]);

fn rating(member: MemberId) -> CommunityKey {
    CommunityKey::Rating { item: ITEM, member }
}

fn review(member: MemberId) -> CommunityKey {
    CommunityKey::Review { item: ITEM, member }
}

fn bookmark(member: MemberId, bookmark: BookmarkId) -> CommunityKey {
    CommunityKey::Bookmark {
        item: ITEM,
        member,
        bookmark,
    }
}

fn wish(member: MemberId) -> CommunityKey {
    CommunityKey::Wish { wish: WISH, member }
}

fn comment(member: MemberId) -> CommunityKey {
    CommunityKey::WishComment { wish: WISH, member }
}

#[test]
fn a_key_reads_back_as_itself() {
    let bob = member();
    for key in [
        rating(bob),
        review(bob),
        bookmark(bob, A),
        bookmark(bob, B),
        wish(bob),
        comment(bob),
    ] {
        let encoded = key.encode();
        assert_eq!(
            CommunityKey::parse(encoded.as_bytes()),
            Some(key),
            "{encoded}"
        );
        assert_eq!(key.member(), bob);
    }
}

#[test]
fn a_key_is_named_by_its_kind_the_item_and_the_member() {
    let bob = member();
    assert_eq!(rating(bob).encode(), format!("rating/{ITEM}/{bob}"));
    assert_eq!(review(bob).encode(), format!("review/{ITEM}/{bob}"));
    assert_eq!(
        bookmark(bob, A).encode(),
        format!("bookmark/{ITEM}/{bob}/a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1")
    );
    assert_eq!(wish(bob).encode(), format!("wish/{WISH}/{bob}"));
    assert_eq!(comment(bob).encode(), format!("wish_comment/{WISH}/{bob}"));
}

/// A second spelling of one member's key would be a second rating by them,
/// and it would pass the author check.
#[test]
fn a_key_counts_only_in_the_spelling_it_is_written_in() {
    let bob = member();
    let item = ITEM.to_string().to_uppercase();
    let who = bob.to_string().to_uppercase();
    let mark = A.to_string().to_uppercase();
    assert_eq!(
        item.parse::<ItemId>().ok(),
        Some(ITEM),
        "the id itself parses"
    );
    assert_eq!(
        CommunityKey::parse(format!("rating/{item}/{bob}").as_bytes()),
        None
    );
    assert_eq!(
        CommunityKey::parse(format!("bookmark/{ITEM}/{bob}/{mark}").as_bytes()),
        None
    );
    let wished = WISH.to_string().to_uppercase();
    assert_eq!(
        CommunityKey::parse(format!("wish/{wished}/{bob}").as_bytes()),
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
        format!("bookmark/{ITEM}/{bob}/{A}/extra"),
        format!("bookmark/{ITEM}/{bob}/a1a1"),
        format!("wish/{A}/{bob}"),
        format!("wish/{WISH}/{bob}/{A}"),
        format!("wish_comment/{WISH}"),
        format!("item/{ITEM}/title"),
        format!("rating/{bob}/{ITEM}"),
    ] {
        assert_eq!(CommunityKey::parse(key.as_bytes()), None, "{key}");
    }
    // And the other way round: an item's keys never read as a rating's.
    assert_eq!(Key::parse(rating(bob).encode().as_bytes()), None);
}

#[test]
fn a_prefix_covers_one_kind_of_key_about_one_item() {
    let bob = member();
    let other = ItemId::from_bytes([8; 32]);

    assert!(
        rating(bob)
            .encode()
            .starts_with(&CommunityKey::ratings_of(ITEM))
    );
    assert!(
        review(bob)
            .encode()
            .starts_with(&CommunityKey::reviews_of(ITEM))
    );
    assert!(
        bookmark(bob, A)
            .encode()
            .starts_with(&CommunityKey::bookmarks_of(ITEM))
    );
    assert!(
        !review(bob)
            .encode()
            .starts_with(&CommunityKey::ratings_of(ITEM))
    );
    assert!(
        !rating(bob)
            .encode()
            .starts_with(&CommunityKey::ratings_of(other))
    );
}

/// `wish/` and `wish_comment/` share four letters, and nothing else.
#[test]
fn a_wishs_entries_and_its_comments_are_read_apart() {
    let bob = member();
    assert!(
        wish(bob)
            .encode()
            .starts_with(&CommunityKey::wish_entries_of(WISH))
    );
    assert!(
        comment(bob)
            .encode()
            .starts_with(&CommunityKey::comments_on(WISH))
    );
    assert!(
        !comment(bob)
            .encode()
            .starts_with(&CommunityKey::wish_entries_of(WISH))
    );
    assert!(
        !wish(bob)
            .encode()
            .starts_with(&CommunityKey::comments_on(WISH))
    );
}

/// Writing a key prunes its author's longer keys beneath it (ground truth
/// 10): a bare `bookmark/X/bob` would delete every bookmark bob left on X.
#[test]
fn no_key_of_a_members_is_a_prefix_of_another_of_theirs() {
    let bob = member();
    let keys = [
        rating(bob),
        review(bob),
        bookmark(bob, A),
        bookmark(bob, B),
        wish(bob),
        comment(bob),
    ]
    .map(|key| key.encode());
    for (n, key) in keys.iter().enumerate() {
        for (m, other) in keys.iter().enumerate() {
            assert!(
                n == m || !other.starts_with(key.as_str()),
                "{key} prefixes {other}"
            );
        }
    }
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

#[test]
fn a_bookmark_is_a_position_a_note_and_when_it_was_made() {
    let mark = Bookmark::new("ch. 12".to_owned(), "the salamanders vote".to_owned(), 17).unwrap();
    assert_eq!(
        (mark.position(), mark.note(), mark.created_at()),
        ("ch. 12", "the salamanders vote", 17)
    );
    assert_eq!(Bookmark::decode(&mark.encode()), Some(mark.clone()));
    // When it last changed is the entry's, not a field of its own.
    let fields: serde_json::Value = serde_json::from_slice(&mark.encode()).unwrap();
    assert_eq!(
        fields,
        serde_json::json!({"position": "ch. 12", "note": "the salamanders vote", "created_at": 17})
    );
}

#[test]
fn a_bookmarks_note_and_position_are_capped_in_bytes() {
    let made = |position: String, note: String| Bookmark::new(position, note, 0);
    assert!(made("p".repeat(POSITION_MAX_BYTES), "n".repeat(NOTE_MAX_BYTES)).is_ok());
    assert!(made("p".repeat(POSITION_MAX_BYTES + 1), String::new()).is_err());
    assert!(made(String::new(), "n".repeat(NOTE_MAX_BYTES + 1)).is_err());
    assert!(made(String::new(), "č".repeat(NOTE_MAX_BYTES / 2 + 1)).is_err());
}

#[test]
fn a_value_that_is_not_a_bookmark_does_not_read_as_one() {
    let long_note = serde_json::json!({
        "position": "1", "note": "n".repeat(NOTE_MAX_BYTES + 1), "created_at": 0
    });
    let no_date = serde_json::json!({"position": "1", "note": ""});
    for value in [
        long_note,
        no_date,
        serde_json::json!("1"),
        serde_json::json!(4),
    ] {
        assert_eq!(
            Bookmark::decode(&serde_json::to_vec(&value).unwrap()),
            None,
            "{value}"
        );
    }
}

#[test]
fn a_bookmark_id_is_sixteen_bytes_of_lowercase_hex() {
    assert_eq!(A.to_string().parse::<BookmarkId>().ok(), Some(A));
    for text in ["a1a1", &"a1".repeat(17), &"A1".repeat(16), &"zz".repeat(16)] {
        assert!(text.parse::<BookmarkId>().is_err(), "{text}");
    }
}

fn fields(status: WishStatus) -> WishFields {
    WishFields {
        kind: None,
        title: None,
        authors: None,
        description: None,
        created_at: None,
        status,
    }
}

/// A creator says what is wished for; a fulfiller says by what — and the
/// JSON is the plan's: `status`, and `item_id` beside it when fulfilled.
#[test]
fn a_wish_entry_is_what_its_member_says_and_no_more() {
    let made = Wish::new(WishFields {
        kind: Some(ItemKind::Audiobook),
        title: Some("Hordubal".to_owned()),
        authors: Some(vec!["Karel Čapek".to_owned()]),
        created_at: Some(17),
        ..fields(WishStatus::Open)
    })
    .unwrap();
    let answered = Wish::new(fields(WishStatus::Fulfilled { item_id: ITEM })).unwrap();

    let json = |wish: &Wish| serde_json::from_slice::<serde_json::Value>(&wish.encode()).unwrap();
    assert_eq!(
        json(&made),
        json!({"kind": "audiobook", "title": "Hordubal", "authors": ["Karel Čapek"], "created_at": 17, "status": "open"})
    );
    assert_eq!(
        json(&answered),
        json!({"status": "fulfilled", "item_id": ITEM.to_string()})
    );
    for wish in [made, answered] {
        assert_eq!(Wish::decode(&wish.encode()), Some(wish));
    }
}

#[test]
fn a_value_that_is_not_a_wish_entry_does_not_read_as_one() {
    for value in [
        json!({"status": "fulfilled"}),
        json!({"status": "withdrawn"}),
        json!({"title": "Hordubal"}),
        json!({"status": "open", "title": 4}),
        json!({"status": "open", "kind": "scroll"}),
        json!("open"),
    ] {
        assert_eq!(
            Wish::decode(&serde_json::to_vec(&value).unwrap()),
            None,
            "{value}"
        );
    }
}

/// One cap for the whole entry, as written: whatever its fields say.
#[test]
fn a_wish_entry_is_at_most_sixteen_kibibytes_of_json() {
    let with = |description: String| WishFields {
        description: Some(description),
        ..fields(WishStatus::Open)
    };
    let overhead = r#"{"description":"","status":"open"}"#.len();
    let longest = with("d".repeat(WISH_MAX_BYTES - overhead));
    assert_eq!(serde_json::to_vec(&longest).unwrap().len(), WISH_MAX_BYTES);
    let wish = Wish::new(longest).unwrap();
    assert_eq!(Wish::decode(&wish.encode()), Some(wish));

    let over = with("d".repeat(WISH_MAX_BYTES - overhead + 1));
    assert!(Wish::new(over.clone()).is_err());
    assert_eq!(Wish::decode(&serde_json::to_vec(&over).unwrap()), None);
}

#[test]
fn a_comment_is_at_most_four_kibibytes_of_utf8() {
    let longest = Comment::try_from("c".repeat(COMMENT_MAX_BYTES)).unwrap();
    assert_eq!(Comment::decode(&longest.encode()), Some(longest));
    assert!(Comment::try_from("c".repeat(COMMENT_MAX_BYTES + 1)).is_err());
    let too_long = serde_json::to_vec(&"c".repeat(COMMENT_MAX_BYTES + 1)).unwrap();
    assert_eq!(Comment::decode(&too_long), None);
}

#[test]
fn a_wish_id_is_thirty_two_bytes_of_lowercase_hex() {
    assert_eq!(WISH.to_string().parse::<WishId>().ok(), Some(WISH));
    for text in [&"cd".repeat(16), &"CD".repeat(32), &"cd".repeat(33)] {
        assert!(text.parse::<WishId>().is_err(), "{text}");
    }
}
