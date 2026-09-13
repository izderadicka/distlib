//! The item schema: what a key says, and what an entry means.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use distlib_core::{
    Absorbed, ContentHash, Field, FileRecord, FileRole, Item, ItemId, ItemKind, Key, Series,
};

fn hash(byte: u8) -> ContentHash {
    ContentHash::from_bytes([byte; 32])
}

fn an_item() -> Item {
    let mut item = Item::new(ItemId::from_content_hashes(&[[1u8; 32], [2u8; 32]]));
    item.kind = Some(ItemKind::Audiobook);
    item.title = Some("The Left Hand of Darkness".to_owned());
    item.authors = Some(vec!["Ursula K. Le Guin".to_owned()]);
    item.genres = Some(vec!["science fiction".to_owned()]);
    item.series = Some(Series {
        name: "Hainish Cycle".to_owned(),
        index: Some(4.5),
    });
    item.year = Some(1969);
    item.lang = Some("en".to_owned());
    item.description = Some("Winter, and an envoy.".to_owned());
    item.replicas = Some(3);
    item.files
        .insert(hash(1), a_file(FileRole::Content, "01.mp3"));
    item.files
        .insert(hash(2), a_file(FileRole::Content, "02.mp3"));
    item
}

fn a_file(role: FileRole, filename: &str) -> FileRecord {
    FileRecord {
        role,
        format: "mp3".to_owned(),
        size: 4_200_000,
        filename: filename.to_owned(),
        seq: Some(1),
        disc: None,
        title: None,
        duration: Some(1_800),
    }
}

/// Write an item out, read it back, and get the same item.
#[test]
fn an_item_survives_the_entries_it_is_made_of() {
    let item = an_item();
    let mut read = Item::new(item.id);
    for (key, value) in item.entries() {
        assert_eq!(
            read.absorb(key.as_bytes(), &value),
            Absorbed::Took,
            "every entry an item produces is one it can read back: {key}"
        );
    }
    assert_eq!(read, item);
}

/// **Absent fields produce no entry.** Writing `title: null` is a write, and
/// under last-writer-wins it would erase a title another member had just
/// filled in — so an item with one field set has to be a one-key update.
#[test]
fn an_item_writes_only_what_it_says() {
    let mut item = Item::new(ItemId::from_bytes([9u8; 32]));
    item.title = Some("Dune".to_owned());

    let entries = item.entries();
    assert_eq!(entries.len(), 1, "one field set, one entry: {entries:?}");
    assert_eq!(entries[0].0, format!("item/{}/title", item.id));
    assert_eq!(entries[0].1, b"\"Dune\"");
}

/// A key this build has never seen is somebody else's, not an error.
///
/// The catalogue is one document for the whole group and grows keys over time
/// — phase 4's ratings land beside these, and a field added next year arrives
/// at a node running this build. Reading an item must survive both.
#[test]
fn an_unknown_key_is_left_alone_and_the_rest_of_the_item_still_reads() {
    let item = an_item();
    let mut read = Item::new(item.id);

    assert_eq!(
        read.absorb(format!("rating/{}/somebody", item.id).as_bytes(), b"5"),
        Absorbed::Unknown,
        "phase 4 writes this beside us"
    );
    assert_eq!(
        read.absorb(format!("item/{}/mood", item.id).as_bytes(), b"\"bleak\""),
        Absorbed::Unknown,
        "a field a newer build added"
    );
    assert_eq!(
        read.absorb(b"item/not-a-hash/title", b"\"Dune\""),
        Absorbed::Unknown
    );

    for (key, value) in item.entries() {
        read.absorb(key.as_bytes(), &value);
    }
    assert_eq!(read, item, "the keys we do know still make the whole item");
}

/// A value we cannot read leaves the field as it was, rather than emptying it.
#[test]
fn an_unreadable_value_does_not_erase_what_was_there() {
    let mut item = Item::new(ItemId::from_bytes([3u8; 32]));
    let title = Key::Field {
        item: item.id,
        field: Field::Title,
    }
    .encode();
    assert_eq!(item.absorb(title.as_bytes(), b"\"Dune\""), Absorbed::Took);

    // Bare, not JSON — what a different encoding would look like arriving here.
    assert_eq!(item.absorb(title.as_bytes(), b"Dune"), Absorbed::Unreadable);
    assert_eq!(item.title.as_deref(), Some("Dune"));

    let year = Key::Field {
        item: item.id,
        field: Field::Year,
    }
    .encode();
    assert_eq!(
        item.absorb(year.as_bytes(), b"\"1969\""),
        Absorbed::Unreadable,
        "a string where a number belongs"
    );
    assert_eq!(item.year, None);
}

/// An entry about another item is not this item's business.
#[test]
fn an_entry_about_another_item_is_refused() {
    let mut item = Item::new(ItemId::from_bytes([4u8; 32]));
    let other = Key::Field {
        item: ItemId::from_bytes([5u8; 32]),
        field: Field::Title,
    }
    .encode();
    assert_eq!(
        item.absorb(other.as_bytes(), b"\"Dune\""),
        Absorbed::NotThisItem
    );
    assert_eq!(item.title, None);
}

/// Keys round-trip, and a key with anything extra on the end is not ours.
#[test]
fn keys_say_what_they_mean() {
    let item = ItemId::from_bytes([7u8; 32]);
    for key in [
        Key::Field {
            item,
            field: Field::Description,
        },
        Key::File {
            item,
            blob: hash(8),
        },
    ] {
        let encoded = key.encode();
        assert_eq!(Key::parse(encoded.as_bytes()), Some(key), "{encoded}");
        assert_eq!(
            Key::parse(format!("{encoded}/extra").as_bytes()),
            None,
            "a deeper key is a different key"
        );
    }
    assert_eq!(
        Key::prefix_of(item),
        format!("item/{item}/"),
        "what reads or watches one item"
    );
}

/// **Only content files make the id**, which is the rule §5.2 says people get
/// wrong: adding a cover must not produce a different item.
#[test]
fn a_cover_does_not_change_what_the_item_is() {
    let mut item = an_item();
    let before = item.fingerprint().unwrap();

    item.files
        .insert(hash(3), a_file(FileRole::Cover, "cover.jpg"));
    item.files
        .insert(hash(4), a_file(FileRole::Subtitle, "en.srt"));
    assert_eq!(item.fingerprint(), Some(before));

    // And a second chapter of content does, because that is a different work
    // in the only sense the fingerprint knows.
    item.files
        .insert(hash(5), a_file(FileRole::Content, "03.mp3"));
    assert_ne!(item.fingerprint(), Some(before));
}

/// An item with no content files has no fingerprint, rather than the same one
/// every empty item would share.
#[test]
fn an_item_with_nothing_in_it_has_no_fingerprint() {
    let mut item = Item::new(ItemId::from_bytes([6u8; 32]));
    assert_eq!(item.fingerprint(), None);
    item.files.insert(hash(1), a_file(FileRole::Cover, "c.jpg"));
    assert_eq!(item.fingerprint(), None, "a cover alone is not an item");
}
