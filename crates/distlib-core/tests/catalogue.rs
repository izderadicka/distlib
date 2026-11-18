//! The item schema: what a key says, and what an entry means.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use distlib_core::{Absorbed, ContentHash, Field, FileRecord, Item, ItemId, ItemKind, Key, Series};

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
    item.files.insert(hash(1), a_file("01.mp3"));
    item.files.insert(hash(2), a_file("02.mp3"));
    item
}

fn a_file(filename: &str) -> FileRecord {
    FileRecord {
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
        Key::Files { item },
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

/// **An item is the files it was added with** (P5-66): a file list that does
/// not fingerprint to the id, one file more or one fewer, is somebody's
/// stray, and leaves the item's files as they were.
#[test]
fn a_file_list_that_is_not_the_items_own_is_left_out() {
    let item = an_item();
    let key = Key::Files { item: item.id }.encode();
    let mut read = Item::new(item.id);
    for (key, value) in item.entries() {
        read.absorb(key.as_bytes(), &value);
    }

    let mut one_more = item.files.clone();
    one_more.insert(hash(3), a_file("03.mp3"));
    let mut one_fewer = item.files.clone();
    one_fewer.remove(&hash(2));
    for stray in [one_more, one_fewer] {
        assert_eq!(
            read.absorb(key.as_bytes(), &serde_json::to_vec(&stray).unwrap()),
            Absorbed::WrongFingerprint
        );
        assert_eq!(read, item);
    }
}

/// An item with no files has no fingerprint, rather than the same one every
/// empty item would share.
#[test]
fn an_item_with_nothing_in_it_has_no_fingerprint() {
    let item = Item::new(ItemId::from_bytes([6u8; 32]));
    assert_eq!(item.fingerprint(), None);
}

/// Every field an item can hold is a field it writes.
///
/// The silent half of what the `fields!` table exists to prevent: a field
/// missing from the write path is never an error anywhere — it simply never
/// reaches another member, and the item reads back short on every node but the
/// one that set it. Counted against `Field::ALL` rather than against a literal
/// nine, so adding a row to the table is the only thing this needs.
#[test]
fn a_fully_populated_item_writes_every_field_there_is() {
    let written: Vec<Field> = an_item()
        .entries()
        .iter()
        .filter_map(|(key, _)| match Key::parse(key.as_bytes()) {
            Some(Key::Field { field, .. }) => Some(field),
            _ => None,
        })
        .collect();
    assert_eq!(written, Field::ALL, "every field, in the table's order");
}

/// And every field reads back under the name it wrote.
///
/// The other silent half: a field the key encoder can write and the key parser
/// cannot recognise is absorbed as `Unknown` and dropped on the floor, on the
/// reading node only.
#[test]
fn every_field_survives_its_own_key() {
    let item = ItemId::from_content_hashes(&[[7u8; 32]]);
    for &field in Field::ALL {
        let key = Key::Field { item, field };
        assert_eq!(
            Key::parse(key.encode().as_bytes()),
            Some(key),
            "`{}` is written but not read back",
            field.as_str()
        );
    }
}
