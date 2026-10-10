//! What the read model must hold, whatever order it was told things in.
//!
//! Three properties, and they are not the same one said three ways.
//!
//! **Round-trip** is the one that catches an encoding bug: a column read at the
//! wrong index, a list that went in as JSON and came back as a string, a
//! `series_index` that lost its fraction. It fails loudly the moment the schema
//! and [`StoredItem`] stop agreeing.
//!
//! **Idempotence** is §10's literal requirement — replay N times = replay once.
//! It is true by construction while the projection re-reads whole items, which
//! is exactly why it is asserted rather than assumed: the construction is a
//! choice somebody could later optimise away.
//!
//! **Order independence** is the one with teeth. It is what stops being true
//! first if the re-read ever becomes a delta applied to what was already there,
//! and unlike the other two it cannot be satisfied by an implementation that
//! quietly accumulates.

use std::collections::BTreeMap;

use distlib_core::{
    ContentHash, FileRecord, FileRole, Item, ItemId, ItemKind, MemberId, Rating, Review, Series,
};
use distlib_store::{Store, StoredItem, StoredMember};
use proptest::prelude::*;

/// A store with nothing in it, in memory.
async fn empty() -> Store {
    Store::open(None)
        .await
        .expect("a fresh in-memory store opens")
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a current-thread runtime starts")
}

// --- strategies -------------------------------------------------------------

fn text() -> impl Strategy<Value = String> {
    // Anything but a NUL, which is the one byte a `TEXT` column cannot hold
    // without the length being the only thing that says where the value ends.
    proptest::string::string_regex("[^\u{0}]{0,24}").expect("the text pattern compiles")
}

fn item_id() -> impl Strategy<Value = ItemId> {
    any::<[u8; 32]>().prop_map(ItemId::from_bytes)
}

fn content_hash() -> impl Strategy<Value = ContentHash> {
    any::<[u8; 32]>().prop_map(ContentHash::from_bytes)
}

fn kind() -> impl Strategy<Value = ItemKind> {
    prop_oneof![
        Just(ItemKind::Ebook),
        Just(ItemKind::Audiobook),
        Just(ItemKind::Video),
        Just(ItemKind::Other),
    ]
}

fn role() -> impl Strategy<Value = FileRole> {
    prop_oneof![
        Just(FileRole::Content),
        Just(FileRole::Cover),
        Just(FileRole::Subtitle),
        Just(FileRole::Metadata),
        Just(FileRole::Other),
    ]
}

fn file_record() -> impl Strategy<Value = FileRecord> {
    (
        role(),
        text(),
        any::<u64>(),
        text(),
        any::<Option<u32>>(),
        any::<Option<u32>>(),
        proptest::option::of(text()),
        any::<Option<u64>>(),
    )
        .prop_map(
            |(role, format, size, filename, seq, disc, title, duration)| FileRecord {
                role,
                format,
                // SQLite's only integer is signed, so a size past `i64::MAX` is
                // refused rather than wrapped. Nothing that size exists; the
                // generator stays inside what can be stored so the property is
                // about the encoding rather than about the refusal.
                size: size >> 1,
                filename,
                seq,
                disc,
                title,
                duration: duration.map(|duration| duration >> 1),
            },
        )
}

fn series() -> impl Strategy<Value = Series> {
    (text(), proptest::option::of(-1.0e6f32..1.0e6f32))
        .prop_map(|(name, index)| Series { name, index })
}

fn item() -> impl Strategy<Value = Item> {
    (
        item_id(),
        proptest::option::of(kind()),
        proptest::option::of(text()),
        proptest::option::of(proptest::collection::vec(text(), 0..4)),
        proptest::option::of(proptest::collection::vec(text(), 0..4)),
        proptest::option::of(series()),
        any::<Option<i32>>(),
        proptest::option::of(text()),
        proptest::option::of(text()),
        any::<Option<u32>>(),
        proptest::collection::btree_map(content_hash(), file_record(), 0..4),
    )
        .prop_map(
            |(
                id,
                kind,
                title,
                authors,
                genres,
                series,
                year,
                lang,
                description,
                replicas,
                files,
            )| Item {
                id,
                kind,
                title,
                authors,
                genres,
                series,
                year,
                lang,
                description,
                replicas,
                files,
            },
        )
}

/// One of a handful of members, so that two values about one item are
/// sometimes by the same member and sometimes not.
fn member() -> impl Strategy<Value = MemberId> {
    (1u8..5).prop_map(|seed| MemberId::from(iroh::SecretKey::from_bytes(&[seed; 32]).public()))
}

fn rating() -> impl Strategy<Value = Rating> {
    (1u8..=5).prop_map(|value| Rating::try_from(value).expect("one to five is a rating"))
}

fn review() -> impl Strategy<Value = Review> {
    text().prop_map(|text| Review::try_from(text).expect("short text is a review"))
}

fn stored() -> impl Strategy<Value = StoredItem> {
    (
        item(),
        proptest::collection::btree_map(member(), rating(), 0..3),
        proptest::collection::btree_map(member(), review(), 0..3),
        any::<u64>(),
    )
        .prop_map(|(item, ratings, reviews, last_modified)| StoredItem {
            item,
            ratings,
            reviews,
            last_modified: last_modified >> 1,
        })
}

/// Items with distinct ids, so a set of them has a well-defined projection.
fn distinct_items(count: std::ops::Range<usize>) -> impl Strategy<Value = Vec<StoredItem>> {
    proptest::collection::vec(stored(), count).prop_map(|items| {
        let mut by_id: BTreeMap<ItemId, StoredItem> = BTreeMap::new();
        for stored in items {
            by_id.insert(stored.item.id, stored);
        }
        by_id.into_values().collect()
    })
}

// --- the properties ---------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Every field survives the trip through SQLite unchanged.
    #[test]
    fn what_is_written_reads_back_the_same_item(stored in stored()) {
        runtime().block_on(async {
            let store = empty().await;
            let id = stored.item.id;
            store.upsert_item(stored.clone()).await.expect("the item is written");
            let read = store.item(id).await.expect("the item is read");
            prop_assert_eq!(read, Some(stored));
            Ok(())
        })?;
    }

    /// §10: replaying N times leaves what replaying once leaves.
    #[test]
    fn projecting_an_item_again_changes_nothing(stored in stored(), times in 2usize..5) {
        runtime().block_on(async {
            let store = empty().await;
            store.upsert_item(stored.clone()).await.expect("the item is written");
            let once = store.items().await.expect("the items are read");
            for _ in 0..times {
                store.upsert_item(stored.clone()).await.expect("the item is written again");
            }
            prop_assert_eq!(store.items().await.expect("the items are read"), once);
            Ok(())
        })?;
    }

    /// The order a set of items arrives in does not reach the tables.
    ///
    /// The regression pin for the whole design: this is what stops holding if
    /// the projection ever becomes an update applied to what was there before.
    #[test]
    fn the_order_items_arrive_in_does_not_matter(
        items in distinct_items(1..6),
        seed in any::<u64>(),
    ) {
        runtime().block_on(async {
            let forwards = empty().await;
            for stored in &items {
                forwards.upsert_item(stored.clone()).await.expect("the item is written");
            }

            // A deterministic shuffle, so a failing case is a failing case
            // rather than something that happened once.
            let mut shuffled = items.clone();
            let mut state = seed | 1;
            for index in (1..shuffled.len()).rev() {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                shuffled.swap(index, (state % (index as u64 + 1)) as usize);
            }

            let backwards = empty().await;
            for stored in shuffled.iter().rev() {
                backwards.upsert_item(stored.clone()).await.expect("the item is written");
            }

            prop_assert_eq!(
                forwards.items().await.expect("the items are read"),
                backwards.items().await.expect("the items are read"),
            );
            Ok(())
        })?;
    }
}

/// An item that reads with less in it than last time is stored with less in it.
///
/// **The decision this pins is deliberate and costs something**, so it is
/// pinned rather than left to be re-argued. A field whose newest entry has
/// arrived but whose bytes have not is left out of the item, so projecting it
/// writes `NULL` over a value that was there — and the item is missing that
/// field until the bytes land. Keeping the old value instead would make these
/// tables depend on what this node happened to see, which is exactly what makes
/// a restarted node differ from one that never restarted.
#[tokio::test]
async fn an_item_whose_content_went_away_loses_the_field() {
    let store = empty().await;
    let id = ItemId::from_bytes([7; 32]);
    let blob = ContentHash::from_bytes([9; 32]);
    let member = MemberId::from(iroh::SecretKey::from_bytes(&[1; 32]).public());

    let whole = StoredItem {
        item: Item {
            title: Some("Dune".to_owned()),
            kind: Some(ItemKind::Ebook),
            files: BTreeMap::from([(
                blob,
                FileRecord {
                    role: FileRole::Content,
                    format: "epub".to_owned(),
                    size: 1024,
                    filename: "dune.epub".to_owned(),
                    seq: None,
                    disc: None,
                    title: None,
                    duration: None,
                },
            )]),
            ..Item::new(id)
        },
        ratings: BTreeMap::from([(member, Rating::try_from(4).expect("a rating"))]),
        reviews: BTreeMap::from([(
            member,
            Review::try_from("Spice".to_owned()).expect("a review"),
        )]),
        last_modified: 100,
    };
    store
        .upsert_item(whole)
        .await
        .expect("the whole item is written");

    // The same item as it reads when the title's newest value and the file
    // record are both entries whose content has not arrived — and when the
    // member who rated and reviewed it is no longer one.
    let partial = StoredItem {
        item: Item {
            kind: Some(ItemKind::Ebook),
            ..Item::new(id)
        },
        ratings: BTreeMap::new(),
        reviews: BTreeMap::new(),
        last_modified: 200,
    };
    store
        .upsert_item(partial.clone())
        .await
        .expect("the partial item is written");

    let read = store.item(id).await.expect("the item is read");
    assert_eq!(
        read,
        Some(partial),
        "the title, the file row, the rating and the review are gone"
    );
}

#[tokio::test]
async fn members_are_replaced_rather_than_merged() {
    let store = empty().await;
    let alice = MemberId::from(iroh::SecretKey::from_bytes(&[1; 32]).public());
    let bob = MemberId::from(iroh::SecretKey::from_bytes(&[2; 32]).public());

    let member = |member, name: &str, is_core| StoredMember {
        member,
        display_name: name.to_owned(),
        pledge_bytes: 1 << 40,
        is_core,
    };

    store
        .set_members(vec![
            member(alice, "alice", true),
            member(bob, "bob", false),
        ])
        .await
        .expect("both members are written");
    assert_eq!(store.members().await.expect("read").len(), 2);

    // Bob is expelled: the log says who *is* a member, so he is not merged
    // into what was there, he is absent from it.
    store
        .set_members(vec![member(alice, "alice", true)])
        .await
        .expect("the remaining member is written");
    assert_eq!(
        store.members().await.expect("read"),
        vec![member(alice, "alice", true)],
    );
}

/// The schema is created by opening, and opening again finds it already there.
#[tokio::test]
async fn a_store_reopens_onto_what_it_already_held() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let id = ItemId::from_bytes([3; 32]);
    let stored = StoredItem {
        item: Item {
            title: Some("Neuromancer".to_owned()),
            ..Item::new(id)
        },
        ratings: BTreeMap::new(),
        reviews: BTreeMap::new(),
        last_modified: 42,
    };

    let first = Store::open(Some(dir.path().to_path_buf()))
        .await
        .expect("the store opens");
    first.upsert_item(stored.clone()).await.expect("written");
    drop(first);

    let second = Store::open(Some(dir.path().to_path_buf()))
        .await
        .expect("the store opens again");
    assert_eq!(second.item(id).await.expect("read"), Some(stored));
}

/// **A read model of another version is thrown away, not migrated**: its tables
/// go, whatever they were, and the replay at every start refills them.
///
/// The old database here has an `items` table of another shape and a table this
/// build has never heard of — what a version from before or after this one
/// leaves behind. Kept, the first would refuse every write this build makes.
#[tokio::test]
async fn a_store_of_another_version_is_started_afresh() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    {
        let old = rusqlite::Connection::open(dir.path().join("read-model.sqlite"))
            .expect("the file opens");
        old.execute_batch(
            "CREATE TABLE items (id TEXT PRIMARY KEY, title TEXT, last_modified INTEGER);
             INSERT INTO items VALUES ('left over', 'Left over', 1);
             CREATE TABLE someday (x INTEGER);",
        )
        .expect("an old read model is written");
    }

    let store = Store::open(Some(dir.path().to_path_buf()))
        .await
        .expect("an old read model opens");
    let id = ItemId::from_bytes([3; 32]);
    let stored = StoredItem {
        item: Item {
            title: Some("Neuromancer".to_owned()),
            lang: Some("en".to_owned()),
            ..Item::new(id)
        },
        ratings: BTreeMap::new(),
        reviews: BTreeMap::new(),
        last_modified: 42,
    };
    store.upsert_item(stored.clone()).await.expect("written");
    assert_eq!(store.item(id).await.expect("read"), Some(stored));
    drop(store);

    let db =
        rusqlite::Connection::open(dir.path().join("read-model.sqlite")).expect("the file opens");
    let version: u32 = db
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .expect("the version reads");
    assert_eq!(version, distlib_store::schema::READ_MODEL_VERSION);
    let tables: Vec<String> = db
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        .expect("the tables are listed")
        .query_map([], |row| row.get(0))
        .expect("the tables are listed")
        .collect::<rusqlite::Result<_>>()
        .expect("the tables are listed");
    assert_eq!(
        tables,
        ["item_files", "items", "members", "ratings", "reviews"]
    );
    let left: i64 = db
        .query_row("SELECT count(*) FROM items", [], |row| row.get(0))
        .expect("the rows are counted");
    assert_eq!(left, 1, "only the row written since, not the one left over");
}
