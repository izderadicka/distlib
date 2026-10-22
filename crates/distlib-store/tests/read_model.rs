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

#![allow(clippy::too_many_lines)] // a test is a scenario, read top to bottom

use std::{
    collections::BTreeMap,
    time::{Duration, UNIX_EPOCH},
};

use distlib_core::{
    Bookmark, BookmarkId, Comment, ContentHash, CustodyItem, FileRecord, FileRole, Item, ItemId,
    ItemKind, MemberId, Rating, Review, Series, Wish, WishFields, WishId, WishStatus,
};
use distlib_store::{BookmarkFilter, Store, StoredBookmark, StoredItem, StoredMember, StoredWish};
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
        proptest::collection::btree_map(content_hash(), file_record(), 0..4),
    )
        .prop_map(
            |(id, kind, title, authors, genres, series, year, lang, description, files)| Item {
                id,
                kind,
                title,
                authors,
                genres,
                series,
                year,
                lang,
                description,
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
        proptest::collection::btree_map(
            content_hash(),
            proptest::collection::btree_set(member(), 1..3),
            0..3,
        ),
    )
        .prop_map(
            |(item, ratings, reviews, last_modified, file_authors)| StoredItem {
                item,
                ratings,
                reviews,
                last_modified: last_modified >> 1,
                file_authors,
            },
        )
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

/// One member's entry for a wish: any of its optional fields, open or
/// fulfilled.
fn wish_entry() -> impl Strategy<Value = Wish> {
    let status = prop_oneof![
        Just(WishStatus::Open),
        item_id().prop_map(|item_id| WishStatus::Fulfilled { item_id }),
    ];
    (
        proptest::option::of(kind()),
        proptest::option::of(text()),
        proptest::option::of(proptest::collection::vec(text(), 0..3)),
        proptest::option::of(text()),
        proptest::option::of(any::<u64>().prop_map(|at| at >> 1)),
        status,
    )
        .prop_map(|(kind, title, authors, description, created_at, status)| {
            Wish::new(WishFields {
                kind,
                title,
                authors,
                description,
                created_at,
                status,
            })
            .expect("short fields make a wish")
        })
}

fn stored_wish() -> impl Strategy<Value = StoredWish> {
    (
        proptest::collection::btree_map(member(), wish_entry(), 0..3),
        proptest::collection::btree_map(
            member(),
            text().prop_map(|text| Comment::try_from(text).expect("short text is a comment")),
            0..3,
        ),
    )
        .prop_map(|(entries, comments)| StoredWish { entries, comments })
}

/// One item's bookmarks, in the order the store reads them back: by member,
/// then id.
fn item_bookmarks() -> impl Strategy<Value = Vec<StoredBookmark>> {
    let bookmark = (text(), text(), any::<u64>()).prop_map(|(position, note, created_at)| {
        Bookmark::new(position, note, created_at >> 1).expect("short text makes a bookmark")
    });
    proptest::collection::btree_map(
        (member(), any::<[u8; 16]>().prop_map(BookmarkId::from_bytes)),
        (bookmark, any::<u64>()),
        0..4,
    )
    .prop_map(|bookmarks| {
        bookmarks
            .into_iter()
            .map(|((member, id), (bookmark, last_modified))| StoredBookmark {
                member,
                id,
                bookmark,
                last_modified: last_modified >> 1,
            })
            .collect()
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

    /// Custody's narrow read of every item says what each whole item says.
    #[test]
    fn custody_items_are_what_each_stored_item_says(items in distinct_items(0..5)) {
        runtime().block_on(async {
            let store = empty().await;
            for mut stored in items {
                // Real sizes, so that a sum of them is exact.
                for file in stored.item.files.values_mut() {
                    file.size >>= 20;
                }
                store.upsert_item(stored).await.expect("the item is written");
            }
            let whole: Vec<CustodyItem> = store
                .items()
                .await
                .expect("the items are read")
                .iter()
                .map(StoredItem::custody_item)
                .collect();
            prop_assert_eq!(store.custody_items().await.expect("read"), whole);
            Ok(())
        })?;
    }

    /// What each member added is the size of each content file whose key it
    /// wrote.
    #[test]
    fn added_bytes_are_the_content_files_each_member_wrote(
        items in distinct_items(0..5),
        authors in proptest::collection::vec(member(), 1..3),
    ) {
        runtime().block_on(async {
            let store = empty().await;
            let mut expected: BTreeMap<MemberId, u64> = BTreeMap::new();
            for mut stored in items {
                stored.file_authors = stored
                    .item
                    .files
                    .iter_mut()
                    .zip(authors.iter().cycle())
                    .map(|((hash, file), author)| {
                        file.size >>= 20;
                        if file.role == FileRole::Content {
                            *expected.entry(*author).or_default() += file.size;
                        }
                        (*hash, [*author].into())
                    })
                    .collect();
                store.upsert_item(stored).await.expect("the item is written");
            }
            prop_assert_eq!(store.added_bytes().await.expect("read"), expected);
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

    /// A wish's entries and comments read back as written, and writing them
    /// again replaces them rather than adding to them.
    #[test]
    fn a_wish_is_replaced_and_read_back(first in stored_wish(), second in stored_wish()) {
        runtime().block_on(async {
            let store = empty().await;
            let wish = WishId::from_bytes([7; 32]);
            let other = WishId::from_bytes([8; 32]);
            store.replace_wish(other, first.clone()).await.expect("written");
            store.replace_wish(wish, first.clone()).await.expect("written");
            prop_assert_eq!(&store.wish(wish).await.expect("read"), &first);
            store.replace_wish(wish, second.clone()).await.expect("written again");
            prop_assert_eq!(&store.wish(wish).await.expect("read"), &second);
            prop_assert_eq!(
                store.wish(other).await.expect("read"),
                first.clone(),
                "another wish is left alone"
            );
            let expected: BTreeMap<WishId, StoredWish> = [(other, first), (wish, second)]
                .into_iter()
                .filter(|(_, stored)| *stored != StoredWish::default())
                .collect();
            prop_assert_eq!(
                store.all_wishes().await.expect("read"),
                expected,
                "every wish with a row, and none without"
            );
            Ok(())
        })?;
    }

    /// An item's bookmarks read back as written, and writing them again
    /// replaces them rather than adding to them.
    #[test]
    fn an_items_bookmarks_are_replaced_and_read_back(
        first in item_bookmarks(),
        second in item_bookmarks(),
    ) {
        runtime().block_on(async {
            let store = empty().await;
            let item = ItemId::from_bytes([7; 32]);
            let other = ItemId::from_bytes([8; 32]);
            store.replace_bookmarks(other, first.clone()).await.expect("written");
            store.replace_bookmarks(item, first.clone()).await.expect("written");
            prop_assert_eq!(&store.bookmarks_of(item).await.expect("read"), &first);
            store.replace_bookmarks(item, second.clone()).await.expect("written again");
            prop_assert_eq!(store.bookmarks_of(item).await.expect("read"), second);
            prop_assert_eq!(
                store.bookmarks_of(other).await.expect("read"),
                first,
                "another item's bookmarks are left alone"
            );
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
        file_authors: Default::default(),
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
        file_authors: Default::default(),
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

/// `find_bookmarks` narrows by text in the note, the item's title or the
/// member's name — case aside, and with `%` and `_` meaning themselves — and
/// by member and item; newest first.
#[tokio::test]
async fn bookmarks_are_found_by_text_member_and_item() {
    let store = empty().await;
    let alice = MemberId::from(iroh::SecretKey::from_bytes(&[1; 32]).public());
    let bob = MemberId::from(iroh::SecretKey::from_bytes(&[2; 32]).public());
    store
        .set_members(
            [(alice, "alice"), (bob, "bob")]
                .map(|(member, name)| StoredMember {
                    member,
                    display_name: name.to_owned(),
                    pledge_bytes: 0,
                    is_core: false,
                })
                .to_vec(),
        )
        .await
        .expect("written");
    let dune = ItemId::from_bytes([1; 32]);
    let hordubal = ItemId::from_bytes([2; 32]);
    let not_projected = ItemId::from_bytes([9; 32]);
    for (id, title) in [(dune, "Dune"), (hordubal, "Hordubal")] {
        store
            .upsert_item(StoredItem {
                item: Item {
                    title: Some(title.to_owned()),
                    ..Item::new(id)
                },
                ratings: BTreeMap::new(),
                reviews: BTreeMap::new(),
                last_modified: 1,
                file_authors: Default::default(),
            })
            .await
            .expect("written");
    }
    let bookmark = |member, id: u8, note: &str, created_at| StoredBookmark {
        member,
        id: BookmarkId::from_bytes([id; 16]),
        bookmark: Bookmark::new("p. 1".to_owned(), note.to_owned(), created_at)
            .expect("a short bookmark"),
        last_modified: created_at,
    };
    let alices_on_dune = bookmark(alice, 1, "the spice must flow, axb", 4);
    let bobs_on_dune = bookmark(bob, 2, "100% sure: a_b", 3);
    let bobs_on_hordubal = bookmark(bob, 3, "the ending", 2);
    let alices_elsewhere = bookmark(alice, 4, "", 1);
    store
        .replace_bookmarks(dune, vec![alices_on_dune.clone(), bobs_on_dune.clone()])
        .await
        .expect("written");
    store
        .replace_bookmarks(hordubal, vec![bobs_on_hordubal.clone()])
        .await
        .expect("written");
    store
        .replace_bookmarks(not_projected, vec![alices_elsewhere.clone()])
        .await
        .expect("written");

    let found = |filter: BookmarkFilter| {
        let store = store.clone();
        async move {
            store
                .find_bookmarks(filter)
                .await
                .expect("read")
                .into_iter()
                .map(|listed| listed.stored)
                .collect::<Vec<_>>()
        }
    };
    let text = |text: &str| BookmarkFilter {
        text: Some(text.to_owned()),
        ..BookmarkFilter::default()
    };

    assert_eq!(
        found(BookmarkFilter::default()).await,
        [
            &alices_on_dune,
            &bobs_on_dune,
            &bobs_on_hordubal,
            &alices_elsewhere
        ]
        .map(Clone::clone),
        "everything, newest first"
    );
    assert_eq!(
        found(text("SPICE")).await,
        std::slice::from_ref(&alices_on_dune),
        "the note"
    );
    assert_eq!(
        found(text("dune")).await,
        [alices_on_dune.clone(), bobs_on_dune.clone()],
        "the item's title"
    );
    assert_eq!(
        found(text("Bob")).await,
        [bobs_on_dune.clone(), bobs_on_hordubal.clone()],
        "the member's name"
    );
    assert_eq!(
        found(text("%")).await,
        std::slice::from_ref(&bobs_on_dune),
        "`%` is itself"
    );
    assert_eq!(
        found(text("a_b")).await,
        std::slice::from_ref(&bobs_on_dune),
        "`_` is itself"
    );
    assert_eq!(
        found(BookmarkFilter {
            member: Some(bob),
            item: Some(dune),
            ..BookmarkFilter::default()
        })
        .await,
        std::slice::from_ref(&bobs_on_dune),
        "one member's, on one item"
    );

    let listed = store
        .find_bookmarks(BookmarkFilter {
            item: Some(not_projected),
            ..BookmarkFilter::default()
        })
        .await
        .expect("read");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].item, not_projected);
    assert_eq!(
        listed[0].item_title, None,
        "an item not projected has no title"
    );
    assert_eq!(listed[0].member_name.as_deref(), Some("alice"));
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
        file_authors: Default::default(),
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
        file_authors: Default::default(),
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
        [
            "bookmarks",
            "file_authors",
            "item_files",
            "items",
            "last_used",
            "members",
            "ratings",
            "reviews",
            "wish_comments",
            "wish_entries"
        ]
    );
    let left: i64 = db
        .query_row("SELECT count(*) FROM items", [], |row| row.get(0))
        .expect("the rows are counted");
    assert_eq!(left, 1, "only the row written since, not the one left over");
}

/// An item with one content file, `blob`, as the projection would write it.
fn with_file(id: ItemId, blob: ContentHash) -> StoredItem {
    let file = FileRecord {
        role: FileRole::Content,
        format: "epub".to_owned(),
        size: 10,
        filename: "book.epub".to_owned(),
        seq: None,
        disc: None,
        title: None,
        duration: None,
    };
    StoredItem {
        item: Item {
            files: BTreeMap::from([(blob, file)]),
            ..Item::new(id)
        },
        ratings: BTreeMap::new(),
        reviews: BTreeMap::new(),
        last_modified: 1,
        file_authors: Default::default(),
    }
}

/// When an item was last used is stamped by item or by any of its blobs, a
/// later stamp replacing an earlier one, and a replay of the item keeps it.
#[tokio::test]
async fn an_items_last_use_is_stamped_by_item_or_by_blob() {
    let store = empty().await;
    let (one, other, unused) = (
        ItemId::from_bytes([1; 32]),
        ItemId::from_bytes([2; 32]),
        ItemId::from_bytes([3; 32]),
    );
    let shared = ContentHash::from_bytes([9; 32]);
    for id in [one, other] {
        store
            .upsert_item(with_file(id, shared))
            .await
            .expect("written");
    }
    store
        .upsert_item(with_file(unused, ContentHash::from_bytes([8; 32])))
        .await
        .expect("written");
    let at = |secs| UNIX_EPOCH + Duration::from_secs(secs);

    store.mark_used(one, at(100)).await.expect("written");
    assert_eq!(
        store.last_used().await.expect("read"),
        BTreeMap::from([(one, at(100))])
    );

    store
        .mark_used_by_blob(shared, at(200))
        .await
        .expect("written");
    store
        .upsert_item(with_file(one, shared))
        .await
        .expect("written");
    assert_eq!(
        store.last_used().await.expect("read"),
        BTreeMap::from([(one, at(200)), (other, at(200))]),
        "every item of the blob, the later stamp, kept through a replay"
    );
}
