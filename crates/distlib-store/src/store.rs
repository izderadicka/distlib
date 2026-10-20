//! The SQLite database itself: opening it, and the rows it holds.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use distlib_core::{
    Bookmark, BookmarkId, Comment, ContentHash, CustodyItem, FileRecord, Item, ItemId, MemberId,
    Rating, Review, Series, Wish, WishId,
};
use rusqlite::{Connection, Row, Statement, Transaction, params, types::Type};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    error::{Result, StoreError},
    schema,
};

/// The file the read model lives in, inside the directory the caller names.
const DATABASE: &str = "read-model.sqlite";

/// What [`Store::upsert_item`] did: wrote a row this read model did not have,
/// or wrote over one it did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Upserted {
    Created,
    Updated,
}

/// One page of a longer answer, and how long the whole answer is.
///
/// The total is what lets a caller show "page 2 of 5", and know when to stop
/// asking, without asking for one more page to find it empty.
#[derive(Debug, Clone, PartialEq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub total: usize,
}

/// One row of `items`, with the `item_files` rows that belong to it.
///
/// Carries an [`Item`] rather than restating its fields: the projection writes
/// one and reads one back, so a column that stopped matching its field fails
/// the round-trip property rather than being found by whatever queried it next.
/// The columns are an encoding of this type, not a second schema.
///
/// With what members said about it, written and read back with it the same
/// way its files are — and left empty by the same reads that leave `files`
/// empty.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredItem {
    pub item: Item,
    /// Each current member's rating.
    pub ratings: BTreeMap<MemberId, Rating>,
    /// Each current member's review.
    pub reviews: BTreeMap<MemberId, Review>,
    /// Who wrote each of the item's file keys, members or not.
    pub file_authors: BTreeMap<ContentHash, BTreeSet<MemberId>>,
    /// The newest entry timestamp this item was projected from — §5.2's
    /// `last_modified`, in microseconds since the epoch.
    pub last_modified: u64,
}

impl StoredItem {
    /// Who added the item: whoever wrote any of its file keys.
    pub fn creators(&self) -> BTreeSet<MemberId> {
        creators_of(&self.file_authors)
    }

    /// The item as custody sees it: its creators, and all of its files' size.
    pub fn custody_item(&self) -> CustodyItem {
        CustodyItem {
            item: self.item.id,
            creators: self.creators().into_iter().collect(),
            size_bytes: self
                .item
                .files
                .values()
                .fold(0, |total, file| total.saturating_add(file.size)),
        }
    }
}

fn creators_of(file_authors: &BTreeMap<ContentHash, BTreeSet<MemberId>>) -> BTreeSet<MemberId> {
    file_authors.values().flatten().copied().collect()
}

/// One row of `bookmarks`: a member's bookmark on an item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredBookmark {
    pub member: MemberId,
    pub id: BookmarkId,
    pub bookmark: Bookmark,
    /// When its entry was last written, in microseconds since the epoch.
    pub last_modified: u64,
}

/// A search over every bookmark: each condition given narrows it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BookmarkFilter {
    /// Text anywhere in the note, the item's title or the member's name.
    pub text: Option<String>,
    pub member: Option<MemberId>,
    pub item: Option<ItemId>,
}

/// A bookmark as a listing shows it: with its item's title and its member's
/// name, either of which the read model may not hold yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedBookmark {
    pub item: ItemId,
    pub item_title: Option<String>,
    pub member_name: Option<String>,
    pub stored: StoredBookmark,
}

/// One wish's rows: each current member's entry and comment. Empty for a
/// wish the read model holds nothing of.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoredWish {
    pub entries: BTreeMap<MemberId, Wish>,
    pub comments: BTreeMap<MemberId, Comment>,
}

/// One row of `members`, projected from the membership log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredMember {
    pub member: MemberId,
    pub display_name: String,
    pub pledge_bytes: u64,
    /// Whether this member votes — a fact about the log's node map rather than
    /// about the member's own record, which is why it is stored beside it.
    pub is_core: bool,
}

/// The read model's database.
///
/// Cheap to clone, and every clone is the same database: one connection behind
/// a lock rather than a pool. SQLite takes a write lock over the whole file
/// anyway, and the projection task is the writer that matters (the others
/// only stamp `last_used`), so a pool would buy
/// concurrent *reads* at the cost of a second path that can create the schema.
/// When 2a-4's queries make that a measurement rather than a guess, this is the
/// one type that has to change.
#[derive(Debug, Clone)]
pub struct Store {
    conn: Arc<Mutex<Connection>>,
}

impl Store {
    /// Opens the read model under `dir`, or in memory when there is no `dir`.
    ///
    /// In memory is not a lesser mode: everything in these tables is derived
    /// from the document and rebuilt from it at every start, so the difference
    /// between the two is how much of that start is already done.
    pub async fn open(dir: Option<PathBuf>) -> Result<Self> {
        tokio::task::spawn_blocking(move || Self::open_blocking(dir.as_deref()))
            .await
            .map_err(StoreError::Stopped)?
    }

    fn open_blocking(dir: Option<&Path>) -> Result<Self> {
        let mut conn = match dir {
            Some(dir) => {
                let failed = |source: Box<dyn std::error::Error + Send + Sync>| StoreError::Open {
                    path: dir.to_path_buf(),
                    source,
                };
                std::fs::create_dir_all(dir).map_err(|source| failed(source.into()))?;
                Connection::open(dir.join(DATABASE)).map_err(|source| failed(source.into()))?
            }
            None => Connection::open_in_memory().map_err(|source| StoreError::Open {
                path: PathBuf::from(":memory:"),
                source: source.into(),
            })?,
        };
        conn.execute_batch(schema::PRAGMAS)
            .map_err(StoreError::sql("set up"))?;
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .map_err(StoreError::sql("asked its version"))?;
        if version != schema::READ_MODEL_VERSION {
            discard_tables(&mut conn).map_err(StoreError::sql("cleared for a new version"))?;
        }
        conn.execute_batch(schema::TABLES)
            .map_err(StoreError::sql("given its tables"))?;
        conn.pragma_update(None, "user_version", schema::READ_MODEL_VERSION)
            .map_err(StoreError::sql("given its version"))?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Writes `stored` over whatever this item's rows held before.
    ///
    /// **A whole-item overwrite, inside one transaction**, its files included:
    /// the input is the item as the document has it *now*, so a file row no
    /// longer in it is a row that must go. That is what makes replaying a
    /// document converge on the same tables however many times it is replayed,
    /// and in whatever order.
    ///
    /// Says whether the item was new here. Asked inside the same transaction
    /// as the write, so two writes of one new item cannot both be told it was
    /// new — and so the answer is about this read model rather than about the
    /// document, which is what a watcher of this node wants to know.
    pub async fn upsert_item(&self, stored: StoredItem) -> Result<Upserted> {
        self.write("written to", move |tx| upsert_item(tx, &stored))
            .await
    }

    /// Replaces `item`'s bookmarks with `bookmarks`.
    pub async fn replace_bookmarks(
        &self,
        item: ItemId,
        bookmarks: Vec<StoredBookmark>,
    ) -> Result<()> {
        self.write("given an item's bookmarks", move |tx| {
            let item = item.to_string();
            BOOKMARKS.replace(tx, &item, &bookmarks, |insert, stored| {
                insert.execute(params![
                    item,
                    stored.member.to_string(),
                    stored.id.to_string(),
                    stored.bookmark.position(),
                    stored.bookmark.note(),
                    stored.bookmark.created_at(),
                    stored.last_modified,
                ])
            })
        })
        .await
    }

    /// `item`'s bookmarks, by member, then id.
    pub async fn bookmarks_of(&self, item: ItemId) -> Result<Vec<StoredBookmark>> {
        self.read(move |conn| {
            let mut statement = conn.prepare(
                "SELECT member, id, position, note, created_at, last_modified \
                 FROM bookmarks WHERE item = ?1 ORDER BY member, id",
            )?;
            statement
                .query_map(params![item.to_string()], |row| stored_bookmark(row, 0))?
                .collect()
        })
        .await
    }

    /// The bookmarks `filter` lets through, newest first. A `LIKE` search,
    /// not the search index: a group's bookmarks are few next to its items
    /// (D13).
    pub async fn find_bookmarks(&self, filter: BookmarkFilter) -> Result<Vec<ListedBookmark>> {
        self.read(move |conn| {
            let mut statement = conn.prepare(
                r"SELECT b.item, i.title, m.display_name,
                         b.member, b.id, b.position, b.note, b.created_at, b.last_modified
                  FROM bookmarks b
                  LEFT JOIN items i ON i.id = b.item
                  LEFT JOIN members m ON m.id = b.member
                  WHERE (?1 IS NULL
                         OR b.note LIKE ?1 ESCAPE '\'
                         OR i.title LIKE ?1 ESCAPE '\'
                         OR m.display_name LIKE ?1 ESCAPE '\')
                    AND (?2 IS NULL OR b.member = ?2)
                    AND (?3 IS NULL OR b.item = ?3)
                  ORDER BY b.created_at DESC, b.item, b.member, b.id",
            )?;
            statement
                .query_map(
                    params![
                        filter.text.as_deref().map(like_anywhere),
                        filter.member.map(|member| member.to_string()),
                        filter.item.map(|item| item.to_string()),
                    ],
                    |row| {
                        Ok(ListedBookmark {
                            item: identifier(row, 0, "item")?,
                            item_title: row.get(1)?,
                            member_name: row.get(2)?,
                            stored: stored_bookmark(row, 3)?,
                        })
                    },
                )?
                .collect()
        })
        .await
    }

    /// Replaces `wish`'s entries and comments with `stored`'s.
    pub async fn replace_wish(&self, wish: WishId, stored: StoredWish) -> Result<()> {
        self.write("given a wish", move |tx| {
            let wish = wish.to_string();
            WISH_ENTRIES.replace(tx, &wish, &stored.entries, |insert, (member, entry)| {
                insert.execute(params![wish, member.to_string(), json(entry)?])
            })?;
            WISH_COMMENTS.replace(tx, &wish, &stored.comments, |insert, (member, comment)| {
                insert.execute(params![wish, member.to_string(), comment.as_str()])
            })
        })
        .await
    }

    /// `wish`'s entries and comments.
    pub async fn wish(&self, wish: WishId) -> Result<StoredWish> {
        self.read(move |conn| {
            let wish = wish.to_string();
            let entries = conn
                .prepare("SELECT member, entry FROM wish_entries WHERE wish = ?1")?
                .query_map(params![wish], |row| {
                    Ok((identifier(row, 0, "member")?, unjson(row, 1, "entry")?))
                })?
                .collect::<rusqlite::Result<_>>()?;
            let comments = conn
                .prepare("SELECT member, comment FROM wish_comments WHERE wish = ?1")?
                .query_map(params![wish], |row| {
                    let comment = Comment::try_from(row.get::<_, String>(1)?)
                        .map_err(unreadable(1, "comment"))?;
                    Ok((identifier(row, 0, "member")?, comment))
                })?
                .collect::<rusqlite::Result<_>>()?;
            Ok(StoredWish { entries, comments })
        })
        .await
    }

    /// Every wish the read model holds something of, by id.
    pub async fn all_wishes(&self) -> Result<BTreeMap<WishId, StoredWish>> {
        self.read(|conn| {
            let mut wishes: BTreeMap<WishId, StoredWish> = BTreeMap::new();
            let mut entries = conn.prepare("SELECT wish, member, entry FROM wish_entries")?;
            let mut rows = entries.query([])?;
            while let Some(row) = rows.next()? {
                wishes
                    .entry(identifier(row, 0, "wish")?)
                    .or_default()
                    .entries
                    .insert(identifier(row, 1, "member")?, unjson(row, 2, "entry")?);
            }
            let mut comments = conn.prepare("SELECT wish, member, comment FROM wish_comments")?;
            let mut rows = comments.query([])?;
            while let Some(row) = rows.next()? {
                let comment = Comment::try_from(row.get::<_, String>(2)?)
                    .map_err(unreadable(2, "comment"))?;
                wishes
                    .entry(identifier(row, 0, "wish")?)
                    .or_default()
                    .comments
                    .insert(identifier(row, 1, "member")?, comment);
            }
            Ok(wishes)
        })
        .await
    }

    /// Replaces the `members` table with `members`.
    ///
    /// Replaced rather than merged, for the same reason the projection is a
    /// re-read: the membership state is delivered whole and says who *is* a
    /// member, so anybody missing from it has been expelled.
    pub async fn set_members(&self, members: Vec<StoredMember>) -> Result<()> {
        self.write("given the group's members", move |tx| {
            tx.execute("DELETE FROM members", [])?;
            let mut insert = tx.prepare(
                "INSERT INTO members (id, display_name, pledge_bytes, is_core) \
                 VALUES (?1, ?2, ?3, ?4)",
            )?;
            for member in &members {
                insert.execute(params![
                    member.member.to_string(),
                    member.display_name,
                    member.pledge_bytes,
                    member.is_core,
                ])?;
            }
            Ok(())
        })
        .await
    }

    /// Records that this node used `item` at `at`.
    pub async fn mark_used(&self, item: ItemId, at: SystemTime) -> Result<()> {
        self.write("told an item was used", move |tx| {
            tx.execute(
                "INSERT INTO last_used (item, used_at) VALUES (?1, ?2) \
                 ON CONFLICT (item) DO UPDATE SET used_at = excluded.used_at",
                params![item.to_string(), millis_since_epoch(at)],
            )
            .map(|_| ())
        })
        .await
    }

    /// Records that this node used every item `blob` is a file of at `at`.
    pub async fn mark_used_by_blob(&self, blob: ContentHash, at: SystemTime) -> Result<()> {
        self.write("told a blob was used", move |tx| {
            tx.execute(
                "INSERT INTO last_used (item, used_at) \
                 SELECT item, ?2 FROM item_files WHERE blob = ?1 \
                 ON CONFLICT (item) DO UPDATE SET used_at = excluded.used_at",
                params![blob.to_string(), millis_since_epoch(at)],
            )
            .map(|_| ())
        })
        .await
    }

    /// When this node last used each item it has used.
    pub async fn last_used(&self) -> Result<BTreeMap<ItemId, SystemTime>> {
        self.read(|conn| {
            conn.prepare("SELECT item, used_at FROM last_used")?
                .query_map([], |row| {
                    let millis: u64 = row.get(1)?;
                    Ok((
                        identifier(row, 0, "item")?,
                        UNIX_EPOCH + Duration::from_millis(millis),
                    ))
                })?
                .collect()
        })
        .await
    }

    /// One item, or `None` if the projection has not written it.
    pub async fn item(&self, id: ItemId) -> Result<Option<StoredItem>> {
        self.read(move |conn| {
            let mut details = ItemDetails::read(conn, Some(id))?;
            let mut statement =
                conn.prepare(&format!("SELECT {ITEM_COLUMNS} FROM items WHERE id = ?1"))?;
            let mut rows = statement.query(params![id.to_string()])?;
            let Some(row) = rows.next()? else {
                return Ok(None);
            };
            let mut stored = row_to_item(row)?;
            details.attach_to(&mut stored);
            Ok(Some(stored))
        })
        .await
    }

    /// Every item as custody sees it, as [`StoredItem::custody_item`] would
    /// give it, without reading the rest of each item.
    pub async fn custody_items(&self) -> Result<Vec<CustodyItem>> {
        self.read(|conn| {
            let mut file_authors = file_authors_of(conn, None)?;
            // `TOTAL` rather than `SUM`, which fails on an overflow: one absurd
            // size written by one member must not stop every node's custody.
            let mut statement = conn.prepare(
                "SELECT items.id, TOTAL(item_files.size) FROM items \
                 LEFT JOIN item_files ON item_files.item = items.id \
                 GROUP BY items.id ORDER BY items.id",
            )?;
            let mut rows = statement.query([])?;
            let mut items = Vec::new();
            while let Some(row) = rows.next()? {
                let item: ItemId = identifier(row, 0, "id")?;
                let creators = file_authors
                    .remove(&item)
                    .map(|authors| creators_of(&authors).into_iter().collect())
                    .unwrap_or_default();
                items.push(CustodyItem {
                    item,
                    creators,
                    // Saturating, as the sum in `custody_item` is.
                    size_bytes: row.get::<_, f64>(1)? as u64,
                });
            }
            Ok(items)
        })
        .await
    }

    /// One item's own fields, `files`, `ratings` and `reviews` always empty.
    ///
    /// [`Store::item`] minus the queries that read an item's details — its
    /// files, ratings and reviews — for a caller that is not going to look
    /// at them anyway. `library.search` is the reason this exists: a page of
    /// hits reads this once per hit, and `library.item` is the one place a
    /// caller reads the rest, so paying for it here would be work every search
    /// result throws away.
    pub async fn item_fields(&self, id: ItemId) -> Result<Option<StoredItem>> {
        self.read(move |conn| {
            let mut statement =
                conn.prepare(&format!("SELECT {ITEM_COLUMNS} FROM items WHERE id = ?1"))?;
            let mut rows = statement.query(params![id.to_string()])?;
            let Some(row) = rows.next()? else {
                return Ok(None);
            };
            row_to_item(row).map(Some)
        })
        .await
    }

    /// Every item the projection has written, by id.
    ///
    /// One query per table rather than one per item: each table's rows are
    /// fetched in a single pass and grouped here, so this costs O(rows)
    /// rather than O(items) round trips through SQLite.
    pub async fn items(&self) -> Result<Vec<StoredItem>> {
        self.read(|conn| {
            let mut details = ItemDetails::read(conn, None)?;
            let mut statement =
                conn.prepare(&format!("SELECT {ITEM_COLUMNS} FROM items ORDER BY id"))?;
            let mut rows = statement.query([])?;
            let mut items = Vec::new();
            while let Some(row) = rows.next()? {
                let mut stored = row_to_item(row)?;
                details.attach_to(&mut stored);
                items.push(stored);
            }
            Ok(items)
        })
        .await
    }

    /// One page of items, `files`, `ratings` and `reviews` always empty, in
    /// browsing order: by title, ignoring case, untitled last, and by id among
    /// equal titles — so that the order is total and pages neither overlap nor
    /// leave a gap.
    ///
    /// **Case is folded for ASCII only** — SQLite's `NOCASE` — so a title
    /// starting with an accented letter sorts after `Z`. A locale-aware
    /// collation is a dependency this has not yet needed.
    ///
    /// The count and the page are read under one lock, so they agree with
    /// each other. Between two pages the read model may change — an offset is
    /// a position, and an item added before it moves everything after along
    /// by one — which is why a page that hears `catalogue.item_added`
    /// refetches rather than trusting the page it already holds.
    pub async fn page(&self, offset: usize, limit: usize) -> Result<Page<StoredItem>> {
        // SQLite's integers are signed; past `i64::MAX` is past the end anyway.
        let offset = i64::try_from(offset).unwrap_or(i64::MAX);
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.read(move |conn| {
            let total = conn.query_row("SELECT COUNT(*) FROM items", [], |row| row.get(0))?;
            let mut statement = conn.prepare(&format!(
                "SELECT {ITEM_COLUMNS} FROM items
                 ORDER BY title IS NULL, title COLLATE NOCASE, id
                 LIMIT ?1 OFFSET ?2"
            ))?;
            let items = statement
                .query_map(params![limit, offset], row_to_item)?
                .collect::<rusqlite::Result<_>>()?;
            Ok(Page { items, total })
        })
        .await
    }

    /// Every member the projection has written, by id.
    pub async fn members(&self) -> Result<Vec<StoredMember>> {
        self.read(|conn| {
            let mut statement = conn.prepare(
                "SELECT id, display_name, pledge_bytes, is_core FROM members ORDER BY id",
            )?;
            let mut rows = statement.query([])?;
            let mut members = Vec::new();
            while let Some(row) = rows.next()? {
                members.push(StoredMember {
                    member: identifier(row, 0, "id")?,
                    display_name: row.get(1)?,
                    pledge_bytes: row.get(2)?,
                    is_core: row.get(3)?,
                });
            }
            Ok(members)
        })
        .await
    }

    /// Runs `work` in a transaction on the blocking pool.
    ///
    /// On the pool for the reason redb is: SQLite's commit reaches the disk
    /// synchronously, and a fsync on an async worker stalls every other task
    /// that worker was going to poll.
    async fn write<T, F>(&self, doing: &'static str, work: F) -> Result<T>
    where
        F: FnOnce(&Transaction<'_>) -> rusqlite::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let mut conn = conn.lock().unwrap_or_else(PoisonError::into_inner);
            let tx = conn.transaction()?;
            let done = work(&tx)?;
            tx.commit()?;
            Ok(done)
        })
        .await
        .map_err(StoreError::Stopped)?
        .map_err(StoreError::sql(doing))
    }

    /// Runs `work` on the blocking pool, outside a transaction.
    async fn read<T, F>(&self, work: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> rusqlite::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap_or_else(PoisonError::into_inner);
            work(&conn)
        })
        .await
        .map_err(StoreError::Stopped)?
        .map_err(StoreError::sql("read from"))
    }
}

/// The `items` columns, in the order [`row_to_item`] reads them.
///
/// One list, used by every query that reads them, because the two halves are a
/// positional agreement: a column added to one and not the other reads the
/// wrong value out of the right row, and nothing would catch it.
const ITEM_COLUMNS: &str = "id, kind, title, authors, genres, series, series_index, \
                            year, lang, description, last_modified";

/// The `item_files` columns, in the order [`files_of`] reads them.
const FILE_COLUMNS: &[&str] = &[
    "item", "blob", "role", "format", "size", "filename", "seq", "disc", "title", "duration",
];

/// A table whose rows belong to one key, in its first column: an item's
/// files, ratings, reviews and bookmarks, a wish's entries and comments.
///
/// They are replaced together, never merged. A row the projection no longer
/// gives, such as an expelled member's rating or a file no longer readable,
/// goes, so the table never depends on what was projected into it before.
struct OwnedRows {
    table: &'static str,
    /// The key first.
    columns: &'static [&'static str],
}

const FILES: OwnedRows = OwnedRows {
    table: "item_files",
    columns: FILE_COLUMNS,
};
const FILE_AUTHORS: OwnedRows = OwnedRows {
    table: "file_authors",
    columns: &["item", "blob", "member"],
};
const RATINGS: OwnedRows = OwnedRows {
    table: "ratings",
    columns: &["item", "member", "rating"],
};
const REVIEWS: OwnedRows = OwnedRows {
    table: "reviews",
    columns: &["item", "member", "review"],
};
const BOOKMARKS: OwnedRows = OwnedRows {
    table: "bookmarks",
    columns: &[
        "item",
        "member",
        "id",
        "position",
        "note",
        "created_at",
        "last_modified",
    ],
};
const WISH_ENTRIES: OwnedRows = OwnedRows {
    table: "wish_entries",
    columns: &["wish", "member", "entry"],
};
const WISH_COMMENTS: OwnedRows = OwnedRows {
    table: "wish_comments",
    columns: &["wish", "member", "comment"],
};

impl OwnedRows {
    /// Deletes `key`'s rows, then inserts one for each of `rows` through
    /// `insert`, given the statement and the row.
    fn replace<T>(
        &self,
        tx: &Transaction<'_>,
        key: &str,
        rows: impl IntoIterator<Item = T>,
        mut insert: impl FnMut(&mut Statement<'_>, T) -> rusqlite::Result<usize>,
    ) -> rusqlite::Result<()> {
        let Self { table, columns } = self;
        let key_column = columns
            .first()
            .expect("an OwnedRows names its columns, the key first");
        tx.execute(
            &format!("DELETE FROM {table} WHERE {key_column} = ?1"),
            params![key],
        )?;
        let placeholders: Vec<String> = (1..=columns.len())
            .map(|number| format!("?{number}"))
            .collect();
        let mut statement = tx.prepare(&format!(
            "INSERT INTO {table} ({}) VALUES ({})",
            columns.join(", "),
            placeholders.join(", ")
        ))?;
        for row in rows {
            insert(&mut statement, row)?;
        }
        Ok(())
    }
}

/// Drops every table a read model of another version left behind.
///
/// Every table, not the ones this build knows: a read model written by a newer
/// build can hold tables this one has never heard of. Equivalent to deleting the
/// file and simpler — no open file to remove on Windows, and the same for a
/// database in memory. Foreign keys are off while it runs, so the order the
/// tables go in does not matter; SQLite ignores that pragma inside a
/// transaction, which is why it is set around one rather than in it.
fn discard_tables(conn: &mut Connection) -> rusqlite::Result<()> {
    conn.pragma_update(None, "foreign_keys", false)?;
    let tx = conn.transaction()?;
    let tables: Vec<String> = tx
        .prepare(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
        )?
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    for table in tables {
        tx.execute_batch(&format!("DROP TABLE \"{}\"", table.replace('"', "\"\"")))?;
    }
    tx.commit()?;
    conn.pragma_update(None, "foreign_keys", true)
}

fn upsert_item(tx: &Transaction<'_>, stored: &StoredItem) -> rusqlite::Result<Upserted> {
    let item = &stored.item;
    let id = item.id.to_string();
    let existed: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM items WHERE id = ?1)",
        params![id],
        |row| row.get(0),
    )?;
    tx.execute(
        "INSERT INTO items (id, kind, title, authors, genres, series, series_index, \
                            year, lang, description, last_modified) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) \
         ON CONFLICT(id) DO UPDATE SET \
             kind = excluded.kind, title = excluded.title, authors = excluded.authors, \
             genres = excluded.genres, series = excluded.series, \
             series_index = excluded.series_index, year = excluded.year, \
             lang = excluded.lang, description = excluded.description, \
             last_modified = excluded.last_modified",
        params![
            id,
            item.kind.as_ref().map(word).transpose()?,
            item.title,
            item.authors.as_ref().map(json).transpose()?,
            item.genres.as_ref().map(json).transpose()?,
            item.series.as_ref().map(|series| &series.name),
            item.series.as_ref().and_then(|series| series.index),
            item.year,
            item.lang,
            item.description,
            stored.last_modified,
        ],
    )?;

    FILES.replace(tx, &id, &item.files, |insert, (blob, file)| {
        insert.execute(params![
            id,
            blob.to_string(),
            word(&file.role)?,
            file.format,
            file.size,
            file.filename,
            file.seq,
            file.disc,
            file.title,
            file.duration,
        ])
    })?;
    let authorship = stored
        .file_authors
        .iter()
        .flat_map(|(blob, authors)| authors.iter().map(move |author| (blob, author)));
    FILE_AUTHORS.replace(tx, &id, authorship, |insert, (blob, author)| {
        insert.execute(params![id, blob.to_string(), author.to_string()])
    })?;
    RATINGS.replace(tx, &id, &stored.ratings, |insert, (member, rating)| {
        insert.execute(params![id, member.to_string(), rating.get()])
    })?;
    REVIEWS.replace(tx, &id, &stored.reviews, |insert, (member, review)| {
        insert.execute(params![id, member.to_string(), review.as_str()])
    })?;

    Ok(if existed {
        Upserted::Updated
    } else {
        Upserted::Created
    })
}

/// Every file row, or just one item's, grouped by the item it belongs to.
fn files_of(
    conn: &Connection,
    only: Option<ItemId>,
) -> rusqlite::Result<BTreeMap<ItemId, BTreeMap<ContentHash, FileRecord>>> {
    let (sql, params) = select_item_rows(
        &format!("{} FROM item_files", FILE_COLUMNS.join(", ")),
        only,
    );
    let mut statement = conn.prepare(&sql)?;
    let mut rows = statement.query(rusqlite::params_from_iter(params))?;

    let mut grouped: BTreeMap<ItemId, BTreeMap<ContentHash, FileRecord>> = BTreeMap::new();
    while let Some(row) = rows.next()? {
        let record = FileRecord {
            role: from_word(row, 2, "role")?,
            format: row.get(3)?,
            size: row.get(4)?,
            filename: row.get(5)?,
            seq: row.get(6)?,
            disc: row.get(7)?,
            title: row.get(8)?,
            duration: row.get(9)?,
        };
        grouped
            .entry(identifier(row, 0, "item")?)
            .or_default()
            .insert(identifier(row, 1, "blob")?, record);
    }
    Ok(grouped)
}

/// An item's details: the rows in other tables that belong to it — its files,
/// ratings and reviews. Read for every item, or for just one, in one query
/// per table, and handed to each item as it is read.
struct ItemDetails {
    files: BTreeMap<ItemId, BTreeMap<ContentHash, FileRecord>>,
    file_authors: BTreeMap<ItemId, BTreeMap<ContentHash, BTreeSet<MemberId>>>,
    ratings: BTreeMap<ItemId, BTreeMap<MemberId, Rating>>,
    reviews: BTreeMap<ItemId, BTreeMap<MemberId, Review>>,
}

impl ItemDetails {
    fn read(conn: &Connection, only: Option<ItemId>) -> rusqlite::Result<Self> {
        Ok(Self {
            files: files_of(conn, only)?,
            file_authors: file_authors_of(conn, only)?,
            ratings: member_values_by_item(conn, "ratings", "rating", only, |row| {
                Rating::try_from(row.get::<_, u8>(2)?).map_err(unreadable(2, "rating"))
            })?,
            reviews: member_values_by_item(conn, "reviews", "review", only, |row| {
                Review::try_from(row.get::<_, String>(2)?).map_err(unreadable(2, "review"))
            })?,
        })
    }

    /// Moves `stored`'s details onto it.
    fn attach_to(&mut self, stored: &mut StoredItem) {
        let id = stored.item.id;
        stored.item.files = self.files.remove(&id).unwrap_or_default();
        stored.file_authors = self.file_authors.remove(&id).unwrap_or_default();
        stored.ratings = self.ratings.remove(&id).unwrap_or_default();
        stored.reviews = self.reviews.remove(&id).unwrap_or_default();
    }
}

/// Every file key's authors, or just one item's, grouped by item, then blob.
fn file_authors_of(
    conn: &Connection,
    only: Option<ItemId>,
) -> rusqlite::Result<BTreeMap<ItemId, BTreeMap<ContentHash, BTreeSet<MemberId>>>> {
    let (sql, params) = select_item_rows("item, blob, member FROM file_authors", only);
    let mut statement = conn.prepare(&sql)?;
    let mut rows = statement.query(rusqlite::params_from_iter(params))?;

    let mut grouped: BTreeMap<ItemId, BTreeMap<ContentHash, BTreeSet<MemberId>>> = BTreeMap::new();
    while let Some(row) = rows.next()? {
        grouped
            .entry(identifier(row, 0, "item")?)
            .or_default()
            .entry(identifier(row, 1, "blob")?)
            .or_default()
            .insert(identifier(row, 2, "member")?);
    }
    Ok(grouped)
}

/// The `column` of every row of `table` — or of just one item's rows —
/// grouped by item, then by member: for tables holding one value per member
/// per item, as `ratings` and `reviews` do.
fn member_values_by_item<T>(
    conn: &Connection,
    table: &str,
    column: &str,
    only: Option<ItemId>,
    value: impl Fn(&Row<'_>) -> rusqlite::Result<T>,
) -> rusqlite::Result<BTreeMap<ItemId, BTreeMap<MemberId, T>>> {
    let (sql, params) = select_item_rows(&format!("item, member, {column} FROM {table}"), only);
    let mut statement = conn.prepare(&sql)?;
    let mut rows = statement.query(rusqlite::params_from_iter(params))?;

    let mut grouped: BTreeMap<ItemId, BTreeMap<MemberId, T>> = BTreeMap::new();
    while let Some(row) = rows.next()? {
        grouped
            .entry(identifier(row, 0, "item")?)
            .or_default()
            .insert(identifier(row, 1, "member")?, value(row)?);
    }
    Ok(grouped)
}

/// `SELECT {columns_from}` over every item's rows, or only one item's, with
/// the parameters it takes.
fn select_item_rows(columns_from: &str, only: Option<ItemId>) -> (String, Vec<String>) {
    match only {
        Some(id) => (
            format!("SELECT {columns_from} WHERE item = ?1"),
            vec![id.to_string()],
        ),
        None => (format!("SELECT {columns_from}"), Vec::new()),
    }
}

/// One `items` row, without its files — the caller attaches those.
fn row_to_item(row: &Row<'_>) -> rusqlite::Result<StoredItem> {
    Ok(StoredItem {
        item: Item {
            id: identifier(row, 0, "id")?,
            kind: row
                .get::<_, Option<String>>(1)?
                .map(|_| from_word(row, 1, "kind"))
                .transpose()?,
            title: row.get(2)?,
            authors: row
                .get::<_, Option<String>>(3)?
                .map(|_| unjson(row, 3, "authors"))
                .transpose()?,
            genres: row
                .get::<_, Option<String>>(4)?
                .map(|_| unjson(row, 4, "genres"))
                .transpose()?,
            series: row.get::<_, Option<String>>(5)?.map(|name| Series {
                name,
                index: row
                    .get::<_, Option<f64>>(6)
                    .ok()
                    .flatten()
                    .map(|index| index as f32),
            }),
            year: row.get(7)?,
            lang: row.get(8)?,
            description: row.get(9)?,
            files: BTreeMap::new(),
        },
        ratings: BTreeMap::new(),
        reviews: BTreeMap::new(),
        last_modified: row.get(10)?,
        file_authors: BTreeMap::new(),
    })
}

/// A bookmark read from six columns starting at `first`: member, id,
/// position, note, created_at and last_modified.
fn stored_bookmark(row: &Row<'_>, first: usize) -> rusqlite::Result<StoredBookmark> {
    Ok(StoredBookmark {
        member: identifier(row, first, "member")?,
        id: identifier(row, first + 1, "id")?,
        bookmark: Bookmark::new(
            row.get(first + 2)?,
            row.get(first + 3)?,
            row.get(first + 4)?,
        )
        .map_err(unreadable(first + 2, "bookmark"))?,
        last_modified: row.get(first + 5)?,
    })
}

/// `text` as a `LIKE` pattern matching it anywhere, with its own `%`, `_`
/// and `\` taken literally.
fn like_anywhere(text: &str) -> String {
    let escaped = text
        .replace('\\', r"\\")
        .replace('%', r"\%")
        .replace('_', r"\_");
    format!("%{escaped}%")
}

/// `at` as `last_used` stores it; a time before the epoch as the epoch.
fn millis_since_epoch(at: SystemTime) -> u64 {
    let millis = at
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

/// Reads one of the catalogue's identifiers back out of a text column.
fn identifier<T>(row: &Row<'_>, index: usize, column: &'static str) -> rusqlite::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    row.get::<_, String>(index)?
        .parse()
        .map_err(unreadable(index, column))
}

/// A `#[serde(rename_all = "lowercase")]` enum as the one word it becomes.
///
/// Through serde rather than a table written out here, so the column holds
/// exactly the word the catalogue's own JSON holds for the same value. A second
/// spelling of `audiobook` is the kind of thing that works until somebody
/// queries both.
fn word<T: Serialize>(value: &T) -> rusqlite::Result<String> {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(word)) => Ok(word),
        other => Err(rusqlite::Error::ToSqlConversionFailure(
            format!("expected a one-word value, got {other:?}").into(),
        )),
    }
}

fn from_word<T: DeserializeOwned>(
    row: &Row<'_>,
    index: usize,
    column: &'static str,
) -> rusqlite::Result<T> {
    let word = row.get::<_, String>(index)?;
    serde_json::from_value(serde_json::Value::String(word)).map_err(unreadable(index, column))
}

/// A list field as the JSON the catalogue already writes for it.
fn json<T: Serialize>(value: &T) -> rusqlite::Result<String> {
    serde_json::to_string(value)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(error.into()))
}

fn unjson<T: DeserializeOwned>(
    row: &Row<'_>,
    index: usize,
    column: &'static str,
) -> rusqlite::Result<T> {
    serde_json::from_str(&row.get::<_, String>(index)?).map_err(unreadable(index, column))
}

/// Reports a column this build cannot read back, naming it.
///
/// Every value in these tables was written by the projection, so one it cannot
/// read is a bug in the encoding rather than a database that went wrong — which
/// is why the column name is worth carrying all the way out.
fn unreadable<E: std::error::Error + Send + Sync + 'static>(
    index: usize,
    column: &'static str,
) -> impl Fn(E) -> rusqlite::Error {
    move |source| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            Type::Text,
            format!("the read model's {column} column: {source}").into(),
        )
    }
}
