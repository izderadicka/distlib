//! The SQLite database itself: opening it, and the rows it holds.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, PoisonError},
};

use distlib_core::{ContentHash, FileRecord, Item, ItemId, MemberId, Series};
use rusqlite::{Connection, Row, Transaction, params, types::Type};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    error::{Result, StoreError},
    schema,
};

/// The file the read model lives in, inside the directory the caller names.
const DATABASE: &str = "read-model.sqlite";

/// One row of `items`, with the `item_files` rows that belong to it.
///
/// Carries an [`Item`] rather than restating its fields: the projection writes
/// one and reads one back, so a column that stopped matching its field fails
/// the round-trip property rather than being found by whatever queried it next.
/// The columns are an encoding of this type, not a second schema.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredItem {
    pub item: Item,
    /// The newest entry timestamp this item was projected from — §5.2's
    /// `last_modified`, in microseconds since the epoch.
    pub last_modified: u64,
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
/// anyway and the only writer is the projection task, so a pool would buy
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
        let conn = match dir {
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
        conn.execute_batch(schema::TABLES)
            .map_err(StoreError::sql("given its tables"))?;
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
    pub async fn upsert_item(&self, stored: StoredItem) -> Result<()> {
        self.write("written to", move |tx| upsert_item(tx, &stored))
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

    /// One item, or `None` if the projection has not written it.
    pub async fn item(&self, id: ItemId) -> Result<Option<StoredItem>> {
        self.read(move |conn| {
            let mut files = files_of(conn, Some(id))?;
            let mut statement =
                conn.prepare(&format!("SELECT {ITEM_COLUMNS} FROM items WHERE id = ?1"))?;
            let mut rows = statement.query(params![id.to_string()])?;
            let Some(row) = rows.next()? else {
                return Ok(None);
            };
            let mut stored = row_to_item(row)?;
            stored.item.files = files.remove(&stored.item.id).unwrap_or_default();
            Ok(Some(stored))
        })
        .await
    }

    /// Every item the projection has written, by id.
    ///
    /// Two queries rather than one per item: the file rows are fetched in a
    /// single pass and grouped here, so this costs O(rows) rather than O(items)
    /// round trips through SQLite.
    pub async fn items(&self) -> Result<Vec<StoredItem>> {
        self.read(|conn| {
            let mut files = files_of(conn, None)?;
            let mut statement =
                conn.prepare(&format!("SELECT {ITEM_COLUMNS} FROM items ORDER BY id"))?;
            let mut rows = statement.query([])?;
            let mut items = Vec::new();
            while let Some(row) = rows.next()? {
                let mut stored = row_to_item(row)?;
                stored.item.files = files.remove(&stored.item.id).unwrap_or_default();
                items.push(stored);
            }
            Ok(items)
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
    async fn write<F>(&self, doing: &'static str, work: F) -> Result<()>
    where
        F: FnOnce(&Transaction<'_>) -> rusqlite::Result<()> + Send + 'static,
    {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let mut conn = conn.lock().unwrap_or_else(PoisonError::into_inner);
            let tx = conn.transaction()?;
            work(&tx)?;
            tx.commit()
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
                            year, lang, description, replicas, last_modified";

/// The `item_files` columns, in the order [`files_of`] reads them.
const FILE_COLUMNS: &str = "item, blob, role, format, size, filename, seq, disc, title, duration";

fn upsert_item(tx: &Transaction<'_>, stored: &StoredItem) -> rusqlite::Result<()> {
    let item = &stored.item;
    let id = item.id.to_string();
    tx.execute(
        "INSERT INTO items (id, kind, title, authors, genres, series, series_index, \
                            year, lang, description, replicas, last_modified) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12) \
         ON CONFLICT(id) DO UPDATE SET \
             kind = excluded.kind, title = excluded.title, authors = excluded.authors, \
             genres = excluded.genres, series = excluded.series, \
             series_index = excluded.series_index, year = excluded.year, \
             lang = excluded.lang, description = excluded.description, \
             replicas = excluded.replicas, last_modified = excluded.last_modified",
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
            item.replicas,
            stored.last_modified,
        ],
    )?;

    // Cleared rather than merged: a file whose record is no longer readable is
    // a file this node can no longer offer, and leaving the row behind would
    // make the tables depend on what was projected into them before.
    tx.execute("DELETE FROM item_files WHERE item = ?1", params![id])?;
    let mut insert = tx.prepare(&format!(
        "INSERT INTO item_files ({FILE_COLUMNS}) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)"
    ))?;
    for (blob, file) in &item.files {
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
        ])?;
    }
    Ok(())
}

/// Every file row, or just one item's, grouped by the item it belongs to.
fn files_of(
    conn: &Connection,
    only: Option<ItemId>,
) -> rusqlite::Result<BTreeMap<ItemId, BTreeMap<ContentHash, FileRecord>>> {
    let (sql, params) = match &only {
        Some(id) => (
            format!("SELECT {FILE_COLUMNS} FROM item_files WHERE item = ?1"),
            vec![id.to_string()],
        ),
        None => (format!("SELECT {FILE_COLUMNS} FROM item_files"), Vec::new()),
    };
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
            replicas: row.get(10)?,
            files: BTreeMap::new(),
        },
        last_modified: row.get(11)?,
    })
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
