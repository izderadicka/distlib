//! The tantivy half of §5.4's read model: `title, authors, description,
//! genres, series`, searchable with per-field boosts.
//!
//! **SQLite answers `item`; this answers `search`.** [`crate::store::Store`]
//! is the record — every column §5.2 names, queryable by id. This is a
//! ranking over five of those columns, and it is deliberately not a second
//! copy of them: a hit carries only the item id, and the caller reads the
//! item back out of the store the same way `library.item` would. That is what
//! keeps content in one place — a title that changed would otherwise have to
//! change here too, and a schema with two sources of truth is a schema that
//! disagrees with itself eventually.
//!
//! **Kept in step the same way the tables are**: [`crate::projection`] calls
//! [`SearchIndex::index_item`] wherever it calls `Store::upsert_item`, on the
//! same whole-item re-read. There is no delta path here either, for the same
//! reason store.rs gives: an item's document has already decided what its
//! fields are, so indexing it is overwriting a document, not applying one.
//!
//! **`admin.reindex`, a start, and a restart are one operation**, per P2-19 —
//! this crate does not add a second way to rebuild the index. What is added
//! here is a way to *ask* the running projection task to run that operation
//! again; see [`crate::projection::Projection::reindex`].

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, PoisonError},
};

use distlib_core::{Item, ItemId};
use tantivy::{
    Index as TantivyIndex, IndexReader, IndexWriter, ReloadPolicy, Term,
    collector::TopDocs,
    directory::MmapDirectory,
    query::QueryParser,
    schema::{Field, STORED, STRING, Schema, TEXT, TantivyDocument, Value},
};

use crate::error::{Result, StoreError};

/// tantivy refuses less than this per indexing thread; see
/// `tantivy::indexer::index_writer::MEMORY_BUDGET_NUM_BYTES_MIN`, not
/// re-exported at the crate root. One thread, because — like [`Store`]'s one
/// connection — there is exactly one writer, the projection task, and a
/// second thread would only let tantivy parallelise merges this corpus is
/// too small to need.
///
/// [`Store`]: crate::store::Store
const WRITER_MEMORY_BYTES: usize = 15_000_000;

/// §5.4 asks for "per-field boosts" and does not say what they are. **These
/// are a starting guess, not a measurement** — nobody has run a query against
/// a real catalogue yet — and the numbers here are picked on one intuition
/// only: a hit on what an item *is called* should outrank a hit buried in its
/// blurb. Title highest because it is what a member actually remembers;
/// authors and series next because both are exact, high-signal names rather
/// than prose; genres above description because a genre match is a
/// controlled vocabulary word, not an incidental mention. Revisit once a real
/// group's searches make this an argument with evidence in it instead of one
/// without.
const TITLE_BOOST: f32 = 3.0;
const AUTHORS_BOOST: f32 = 2.0;
const SERIES_BOOST: f32 = 1.5;
const GENRES_BOOST: f32 = 1.25;
const DESCRIPTION_BOOST: f32 = 1.0;

/// The tantivy fields §5.4 names, minus `reviews` — phase 4/5 owns that table
/// and nothing writes it yet, the same argument schema.rs makes for the SQL
/// side having three tables and not eight.
#[derive(Debug, Clone, Copy)]
struct Fields {
    /// `STRING` (untokenized) and `STORED`: this is the one field a hit is
    /// read back by, and it is matched whole — never a search term itself.
    id: Field,
    title: Field,
    authors: Field,
    genres: Field,
    series: Field,
    description: Field,
}

fn schema() -> (Schema, Fields) {
    let mut builder = Schema::builder();
    let fields = Fields {
        id: builder.add_text_field("id", STRING | STORED),
        title: builder.add_text_field("title", TEXT),
        authors: builder.add_text_field("authors", TEXT),
        genres: builder.add_text_field("genres", TEXT),
        series: builder.add_text_field("series", TEXT),
        description: builder.add_text_field("description", TEXT),
    };
    (builder.build(), fields)
}

/// The read model's search index.
///
/// Cheap to clone, and every clone is the same index — one writer behind a
/// lock, mirroring [`Store`](crate::store::Store) for the same reason: the
/// projection task is the only writer there will ever be, so a pool would buy
/// nothing.
#[derive(Clone)]
pub struct SearchIndex {
    index: TantivyIndex,
    reader: IndexReader,
    /// `None` after [`Self::close`] — see there for why a shared handle needs
    /// an explicit close rather than relying on the last clone's `Drop`.
    writer: Arc<Mutex<Option<IndexWriter>>>,
    fields: Fields,
}

// Neither `IndexReader` nor `IndexWriter` implements `Debug`, so this names
// the type and stops there rather than reaching into tantivy's internals for
// a derive it does not offer.
impl std::fmt::Debug for SearchIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SearchIndex")
    }
}

impl SearchIndex {
    /// Opens the search index under `dir`, or in memory when there is no
    /// `dir` — the same contract as `Store::open`.
    pub async fn open(dir: Option<PathBuf>) -> Result<Self> {
        tokio::task::spawn_blocking(move || Self::open_blocking(dir.as_deref()))
            .await
            .map_err(StoreError::Stopped)?
    }

    fn open_blocking(dir: Option<&Path>) -> Result<Self> {
        let (schema, fields) = schema();
        let index = match dir {
            Some(dir) => {
                let failed = |source: Box<dyn std::error::Error + Send + Sync>| StoreError::Open {
                    path: dir.to_path_buf(),
                    source,
                };
                std::fs::create_dir_all(dir).map_err(|source| failed(source.into()))?;
                let directory = MmapDirectory::open(dir).map_err(|source| failed(source.into()))?;
                TantivyIndex::open_or_create(directory, schema)
                    .map_err(|source| failed(source.into()))?
            }
            None => TantivyIndex::create_in_ram(schema),
        };
        // `Manual` rather than the default `OnCommitWithDelay`: `commit`
        // below reloads explicitly, on its own caller's schedule, so a second,
        // background reload racing it would only make "when is a write
        // visible" a question with two answers instead of one.
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(StoreError::index("opened for reading"))?;
        let writer = index
            .writer_with_num_threads(1, WRITER_MEMORY_BYTES)
            .map_err(StoreError::index("opened for writing"))?;
        Ok(Self {
            index,
            reader,
            writer: Arc::new(Mutex::new(Some(writer))),
            fields,
        })
    }

    /// Writes `item` over whatever this id's document held before.
    ///
    /// Not committed: the projection batches many of these into one commit,
    /// because a tantivy commit is a segment flush and an fsync, not the
    /// cheap thing a SQLite transaction is. Call [`Self::commit`] after a
    /// batch, or nothing this call did is visible to a search — or safe past
    /// a crash, though nothing here needs it to be: a lost commit is exactly
    /// what the next replay repairs, the same argument P2-19 makes for the
    /// SQLite side.
    pub async fn index_item(&self, item: Item) -> Result<()> {
        let fields = self.fields;
        let writer = Arc::clone(&self.writer);
        tokio::task::spawn_blocking(move || {
            let writer = writer.lock().unwrap_or_else(PoisonError::into_inner);
            upsert(
                writer.as_ref().ok_or(StoreError::IndexClosed)?,
                fields,
                &item,
            )
        })
        .await
        .map_err(StoreError::Stopped)?
    }

    /// Makes every [`Self::index_item`] call since the last commit visible to
    /// [`Self::search`].
    ///
    /// Reloads the reader explicitly rather than leaving it to
    /// `ReloadPolicy::OnCommitWithDelay`'s own background watcher: that policy
    /// picks a commit up within a delay measured in milliseconds, which is
    /// exactly wrong for this method's name — a caller awaiting `commit` means
    /// "visible now", and a search run a moment later must not race it.
    pub async fn commit(&self) -> Result<()> {
        let writer = Arc::clone(&self.writer);
        let reader = self.reader.clone();
        tokio::task::spawn_blocking(move || {
            let mut writer = writer.lock().unwrap_or_else(PoisonError::into_inner);
            let writer = writer.as_mut().ok_or(StoreError::IndexClosed)?;
            writer.commit().map_err(StoreError::index("committed"))?;
            reader.reload().map_err(StoreError::index("reloaded"))
        })
        .await
        .map_err(StoreError::Stopped)??;
        Ok(())
    }

    /// Closes the writer, waiting for tantivy's own merge thread to actually
    /// exit before returning. Idempotent: a second call, or one from another
    /// clone, finds the writer already taken and does nothing.
    ///
    /// **Why this exists at all, rather than letting the last clone's `Drop`
    /// handle it.** `IndexWriter`'s `Drop` joins its indexing worker threads
    /// but only *signals* its segment-merging thread to stop — `kill()`, not
    /// a join — because waiting for it is `wait_merging_threads`, a method
    /// that consumes the writer and so cannot run from `Drop`. That leaves a
    /// real, measured gap: the merge thread can still be holding tantivy's
    /// directory lock for a moment after every `SearchIndex` clone believes
    /// itself gone, which turns "restart in this same process" into a
    /// `LockBusy` race against nothing this crate's own reference counting
    /// controls. Taking the writer out of its `Mutex` here and calling
    /// `wait_merging_threads` explicitly closes that gap regardless of how
    /// many clones of this handle exist or in what order they are dropped.
    pub async fn close(&self) -> Result<()> {
        let writer = Arc::clone(&self.writer);
        tokio::task::spawn_blocking(move || {
            let taken = writer.lock().unwrap_or_else(PoisonError::into_inner).take();
            match taken {
                Some(writer) => writer
                    .wait_merging_threads()
                    .map_err(StoreError::index("closed")),
                None => Ok(()),
            }
        })
        .await
        .map_err(StoreError::Stopped)?
    }

    /// The `limit` best-matching item ids for `query`, ranked by §5.4's
    /// per-field boosts, best first.
    ///
    /// A plain word searches all five fields; `title:foo` searches just one,
    /// tantivy's own query syntax. Reading the actual title back is the
    /// caller's job — this is a ranking, not a second copy of the row.
    pub async fn search(&self, query: &str, limit: usize) -> Result<Vec<ItemId>> {
        let fields = self.fields;
        let mut parser = QueryParser::for_index(
            &self.index,
            vec![
                fields.title,
                fields.authors,
                fields.series,
                fields.genres,
                fields.description,
            ],
        );
        parser.set_field_boost(fields.title, TITLE_BOOST);
        parser.set_field_boost(fields.authors, AUTHORS_BOOST);
        parser.set_field_boost(fields.series, SERIES_BOOST);
        parser.set_field_boost(fields.genres, GENRES_BOOST);
        parser.set_field_boost(fields.description, DESCRIPTION_BOOST);
        let parsed = parser
            .parse_query(query)
            .map_err(|source| StoreError::Query {
                query: query.to_string(),
                source,
            })?;

        let reader = self.reader.clone();
        tokio::task::spawn_blocking(move || {
            let searcher = reader.searcher();
            let hits = searcher
                .search(&parsed, &TopDocs::with_limit(limit).order_by_score())
                .map_err(StoreError::index("searched"))?;
            hits.into_iter()
                .map(|(_score, address)| {
                    let doc: TantivyDocument = searcher
                        .doc(address)
                        .map_err(StoreError::index("read back a search hit"))?;
                    read_id(&doc, fields.id)
                })
                .collect()
        })
        .await
        .map_err(StoreError::Stopped)?
    }
}

/// Deletes whatever this item's id held, then adds it back — an upsert,
/// because tantivy has no such operation of its own.
fn upsert(writer: &IndexWriter, fields: Fields, item: &Item) -> Result<()> {
    let id = item.id.to_string();
    writer.delete_term(Term::from_field_text(fields.id, &id));

    let mut doc = TantivyDocument::default();
    doc.add_text(fields.id, &id);
    if let Some(title) = &item.title {
        doc.add_text(fields.title, title);
    }
    if let Some(authors) = &item.authors {
        doc.add_text(fields.authors, authors.join(" "));
    }
    if let Some(genres) = &item.genres {
        doc.add_text(fields.genres, genres.join(" "));
    }
    if let Some(series) = &item.series {
        doc.add_text(fields.series, &series.name);
    }
    if let Some(description) = &item.description {
        doc.add_text(fields.description, description);
    }
    writer
        .add_document(doc)
        .map_err(StoreError::index("written to"))?;
    Ok(())
}

/// Reads the id field back out of a search hit's stored document.
fn read_id(doc: &TantivyDocument, id: Field) -> Result<ItemId> {
    let text = doc
        .get_first(id)
        .and_then(|value| value.as_str())
        .ok_or_else(|| StoreError::Corrupt("no id field on a search hit".to_string()))?;
    text.parse()
        .map_err(|source| StoreError::Corrupt(format!("{text:?}: {source}")))
}
