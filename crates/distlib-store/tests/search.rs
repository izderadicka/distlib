//! §5.4's tantivy half: what a query finds, and in what order.
//!
//! **Query construction is the fast-lane item the phase plan names** — not
//! "does tantivy work", which is tantivy's own test suite's job, but "does
//! this crate build the query it means to": every default field searched, and
//! the per-field boosts landing where they are supposed to.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use distlib_core::{Item, ItemId};
use distlib_store::SearchIndex;

/// An index with nothing in it, in memory.
async fn empty() -> SearchIndex {
    SearchIndex::open(None)
        .await
        .expect("a fresh in-memory index opens")
}

fn item(seed: u8) -> Item {
    Item::new(ItemId::from_bytes([seed; 32]))
}

#[tokio::test]
async fn a_committed_item_is_found_by_its_title() {
    let index = empty().await;
    let id = ItemId::from_bytes([1; 32]);
    index
        .index_item(Item {
            title: Some("Neuromancer".to_owned()),
            ..item(1)
        })
        .await
        .unwrap();
    index.commit().await.unwrap();

    let hits = index.search("Neuromancer", 10).await.unwrap();
    assert_eq!(hits, vec![id]);
}

/// A tantivy commit is a segment flush, not a cheap SQLite transaction — see
/// `index.rs`'s note on why the projection batches these. This pins the other
/// side of that decision: nothing written is visible until it is committed.
#[tokio::test]
async fn an_uncommitted_write_is_not_yet_searchable() {
    let index = empty().await;
    index
        .index_item(Item {
            title: Some("Neuromancer".to_owned()),
            ..item(1)
        })
        .await
        .unwrap();

    assert_eq!(index.search("Neuromancer", 10).await.unwrap(), Vec::new());
}

/// Re-indexing an id replaces its document rather than adding a second one.
#[tokio::test]
async fn indexing_an_item_again_replaces_it() {
    let index = empty().await;
    let id = ItemId::from_bytes([1; 32]);
    index
        .index_item(Item {
            title: Some("Neuromancer".to_owned()),
            ..item(1)
        })
        .await
        .unwrap();
    index.commit().await.unwrap();

    index
        .index_item(Item {
            title: Some("Count Zero".to_owned()),
            ..item(1)
        })
        .await
        .unwrap();
    index.commit().await.unwrap();

    assert_eq!(index.search("Neuromancer", 10).await.unwrap(), Vec::new());
    assert_eq!(index.search("Count Zero", 10).await.unwrap(), vec![id]);
}

/// The fast-lane pin for §5.4's per-field boosts: a query matching in `title`
/// must outrank one matching only in `description`, because that is the whole
/// point of setting the boosts at all — the query parser has to actually be
/// built with them, not just able to run without them.
///
/// **Both fields hold the query term alone, and nothing else.** A one-word
/// field has the same length in both, so BM25's own length normalization and
/// the term's per-field idf are identical for the two documents — measured
/// directly against tantivy: without a boost the two score *exactly* equal,
/// to the last bit. That tie is broken by document order, so `by_description`
/// is indexed **first** here, putting it ahead on the tie alone; only a
/// boost that actually favours `title` can move `by_title` past it. A field
/// length asymmetry (say, a long title) can't be used for this instead — it
/// stacks BM25's own bias on top of the boost, so a passing test would not
/// say which one did the work. Confirmed by mutation: this fails, in
/// `by_description`, `by_title` order, with every boost constant set to
/// `1.0`.
///
/// **That tie-break is not a documented tantivy guarantee** — nothing pins
/// it beyond this crate's `=0.26.2` version lock. Re-run the same `1.0`
/// mutation by hand after any tantivy version bump: a changed tie-break would
/// make this pass unconditionally, silently, regardless of the boosts.
#[tokio::test]
async fn a_title_match_outranks_a_description_match() {
    let index = empty().await;
    let by_title = ItemId::from_bytes([1; 32]);
    let by_description = ItemId::from_bytes([2; 32]);

    index
        .index_item(Item {
            description: Some("whale".to_owned()),
            ..item(2)
        })
        .await
        .unwrap();
    index
        .index_item(Item {
            title: Some("whale".to_owned()),
            ..item(1)
        })
        .await
        .unwrap();
    index.commit().await.unwrap();

    let hits = index.search("whale", 10).await.unwrap();
    assert_eq!(hits, vec![by_title, by_description]);
}

/// `authors` and `genres` are lists, joined into one searchable field rather
/// than one document per entry — so a query matching any one of them finds
/// the item.
#[tokio::test]
async fn a_query_matches_any_author_in_the_list() {
    let index = empty().await;
    let id = ItemId::from_bytes([1; 32]);
    index
        .index_item(Item {
            authors: Some(vec!["Frank Herbert".to_owned(), "Brian Herbert".to_owned()]),
            ..item(1)
        })
        .await
        .unwrap();
    index.commit().await.unwrap();

    assert_eq!(index.search("Brian", 10).await.unwrap(), vec![id]);
}

/// `limit` is honoured even when more than `limit` items match.
#[tokio::test]
async fn search_stops_at_the_limit() {
    let index = empty().await;
    for seed in 1..=5u8 {
        index
            .index_item(Item {
                title: Some("Dune".to_owned()),
                ..item(seed)
            })
            .await
            .unwrap();
    }
    index.commit().await.unwrap();

    assert_eq!(index.search("Dune", 3).await.unwrap().len(), 3);
}

/// `limit: 0` is answered rather than passed through — tantivy's own
/// `TopDocs::with_limit` panics on `0` instead of returning nothing.
#[tokio::test]
async fn a_zero_limit_finds_nothing_without_panicking() {
    let index = empty().await;
    index
        .index_item(Item {
            title: Some("Dune".to_owned()),
            ..item(1)
        })
        .await
        .unwrap();
    index.commit().await.unwrap();

    assert_eq!(index.search("Dune", 0).await.unwrap(), Vec::new());
}

/// A malformed query is refused rather than panicking or matching everything.
#[tokio::test]
async fn an_unbalanced_query_is_refused() {
    let index = empty().await;
    let error = index.search("\"unterminated", 10).await.unwrap_err();
    assert!(error.to_string().contains("unterminated"));
}

/// The schema is created by opening, and opening again finds it already there
/// — the same property `a_store_reopens_onto_what_it_already_held` pins for
/// SQLite.
#[tokio::test]
async fn an_index_reopens_onto_what_it_already_held() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let id = ItemId::from_bytes([1; 32]);

    let first = SearchIndex::open(Some(dir.path().to_path_buf()))
        .await
        .expect("the index opens");
    first
        .index_item(Item {
            title: Some("Neuromancer".to_owned()),
            ..item(1)
        })
        .await
        .unwrap();
    first.commit().await.unwrap();
    drop(first);

    let second = SearchIndex::open(Some(dir.path().to_path_buf()))
        .await
        .expect("the index opens again");
    assert_eq!(second.search("Neuromancer", 10).await.unwrap(), vec![id]);
}

/// `lang` is searchable, by field and as a plain word — the phase-3 by-hand
/// check found a Czech book could not be found by its language.
#[tokio::test]
async fn an_item_is_found_by_its_language() {
    let index = empty().await;
    let czech = ItemId::from_bytes([1; 32]);
    for (seed, lang) in [(1, "cs"), (2, "en")] {
        index
            .index_item(Item {
                title: Some(format!("Book {seed}")),
                lang: Some(lang.to_owned()),
                ..item(seed)
            })
            .await
            .unwrap();
    }
    index.commit().await.unwrap();

    assert_eq!(index.search("lang:cs", 10).await.unwrap(), vec![czech]);
    assert_eq!(index.search("cs", 10).await.unwrap(), vec![czech]);
}

/// A language code is a weak signal: an item *called* the word ranks above one
/// merely written in a language with that code.
#[tokio::test]
async fn a_title_outranks_a_language() {
    let index = empty().await;
    let titled = ItemId::from_bytes([2; 32]);
    index
        .index_item(Item {
            title: Some("Book".to_owned()),
            lang: Some("cs".to_owned()),
            ..item(1)
        })
        .await
        .unwrap();
    index
        .index_item(Item {
            title: Some("CS".to_owned()),
            ..item(2)
        })
        .await
        .unwrap();
    index.commit().await.unwrap();

    assert_eq!(index.search("cs", 10).await.unwrap()[0], titled);
}

/// An index directory as phase 3 left it: the fields it had then, no `lang`,
/// and no `VERSION` file.
fn a_phase_three_index(dir: &std::path::Path) {
    use tantivy::{
        Index,
        schema::{STORED, STRING, Schema, TEXT, TantivyDocument},
    };
    let mut builder = Schema::builder();
    let id = builder.add_text_field("id", STRING | STORED);
    let title = builder.add_text_field("title", TEXT);
    for field in ["authors", "genres", "series", "description"] {
        builder.add_text_field(field, TEXT);
    }
    let index = Index::create_in_dir(dir, builder.build()).unwrap();
    let mut writer = index.writer_with_num_threads(1, 15_000_000).unwrap();
    let mut doc = TantivyDocument::default();
    doc.add_text(id, ItemId::from_bytes([9; 32]).to_string());
    doc.add_text(title, "Left over");
    writer.add_document(doc).unwrap();
    writer.commit().unwrap();
    writer.wait_merging_threads().unwrap();
}

/// **A phase-3 index opens**, rather than stopping `distlib run` with a schema
/// error: it is started afresh — the replay at every start refills it — and is
/// then written to and searched like any other.
#[tokio::test]
async fn an_index_from_before_versions_is_started_afresh() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    a_phase_three_index(dir.path());

    let index = SearchIndex::open(Some(dir.path().to_path_buf()))
        .await
        .expect("an old index opens");
    assert_eq!(
        index.search("over", 10).await.unwrap(),
        Vec::new(),
        "what the old index held is gone, not carried into the new one"
    );
    index
        .index_item(Item {
            lang: Some("cs".to_owned()),
            ..item(1)
        })
        .await
        .unwrap();
    index.commit().await.unwrap();
    assert_eq!(
        index.search("lang:cs", 10).await.unwrap(),
        vec![ItemId::from_bytes([1; 32])]
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("VERSION")).unwrap(),
        distlib_store::schema::READ_MODEL_VERSION.to_string()
    );
}

/// A field changed without the version being bumped still opens: tantivy's
/// schema check is the second line, behind the version.
#[tokio::test]
async fn an_index_whose_schema_changed_without_a_bump_is_started_afresh() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    a_phase_three_index(dir.path());
    std::fs::write(
        dir.path().join("VERSION"),
        distlib_store::schema::READ_MODEL_VERSION.to_string(),
    )
    .unwrap();

    let index = SearchIndex::open(Some(dir.path().to_path_buf()))
        .await
        .expect("a mismatched index opens");
    assert_eq!(index.search("over", 10).await.unwrap(), Vec::new());
}

/// The version is judged on its own, not only through tantivy's schema check:
/// a bump can change what the fields *mean* — a tokenizer, a boost written into
/// the documents — while their names and types stay the same.
#[tokio::test]
async fn an_index_of_another_version_is_started_afresh_even_with_the_same_fields() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let first = SearchIndex::open(Some(dir.path().to_path_buf()))
        .await
        .unwrap();
    first
        .index_item(Item {
            title: Some("Neuromancer".to_owned()),
            ..item(1)
        })
        .await
        .unwrap();
    first.commit().await.unwrap();
    first.close().await.unwrap();
    drop(first);
    std::fs::write(dir.path().join("VERSION"), "0").unwrap();

    let second = SearchIndex::open(Some(dir.path().to_path_buf()))
        .await
        .unwrap();
    assert_eq!(second.search("Neuromancer", 10).await.unwrap(), Vec::new());
}
