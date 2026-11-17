//! What a catalogue entry *is*: the item schema of §5.2, and the keys it lives
//! under.
//!
//! **Field-level keys, not a record per item.** Two members improving the same
//! item at once — one fixing the title, one adding the year — must not clobber
//! each other, and iroh-docs resolves last-writer-wins *per key*. So every
//! field is its own entry, and a write touches only what it changes.
//!
//! **One key for all of an item's files**, written once by whoever adds it.
//! An item is the files it was added with, fixed as a torrent's are (P5-66):
//! nobody adds a file to it later, so there is nothing to merge. A list
//! somebody else writes there is read only if it fingerprints to the item's
//! id, and any such list is the same set of files.
//!
//! **Every value is JSON**, including the ones §5.2 writes as bare strings.
//! One rule beats a table of exceptions: the reader has one code path, and a
//! `"Dune"` that arrives as `Dune` is a malformed value rather than a second
//! encoding to support.
//!
//! Nothing here knows about iroh-docs. These are keys and bytes; `distlib-sync`
//! is what puts them in a document.

use std::{collections::BTreeMap, str::FromStr as _};

use serde::{Deserialize, Serialize};

use crate::id::{ContentHash, ItemId};

/// The prefix every catalogue item key starts with.
const ITEMS: &str = "item";

/// The last segment of the key holding an item's files.
const FILES: &str = "files";

/// What kind of thing an item is (§5.2's `type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ItemKind {
    Ebook,
    Audiobook,
    Video,
    Other,
}

/// Where an item sits in a series.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Series {
    pub name: String,
    /// Fractional on purpose: novellas are numbered 2.5 and the world is not
    /// going to stop doing that.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<f32>,
}

/// One file of an item.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FileRecord {
    /// Container or codec as an operator would say it: `epub`, `mp3`, `mkv`.
    pub format: String,
    pub size: u64,
    pub filename: String,
    /// Playback order for chapterised media, and the disc it came from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seq: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disc: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<u64>,
}

/// The item's fields, stated once: the variant, the name the key gives it,
/// the member of [`Item`] that holds it, and that member's type.
///
/// Four things follow from every row — a variant, a key name, an entry that
/// gets written, a value that gets read — and **two of them are silent when
/// they go missing**. `as_str` and `absorb_field` are exhaustive matches, so a
/// variant added without them does not compile; a variant missing from `parse`
/// simply never reads back, and one missing from `entries` is never written.
/// Neither is a compile error, and both are the kind of thing noticed a
/// release later. One table, and the question cannot arise.
///
/// [`ItemFields`] comes from the same table, so that whatever takes a partial
/// item from outside — an edit — does not keep a list of its own to fall out
/// of step with this one. [`Item`] itself is still written out by hand, for
/// its doc comments; the table assigns to its members by name and type, so the
/// two cannot disagree and still compile.
macro_rules! fields {
    ($($variant:ident => $name:literal, $member:ident: $ty:ty;)+) => {
        /// One field of an item — one key in the document.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum Field {
            $($variant,)+
        }

        impl Field {
            /// Every field there is, in the order an item writes them.
            pub const ALL: &[Self] = &[$(Self::$variant,)+];

            /// The name this field has in a key.
            ///
            /// [`Field::Kind`] is `type`, because the key is the wire format
            /// and §5.2 named it; `kind` is only what Rust can spell.
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $name,)+
                }
            }

            fn parse(name: &str) -> Option<Self> {
                match name {
                    $($name => Some(Self::$variant),)+
                    _ => None,
                }
            }
        }

        impl Item {
            /// One entry per field this item has something to say about.
            fn field_entries(&self) -> impl Iterator<Item = (String, Vec<u8>)> {
                [$(self.field_entry(Field::$variant, self.$member.as_ref()),)+]
                    .into_iter()
                    .flatten()
            }

            fn absorb_field(&mut self, field: Field, value: &[u8]) -> Absorbed {
                match field {
                    $(Field::$variant => take(&mut self.$member, value),)+
                }
            }

            /// Sets the fields `fields` has a value for, and leaves the rest.
            pub fn set(&mut self, fields: ItemFields) {
                $(
                    if let Some(value) = fields.$member {
                        self.$member = Some(value);
                    }
                )+
            }
        }

        /// Some of an item's fields, each optional — what an edit says.
        ///
        /// A field left out is not a field set to nothing: it is left as it
        /// is. Named as [`Item`]'s members are, so `kind` rather than the
        /// key's `type`, and an unknown name is refused rather than ignored.
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct ItemFields {
            $(
                #[serde(default, skip_serializing_if = "Option::is_none")]
                pub $member: Option<$ty>,
            )+
        }
    };
}

fields! {
    Kind        => "type",        kind:        ItemKind;
    Title       => "title",       title:       String;
    Authors     => "authors",     authors:     Vec<String>;
    Genres      => "genres",      genres:      Vec<String>;
    Series      => "series",      series:      Series;
    Year        => "year",        year:        i32;
    Lang        => "lang",        lang:        String;
    Description => "description", description: String;
}

/// A key in the catalogue document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Key {
    /// `item/{item_id}/{field}`
    Field { item: ItemId, field: Field },
    /// `item/{item_id}/files`
    Files { item: ItemId },
}

impl Key {
    /// The key as it is written in the document.
    pub fn encode(&self) -> String {
        match self {
            Self::Field { item, field } => format!("{ITEMS}/{item}/{}", field.as_str()),
            Self::Files { item } => format!("{ITEMS}/{item}/{FILES}"),
        }
    }

    /// Reads a key back, or `None` if this build does not recognise it.
    ///
    /// **Not an error.** The catalogue is one document for the whole group and
    /// grows keys over time — phase 4's ratings live beside these, and a field
    /// added next year will arrive at a node running this build. Anything not
    /// recognised is somebody else's key, and the answer is to leave it alone.
    pub fn parse(key: &[u8]) -> Option<Self> {
        let key = std::str::from_utf8(key).ok()?;
        let mut parts = key.split('/');
        if parts.next()? != ITEMS {
            return None;
        }
        let item = ItemId::from_str(parts.next()?).ok()?;
        let third = parts.next()?;
        let key = if third == FILES {
            Self::Files { item }
        } else {
            Self::Field {
                item,
                field: Field::parse(third)?,
            }
        };
        parts.next().is_none().then_some(key)
    }

    /// The item this key is about.
    pub const fn item(&self) -> ItemId {
        match self {
            Self::Field { item, .. } | Self::Files { item } => *item,
        }
    }

    /// Every key belonging to `item`, as a prefix to read or watch.
    pub fn prefix_of(item: ItemId) -> String {
        format!("{ITEMS}/{item}/")
    }

    /// Every item key there is, as a prefix to read or watch.
    ///
    /// The whole-catalogue counterpart of [`Self::prefix_of`], and what a read
    /// model replays. Narrower than reading the document, deliberately: the
    /// catalogue is one document with key prefixes, and phase 4's ratings live
    /// beside these.
    pub fn all_items() -> String {
        format!("{ITEMS}/")
    }
}

/// What happened when an entry was folded into an [`Item`].
///
/// Every arm is a fact about the entry rather than a failure, because on a
/// node that is reading what a *different build* wrote, the last two are
/// ordinary. The caller decides which of them are worth a log line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Absorbed {
    /// Folded in.
    Took,
    /// A well-formed key about some other item.
    NotThisItem,
    /// A key this build does not recognise — another item's kind of key, a
    /// field added by a newer build, or phase 4's ratings.
    Unknown,
    /// A key we know, holding a value we cannot read. The field keeps
    /// whatever it had.
    Unreadable,
    /// A file list whose files do not fingerprint to the item's id: not the
    /// files the item was added with. The item keeps whatever it had.
    NotItsFiles,
}

/// One catalogue item, assembled from the entries that make it up.
///
/// Every field is optional because an item *is* its entries: a node that has
/// synced three of them holds an item with three fields, and that is a
/// truthful answer rather than a partial one.
#[derive(Debug, Clone, PartialEq)]
pub struct Item {
    pub id: ItemId,
    pub kind: Option<ItemKind>,
    pub title: Option<String>,
    pub authors: Option<Vec<String>>,
    pub genres: Option<Vec<String>>,
    pub series: Option<Series>,
    pub year: Option<i32>,
    pub lang: Option<String>,
    pub description: Option<String>,
    pub files: BTreeMap<ContentHash, FileRecord>,
}

impl Item {
    /// An item with nothing said about it yet.
    pub fn new(id: ItemId) -> Self {
        Self {
            id,
            kind: None,
            title: None,
            authors: None,
            genres: None,
            series: None,
            year: None,
            lang: None,
            description: None,
            files: BTreeMap::new(),
        }
    }

    /// The entries to write for what this item *says*.
    ///
    /// **Absent fields produce no entry**, which is the whole point: writing
    /// `title: null` is a write, and under last-writer-wins it would erase a
    /// title somebody else had just filled in. An `Item` with one field set is
    /// a one-key update.
    /// The files are one entry, and none when there are no files.
    pub fn entries(&self) -> Vec<(String, Vec<u8>)> {
        let files = (!self.files.is_empty())
            .then(|| {
                Some((
                    Key::Files { item: self.id }.encode(),
                    encode(Some(&self.files))?,
                ))
            })
            .flatten();
        self.field_entries().chain(files).collect()
    }

    /// One field's entry, or nothing at all when the field is not set.
    fn field_entry<T: Serialize>(
        &self,
        field: Field,
        value: Option<&T>,
    ) -> Option<(String, Vec<u8>)> {
        Some((
            Key::Field {
                item: self.id,
                field,
            }
            .encode(),
            encode(value)?,
        ))
    }

    /// Folds one entry in, reporting what it turned out to be.
    pub fn absorb(&mut self, key: &[u8], value: &[u8]) -> Absorbed {
        let Some(key) = Key::parse(key) else {
            return Absorbed::Unknown;
        };
        if key.item() != self.id {
            return Absorbed::NotThisItem;
        }
        match key {
            Key::Files { .. } => self.absorb_files(value),
            Key::Field { field, .. } => self.absorb_field(field, value),
        }
    }

    /// Takes a file list, if it is the one this item was added with.
    fn absorb_files(&mut self, value: &[u8]) -> Absorbed {
        let Ok(files) = serde_json::from_slice::<BTreeMap<ContentHash, FileRecord>>(value) else {
            return Absorbed::Unreadable;
        };
        if fingerprint_of(&files) != Some(self.id) {
            return Absorbed::NotItsFiles;
        }
        self.files = files;
        Absorbed::Took
    }

    /// The fingerprint of this item's files (§5.2, as amended by P0-7).
    ///
    /// Answers `None` when the item has no files: an id derived from an empty
    /// set is the same id for every such item, and two items that are not the
    /// same thing must not share one.
    pub fn fingerprint(&self) -> Option<ItemId> {
        fingerprint_of(&self.files)
    }
}

/// The fingerprint of `files`, or `None` when there are none.
fn fingerprint_of(files: &BTreeMap<ContentHash, FileRecord>) -> Option<ItemId> {
    let hashes: Vec<[u8; 32]> = files.keys().map(|blob| *blob.as_bytes()).collect();
    (!hashes.is_empty()).then(|| ItemId::from_content_hashes(&hashes))
}

/// Reads one field's value, or reports that this build cannot.
///
/// The field is left exactly as it was when the value cannot be read,
/// deliberately: a value this build does not understand is not a reason to
/// forget the one it did.
fn take<T: serde::de::DeserializeOwned>(into: &mut Option<T>, value: &[u8]) -> Absorbed {
    match serde_json::from_slice(value) {
        Ok(parsed) => {
            *into = Some(parsed);
            Absorbed::Took
        }
        Err(_) => Absorbed::Unreadable,
    }
}

/// JSON for a field that is set; nothing for one that is not.
///
/// Serialising cannot fail for any of these types — they are strings, numbers
/// and plain structs — so a failure here means the type grew something
/// unserialisable, and the field is dropped rather than the write refused.
fn encode<T: Serialize>(value: Option<&T>) -> Option<Vec<u8>> {
    let value = value?;
    match serde_json::to_vec(value) {
        Ok(bytes) => Some(bytes),
        Err(error) => {
            tracing::error!(%error, "a catalogue field could not be encoded; leaving it out");
            None
        }
    }
}
