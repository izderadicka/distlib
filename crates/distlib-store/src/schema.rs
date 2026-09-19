//! The tables of §5.4, and the pragmas they are opened under.
//!
//! **`STRICT` on every table**, which is not the SQLite default and is the
//! point: without it a column declared `INTEGER` accepts the string `"no"`, and
//! a projection bug would be found by whatever read the row months later
//! instead of by the write that made it. It needs SQLite 3.37, which is why the
//! dependency is `bundled` rather than the host's.
//!
//! **Three tables, no more.** §5.4 also names `ratings`, `reviews`,
//! `bookmarks`, `wishes` and `custodianships`; those belong to phases 4 and 5
//! and nothing writes them yet. A table nothing fills is a schema commitment
//! made before the thing it describes exists.
//!
//! **No `added_by`, `created` or `modified_by` columns**, which §5.2 does name.
//! There is nothing to fill them from: the catalogue's `fields!` table has no
//! such fields, so a column for one would be `NULL` in every row of every
//! group. `last_modified` is the exception, and it is the exception precisely
//! because it needs no field — the document already timestamps every entry.

/// Run on every connection, before anything else.
///
/// `foreign_keys` is off by default in SQLite, so the `REFERENCES` clause below
/// would otherwise be documentation rather than a constraint — and the cascade
/// that keeps `item_files` from outliving its item would never fire.
///
/// `synchronous = NORMAL` under WAL is the one place this file trades a
/// guarantee for speed, and it is the right trade here and nowhere else in the
/// system: everything in these tables is derived from the document, so the
/// worst a lost transaction costs is that the next start replays it.
pub(crate) const PRAGMAS: &str = "\
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA synchronous = NORMAL;
";

/// The tables themselves, created if they are not already there.
pub(crate) const TABLES: &str = "\
CREATE TABLE IF NOT EXISTS items (
    id            TEXT    PRIMARY KEY,
    kind          TEXT,
    title         TEXT,
    authors       TEXT,
    genres        TEXT,
    series        TEXT,
    series_index  REAL,
    year          INTEGER,
    lang          TEXT,
    description   TEXT,
    replicas      INTEGER,
    last_modified INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS item_files (
    item     TEXT    NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    blob     TEXT    NOT NULL,
    role     TEXT    NOT NULL,
    format   TEXT    NOT NULL,
    size     INTEGER NOT NULL,
    filename TEXT    NOT NULL,
    seq      INTEGER,
    disc     INTEGER,
    title    TEXT,
    duration INTEGER,
    PRIMARY KEY (item, blob)
) STRICT;

CREATE TABLE IF NOT EXISTS members (
    id           TEXT    PRIMARY KEY,
    display_name TEXT    NOT NULL,
    pledge_bytes INTEGER NOT NULL,
    is_core      INTEGER NOT NULL
) STRICT;
";
