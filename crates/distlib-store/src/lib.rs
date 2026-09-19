//! The read model of §5.4: the catalogue, projected into SQLite.
//!
//! **Why a second copy of the catalogue exists at all.** The document is a
//! key-value store with last-writer-wins per key, which is what makes it
//! conflict-free and what makes it useless to query: "every audiobook by this
//! author, newest first" is a scan of every entry in the group. So the
//! document stays the record, and this is a derived index of it — rebuilt from
//! the record rather than maintained beside it, which is the whole of the
//! design.
//!
//! **The projection re-reads, it does not apply deltas.** A change says *which
//! item* to look at; the projection then reads that whole item out of the
//! document and overwrites its rows. This is the thing that makes §10's
//! requirement — replay N times = replay once — true by construction rather
//! than by argument: there is no accumulated state to replay into. It is also
//! why order does not matter, which is the property the proptests pin, because
//! it is the one that would quietly stop holding if somebody later optimised
//! the re-read into an update.
//!
//! **What is here is a pure function of what the document holds now**, and that
//! has one consequence worth stating rather than discovering. An item whose
//! newest `title` entry has arrived but whose *bytes* have not reads with no
//! title at all — [`distlib_sync::Catalogue::read_item`] leaves an unreadable
//! entry out — so projecting it writes `title = NULL` over a title that was
//! there a moment ago, until the bytes land and it is projected again.
//!
//! Keeping the old value instead was considered and rejected: it would make
//! these tables depend on *what this node happened to see*, so a node that
//! restarted and one that did not would hold different rows for the same
//! document. That is precisely the thing 2a-3's acceptance forbids, and it
//! would trade a gap measured in seconds for a divergence with no repair path.

pub mod error;
pub mod index;
pub mod projection;
pub mod schema;
pub mod store;

pub use error::{Result, StoreError};
pub use index::SearchIndex;
pub use projection::{Projection, ReindexHandle};
pub use store::{Store, StoredItem, StoredMember};
