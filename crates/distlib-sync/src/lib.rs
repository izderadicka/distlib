//! The group's catalogue, and the only crate that knows how it is replicated.
//!
//! §5.1's rule, and the reason this crate exists: iroh-docs and iroh-blobs are
//! wrapped, not exposed. iroh-blobs says of itself that it is not production
//! quality yet, and the version that is cannot be reached from iroh 1.0 — so
//! the mitigation is containment. Everything above this sees typed records and
//! hashes, and if either crate has to be replaced, one layer is replaced.

pub mod catalogue;
pub mod error;

pub use catalogue::{Catalogue, alpns, catalogue_key};
pub use error::{Result, SyncError};
