//! What keeps the catalogue's values through a garbage collection pass.
//!
//! A pass asks the documents which values their entries reference, then lists
//! tags and temporary tags, then deletes every other blob. iroh-docs writes an
//! entry by importing its value under a temporary tag, inserting the entry and
//! dropping the tag: a write between the documents' answer and the tag listing
//! is in neither, and loses its value (5f-7c, `tests/collection.rs`). So each
//! of this node's writes keeps its value's temporary tag until the next pass
//! begins: its entry is in by then, and the documents' answer has it.

use std::sync::{Arc, Mutex, PoisonError};

use iroh_blobs::{api::TempTag, store::ProtectCb};
use iroh_docs::engine::ProtectCallbackHandler;

/// The catalogue's half of a blob store's protect callback: the documents'
/// handler, and the values of this node's writes since the last pass began.
#[derive(Debug)]
pub struct CatalogueProtection {
    pub(crate) documents: ProtectCallbackHandler,
    pub(crate) writes: WritesSinceTheLastPass,
}

impl CatalogueProtection {
    /// The protection, and the callback for the blob store's `GcConfig`.
    pub fn new() -> (Self, ProtectCb) {
        let (documents, referenced_by_documents) = ProtectCallbackHandler::new();
        let writes = WritesSinceTheLastPass::default();
        let unseen = writes.clone();
        let protected: ProtectCb = Arc::new(move |live| {
            // Each of these entries is in already, so the documents' answer,
            // asked after, has its value.
            unseen.release();
            referenced_by_documents(live)
        });
        (Self { documents, writes }, protected)
    }
}

/// The temporary tags of the values this node wrote since the last pass
/// began.
#[derive(Debug, Clone, Default)]
pub(crate) struct WritesSinceTheLastPass(Arc<Mutex<Vec<TempTag>>>);

impl WritesSinceTheLastPass {
    /// Keeps `value` until the next pass begins.
    pub(crate) fn hold(&self, value: TempTag) {
        self.lock().push(value);
    }

    /// Lets go of every value kept so far.
    fn release(&self) {
        let released = std::mem::take(&mut *self.lock());
        drop(released);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<TempTag>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use iroh_blobs::store::mem::MemStore;

    use super::*;

    #[tokio::test]
    async fn the_values_held_are_let_go_when_a_pass_begins() {
        let blobs = MemStore::new();
        let (protection, protected) = CatalogueProtection::new();
        let value = blobs
            .add_bytes(b"Dune".as_slice())
            .temp_tag()
            .await
            .expect("a memory store takes bytes");
        protection.writes.hold(value);

        // Let go as the pass asks, before the documents answer.
        drop(protected(&mut HashSet::new()));

        assert!(protection.writes.lock().is_empty());
    }
}
