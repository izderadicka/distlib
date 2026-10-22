//! Who this node sends an item's content to (phase 5's 5b-5b): only members
//! pledging the group's minimum, so a member that pledges nothing takes
//! nothing from the group, whatever client it runs.

use distlib_consensus::MembershipState;
use distlib_core::{ContentHash, MemberId};
use distlib_net::blobs::ServingGate;
use distlib_store::Store;
use tokio::sync::watch;

/// Serves anything to a member pledging the group's minimum, and to one
/// pledging less everything but items' content files: the catalogue's entries
/// and the availability base lists still reach it, so it can look around
/// before it pledges.
#[derive(Debug, Clone)]
pub struct PledgedOnly {
    membership: watch::Receiver<MembershipState>,
    store: Store,
}

impl PledgedOnly {
    /// Judges by the pledges in `membership` and the files in `store`.
    pub fn new(membership: watch::Receiver<MembershipState>, store: Store) -> Self {
        Self { membership, store }
    }
}

impl ServingGate for PledgedOnly {
    async fn may_serve(&self, requester: MemberId, hash: ContentHash) -> bool {
        if self.membership.borrow().pledges_the_minimum(&requester) {
            return true;
        }
        match self.store.is_content_file(hash).await {
            Ok(content) => !content,
            Err(error) => {
                tracing::warn!(%hash, %error, "could not tell whether a blob is an item's content; refused to a member below the minimum pledge");
                false
            }
        }
    }
}
