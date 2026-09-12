//! The handles a process speaks on, made once and shared by everything that
//! speaks.
//!
//! An `Endpoint` and the `Gossip` over it are two halves of one decision: a
//! process has exactly one of each, and a second gossip over the same endpoint
//! would not see the topics the first had joined. Every subsystem that talks to
//! other members needs both — consensus announces the memberlog on the gossip
//! today, and from 2a-2 iroh-docs is handed the same instance — so they are
//! passed together, by whoever assembles the process.
//!
//! Here rather than in the crate that first needed them, because "the
//! transport this node is served on" is not consensus's idea; it is this
//! crate's, and consensus is one of its users.

use iroh::Endpoint;
use iroh_gossip::net::Gossip;

/// The endpoint a node answers on, and the gossip over it.
///
/// Cheap to clone: both halves are handles to something spawned once, and
/// cloning is how a second subsystem comes to speak on the same transport
/// rather than starting its own.
#[derive(Debug, Clone)]
pub struct Transport {
    /// What this node answers and dials on.
    ///
    /// Its ALPNs must be everything the router will be given — see
    /// [`crate::serve`].
    pub endpoint: Endpoint,
    /// The swarm this node's topics are joined on.
    ///
    /// Spawned by the caller rather than by whoever subscribes first, because
    /// a topic joined on one instance is invisible to another.
    pub gossip: Gossip,
}
