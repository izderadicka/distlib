//! Transport layer: the iroh endpoint, the gossip over it, the ALPN registry
//! and the protocols spoken between members.
//!
//! From phase 2 a process has more than one subsystem on the wire, so what
//! they share is defined here rather than in whichever of them needed it
//! first: [`Transport`] is the endpoint and gossip they all speak on, and
//! [`serve`] is how each one's handlers reach a single router.
//!
//! Membership is enforced at a single choke point — [`hooks::AllowlistHooks`],
//! an `iroh::endpoint::EndpointHooks` implementation installed on the endpoint
//! itself. That placement is deliberate: later phases register protocol
//! handlers this crate does not author (iroh-blobs, iroh-docs, iroh-gossip),
//! and a check inside any single handler would not cover them.

pub mod addresses;
pub mod allowlist;
pub mod alpn;
pub mod connections;
pub mod endpoint;
pub mod error;
pub mod hooks;
pub mod node;
pub mod ping;
pub mod router;
pub mod transport;

pub use addresses::AddressBook;
pub use allowlist::{Allowlist, AllowlistWriter, allowlist};
pub use connections::Connections;
pub use endpoint::build_endpoint;
pub use error::{IsRejection, NetError, Result};
pub use hooks::{AllowlistHooks, NOT_A_VOTER_REASON, close_code};
pub use node::Node;
pub use router::{Protocols, serve};
pub use transport::Transport;
