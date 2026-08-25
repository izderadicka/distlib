//! Transport layer: the iroh endpoint, the ALPN registry and the protocols
//! spoken between members.
//!
//! Membership will be enforced here at a single choke point — an
//! `iroh::endpoint::EndpointHooks` implementation that rejects connections from
//! non-members after the TLS handshake. That hook is endpoint-level rather than
//! per-protocol on purpose: later phases register protocol handlers this crate
//! does not author (iroh-blobs, iroh-docs, iroh-gossip), and a check inside any
//! single handler would not cover them.

pub mod alpn;
pub mod endpoint;
pub mod error;
pub mod node;
pub mod ping;

pub use endpoint::build_endpoint;
pub use error::{NetError, Result};
pub use node::Node;
