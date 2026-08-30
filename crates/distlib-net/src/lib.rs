//! Transport layer: the iroh endpoint, the ALPN registry and the protocols
//! spoken between members.
//!
//! Membership is enforced at a single choke point — [`hooks::AllowlistHooks`],
//! an `iroh::endpoint::EndpointHooks` implementation installed on the endpoint
//! itself. That placement is deliberate: later phases register protocol
//! handlers this crate does not author (iroh-blobs, iroh-docs, iroh-gossip),
//! and a check inside any single handler would not cover them.

pub mod allowlist;
pub mod alpn;
pub mod connections;
pub mod endpoint;
pub mod error;
pub mod hooks;
pub mod node;
pub mod ping;

pub use allowlist::{Allowlist, AllowlistWriter, allowlist};
pub use connections::Connections;
pub use endpoint::build_endpoint;
pub use error::{NetError, Result};
pub use hooks::{AllowlistHooks, close_code};
pub use node::Node;
