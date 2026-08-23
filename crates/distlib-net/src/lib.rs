//! Transport layer: the iroh endpoint, the ALPN registry and allowlist enforcement.
//!
//! Membership is enforced here at a single choke point — an `iroh::endpoint::EndpointHooks`
//! implementation that rejects connections from non-members after the TLS handshake. That hook
//! is endpoint-level rather than per-protocol on purpose: later phases register protocol
//! handlers this crate does not author (iroh-blobs, iroh-docs, iroh-gossip), and a check inside
//! any single handler would not cover them.
