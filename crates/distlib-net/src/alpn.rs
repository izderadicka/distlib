//! The ALPN registry.
//!
//! Every protocol distlib speaks over an iroh connection is named here, so two
//! phases cannot pick colliding identifiers and the set offered by the endpoint
//! is derived from one list rather than assembled at each call site.
//!
//! Names follow `distlib/<protocol>/<version>`. The version is a wire-format
//! version: a breaking change to the framing gets a new ALPN, so old and new
//! nodes fail to negotiate rather than misinterpreting each other.
//!
//! `distlib/memberlog/0` joins them when phase 1b adds log replication for
//! non-core followers.

/// Liveness check. Echoes a payload back with a `pong:` prefix.
pub const PING: &[u8] = b"distlib/ping/0";

/// Raft consensus RPC for the membership log (§4.5).
///
/// Served by `distlib-consensus`, not by [`crate::Node`] — see
/// [`registered`].
pub const RAFT: &[u8] = b"distlib/raft/0";

/// The ALPNs [`crate::Node`] serves.
///
/// Not "every ALPN that exists": an endpoint must offer exactly what its router
/// handles, since advertising a protocol with no handler means negotiating it
/// successfully and then refusing every stream. [`RAFT`] is deliberately absent
/// — `Node` has no Raft to serve it with, and a node running consensus builds
/// its own router and passes the wider set to
/// [`crate::endpoint::configure`].
pub fn registered() -> Vec<Vec<u8>> {
    vec![PING.to_vec()]
}
