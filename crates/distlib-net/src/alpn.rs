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
//! Later phases add `distlib/raft/0` (phase 1 consensus) and
//! `distlib/memberlog/0` (phase 1 log replication) as they gain handlers.

/// Liveness check. Echoes a payload back with a `pong:` prefix.
pub const PING: &[u8] = b"distlib/ping/0";

/// Every ALPN this build accepts, for `Builder::alpns`.
///
/// An ALPN must appear here *and* have a handler registered on the router;
/// offering one without a handler would advertise a protocol that then refuses
/// every stream.
pub fn registered() -> Vec<Vec<u8>> {
    vec![PING.to_vec()]
}
