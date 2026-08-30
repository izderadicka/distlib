//! Open connections to peers, shared by every protocol on this node.
//!
//! Keyed by protocol as well as peer, which is not an optimisation to be tidied
//! away later: ALPN is negotiated once in the TLS handshake, and iroh's router
//! reads it once and hands the *whole* connection to that protocol's handler.
//! Streams multiplex within a protocol, never across one. Handing a raft
//! connection to a caller wanting `distlib/ping/0` would deliver its streams to
//! the remote's raft handler, which would fail to decode them.
//!
//! So a node holds one connection per peer per protocol. The saving here is on
//! *repeated* use of the same protocol, which is the common case — a follower
//! answers heartbeats continuously, and a handshake per heartbeat would be
//! waste. The expensive part of reaching a peer at all is not repeated either
//! way: iroh keeps address lookup, hole punching and path selection per remote,
//! so a second connection to a peer already spoken to is a QUIC handshake over
//! an established path.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use distlib_core::MemberId;
use iroh::{Endpoint, EndpointAddr, endpoint::Connection};

use crate::error::{NetError, Result};

/// One connection per peer per protocol.
type Key = (MemberId, Vec<u8>);

/// The connections this node holds open.
///
/// Cheap to clone; every clone shares the same set, which is the point — the
/// consensus network layer and anything added later reuse one connection to a
/// peer rather than opening one each.
#[derive(Debug, Clone, Default)]
pub struct Connections {
    open: Arc<Mutex<HashMap<Key, Connection>>>,
}

impl Connections {
    /// An empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// The connection to `peer` for `alpn`, dialling if there is not one.
    ///
    /// A cached connection that has since closed is replaced rather than
    /// returned: expulsion closes connections without going through whoever
    /// opened them (§4.4), so "in the map" does not mean "usable".
    pub async fn get_or_connect(
        &self,
        endpoint: &Endpoint,
        peer: MemberId,
        addr: EndpointAddr,
        alpn: &[u8],
    ) -> Result<Connection> {
        if let Some(connection) = self.cached(peer, alpn) {
            return Ok(connection);
        }

        // Deliberately not holding the lock across the dial. One mutex covers
        // every peer, so holding it here would make a single unreachable member
        // stall dials to all the others until its connect timed out. The cost
        // is that two callers racing for the same peer may both dial and one
        // result is dropped, which is far cheaper than serialising everyone.
        let connection = endpoint
            .connect(addr, alpn)
            .await
            .map_err(|error| NetError::peer(peer, error))?;

        self.lock()
            .insert((peer, alpn.to_vec()), connection.clone());
        Ok(connection)
    }

    /// Drops the connection to `peer` for `alpn`, so the next call redials.
    ///
    /// For a caller whose RPC failed on it: the connection may be half-open,
    /// and one that is never replaced would fail every later call to that peer.
    pub fn forget(&self, peer: MemberId, alpn: &[u8]) {
        self.lock().remove(&(peer, alpn.to_vec()));
    }

    /// Drops every connection to `peer`, whatever the protocol.
    pub fn forget_peer(&self, peer: MemberId) {
        self.lock().retain(|(held, _), _| *held != peer);
    }

    /// How many connections are held. For diagnostics and tests.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether none are held.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The cached connection, if there is one and it is still usable.
    fn cached(&self, peer: MemberId, alpn: &[u8]) -> Option<Connection> {
        let mut open = self.lock();
        let key = (peer, alpn.to_vec());

        match open.get(&key) {
            Some(connection) if is_live(connection) => Some(connection.clone()),
            // Closed since it was cached. Forget it here so the caller dials a
            // fresh one rather than being handed something already dead.
            Some(_) => {
                open.remove(&key);
                None
            }
            None => None,
        }
    }

    /// A poison-tolerant lock.
    ///
    /// Deliberately `std::sync::Mutex` rather than tokio's. A blocking guard
    /// cannot be held across an await in a future that must be `Send`, so the
    /// compiler refuses the mistake this type previously made — one mutex over
    /// every peer, held while dialling, so a single unreachable member stalled
    /// dials to all the others. An async mutex compiles that happily.
    ///
    /// Poison-tolerant because nothing here panics while holding it: a poisoned
    /// lock means an unrelated panic elsewhere, and refusing to hand out
    /// connections afterwards would turn that into a second failure.
    fn lock(&self) -> MutexGuard<'_, HashMap<Key, Connection>> {
        self.open.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Whether a connection is still usable.
///
/// The single definition, because getting it wrong is not obvious: a handle can
/// look alive long after the connection is finished. `close_reason` is what
/// actually reports it is over.
pub(crate) fn is_live(connection: &Connection) -> bool {
    connection.close_reason().is_none()
}
