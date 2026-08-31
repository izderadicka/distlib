//! Membership enforcement at the endpoint.
//!
//! This is the single choke point. It is installed on the *endpoint*, not on a
//! protocol handler, and that placement is the whole point: phases 2–5 register
//! handlers this crate does not author (iroh-blobs, iroh-docs, iroh-gossip), so
//! a check inside `PingProtocol` would protect `distlib/ping/0` and nothing
//! else. **Do not move this into a protocol handler.**

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, PoisonError},
};

use distlib_core::MemberId;
use iroh::{
    EndpointAddr,
    endpoint::{
        AfterHandshakeOutcome, BeforeConnectOutcome, Connection, EndpointHooks,
        WeakConnectionHandle,
    },
};

use crate::allowlist::Allowlist;

/// QUIC application close codes distlib sends.
pub mod close_code {
    use iroh::endpoint::VarInt;

    /// The peer is not a member of this group.
    ///
    /// A distinct code so the far side can tell "refused by policy" from a
    /// network failure and stop retrying.
    pub const NOT_A_MEMBER: VarInt = VarInt::from_u32(0x1000);

    /// The peer is a member, but not a Raft voter.
    ///
    /// Distinct from [`NOT_A_MEMBER`] because they mean different things to
    /// whoever reads a packet capture: one is "you are not in this group", the
    /// other "you are, but consensus is not yours to take part in".
    pub const NOT_A_VOTER: VarInt = VarInt::from_u32(0x1001);
}

/// Human-readable reason sent alongside [`close_code::NOT_A_MEMBER`].
pub const NOT_A_MEMBER_REASON: &[u8] = b"not a member";

/// Human-readable reason sent alongside [`close_code::NOT_A_VOTER`].
pub const NOT_A_VOTER_REASON: &[u8] = b"not a voter";

/// Refuses every connection to or from a non-member, and closes the ones an
/// expulsion invalidates.
///
/// Clone to hold one copy while the endpoint owns another: every clone shares
/// the same connection table, which is what lets [`Self::evict_expelled`] reach
/// connections the hook recorded.
#[derive(Debug, Clone)]
pub struct AllowlistHooks {
    allowlist: Allowlist,
    /// Connections seen since start-up, by peer.
    ///
    /// Weak handles, because [`EndpointHooks`] is explicit that holding a
    /// strong [`Connection`] keeps it alive and disables close-on-drop. A dead
    /// entry simply fails to upgrade, and is pruned when noticed.
    connections: Arc<Mutex<HashMap<MemberId, Vec<WeakConnectionHandle>>>>,
}

impl AllowlistHooks {
    /// Enforces `allowlist` on the endpoint it is installed on.
    pub fn new(allowlist: Allowlist) -> Self {
        Self {
            allowlist,
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// The allowlist being enforced.
    pub fn allowlist(&self) -> &Allowlist {
        &self.allowlist
    }

    /// Closes open connections whenever the allowlist stops admitting a peer.
    ///
    /// §4.4 requires this: refusing a removed member's *next* attempt is not
    /// enough, because a member expelled mid-transfer would otherwise carry on
    /// with the connection they already had. Run it as a task alongside the
    /// endpoint.
    ///
    /// Returns when the allowlist can no longer change — the writer has been
    /// dropped — since there is nothing further to react to.
    pub async fn evict_expelled(mut self) {
        while self.allowlist.changed().await.is_ok() {
            self.evict_once();
        }
    }

    /// Whether a recorded connection is still worth keeping.
    ///
    /// `upgrade` alone is not the test. The handle holds a `Weak` on iroh's
    /// internal connection state, which outlives the `Connection` the protocol
    /// handler dropped, so a finished connection still upgrades — for the
    /// lifetime of the endpoint. `close_reason` is what actually says it is
    /// over.
    fn is_live(handle: &WeakConnectionHandle) -> bool {
        handle
            .upgrade()
            .is_some_and(|connection| crate::connections::is_live(&connection))
    }

    /// One pass: close what is no longer admitted, forget what has died.
    fn evict_once(&self) {
        let mut connections = self.lock();
        connections.retain(|peer, handles| {
            if self.allowlist.is_allowed(peer) {
                // Still a member; keep only the connections still open.
                handles.retain(Self::is_live);
                return !handles.is_empty();
            }

            for connection in handles.iter().filter_map(WeakConnectionHandle::upgrade) {
                // The same code a refused handshake gets, so an evicted peer
                // and a rejected one cannot tell the two apart — and both learn
                // to stop retrying.
                connection.close(close_code::NOT_A_MEMBER, NOT_A_MEMBER_REASON);
            }
            tracing::info!(peer = %peer, "closed connections to a former member");
            false
        });
    }

    /// Records a connection so an expulsion can find it later.
    ///
    /// Prunes this peer's finished connections on the way in. A reconnect is
    /// the natural moment to notice the previous one has ended, and doing it
    /// here keeps the table bounded without a timer or a task per connection: a
    /// peer that reconnects repeatedly cleans up after itself, and one that
    /// never reconnects leaves at most a single closed handle behind. The total
    /// is bounded by the size of the group rather than by uptime.
    fn remember(&self, peer: MemberId, connection: &Connection) {
        let mut connections = self.lock();
        let handles = connections.entry(peer).or_default();
        handles.retain(Self::is_live);
        handles.push(connection.weak_handle());
    }

    /// How many connections are currently tracked.
    ///
    /// The number [`Self::evict_expelled`] would have to walk. Worth being able
    /// to see: it is the quantity that would reveal a leak here. It stays small
    /// because a peer's finished connections are dropped when it reconnects, so
    /// the total is bounded by the size of the group rather than by uptime.
    pub fn tracked_connections(&self) -> usize {
        self.lock().values().map(Vec::len).sum()
    }

    /// A poison-tolerant lock.
    ///
    /// Nothing here panics while holding it, so a poisoned lock means an
    /// unrelated panic elsewhere; refusing to enforce membership afterwards
    /// would turn that into a security failure rather than a second bug.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<MemberId, Vec<WeakConnectionHandle>>> {
        self.connections
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl EndpointHooks for AllowlistHooks {
    /// Refuses to dial a non-member.
    ///
    /// This fires before any packet is sent, so an expelled member learns
    /// nothing — not even that this node is running. The identity here is the
    /// one we intend to dial rather than a verified one, which is exactly
    /// right: this is a policy decision about who we choose to contact.
    /// Verification is [`Self::after_handshake`]'s job.
    async fn before_connect(&self, addr: &EndpointAddr, alpn: &[u8]) -> BeforeConnectOutcome {
        let peer = MemberId::from(addr.id);
        if self.allowlist.is_allowed(&peer) {
            return BeforeConnectOutcome::Accept;
        }
        tracing::info!(
            peer = %peer,
            alpn = %String::from_utf8_lossy(alpn),
            "refused to dial a non-member",
        );
        BeforeConnectOutcome::Reject
    }

    /// Refuses a connection whose verified peer is not a member.
    ///
    /// Runs on incoming *and* outgoing connections. This is the authoritative
    /// check: only after the TLS handshake is the remote's identity proven
    /// rather than claimed.
    async fn after_handshake(&self, conn: &Connection) -> AfterHandshakeOutcome {
        let peer = MemberId::from(conn.remote_id());
        if self.allowlist.is_allowed(&peer) {
            // Recorded only once admitted: a rejected connection is closed
            // here and there is nothing left to evict later.
            self.remember(peer, conn);
            return AfterHandshakeOutcome::Accept;
        }
        // Logged at info, not debug: this is the audit trail for who tried to
        // reach a closed group.
        tracing::info!(peer = %peer, "rejected a connection from a non-member");
        AfterHandshakeOutcome::Reject {
            error_code: close_code::NOT_A_MEMBER,
            reason: NOT_A_MEMBER_REASON.to_vec(),
        }
    }
}
