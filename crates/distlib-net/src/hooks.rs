//! Membership enforcement at the endpoint.
//!
//! This is the single choke point. It is installed on the *endpoint*, not on a
//! protocol handler, and that placement is the whole point: phases 2–5 register
//! handlers this crate does not author (iroh-blobs, iroh-docs, iroh-gossip), so
//! a check inside `PingProtocol` would protect `distlib/ping/0` and nothing
//! else. **Do not move this into a protocol handler.**

use distlib_core::MemberId;
use iroh::{
    EndpointAddr,
    endpoint::{AfterHandshakeOutcome, BeforeConnectOutcome, Connection, EndpointHooks},
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
}

/// Human-readable reason sent alongside [`close_code::NOT_A_MEMBER`].
pub const NOT_A_MEMBER_REASON: &[u8] = b"not a member";

/// Refuses every connection to or from a non-member.
#[derive(Debug, Clone)]
pub struct AllowlistHooks {
    allowlist: Allowlist,
}

impl AllowlistHooks {
    /// Enforces `allowlist` on the endpoint it is installed on.
    pub fn new(allowlist: Allowlist) -> Self {
        Self { allowlist }
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
