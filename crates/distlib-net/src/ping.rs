//! `distlib/ping/0` — the liveness check.
//!
//! One bidirectional stream, one message each way: the initiator writes a
//! payload and closes its side, the responder writes back [`PONG_PREFIX`]
//! followed by the same bytes.
//!
//! Deliberately raw bytes rather than an RPC framework. Phase 1 introduces real
//! request/response traffic and can pick one then; a ping does not justify the
//! dependency now.

use std::time::Duration;

use distlib_core::MemberId;
use iroh::{
    Endpoint, EndpointAddr,
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler},
};

use crate::{
    alpn,
    error::{NetError, Result},
};

/// Prefix the responder puts in front of the echoed payload.
pub const PONG_PREFIX: &[u8] = b"pong:";

/// Largest payload accepted in either direction.
///
/// A cap is required, not decorative: `read_to_end` on an unbounded stream lets
/// any peer that completes a handshake allocate without limit.
pub const MAX_PAYLOAD: usize = 4096;

/// How long [`ping`] waits for the whole exchange.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Answers `distlib/ping/0`.
#[derive(Debug, Clone, Default)]
pub struct PingProtocol;

impl ProtocolHandler for PingProtocol {
    async fn accept(&self, connection: Connection) -> std::result::Result<(), AcceptError> {
        let (mut send, mut recv) = connection.accept_bi().await?;

        let payload = recv
            .read_to_end(MAX_PAYLOAD)
            .await
            .map_err(AcceptError::from_err)?;

        let mut reply = Vec::with_capacity(PONG_PREFIX.len() + payload.len());
        reply.extend_from_slice(PONG_PREFIX);
        reply.extend_from_slice(&payload);

        send.write_all(&reply)
            .await
            .map_err(AcceptError::from_err)?;
        send.finish().map_err(AcceptError::from_err)?;

        // Wait for the initiator to close, so the reply is not discarded by the
        // connection being dropped the moment this handler returns.
        connection.closed().await;
        Ok(())
    }
}

/// Pings `addr` and returns the echoed payload, without the `pong:` prefix.
pub async fn ping(endpoint: &Endpoint, addr: EndpointAddr, payload: &[u8]) -> Result<Vec<u8>> {
    ping_with_timeout(endpoint, addr, payload, DEFAULT_TIMEOUT).await
}

/// [`ping`] with an explicit deadline for the whole exchange.
pub async fn ping_with_timeout(
    endpoint: &Endpoint,
    addr: EndpointAddr,
    payload: &[u8],
    timeout: Duration,
) -> Result<Vec<u8>> {
    if payload.len() > MAX_PAYLOAD {
        return Err(NetError::PayloadTooLarge {
            len: payload.len(),
            max: MAX_PAYLOAD,
        });
    }
    let peer = MemberId::from(addr.id);

    tokio::time::timeout(timeout, exchange(endpoint, addr, payload, peer))
        .await
        .map_err(|_| NetError::Timeout { peer, timeout })?
}

async fn exchange(
    endpoint: &Endpoint,
    addr: EndpointAddr,
    payload: &[u8],
    peer: MemberId,
) -> Result<Vec<u8>> {
    let connection = endpoint
        .connect(addr, alpn::PING)
        .await
        .map_err(|error| NetError::peer(peer, error))?;

    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|error| NetError::peer(peer, error))?;

    send.write_all(payload)
        .await
        .map_err(|error| NetError::peer(peer, error))?;
    // Closing our side is what tells the responder the payload is complete.
    send.finish().map_err(|error| NetError::peer(peer, error))?;

    let reply = recv
        .read_to_end(PONG_PREFIX.len() + MAX_PAYLOAD)
        .await
        .map_err(|error| NetError::peer(peer, error))?;

    connection.close(0u32.into(), b"done");

    reply
        .strip_prefix(PONG_PREFIX)
        .map(<[u8]>::to_vec)
        .ok_or(NetError::MalformedReply { peer })
}
