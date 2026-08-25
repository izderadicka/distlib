//! The error type for everything in `distlib-net`.

use std::time::Duration;

use distlib_core::MemberId;
use iroh::endpoint::{BindError, ConnectError, ConnectWithOptsError, ConnectionError};

use crate::hooks::close_code;
use thiserror::Error;

/// The result of any fallible operation in this crate.
pub type Result<T> = std::result::Result<T, NetError>;

/// Errors from binding, dialling or talking to another member.
#[derive(Debug, Error)]
pub enum NetError {
    /// The local endpoint could not be bound.
    #[error("could not bind the endpoint")]
    Bind(#[from] Box<BindError>),

    /// Dialling a member failed before a connection existed.
    #[error("could not connect to {peer}")]
    Connect {
        peer: MemberId,
        #[source]
        source: Box<ConnectError>,
    },

    /// An established connection failed.
    #[error("connection to {peer} failed")]
    Connection {
        peer: MemberId,
        #[source]
        source: Box<ConnectionError>,
    },

    /// A stream on an established connection failed.
    #[error("stream to {peer} failed")]
    Stream {
        peer: MemberId,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// The peer did not answer in time.
    #[error("{peer} did not answer within {timeout:?}")]
    Timeout { peer: MemberId, timeout: Duration },

    /// Refused by an allowlist — ours or the peer's.
    ///
    /// Distinct from a network failure because the caller should stop rather
    /// than retry: nothing about waiting or trying another path will help.
    #[error("{peer} is not a member, or does not consider us one")]
    Rejected { peer: MemberId },

    /// The peer answered, but not with something this protocol recognises.
    #[error("{peer} sent a reply this protocol does not recognise")]
    MalformedReply { peer: MemberId },

    /// The caller asked to send more than the protocol permits.
    #[error("payload is {len} bytes; the maximum is {max}")]
    PayloadTooLarge { len: usize, max: usize },

    /// A relay URL in the configuration could not be parsed.
    #[error("invalid relay url {url}")]
    InvalidRelayUrl { url: String },

    /// `relay_mode = \"custom\"` was set with no relays listed.
    #[error("relay_mode is \"custom\" but relay_urls is empty")]
    NoCustomRelays,
}

// The iroh error types are large enough that returning them unboxed would bloat
// every Result in the crate; clippy's `result_large_err` agrees. Manual `From`
// impls keep `?` working at the call sites.
impl From<BindError> for NetError {
    fn from(source: BindError) -> Self {
        Self::Bind(Box::new(source))
    }
}

impl NetError {
    pub(crate) fn connect(peer: MemberId) -> impl FnOnce(ConnectError) -> Self {
        move |source| {
            rejection(peer, &source).unwrap_or_else(|| Self::Connect {
                peer,
                source: Box::new(source),
            })
        }
    }

    pub(crate) fn connection(peer: MemberId) -> impl FnOnce(ConnectionError) -> Self {
        move |source| {
            rejection(peer, &source).unwrap_or_else(|| Self::Connection {
                peer,
                source: Box::new(source),
            })
        }
    }

    pub(crate) fn stream<E>(peer: MemberId) -> impl FnOnce(E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        move |source| {
            rejection(peer, &source).unwrap_or_else(|| Self::Stream {
                peer,
                source: Box::new(source),
            })
        }
    }
}

/// Recognises an allowlist rejection anywhere in an error chain.
///
/// Two shapes mean the same thing to a caller, and neither is tied to a fixed
/// call site:
///
/// * `ConnectWithOptsError::LocallyRejected` — *our* hook refused to dial,
///   before any packet left the machine.
/// * `ConnectionError::ApplicationClosed` carrying [`close_code::NOT_A_MEMBER`]
///   — the *remote* refused us after the handshake. Because that arrives after
///   the connection appears established, the initiator may well see `connect`
///   succeed and fail on its first stream operation instead.
///
/// Matching wherever it appears in the chain keeps callers out of that timing
/// detail; asserting on which call returned the error would pin an iroh
/// internal.
fn rejection(peer: MemberId, error: &(dyn std::error::Error + 'static)) -> Option<NetError> {
    let mut current = Some(error);
    while let Some(error) = current {
        if matches!(
            error.downcast_ref::<ConnectWithOptsError>(),
            Some(ConnectWithOptsError::LocallyRejected { .. })
        ) {
            return Some(NetError::Rejected { peer });
        }
        if let Some(ConnectionError::ApplicationClosed(close)) =
            error.downcast_ref::<ConnectionError>()
            && close.error_code == close_code::NOT_A_MEMBER
        {
            return Some(NetError::Rejected { peer });
        }
        current = error.source();
    }
    None
}
