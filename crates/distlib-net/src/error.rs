//! The error type for everything in `distlib-net`.

use std::time::Duration;

use distlib_core::MemberId;
use iroh::endpoint::{
    BindError, ClosedStream, ConnectError, ConnectWithOptsError, ConnectionError, ReadError,
    ReadToEndError, WriteError,
};
use thiserror::Error;

use crate::hooks::close_code;

/// The result of any fallible operation in this crate.
pub type Result<T> = std::result::Result<T, NetError>;

/// Errors from binding, dialling or talking to another member.
#[derive(Debug, Error)]
pub enum NetError {
    /// The local endpoint could not be bound.
    #[error("could not bind the endpoint")]
    Bind(#[from] Box<BindError>),

    /// Talking to a member failed.
    ///
    /// Deliberately not split by which call failed — connecting, opening a
    /// stream and reading a reply are all "we could not complete an exchange
    /// with this peer", and no caller can act differently on the distinction.
    /// The one that matters, [`Self::Rejected`], is separate. The underlying
    /// cause stays reachable through [`std::error::Error::source`].
    #[error("communication with {peer} failed")]
    Peer {
        peer: MemberId,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Refused by an allowlist — ours or the peer's.
    ///
    /// Distinct from a network failure because the caller should stop rather
    /// than retry: nothing about waiting or trying another path will help.
    #[error("{peer} is not a member, or does not consider us one")]
    Rejected { peer: MemberId },

    /// The peer did not answer in time.
    #[error("{peer} did not answer within {timeout:?}")]
    Timeout { peer: MemberId, timeout: Duration },

    /// The peer answered, but not with something this protocol recognises.
    #[error("{peer} sent a reply this protocol does not recognise")]
    MalformedReply { peer: MemberId },

    /// The caller asked to send more than the protocol permits.
    #[error("payload is {len} bytes; the maximum is {max}")]
    PayloadTooLarge { len: usize, max: usize },

    /// A relay URL in the configuration could not be parsed.
    #[error("invalid relay url {url}")]
    InvalidRelayUrl { url: String },

    /// `relay_mode = "custom"` was set with no relays listed.
    #[error("relay_mode is \"custom\" but relay_urls is empty")]
    NoCustomRelays,
}

// `BindError` is large enough that returning it unboxed would bloat every
// Result in the crate; clippy's `result_large_err` agrees. A manual `From`
// keeps `?` working at the call sites.
impl From<BindError> for NetError {
    fn from(source: BindError) -> Self {
        Self::Bind(Box::new(source))
    }
}

impl NetError {
    /// Classifies a failure from talking to `peer`.
    ///
    /// The single point where a transport error becomes a `NetError`, so the
    /// "was this a policy refusal?" question is asked once rather than at every
    /// call site.
    pub(crate) fn peer<E>(peer: MemberId, source: E) -> Self
    where
        E: IsRejection + std::error::Error + Send + Sync + 'static,
    {
        if source.is_rejection() {
            Self::Rejected { peer }
        } else {
            Self::Peer {
                peer,
                source: Box::new(source),
            }
        }
    }
}

/// Whether a transport failure means "refused by an allowlist".
///
/// Two shapes mean that, and which one surfaces is not fixed. Our own
/// `before_connect` hook refuses to dial before a packet leaves the machine; a
/// remote's `after_handshake` hook closes the connection *after* it looks
/// established, so the initiator may well see `connect` succeed and fail on its
/// first stream operation instead. Callers should not have to know which.
///
/// Implemented per concrete error type rather than by downcasting an
/// `Error` chain, so that a change in iroh's error shape breaks the build
/// instead of silently ceasing to recognise rejections — a failure that would
/// leave enforcement working while its reporting quietly degraded.
pub(crate) trait IsRejection {
    fn is_rejection(&self) -> bool;
}

/// The base case: every other implementation eventually delegates here.
impl IsRejection for ConnectionError {
    fn is_rejection(&self) -> bool {
        matches!(self, Self::ApplicationClosed(close) if close.error_code == close_code::NOT_A_MEMBER)
    }
}

impl IsRejection for ConnectError {
    fn is_rejection(&self) -> bool {
        match self {
            Self::Connect { source, .. } => source.is_rejection(),
            Self::Connection { source, .. } => source.is_rejection(),
            // `#[non_exhaustive]`, so a catch-all is unavoidable here.
            _ => false,
        }
    }
}

impl IsRejection for ConnectWithOptsError {
    fn is_rejection(&self) -> bool {
        // Our own hook declined to dial a non-member.
        matches!(self, Self::LocallyRejected { .. })
    }
}

impl IsRejection for WriteError {
    fn is_rejection(&self) -> bool {
        match self {
            Self::ConnectionLost(source) => source.is_rejection(),
            Self::Stopped(_) | Self::ClosedStream | Self::ZeroRttRejected => false,
        }
    }
}

impl IsRejection for ReadToEndError {
    fn is_rejection(&self) -> bool {
        match self {
            Self::Read(source) => source.is_rejection(),
            Self::TooLong => false,
        }
    }
}

impl IsRejection for ReadError {
    fn is_rejection(&self) -> bool {
        match self {
            Self::ConnectionLost(source) => source.is_rejection(),
            Self::Reset(_) | Self::ClosedStream | Self::ZeroRttRejected => false,
        }
    }
}

impl IsRejection for ClosedStream {
    fn is_rejection(&self) -> bool {
        // Finishing a stream that is already closed says nothing about why.
        false
    }
}
