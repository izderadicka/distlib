//! The error type for everything in `distlib-consensus`.

use distlib_core::MemberId;
use thiserror::Error;

/// The result of any fallible operation in this crate.
pub type Result<T> = std::result::Result<T, ConsensusError>;

/// Why an event could not be produced or applied.
///
/// Every variant here means "this entry does not belong in the state", and a
/// node that sees one has been handed something invalid. They are worth
/// distinguishing because they say very different things about *who* is at
/// fault — a bad signature implicates whoever served it, a rule violation
/// implicates the proposer.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConsensusError {
    /// The signature does not match the proposer's key.
    #[error("event carries a signature that {proposer} did not produce")]
    BadSignature { proposer: MemberId },

    /// An event arrived before the group was founded.
    #[error("the group has not been founded yet")]
    NotFounded,

    /// A second `GroupFounded` arrived.
    #[error("the group is already founded")]
    AlreadyFounded,

    /// The founding event did not include the member who signed it.
    #[error("{proposer} founded a group without being one of its founders")]
    FounderNotIncluded { proposer: MemberId },

    /// A group was founded with nobody in it.
    #[error("a group must be founded with at least one member")]
    NoFounders,

    /// The proposer is not currently a member.
    ///
    /// The check that keeps the log closed: only people already inside the
    /// group can change who is inside it.
    #[error("{proposer} is not a member and cannot propose changes")]
    ProposerNotAMember { proposer: MemberId },

    /// The event refers to somebody who is not a member.
    #[error("{member} is not a member")]
    UnknownMember { member: MemberId },

    /// A core group was proposed containing a non-member, or nobody at all.
    #[error("the core group must be a non-empty subset of the membership")]
    InvalidCoreGroup,

    /// Serialising an event failed.
    #[error("could not encode an event")]
    Encode {
        #[from]
        source: postcard::Error,
    },
}
