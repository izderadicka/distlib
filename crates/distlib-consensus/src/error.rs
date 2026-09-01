//! The error type for everything in `distlib-consensus`.

use distlib_core::MemberId;
use thiserror::Error;

/// The result of any fallible operation in this crate.
pub type Result<T> = std::result::Result<T, ConsensusError>;

/// Why an event could not be produced or applied.
///
/// Serialisable because it is what the state machine reports back to whoever
/// proposed the event, which may be another node.
///
/// Every variant here means "this entry does not belong in the state", and a
/// node that sees one has been handed something invalid. They are worth
/// distinguishing because they say very different things about *who* is at
/// fault — a bad signature implicates whoever served it, a rule violation
/// implicates the proposer.
#[derive(Debug, Error, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

    /// The same member appeared twice in a founder set.
    ///
    /// Rejected rather than collapsed because the group id is derived from the
    /// founder list: a duplicate would leave the id describing a different set
    /// from the membership the event establishes.
    #[error("{member} appears more than once in the founder set")]
    DuplicateFounder { member: MemberId },

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

    /// A member tried to set somebody else's pledge.
    #[error("{proposer} cannot set the pledge of {member}; a pledge is the member's own")]
    PledgeNotOwn {
        proposer: MemberId,
        member: MemberId,
    },

    /// A non-core member tried to change the voter set.
    #[error("{proposer} is not a core member and cannot change the core group")]
    NotCoreMember { proposer: MemberId },

    /// The proposal was made against a membership that has since changed.
    #[error(
        "the group changed at index {current} but this was proposed against {seen}; \
         the proposer's view is out of date"
    )]
    StaleProposal { seen: u64, current: u64 },

    /// Serialising an event failed.
    ///
    /// Carries the message rather than `postcard::Error`, because this type is
    /// the verdict Raft returns to a proposer and so has to cross the wire —
    /// and because a domain error has no business naming a codec's type.
    #[error("could not encode an event: {message}")]
    Encode { message: String },
}

impl From<postcard::Error> for ConsensusError {
    fn from(source: postcard::Error) -> Self {
        Self::Encode {
            message: source.to_string(),
        }
    }
}
