//! The membership log: the only replicated state that consensus decides.
//!
//! A group is defined by an append-only log of signed events. Every node folds
//! that log into a [`MembershipState`] and *derives* from it what it enforces —
//! the connection allowlist, the pledge table, the set of Raft voters. Nothing
//! here is configured; the log is the only source.
//!
//! This crate deliberately splits into a pure part and, from the next PR, an
//! openraft part. Everything in this module — the events, their signatures and
//! the projection — is a pure function of its inputs, so it can be tested
//! exhaustively without anything being able to fail for a network reason.

pub mod error;
pub mod event;
pub mod node;
pub mod raft;
pub mod signed;
pub mod state;

pub use error::{ConsensusError, Result};
pub use event::{MemberRecord, MembershipEvent, Timestamp};
pub use node::{MembershipNode, RAFT_DB, alpns};
pub use raft::{
    FetchFailed, Fetched, LogStore, MemberlogClient, MemberlogProtocol, ProposeError,
    ProposeOutcome, RaftClient, RaftNetworkFactoryImpl, RaftProtocol, Source, StateMachineStore,
    TypeConfig,
};
pub use signed::SignedEvent;
pub use state::MembershipState;
