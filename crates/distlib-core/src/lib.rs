//! Domain types shared across the `distlib` workspace.
//!
//! This crate holds the vocabulary every other crate speaks: identifiers, the
//! configuration model, the data directory layout and the core error type. It
//! deliberately has no knowledge of transport, storage or consensus.

pub mod config;
pub mod error;
pub mod id;
pub mod identity;
pub mod paths;

pub use config::{Config, ConsensusConfig, CoreMember, NetConfig, RelayMode};
pub use error::CoreError;
pub use id::{GroupId, ItemId, MemberId, RawMemberId};
pub use paths::DataDir;
