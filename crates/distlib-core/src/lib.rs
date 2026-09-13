//! Domain types shared across the `distlib` workspace.
//!
//! This crate holds the vocabulary every other crate speaks: identifiers, the
//! configuration model, the data directory layout and the core error type. It
//! deliberately has no knowledge of transport, storage or consensus.

pub mod addr;
pub mod catalogue;
pub mod config;
pub mod error;
pub mod id;
pub mod identity;
pub mod paths;
pub mod private_file;
pub mod ticket;
pub mod token;
pub mod whereabouts;

pub use addr::{BadRelayUrl, NodeAddr};
pub use catalogue::{Absorbed, Field, FileRecord, FileRole, Item, ItemKind, Key, Series};
pub use config::{ApiConfig, Config, ConsensusConfig, CoreMember, NetConfig, RelayMode};
pub use error::CoreError;
pub use id::{ContentHash, GroupId, ItemId, MemberId, RawMemberId};
pub use paths::DataDir;
pub use ticket::Ticket;
pub use whereabouts::SignedAddress;
