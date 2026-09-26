//! Domain types shared across the `distlib` workspace.
//!
//! This crate holds the vocabulary every other crate speaks: identifiers, the
//! configuration model, the data directory layout and the core error type. It
//! deliberately has no knowledge of transport, storage or consensus.

pub mod addr;
pub mod catalogue;
pub mod config;
pub mod error;
pub mod event;
pub mod id;
pub mod identity;
pub mod paths;
pub mod private_file;
pub mod ticket;
pub mod token;

pub use addr::{BadRelayUrl, NodeAddr, SignedAddress};
pub use catalogue::{Absorbed, Field, FileRecord, FileRole, Item, ItemKind, Key, Series};
pub use config::{ApiConfig, Config, ConsensusConfig, CoreMember, NetConfig, RelayMode};
pub use error::CoreError;
pub use event::Event;
pub use id::{ContentHash, GroupId, ItemId, MemberId, RawMemberId};
pub use paths::DataDir;
pub use ticket::Ticket;
