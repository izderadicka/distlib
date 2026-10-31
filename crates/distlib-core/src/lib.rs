//! Domain types shared across the `distlib` workspace.
//!
//! This crate holds the vocabulary every other crate speaks: identifiers, the
//! configuration model, the data directory layout and the core error type. It
//! deliberately has no knowledge of transport, storage or consensus.

pub mod addr;
pub mod availability;
pub mod catalogue;
pub mod community;
pub mod config;
pub mod custody;
pub mod error;
pub mod event;
pub mod group_settings;
pub mod id;
pub mod identity;
pub mod keeping;
pub mod paths;
pub mod private_file;
pub mod ticket;
pub mod token;

pub use addr::{BadRelayUrl, NodeAddr, SignedAddress};
pub use availability::{GOSSIP_MAX_MESSAGE, Heartbeat, Holdings, SignedHeartbeat};
pub use catalogue::{
    Absorbed, Field, FileRecord, FileRole, Item, ItemFields, ItemKind, Key, Series,
};
pub use community::{
    Bookmark, BookmarkId, COMMENT_MAX_BYTES, Comment, CommunityKey, NOTE_MAX_BYTES,
    POSITION_MAX_BYTES, ProbeFailure, REVIEW_MAX_BYTES, Rating, ResolvedWish, Review,
    WISH_MAX_BYTES, Wish, WishFields, WishId, WishStatus,
};
pub use config::{
    ApiConfig, AvailabilityConfig, Config, ConsensusConfig, CoreMember, LibraryConfig, NetConfig,
    RelayMode, StorageConfig, TransferConfig,
};
pub use custody::{CustodyGroup, CustodyItem, WeightRule};
pub use error::CoreError;
pub use event::{Event, Progress, TaskId};
pub use group_settings::{GroupSettings, PledgeStanding};
pub use id::{ContentHash, GroupId, ItemId, MemberId, RawMemberId};
pub use keeping::{Evictions, KeepReason, KeptBlobs, StorageUse};
pub use paths::{DATA_FORMAT, DataDir};
pub use ticket::Ticket;
