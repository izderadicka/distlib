//! Where the *rest* of the group can be reached.
//!
//! [`crate::AddressBook`] holds the core group, because those are the only
//! addresses anything writes down: a `MemberRecord` carries an id, a name and a
//! pledge, and Raft's node map holds voters. A follower's address is recorded
//! nowhere, and the consequence was measured rather than guessed — a follower
//! asked to dial another follower by bare id answers `No addressing
//! information available`, with the group converging normally at the time
//! (P2-14). Everything the catalogue does between two followers was therefore
//! going through a core node.
//!
//! This is the other half: what members *say* about themselves, heard on the
//! group's gossip topic. Installed on the endpoint the same way and read by
//! iroh the same way — we write, iroh reads, and protocols we did not author
//! get to dial a bare id.
//!
//! **Kept apart from [`crate::AddressBook`] on purpose**, rather than widening
//! it. The two differ in both provenance and lifetime:
//!
//! * the book holds what the *log* committed — durable, agreed, and true for as
//!   long as the entry stands;
//! * this holds what a member *said a moment ago* — self-reported, and only the
//!   latest statement is wanted.
//!
//! Which is why this one **replaces** a member's addresses and the book merges
//! them. `MemoryLookup::add_endpoint_info` merges, so a laptop that moves
//! between networks would otherwise leave every address it ever had behind, to
//! be raced on every dial for the life of the process.
//!
//! Not a security boundary, any more than the book is. Being resolvable is not
//! being admitted: [`crate::hooks::AllowlistHooks`] refuses a non-member in
//! both directions and reads the log rather than this.

use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use distlib_core::{MemberId, NodeAddr, SignedAddress};
use iroh::{Endpoint, address_lookup::memory::MemoryLookup};

use crate::error::{NetError, Result};

/// The addresses members have announced for themselves.
///
/// Cheap to clone, and every clone shares one set — the same shape as
/// [`crate::AddressBook`] and [`crate::Allowlist`], for the same reason:
/// several tasks learn addresses and they must all land where the endpoint
/// reads.
#[derive(Debug, Clone, Default)]
pub struct Directory {
    lookup: MemoryLookup,
    /// The latest statement held for each member.
    ///
    /// Beside the lookup rather than in it: `MemoryLookup` answers iroh's
    /// question — "where is this id" — and this is the bookkeeping that decides
    /// which answers belong there, plus what this node can say about itself
    /// when asked who it can reach.
    heard: Arc<RwLock<BTreeMap<MemberId, u64>>>,
}

impl Directory {
    /// Installs a directory on an endpoint that is already bound.
    ///
    /// After the fact for the reason [`crate::AddressBook::install`] is: iroh's
    /// lookup services take `&self` and publish anything already known to a
    /// service added later, so whoever *learns* addresses can own this without
    /// every caller that builds an endpoint threading one through.
    pub fn install(endpoint: &Endpoint) -> Result<Self> {
        let directory = Self::default();
        endpoint
            .address_lookup()
            .map_err(|_| NetError::EndpointClosed)?
            .add(directory.lookup.clone());
        Ok(directory)
    }

    /// Records what `announced` says, if it is not older than what is held.
    ///
    /// Answers whether it was taken, which is what a caller logs or counts —
    /// not an error, because a statement arriving out of order is ordinary on a
    /// best-effort broadcast rather than a fault.
    ///
    /// **Verification happens here**, so there is no way into the directory
    /// that skips it: the address is only reachable through
    /// [`SignedAddress::addr`], which checks the signature first. Gossip
    /// relays these, so the peer that handed one over is almost never the
    /// member it is about.
    pub fn learn(&self, announced: &SignedAddress) -> Result<bool> {
        let member = announced.member();
        let addr = announced
            .addr()
            .map_err(|source| NetError::BadAddress { member, source })?;

        // An address with nothing in it means "find them some other way",
        // which is what happens anyway when nothing here matches — but it must
        // not silently retire a good address we already hold.
        if addr.relay.is_none() && addr.direct.is_empty() {
            return Ok(false);
        }
        // A relay url that will not parse came from a peer, so it is data
        // rather than a bug — the same judgement `AddressBook::learn` makes.
        // It does mean somebody is unreachable, and silence would make that
        // look like a network problem.
        let endpoint_addr = match addr.to_endpoint_addr(member) {
            Ok(endpoint_addr) => endpoint_addr,
            Err(error) => {
                tracing::warn!(%member, %error, "ignoring an announced address that will not parse");
                return Ok(false);
            }
        };

        // Held across the write, so two announcements arriving at once cannot
        // interleave into "the older one wins".
        let mut heard = self
            .heard
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if heard
            .get(&member)
            .is_some_and(|held| announced.applied() < *held)
        {
            return Ok(false);
        }

        // Replace rather than add. See the module docs: `add_endpoint_info`
        // merges, and a member that moves would otherwise keep every address it
        // has ever had.
        self.lookup.remove_endpoint_info(member.endpoint_id());
        self.lookup.add_endpoint_info(endpoint_addr);
        heard.insert(member, announced.applied());
        Ok(true)
    }

    /// The log position of what is held for `member`, if anything.
    pub fn position_of(&self, member: MemberId) -> Option<u64> {
        self.heard
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&member)
            .copied()
    }

    /// Where `member` was last heard to be — **read back out of the lookup**,
    /// not out of this type's own bookkeeping.
    ///
    /// Deliberately: this is what iroh would resolve `member` to, which is the
    /// only thing worth reporting and the only thing worth asserting. Answering
    /// from a copy beside the lookup would agree with itself while the lookup
    /// held something else entirely — which is exactly the bug the replace rule
    /// exists to prevent.
    pub fn address_of(&self, member: MemberId) -> Option<NodeAddr> {
        let held = iroh::EndpointAddr::from(self.lookup.get_endpoint_info(member.endpoint_id())?);
        Some(NodeAddr::from(&held))
    }
}
