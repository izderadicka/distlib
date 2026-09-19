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

use tokio::sync::watch;

use distlib_core::{MemberId, NodeAddr, SignedAddress};
use iroh::{Endpoint, address_lookup::memory::MemoryLookup};

use crate::error::{NetError, Result};

/// The addresses members have announced for themselves.
///
/// Cheap to clone, and every clone shares one set — the same shape as
/// [`crate::AddressBook`] and [`crate::Allowlist`], for the same reason:
/// several tasks learn addresses and they must all land where the endpoint
/// reads.
#[derive(Debug, Clone)]
pub struct Directory {
    lookup: MemoryLookup,
    /// The latest statement held for each member.
    ///
    /// Beside the lookup rather than in it: `MemoryLookup` answers iroh's
    /// question — "where is this id" — and this is the bookkeeping that decides
    /// which answers belong there, plus what this node can say about itself
    /// when asked who it can reach.
    ///
    /// **The whole statement, not just the position it carries.** A core node
    /// answers [`Self::everything`] for the group (2a-5), and what it hands over
    /// has to be checkable by whoever asked — a signature the asker verifies
    /// itself, not a claim it has to take on trust. Keeping only `applied` here
    /// would mean a core node could only relay bare addresses, and believing a
    /// core node about where a *follower* is is precisely the trust this design
    /// does not ask for. Still O(members): one statement each, replaced in
    /// place.
    heard: Arc<RwLock<BTreeMap<MemberId, SignedAddress>>>,
    /// Bumped whenever an answer here *changes* — never when one is restated.
    ///
    /// Because knowing where somebody is only matters to whoever wanted to
    /// reach them, and they have usually already given up: iroh-docs hands its
    /// document's peers to gossip once, and a peer that could not be resolved
    /// at that moment is not retried. So this is the signal to offer the peer
    /// set again, and to say where this node is to somebody who has evidently
    /// only just heard of it.
    ///
    /// **Both of those are expensive enough that "we already knew that" must
    /// not fire them.** One offer dials every peer in the set, and one
    /// announcement is a broadcast that every neighbour receives whether or not
    /// it is worth anything. Restating an address we hold is news to nobody, and
    /// a node that took it as news would give its neighbours a reason to restate
    /// theirs — which is a group that never stops talking.
    learned: Arc<watch::Sender<u64>>,
}

impl Default for Directory {
    fn default() -> Self {
        Self {
            lookup: MemoryLookup::default(),
            heard: Arc::default(),
            learned: Arc::new(watch::channel(0).0),
        }
    }
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
    /// Answers whether this node's picture of the group **changed** — not
    /// whether the statement was acceptable. Those differ for the commonest
    /// statement there is: a member restating an address we already hold, which
    /// is accepted (it must be — see [`SignedAddress::applied`], where an equal
    /// position ties rather than losing) and tells us nothing. Saying "changed"
    /// to that would wake everything waiting on [`Self::learned`] for no reason.
    ///
    /// Not an error either way: a statement arriving out of order is ordinary on
    /// a best-effort broadcast rather than a fault.
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
            .is_some_and(|held| announced.applied() < held.applied())
        {
            return Ok(false);
        }

        // **Is this actually news?** Compared as `EndpointAddr`s — the form the
        // lookup stores and the form iroh resolves — rather than as `NodeAddr`s:
        // a relay url that went in as a string and came back through `RelayUrl`
        // can be normalised on the way, and an address that compared unequal to
        // itself would defeat the whole point of asking. `EndpointAddr` is `Eq`
        // over a `BTreeSet`, so this is exact and does not care about order.
        //
        // Read back out of the lookup for the reason `address_of` is: that is
        // what iroh would answer, and bookkeeping kept beside it would agree
        // with itself while the lookup held something else.
        let held = self
            .lookup
            .get_endpoint_info(member.endpoint_id())
            .map(iroh::EndpointAddr::from);
        if held.as_ref() == Some(&endpoint_addr) {
            // The position still advances: it is what supersession is judged on,
            // and letting it go stale would make a later statement from this
            // member look older than it is.
            heard.insert(member, announced.clone());
            return Ok(false);
        }

        // Replace rather than add. See the module docs: `add_endpoint_info`
        // merges, and a member that moves would otherwise keep every address it
        // has ever had.
        self.lookup.remove_endpoint_info(member.endpoint_id());
        self.lookup.add_endpoint_info(endpoint_addr);
        heard.insert(member, announced.clone());
        // `send_modify` rather than `send`: there may be no subscriber, and a
        // directory that refused to record anything because nobody was
        // listening would be a strange thing indeed.
        self.learned
            .send_modify(|learned| *learned = learned.wrapping_add(1));
        Ok(true)
    }

    /// Fires whenever this node learns where somebody is.
    ///
    /// Carries a count rather than what was learned, because the only question
    /// a subscriber asks is "is there anything new to try" — and a subscriber
    /// that was busy while three announcements landed should wake once.
    pub fn learned(&self) -> watch::Receiver<u64> {
        self.learned.subscribe()
    }

    /// The log position of what is held for `member`, if anything.
    pub fn position_of(&self, member: MemberId) -> Option<u64> {
        self.heard
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&member)
            .map(SignedAddress::applied)
    }

    /// Every statement this node currently holds, for relaying to somebody who
    /// has none.
    ///
    /// The statements themselves, so the answer stands on its own: whoever
    /// receives this verifies each signature and is believing the member, not
    /// the node that passed it along. See the memberlog protocol's
    /// `Request::Directory`, which is the only caller — a core node answering
    /// for the group.
    ///
    /// Everything held, unfiltered. Who is still a member is a question about
    /// the log, and the log is not this type's business: the caller intersects
    /// this with the allowlist it already has open.
    pub fn everything(&self) -> Vec<SignedAddress> {
        self.heard
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
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
