//! Where the group's members can be reached.
//!
//! iroh dials an [`iroh::EndpointId`] only if it can turn one into an address:
//! either the caller supplies it, or an address lookup service resolves it.
//! Our own protocols always supply one — they carry a [`NodeAddr`] beside every
//! member id — but a protocol we do not author cannot. iroh-gossip subscribes
//! with bare ids, and with `relay_mode = "disabled"` there is no lookup
//! configured at all, so the only peers it could reach were the ones the
//! endpoint had already spoken to for some other reason. That is not a mesh; it
//! is whatever graph consensus happened to build.
//!
//! So this is the same knowledge, put where anything can use it: every address
//! this node learns — from `[consensus] core`, from a ticket, from Raft's
//! membership, from what a core node says when it serves the log — is written
//! here, and iroh resolves against it.
//!
//! Not a security boundary, and it does not need to be. Being resolvable is not
//! being admitted: [`crate::hooks::AllowlistHooks`] refuses a peer that is not a
//! member in both directions, and it reads the log rather than this. The worst a
//! wrong entry does is waste a dial.

use distlib_core::{MemberId, NodeAddr};
use iroh::{Endpoint, address_lookup::memory::MemoryLookup};

use crate::error::{NetError, Result};

/// The addresses this node knows, as iroh will resolve them.
///
/// Cheap to clone, and every clone writes to the same set — the same shape as
/// [`crate::Allowlist`], for the same reason: several tasks learn addresses and
/// they must all land in the one place the endpoint reads.
#[derive(Debug, Clone, Default)]
pub struct AddressBook(MemoryLookup);

impl AddressBook {
    /// Installs an address book on an endpoint that is already bound.
    ///
    /// After the fact rather than through the builder so that whoever *learns*
    /// addresses can own the book, without every caller that builds an endpoint
    /// having to thread one through. iroh allows it: the lookup services take
    /// `&self` and publish anything already known to a service added later.
    pub fn install(endpoint: &Endpoint) -> Result<Self> {
        let book = Self::default();
        endpoint
            .address_lookup()
            .map_err(|_| NetError::EndpointClosed)?
            .add(book.0.clone());
        Ok(book)
    }

    /// Records where `member` can be reached.
    ///
    /// An address with nothing in it is skipped rather than stored: it means
    /// "find them some other way", which is what happens anyway when nothing
    /// here matches.
    pub fn learn(&self, member: MemberId, addr: &NodeAddr) {
        if addr.relay.is_none() && addr.direct.is_empty() {
            return;
        }
        match addr.to_endpoint_addr(member) {
            Ok(addr) => self.0.add_endpoint_info(addr),
            // The address came from a peer or a config file, so this is data
            // rather than a bug — but it means somebody is unreachable, and
            // silence would make that look like a network problem.
            Err(error) => {
                tracing::warn!(%member, %error, "ignoring an address that will not parse")
            }
        }
    }

    /// Records where each of `members` can be reached.
    pub fn learn_all<'a>(&self, members: impl IntoIterator<Item = (MemberId, &'a NodeAddr)>) {
        for (member, addr) in members {
            self.learn(member, addr);
        }
    }
}
