//! A member's own statement of where it can be reached.
//!
//! **Why this is signed when the log announcement beside it is not.** Both
//! travel on the group's gossip topic, and `Announcement::Applied` is
//! deliberately unsigned because lying about it costs a peer one wasted fetch.
//! An address is different in kind: it is the thing other nodes will *dial*.
//! And the obvious cheap check is not available — iroh-gossip's
//! `Message::delivered_from` is documented as "not the same as the original
//! author", because in an epidemic broadcast most messages arrive relayed. So
//! attributing an address to the member it is about cannot be done by looking
//! at who handed it over; the statement has to carry its own proof.
//!
//! Signed by the member's own key, so it is self-authenticating however many
//! hops it took, and a core node that serves these on to others (the group's
//! seniors answering for it) can neither forge one nor tamper with one. That
//! is what lets the directory be a service rather than a trusted authority.

use iroh::{SecretKey, Signature};
use serde::{Deserialize, Serialize};

use crate::{
    addr::NodeAddr,
    error::{CoreError, Result},
    id::MemberId,
};

/// Domain separation for address signatures.
///
/// Versioned for the same reason [`crate::id::ItemId`]'s tag is: a change to
/// the pre-image below is a new tag rather than a silent change of meaning.
/// Without a bump, a statement signed by an older build would fail as a bad
/// signature — indistinguishable from tampering — rather than as the version
/// mismatch it is.
const SIGNING_DOMAIN: &[u8] = b"distlib.address.v1";

/// Where a member says it is, signed by that member.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedAddress {
    member: MemberId,
    addr: NodeAddr,
    applied: u64,
    signature: Signature,
}

impl SignedAddress {
    /// Signs `addr` as the holder of `secret_key`, as of log position
    /// `applied`.
    ///
    /// See [`Self::applied`] for what that number is for — it is the whole of
    /// this type's freshness story, and it is not a clock.
    pub fn sign(secret_key: &SecretKey, addr: NodeAddr, applied: u64) -> Result<Self> {
        let member = MemberId::from(secret_key.public());
        let payload = signing_payload(&member, &addr, applied)?;
        Ok(Self {
            signature: secret_key.sign(&payload),
            member,
            addr,
            applied,
        })
    }

    /// Checks the signature against the member it names.
    ///
    /// Worth having on its own at a transport boundary, where a statement is
    /// worth rejecting before anything is done with it.
    pub fn verify(&self) -> Result<()> {
        let payload = signing_payload(&self.member, &self.addr, self.applied)?;
        self.member
            .as_public_key()
            .verify(&payload, &self.signature)
            .map_err(|_| CoreError::BadAddressSignature {
                member: self.member,
            })
    }

    /// The address, once its signature checks out.
    ///
    /// The only way to reach it, so there is no path that reads one without
    /// verifying first — a caller cannot forget. The same arrangement
    /// `SignedEvent::event` uses, for the same reason.
    pub fn addr(&self) -> Result<&NodeAddr> {
        self.verify()?;
        Ok(&self.addr)
    }

    /// Who this is about. Only meaningful once verified.
    pub fn member(&self) -> MemberId {
        self.member
    }

    /// The announcer's applied log position when it said this.
    ///
    /// **The freshness rule, and deliberately not a clock.** A statement is
    /// superseded only by one from the same member naming an `applied` that is
    /// not lower; positions are never compared *between* members. The log index
    /// is the one number every member agrees on the meaning of, and unlike a
    /// self-reported time it cannot run backwards: a node's own applied
    /// position only advances, across restarts included.
    ///
    /// A wall-clock timestamp was the obvious alternative and is worse in a way
    /// that bites without an adversary. A clock that jumps back — a restored
    /// VM, an NTP correction, a board with no RTC — makes a node's *newest*
    /// statement sort older than the one its peers already hold, so it is
    /// discarded and the node stays unreachable at its new address until the
    /// clock catches up. It would also be the second clock P1-3 and D3 both
    /// rejected.
    ///
    /// What this buys is narrower than a total order, and that is the point:
    /// within one stretch of an unchanging membership the position does not
    /// move, so two statements from one member tie and the later arrival wins.
    /// The only thing that slips through is a replayed address from the same
    /// stretch, which the next re-announcement corrects.
    pub fn applied(&self) -> u64 {
        self.applied
    }
}

/// The exact bytes a signature covers.
///
/// The domain tag first, then a postcard encoding of everything the signature
/// must bind. postcard is canonical for a given type, so signing and verifying
/// agree without a separate normalisation step.
fn signing_payload(member: &MemberId, addr: &NodeAddr, applied: u64) -> Result<Vec<u8>> {
    let payload = SIGNING_DOMAIN.to_vec();
    Ok(postcard::to_extend(&(member, addr, applied), payload)?)
}
