//! Attribution for log entries.
//!
//! §4.2 calls the log "signed events" and has followers verify what the core
//! serves them. The transport already authenticates *who handed us the log* —
//! iroh connections are mutually authenticated by member key — so what a
//! signature adds is attribution of the entry itself: a compromised core node
//! cannot invent a `MemberExpelled` and attribute it to someone else.
//!
//! It does not defend against a core node that simply refuses to serve entries,
//! or serves a stale prefix. That is Raft's problem, and it is outside §2's
//! threat model in any case.

use distlib_core::MemberId;
use iroh::{SecretKey, Signature};
use serde::{Deserialize, Serialize};

use crate::{
    error::{ConsensusError, Result},
    event::{MembershipEvent, Timestamp},
};

/// Domain separation for membership signatures.
///
/// Versioned, so a change to the encoding below is a new tag rather than a
/// silent change of meaning — the same reason `ItemId` carries one. Note the
/// pre-image includes `MemberId`s in their serde form (a string), so changing
/// how a member id renders would change every signature; that is what bumping
/// this tag is for.
const SIGNING_DOMAIN: &[u8] = b"distlib.membership.v1";

/// A membership event, with the member who proposed it and their signature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedEvent {
    event: MembershipEvent,
    proposer: MemberId,
    at: Timestamp,
    /// The membership this was proposed against — see
    /// [`crate::MembershipState::changed_at`].
    ///
    /// Signed along with the rest, so it is the proposer's own statement about
    /// what they were looking at and cannot be rewritten in transit to make a
    /// stale proposal look current.
    changed_at: u64,
    signature: Signature,
}

impl SignedEvent {
    /// Signs `event` as proposed by the holder of `secret_key`, against the
    /// membership as of `changed_at`.
    pub fn sign(
        secret_key: &SecretKey,
        event: MembershipEvent,
        at: Timestamp,
        changed_at: u64,
    ) -> Result<Self> {
        let proposer = MemberId::from(secret_key.public());
        let payload = signing_payload(&event, &proposer, at, changed_at)?;
        Ok(Self {
            signature: secret_key.sign(&payload),
            event,
            proposer,
            at,
            changed_at,
        })
    }

    /// Checks the signature against the proposer's key.
    ///
    /// Useful on its own at a storage or transport boundary, where an entry is
    /// worth rejecting before it is written down but nothing needs to look
    /// inside it yet.
    pub fn verify(&self) -> Result<()> {
        let payload = signing_payload(&self.event, &self.proposer, self.at, self.changed_at)?;
        self.proposer
            .as_public_key()
            .verify(&payload, &self.signature)
            .map_err(|_| ConsensusError::BadSignature {
                proposer: self.proposer,
            })
    }

    /// The event, once its signature checks out.
    ///
    /// The only way to reach the event, so there is no path that reads one
    /// without verifying it first — a caller cannot forget. Verification is an
    /// ed25519 check on a short message, and the membership log is small, so
    /// paying it per access is not worth an escape hatch.
    pub fn event(&self) -> Result<&MembershipEvent> {
        self.verify()?;
        Ok(&self.event)
    }

    /// The member who proposed this event. Only meaningful once verified.
    pub fn proposer(&self) -> MemberId {
        self.proposer
    }

    /// When the proposer says they proposed it. See [`Timestamp`] — advisory.
    pub fn at(&self) -> Timestamp {
        self.at
    }

    /// The membership this was proposed against. Not advisory — enforced.
    pub fn changed_at(&self) -> u64 {
        self.changed_at
    }
}

/// The exact bytes a signature covers.
///
/// `SIGNING_DOMAIN` first, then a postcard encoding of everything the signature
/// must bind: the event, who proposed it, when, and the membership it was
/// proposed against. postcard is canonical for a given type, so signing and
/// verifying agree without a separate normalisation step.
fn signing_payload(
    event: &MembershipEvent,
    proposer: &MemberId,
    at: Timestamp,
    changed_at: u64,
) -> Result<Vec<u8>> {
    let payload = SIGNING_DOMAIN.to_vec();
    Ok(postcard::to_extend(
        &(event, proposer, at, changed_at),
        payload,
    )?)
}
