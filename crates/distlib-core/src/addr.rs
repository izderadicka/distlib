//! Where to reach a member.
//!
//! Lives here rather than beside the Raft types that first needed it because
//! nothing about it is specific to consensus: it answers "how do I dial this
//! member", which every protocol asks. `distlib/memberlog/0` already does, and
//! phase 1b's gossip and the later document and blob protocols will too.
//!
//! It is also the shape `[consensus] core` carries in the config file, so
//! keeping the two in one place is what stops them drifting.

use std::{collections::BTreeSet, net::SocketAddr};

use iroh::{EndpointAddr, RelayUrl};
use serde::{Deserialize, Serialize};

use crate::id::MemberId;

/// Where to reach a member.
///
/// Empty by default, which is a meaningful value rather than a placeholder: it
/// means "dial by member id alone and let iroh's address lookup find them",
/// which is what works when relays are enabled. The explicit addresses matter
/// for the cases where lookup is unavailable — a LAN, or `relay_mode =
/// "disabled"`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeAddr {
    /// Relay to reach this node through, as a URL.
    ///
    /// A `String` rather than an `iroh::RelayUrl` because this is persisted —
    /// in the Raft log, and in the config file — and sent over the wire: it
    /// should deserialise into something inspectable even if it no longer
    /// parses, rather than failing the whole entry. Parsing happens where it is
    /// dialled, which is why [`BadRelayUrl`] exists.
    pub relay: Option<String>,

    /// Socket addresses to try directly.
    ///
    /// A set, not a list, for three reasons that all point the same way. These
    /// values are persisted in the Raft log and compared across nodes, and
    /// openraft's `Node` bound requires `Eq`; with a `Vec`, the same two
    /// addresses listed in a different order would compare unequal and encode
    /// to different bytes, so openraft would see a membership change where
    /// nothing changed. A set also removes duplicates for free. Nothing is
    /// lost: iroh races the paths it is given rather than treating them as a
    /// preference order.
    pub direct: BTreeSet<SocketAddr>,
}

impl NodeAddr {
    /// A node reachable only through address lookup.
    pub fn lookup_only() -> Self {
        Self::default()
    }

    /// Adds a relay URL.
    pub fn with_relay(mut self, relay: impl Into<String>) -> Self {
        self.relay = Some(relay.into());
        self
    }

    /// Adds a directly dialable socket address.
    pub fn with_direct(mut self, addr: SocketAddr) -> Self {
        self.direct.insert(addr);
        self
    }

    /// Whether this carries no addressing at all.
    pub fn is_empty(&self) -> bool {
        self.relay.is_none() && self.direct.is_empty()
    }

    /// This address as something iroh can dial, for `member`.
    ///
    /// Every protocol that dials a member needs this, which is the reason the
    /// type is here rather than beside any one of them.
    ///
    /// An empty [`NodeAddr`] yields an address carrying only the member id,
    /// which is meaningful rather than broken — it means "find them by id",
    /// which works whenever address lookup is configured.
    pub fn to_endpoint_addr(&self, member: MemberId) -> Result<EndpointAddr, BadRelayUrl> {
        let mut addr = EndpointAddr::new(member.endpoint_id());
        for socket in &self.direct {
            addr = addr.with_ip_addr(*socket);
        }
        if let Some(url) = &self.relay {
            let relay: RelayUrl = url.parse().map_err(|_| BadRelayUrl {
                member,
                url: url.clone(),
            })?;
            addr = addr.with_relay_url(relay);
        }
        Ok(addr)
    }
}

/// A [`NodeAddr`] whose relay URL no longer parses.
///
/// Its own error because the URL is stored as a `String` — a log entry or a
/// config file must stay readable even when one field in it has stopped making
/// sense — so the parse happens at dial time and can fail there.
#[derive(Debug, thiserror::Error)]
#[error("member {member} lists an unparseable relay url: {url}")]
pub struct BadRelayUrl {
    pub member: MemberId,
    pub url: String,
}
