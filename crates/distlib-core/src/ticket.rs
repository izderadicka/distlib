//! What somebody needs to join a group they have been admitted to.
//!
//! §4.3 step 4: "New member receives a join ticket (group ID + core node
//! addresses + relay config), connects, fetches the log, starts syncing." This
//! is that ticket.
//!
//! It is **not** a credential, and holding one grants nothing. Admission
//! happens in the log — somebody proposes a `MemberAdded` and the group commits
//! it — and until that entry exists, a ticket-holder is refused at the
//! allowlist like anybody else. What a ticket carries is *directions*: which
//! group, which nodes to ask, and how to reach them. That is why it can be
//! pasted into a chat window without ceremony.
//!
//! One line rather than a block of TOML, because it is sent to a person: a
//! postcard encoding in base32, which survives being wrapped, quoted and
//! retyped in a way a nested table does not.

use std::str::FromStr;

use data_encoding::BASE32_NOPAD;
use serde::{Deserialize, Serialize};

use crate::{
    addr::NodeAddr,
    config::RelayMode,
    error::{CoreError, Result},
    id::{GroupId, MemberId},
};

/// Marks a string as a distlib ticket, and says which version.
///
/// Present so a ticket from a later format fails as the wrong version rather
/// than as corrupt data, and so somebody pasting the wrong string entirely gets
/// told that instead of a decoding error.
const PREFIX: &str = "distlib1";

/// Directions to a group, for a member who has been admitted to it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ticket {
    /// Which group. Checked against the log once it arrives.
    pub group: GroupId,

    /// The core nodes, each with somewhere to reach them.
    ///
    /// The bootstrap seed: a joiner cannot read the log without connecting, and
    /// cannot connect without an allowlist, so the first set has to come from
    /// outside the log exactly once — the same problem founding has, solved the
    /// same way.
    pub core: Vec<(MemberId, NodeAddr)>,

    /// How the group reaches the network.
    ///
    /// Part of the directions rather than the joiner's own preference: a group
    /// on `relay_mode = "disabled"` is reachable only by a node that is also
    /// disabled and has direct addresses, and one on custom relays needs those
    /// relays configured to be reached at all.
    pub relay_mode: RelayMode,

    /// Relay URLs, meaningful only when `relay_mode` is [`RelayMode::Custom`].
    pub relay_urls: Vec<String>,
}

impl std::fmt::Display for Ticket {
    /// The pasteable form.
    ///
    /// Infallible by construction: postcard cannot fail on a type with no
    /// borrowed data and no maps, and `Display` has nowhere to put an error
    /// anyway. An encoding failure would be a bug in this type's shape rather
    /// than anything a caller could act on, so it renders as a marker that
    /// cannot be mistaken for a ticket.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match postcard::to_stdvec(self) {
            Ok(bytes) => write!(f, "{PREFIX}{}", BASE32_NOPAD.encode(&bytes)),
            Err(_) => write!(f, "{PREFIX}<unencodable>"),
        }
    }
}

impl FromStr for Ticket {
    type Err = CoreError;

    fn from_str(text: &str) -> Result<Self> {
        let malformed = |reason: &str| CoreError::MalformedTicket {
            reason: reason.to_owned(),
        };

        let body = text
            .trim()
            .strip_prefix(PREFIX)
            .ok_or_else(|| malformed("it does not start with `distlib1`"))?;
        let bytes = BASE32_NOPAD
            .decode(body.as_bytes())
            .map_err(|_| malformed("it is not valid base32 after the prefix"))?;

        postcard::from_bytes(&bytes).map_err(|_| malformed("the contents did not decode"))
    }
}
