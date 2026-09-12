//! The document namespaces a group replicates, and the keys to them.
//!
//! A namespace is one iroh-docs replica: a set of key/value entries every
//! member converges on. The group has one per kind of thing it agrees about —
//! the catalogue now, community metadata and works later — and each is opened
//! from a 32-byte secret that whoever holds it may write to.
//!
//! **The secret travels in the membership log** (§5.1 says "distributed via
//! the join flow"; the log *is* that flow, since a joiner fetches it before it
//! can sync anything). It is therefore not the security boundary — the
//! connection allowlist is, and it reads the same log. What the secret buys is
//! that a namespace cannot be written by somebody who never held it, which is
//! convenience rather than enforcement: an expelled member keeps the bytes and
//! is stopped at the endpoint.
//!
//! Nothing here knows about iroh-docs. The secret crosses that boundary as 32
//! bytes, so `distlib-sync` stays the only crate that depends on it — the
//! containment rule Phase 2 rests on, since iroh-blobs says of itself that it
//! is not production quality yet.

use serde::{Deserialize, Serialize};

use crate::error::{CoreError, Result};

/// What a namespace is *for*.
///
/// **Append only.** postcard encodes a variant by its declaration index, and
/// this rides inside a membership event: inserting a variant above another
/// would change what entries already written mean, and every signature over
/// them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Namespace {
    /// Items and their metadata (§5.2) — what a member searches and downloads.
    Catalogue,
}

/// Named as an operator would say it, for error messages and the CLI.
impl std::fmt::Display for Namespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Catalogue => f.write_str("catalogue"),
        }
    }
}

/// Bytes of secret behind a namespace. 256 bits, the size iroh-docs takes.
const SECRET_BYTES: usize = 32;

/// The key that opens a namespace for writing.
///
/// A plain newtype rather than [`secrecy::SecretBox`], which is a **deliberate
/// exception** to the rule that sensitive values use `secrecy`: this rides
/// inside a `MembershipEvent`, so it must be `Serialize`, `Deserialize`,
/// `Clone` and `PartialEq`, none of which `SecretBox` offers. What `secrecy`
/// would actually buy here is the redacted `Debug` — which is written out
/// below and tested — and zeroing on drop, which is unreachable anyway while
/// the log holds this value in plaintext and every `membership()` hands out a
/// clone of the state containing it.
///
/// The protections that do apply: it is never rendered (see the `Debug` below,
/// and `distlib-api`'s `describe`), and the database holding the log is mode
/// 0600 from the moment it can contain one.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceSecret([u8; SECRET_BYTES]);

impl NamespaceSecret {
    /// Makes a new one.
    ///
    /// **Only ever called by whoever proposes the namespace**, never while
    /// folding the log: a fold that generated anything would reach a different
    /// answer on every node, which is the one thing a projection may not do.
    pub fn generate() -> Result<Self> {
        let mut bytes = [0u8; SECRET_BYTES];
        getrandom::fill(&mut bytes).map_err(|error| CoreError::Random {
            message: error.to_string(),
        })?;
        Ok(Self(bytes))
    }

    /// Takes the bytes as they came out of the log.
    pub fn from_bytes(bytes: [u8; SECRET_BYTES]) -> Self {
        Self(bytes)
    }

    /// The bytes, for the one crate that opens a replica with them.
    ///
    /// Named for what it does, on the `secrecy` pattern: reading a secret
    /// should be a thing somebody wrote down deliberately.
    pub fn expose(&self) -> &[u8; SECRET_BYTES] {
        &self.0
    }
}

/// Redacted, and that is the whole point of writing it by hand.
///
/// A `MembershipEvent` is `Debug` and lands in error messages, test failures
/// and anything an operator dumps; the derived version would put the key to
/// the group's catalogue in all three.
impl std::fmt::Debug for NamespaceSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NamespaceSecret(<redacted>)")
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

    use super::*;

    #[test]
    fn a_secret_never_prints_itself() {
        let secret = NamespaceSecret::from_bytes([0xab; SECRET_BYTES]);
        let shown = format!("{secret:?}");
        assert_eq!(shown, "NamespaceSecret(<redacted>)");
        assert!(
            !shown.contains("ab") && !shown.contains("171"),
            "neither hex nor decimal bytes may appear: {shown}"
        );
    }

    #[test]
    fn a_secret_survives_the_log_unchanged() {
        let secret = NamespaceSecret::generate().unwrap();
        let bytes = postcard::to_stdvec(&secret).unwrap();
        let back: NamespaceSecret = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(secret, back);
        assert_eq!(bytes.len(), SECRET_BYTES, "no length prefix, no framing");
    }

    #[test]
    fn two_secrets_differ() {
        assert_ne!(
            NamespaceSecret::generate().unwrap(),
            NamespaceSecret::generate().unwrap()
        );
    }
}
