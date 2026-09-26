//! What a node tells whoever is watching it: §7.2's event stream.
//!
//! **Ids, never values** (phase 3's D2). An event says *what* changed, and a
//! page that cares asks the API for the new state. That is one extra round
//! trip against a loopback listener, and it buys three things: a new event
//! type cannot break a page written before it, a page that missed events
//! recovers by the same refetch as everything else, and nothing here has to
//! decide how much of a record is worth sending.
//!
//! Here rather than in `distlib-api` because the producers are elsewhere — the
//! read model's projection will publish catalogue events, and it sits below the
//! API in the dependency graph.

use serde::Serialize;

/// One thing a watcher may want to refetch.
///
/// Serialised with its name as `type`, so an event's data reads on its own
/// without the SSE frame around it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type")]
pub enum Event {
    /// The membership changed: a member came or went, a proposal was made,
    /// approved or withdrawn, a pledge or the core group moved.
    ///
    /// No payload. The membership is one thing, so there is nothing to name —
    /// `group.members` and `group.pending` are what to ask.
    #[serde(rename = "membership.changed")]
    MembershipChanged,
}

impl Event {
    /// The name §7.2 gives it, which is also its SSE `event:` field.
    pub fn name(&self) -> &'static str {
        match self {
            Self::MembershipChanged => "membership.changed",
        }
    }
}
