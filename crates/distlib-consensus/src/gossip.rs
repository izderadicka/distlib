//! Telling the group the log has moved.
//!
//! §4.2: "committed entries announced via gossip; peers fetch missing suffix
//! from any core node". This is the announcing half — the fetching half is
//! [`crate::raft::memberlog`], and it stays exactly as it was. Nothing here
//! carries the log, decides anything, or is trusted.
//!
//! **The announcement needs no security, and gets none.** It says only "the log
//! reaches index N". A member who lies high makes followers ask a core node and
//! find nothing new; one who lies low is ignored, because a follower keeps the
//! highest it has seen and its own cursor decides what it asks for. Everything
//! that matters is verified when the entries themselves arrive: signed events,
//! folded by the same rules a core node applies.
//!
//! So this is a hint, and the poll behind it is what makes it safe to treat as
//! one. Gossip is best-effort — a member that misses a message must not stall
//! until the next change — so the follow loop keeps its own timer and this only
//! makes it prompt.
//!
//! Why gossip rather than every follower holding a connection to a core node:
//! §2 allows for thousands of members against three to seven core nodes. Asking
//! each of them to keep a subscription open, or to poll briskly, does not
//! survive that. Epidemic broadcast does, which is why the design names it.

use bytes::Bytes;
use distlib_core::GroupId;
use futures_lite::StreamExt as _;
use iroh_gossip::{
    api::{Event, GossipReceiver, GossipSender},
    proto::TopicId,
};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::raft::state_machine::StateMachineStore;

/// Why the follow loop should look at the log again.
///
/// Carries the index rather than only poking, so a follower that already holds
/// what was announced can go back to waiting instead of spending a round trip
/// finding out it was up to date.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Hint {
    /// Somebody says the log reaches this index.
    ///
    /// Their claim, not a fact — see the module docs. It is compared against
    /// this node's own cursor and used for nothing else.
    Reaches(u64),

    /// Something happened that may have hidden announcements from this node.
    ///
    /// No index to compare, so the only safe reading is "go and look": there is
    /// no way to tell what was missed while it could not hear.
    MayHaveMissed,
}

/// The channel a follow loop waits on.
pub type Hints = watch::Receiver<Hint>;

/// What a core node says when its log advances.
///
/// An enum rather than a bare `u64` for the reason `memberlog::Request` is one:
/// postcard writes a variant discriminant, so a second kind of announcement
/// later leaves this one's encoding alone.
#[derive(Debug, Serialize, Deserialize)]
enum Announcement {
    /// The sender has applied the log up to this index.
    Applied { up_to: u64 },
}

/// The gossip topic a group talks on.
///
/// The group id itself: both are 32 bytes, it is already the group's name, and
/// deriving it any other way would be a second thing to agree on.
pub fn topic_for(group: GroupId) -> TopicId {
    TopicId::from_bytes(*group.as_bytes())
}

/// Announces this node's applied index whenever the membership changes.
///
/// Runs on core nodes. Triggered by the membership rather than by every entry,
/// because that is what a follower is waiting to hear about — Raft's blank
/// entries move the log without moving anything a follower would derive.
pub async fn announce(state_machine: StateMachineStore, sender: GossipSender) {
    let mut memberships = state_machine.subscribe();
    loop {
        // Read before waiting, so a node that applied entries before this task
        // started announces them rather than staying quiet until the next
        // change.
        let up_to = state_machine.last_applied_index();
        if up_to > 0 {
            match postcard::to_stdvec(&Announcement::Applied { up_to }) {
                Ok(encoded) => {
                    if let Err(error) = sender.broadcast(Bytes::from(encoded)).await {
                        // Not fatal and not retried: the followers' own poll is
                        // the guarantee, and this is the optimisation on top.
                        tracing::debug!(%error, "could not announce the log");
                    } else {
                        tracing::debug!(up_to, "announced the log");
                    }
                }
                Err(error) => tracing::warn!(%error, "could not encode an announcement"),
            }
        }

        if memberships.changed().await.is_err() {
            // Unreachable while this task holds the state machine, which owns
            // the sender — so getting here means something has gone wrong that
            // this node cannot see. Loud, because the silent version of it is a
            // group whose followers quietly stop being told anything.
            tracing::error!("the membership channel closed; this node will announce nothing more");
            return;
        }
    }
}

/// Wakes the follow loop whenever somebody announces a longer log.
///
/// Runs on followers. Deliberately ignores what the announcement *says* beyond
/// "there may be more": the follow loop knows its own cursor, and acting on a
/// number a peer supplied would be trusting one.
pub async fn listen(mut receiver: GossipReceiver, hints: watch::Sender<Hint>) {
    while let Some(event) = receiver.next().await {
        let hint = match event {
            Ok(Event::Received(message)) => {
                match postcard::from_bytes::<Announcement>(&message.content) {
                    Ok(Announcement::Applied { up_to }) => {
                        tracing::debug!(up_to, from = %message.delivered_from, "heard an announcement");
                        Hint::Reaches(up_to)
                    }
                    // A member running something else, or a future version.
                    // Worth a line: a group where this happens constantly is
                    // one running two versions of the protocol.
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            from = %message.delivered_from,
                            "ignoring an announcement that did not decode"
                        );
                        continue;
                    }
                }
            }

            // Messages were dropped before this node read them, and gossip does
            // not replay. There is no index to compare, so the only safe
            // reading is that something may have been missed.
            Ok(Event::Lagged) => {
                tracing::warn!("fell behind on gossip; fetching rather than guessing");
                Hint::MayHaveMissed
            }

            // The first moment this node can hear a given peer. Anything
            // announced before now went past it, and there is no way to know
            // what — so look. Narrows the window between joining a topic and
            // being reachable on it, which the loop's own timer otherwise
            // covers at thirty seconds.
            Ok(Event::NeighborUp(_)) => Hint::MayHaveMissed,

            // A neighbour has gone. Whatever it would have relayed goes
            // unheard, so this is the same "look" as any other gap — and it is
            // what an expelled member sees first, since the group closing its
            // connections is how it finds out at all. Waiting out the timer
            // instead would leave it asking refused questions for half a
            // minute before noticing.
            Ok(Event::NeighborDown(_)) => Hint::MayHaveMissed,

            Err(error) => {
                // This node has just lost its prompt updates and is back to the
                // timer. Not fatal, but not routine either.
                tracing::warn!(%error, "gossip stream failed; falling back to the poll");
                return;
            }
        };

        if hints.send(hint).is_err() {
            tracing::debug!("nothing is following the log any more; stopping");
            return;
        }
    }
}
