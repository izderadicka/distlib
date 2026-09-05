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

use std::sync::Arc;

use bytes::Bytes;
use distlib_core::GroupId;
use futures_lite::StreamExt as _;
use iroh_gossip::{
    api::{Event, GossipReceiver, GossipSender},
    proto::TopicId,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use crate::raft::state_machine::StateMachineStore;

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
            return;
        }
    }
}

/// Wakes the follow loop whenever somebody announces a longer log.
///
/// Runs on followers. Deliberately ignores what the announcement *says* beyond
/// "there may be more": the follow loop knows its own cursor, and acting on a
/// number a peer supplied would be trusting one.
pub async fn listen(mut receiver: GossipReceiver, wake: Arc<Notify>) {
    while let Some(event) = receiver.next().await {
        match event {
            Ok(Event::Received(message)) => {
                match postcard::from_bytes::<Announcement>(&message.content) {
                    Ok(Announcement::Applied { up_to }) => {
                        tracing::debug!(up_to, from = %message.delivered_from, "heard an announcement");
                        wake.notify_one();
                    }
                    // A member running something else, or a future version.
                    // Neither is worth more than a line in the log.
                    Err(error) => {
                        tracing::debug!(%error, "ignoring an announcement that did not decode");
                    }
                }
            }

            // The receiver fell behind. Whatever was missed was a hint, and the
            // right response to having missed hints is to go and look.
            Ok(Event::Lagged) => wake.notify_one(),

            // A new neighbour is a reason to look: anything announced before
            // this node could hear it was missed, and gossip does not replay.
            // Narrows the window between joining a topic and being reachable on
            // it — it does not close it, which is what the follow loop's own
            // timer is for.
            Ok(Event::NeighborUp(_)) => wake.notify_one(),

            Ok(Event::NeighborDown(_)) => {}

            Err(error) => {
                tracing::debug!(%error, "gossip stream failed; the poll still stands");
                return;
            }
        }
    }
}
