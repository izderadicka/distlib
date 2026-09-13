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

use std::time::Duration;

use bytes::Bytes;
use distlib_core::{GroupId, NodeAddr, SignedAddress};
use futures_lite::StreamExt as _;
use iroh::{Endpoint, SecretKey, Watcher as _};
use iroh_gossip::{
    api::{Event, GossipReceiver, GossipSender},
    proto::TopicId,
};

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use distlib_net::Directory;

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

    /// Where a member says it can be reached.
    ///
    /// **Signed, unlike [`Self::Applied`] beside it**, and the asymmetry is the
    /// point. Lying about the log costs a peer one wasted fetch; an address is
    /// the thing other nodes will *dial*. And the check that would otherwise be
    /// free is unavailable — `Message::delivered_from` names the neighbour that
    /// relayed a message, not the member that made it, so an epidemic broadcast
    /// cannot attribute anything by itself. See [`SignedAddress`].
    ReachableAt(Box<SignedAddress>),
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
pub async fn announce_log(state_machine: StateMachineStore, sender: &GossipSender) {
    let mut memberships = state_machine.subscribe();
    loop {
        // Read before waiting, so a node that applied entries before this task
        // started announces them rather than staying quiet until the next
        // change.
        let up_to = state_machine.last_applied_index();
        if up_to > 0 {
            broadcast(sender, &Announcement::Applied { up_to }).await;
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

/// The least time between two address announcements from this node.
///
/// A floor rather than a schedule: nothing announces on a timer. It exists
/// because one of the triggers is a neighbour arriving, and in a group the size
/// §2 allows for — thousands of members against three to seven core nodes —
/// neighbours arrive in bursts. Without the floor, a node joining a busy swarm
/// would broadcast once per neighbour it acquires.
const LEAST_BETWEEN_ANNOUNCEMENTS: Duration = Duration::from_secs(5);

/// Tells the group where this node can be reached, and keeps telling it.
///
/// Runs on **every** member, core and follower alike, which is new: a follower
/// used to hold only the receiving half of the topic and never say anything.
/// It has to now, because a follower's address is the one thing the log cannot
/// carry — `MemberRecord` has no address field and Raft's node map holds voters
/// — so if a follower does not say where it is, nobody can find out (P2-14).
///
/// **Three triggers, no timer.** Joining the topic, this endpoint's own address
/// changing, and a neighbour arriving. The last is what makes the whole thing
/// self-healing without a periodic broadcast: a node that joins late, or a core
/// node that comes back after a restart, is a new neighbour to everyone already
/// present, and they answer by saying where they are. A node that misses an
/// announcement therefore waits for the next arrival rather than for a timer.
///
/// What it cannot do is reach somebody who never becomes anyone's neighbour.
/// That is what the core group's directory is for, and it is a separate piece
/// of work.
pub async fn announce_address(
    endpoint: &Endpoint,
    secret: &SecretKey,
    state_machine: &StateMachineStore,
    sender: &GossipSender,
    mut arrivals: watch::Receiver<u64>,
) {
    let mut addresses = endpoint.watch_addr().stream();

    loop {
        // iroh's own answer about where this node is, which is not the same as
        // what it bound: a bound socket may be `0.0.0.0`, and what peers need
        // is what iroh has actually discovered about itself.
        let addr = NodeAddr::from(&endpoint.addr());
        if !addr.is_empty() {
            match SignedAddress::sign(secret, addr, state_machine.position()) {
                Ok(signed) => broadcast(sender, &Announcement::ReachableAt(Box::new(signed))).await,
                // Signing is an ed25519 operation over a short message and the
                // encoding cannot realistically fail, so this is not a
                // condition to handle — but it would leave this node
                // unreachable, which is too quiet a way to fail.
                Err(error) => tracing::error!(%error, "could not sign this node's address"),
            }
        }

        // The floor comes before the wait, not after it, so a reason arriving
        // once the floor has passed is acted on at once rather than delayed by
        // it. Both channels keep their latest value, so a reason that arrives
        // *during* the floor is still waiting afterwards.
        tokio::time::sleep(LEAST_BETWEEN_ANNOUNCEMENTS).await;
        tokio::select! {
            moved = addresses.next() => if moved.is_none() {
                tracing::debug!("the endpoint stopped reporting its address; announcing no more");
                return;
            },
            arrived = arrivals.changed() => if arrived.is_err() {
                tracing::debug!("nothing is listening to the topic any more; announcing no more");
                return;
            },
        }
    }
}

/// Encodes and sends one announcement, saying so if it cannot.
///
/// Neither failure is retried: gossip is best-effort by design, the follow
/// loop's own timer is the guarantee behind `Applied`, and the next arrival is
/// the guarantee behind `ReachableAt`.
async fn broadcast(sender: &GossipSender, announcement: &Announcement) {
    let encoded = match postcard::to_stdvec(announcement) {
        Ok(encoded) => encoded,
        Err(error) => {
            tracing::warn!(%error, "could not encode an announcement");
            return;
        }
    };
    match sender.broadcast(Bytes::from(encoded)).await {
        Ok(()) => tracing::debug!(?announcement, "announced"),
        Err(error) => tracing::debug!(%error, "could not announce"),
    }
}

/// Hears what the group says: where its members are, and how far the log goes.
///
/// **Runs on every member now.** A core node used to drop the receiving half
/// entirely, because everything `Applied` could tell it was something Raft had
/// already told it. It has to listen now, since `ReachableAt` is the only way
/// any node learns where a follower is — including a core node, which has to
/// know in order to serve the group's directory.
///
/// The cost of that is worth naming rather than discovering: every core node
/// now decodes every announcement from every member, and §2 allows for
/// thousands of members against three to seven core nodes. So `Applied` is
/// discarded on a core node *at the point of decoding*, and no hint is derived
/// from anything on one.
pub async fn listen(
    mut receiver: GossipReceiver,
    hints: watch::Sender<Hint>,
    directory: Directory,
    is_core: bool,
    arrivals: watch::Sender<u64>,
) {
    while let Some(event) = receiver.next().await {
        let hint = match event {
            Ok(Event::Received(message)) => {
                match postcard::from_bytes::<Announcement>(&message.content) {
                    Ok(Announcement::ReachableAt(signed)) => {
                        note_where_a_member_is(&directory, &signed, message.delivered_from);
                        continue;
                    }

                    // A core node learns the log through Raft, so this says
                    // nothing it does not already know.
                    Ok(Announcement::Applied { .. }) if is_core => continue,
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
            // what — so look.
            //
            // **And say where we are.** A neighbour arriving is somebody who
            // may never have heard this node's address, and answering an
            // arrival is what makes the whole scheme self-healing without any
            // node broadcasting on a timer.
            Ok(Event::NeighborUp(_)) => {
                arrivals.send_modify(|arrivals| *arrivals = arrivals.wrapping_add(1));
                Hint::MayHaveMissed
            }

            // A neighbour has gone. Whatever it would have relayed goes
            // unheard, so this is the same "look" as any other gap — and it is
            // what an expelled member sees first, since the group closing its
            // connections is how it finds out at all.
            Ok(Event::NeighborDown(_)) => Hint::MayHaveMissed,

            Err(error) => {
                // This node has just lost its prompt updates and is back to the
                // timer. Not fatal, but not routine either.
                tracing::warn!(%error, "gossip stream failed; falling back to the poll");
                return;
            }
        };

        // Nothing on a core node follows the log, so there is nobody to tell.
        if is_core {
            continue;
        }
        if hints.send(hint).is_err() {
            tracing::debug!("nothing is following the log any more; stopping");
            return;
        }
    }
}

/// Folds one address announcement into the directory.
///
/// Nothing here fails the listener. A statement that does not verify is data —
/// these arrive relayed, by whoever happened to carry them — and one that is
/// merely older than what is held is ordinary on a best-effort broadcast.
fn note_where_a_member_is(
    directory: &Directory,
    signed: &SignedAddress,
    relayed_by: iroh::EndpointId,
) {
    match directory.learn(signed) {
        Ok(true) => tracing::debug!(member = %signed.member(), "learned where a member is"),
        Ok(false) => {
            tracing::trace!(member = %signed.member(), "nothing new about where a member is")
        }
        // Worth a warning rather than a debug line: somebody on this topic is
        // announcing addresses for a member that did not sign them.
        Err(error) => tracing::warn!(
            %error,
            from = %relayed_by,
            "ignoring an address announcement that did not verify"
        ),
    }
}
