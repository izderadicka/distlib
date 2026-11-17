//! The availability topic's way back from no neighbours (5f-6, phase 4's
//! D7(c)).
//!
//! The topic is joined once, with the members reachable at that moment. A
//! node that then has no neighbour hears no heartbeat: it cannot tell who is
//! online or who holds what. Asking gossip to join again is not enough on its
//! own, since one failed dial leaves that peer stranded in iroh-gossip on every
//! topic (phase 4's ground truth 18): every message to it, a join included,
//! waits for ever. A connection that arrives from the peer is the one thing
//! that frees it, so this node dials each reachable member itself and hands
//! the connection to gossip, as an accepted one would be, before joining.

use std::{convert::Infallible, time::Duration};

use distlib_consensus::{MembershipState, gossip};
use distlib_core::{MemberId, NodeAddr};
use distlib_net::Transport;
use iroh::{Endpoint, EndpointId};
use iroh_gossip::{
    api::GossipSender,
    net::{GOSSIP_ALPN, Gossip},
};
use tokio::{
    sync::watch::{self, error::RecvError},
    task::JoinSet,
};

/// The first wait before joining again, and what any news resets it to.
const FIRST_WAIT: Duration = Duration::from_secs(1);

/// The longest wait between two tries while nothing changes.
const LONGEST_WAIT: Duration = Duration::from_secs(60);

/// How long one dial may take: a stale address otherwise holds a try for the
/// endpoint's own timeout, thirty seconds or more.
const DIAL_WITHIN: Duration = Duration::from_secs(10);

/// For as long as the topic runs: whenever it has no neighbour, by
/// `neighbours`, joins again after a wait that doubles up to
/// [`LONGEST_WAIT`], and at once when an address is learned or the membership
/// changes.
pub(super) async fn rejoin_while_alone(
    me: MemberId,
    transport: Transport,
    sender: GossipSender,
    membership: watch::Receiver<MembershipState>,
    neighbours: watch::Receiver<usize>,
) {
    let Err(error) = join_again_whenever_alone(me, transport, sender, membership, neighbours).await;
    tracing::error!(%error, "the availability topic will not be joined again once it has no neighbour");
}

/// Answers only when a channel it watches has closed.
async fn join_again_whenever_alone(
    me: MemberId,
    transport: Transport,
    sender: GossipSender,
    mut membership: watch::Receiver<MembershipState>,
    mut neighbours: watch::Receiver<usize>,
) -> Result<Infallible, RecvError> {
    let Transport {
        endpoint,
        gossip,
        directory,
    } = transport;
    let mut learned = directory.learned();
    let mut wait = FIRST_WAIT;
    loop {
        neighbours.wait_for(|count| *count == 0).await?;
        tokio::select! {
            () = tokio::time::sleep(wait) => wait = (wait * 2).min(LONGEST_WAIT),
            changed = learned.changed() => {
                changed?;
                wait = FIRST_WAIT;
            }
            changed = membership.changed() => {
                changed?;
                wait = FIRST_WAIT;
            }
            joined = neighbours.wait_for(|count| *count > 0) => {
                joined?;
                wait = FIRST_WAIT;
                continue;
            }
        }
        let reachable: Vec<(MemberId, NodeAddr)> =
            gossip::reachable(&membership.borrow(), me, &directory).collect();
        let connected = connect(&gossip, &endpoint, reachable).await;
        tracing::debug!(
            connected = connected.len(),
            "no neighbour on the availability topic; joining it again"
        );
        if connected.is_empty() {
            continue;
        }
        if let Err(error) = sender.join_peers(connected).await {
            tracing::warn!(%error, "could not join the availability topic again");
        }
    }
}

/// Dials each of `members` on gossip, all at once, and hands gossip every
/// connection made; answers who was connected.
async fn connect(
    gossip: &Gossip,
    endpoint: &Endpoint,
    members: Vec<(MemberId, NodeAddr)>,
) -> Vec<EndpointId> {
    let mut dials: JoinSet<_> = members
        .into_iter()
        .map(|(member, addr)| {
            let endpoint = endpoint.clone();
            async move {
                let addr = addr.to_endpoint_addr(member).ok()?;
                tokio::time::timeout(DIAL_WITHIN, endpoint.connect(addr, GOSSIP_ALPN))
                    .await
                    .ok()?
                    .inspect_err(|error| {
                        tracing::debug!(%member, %error, "could not dial a member to join the availability topic");
                    })
                    .ok()
            }
        })
        .collect();
    let mut connected = Vec::new();
    while let Some(dialled) = dials.join_next().await {
        let Ok(Some(connection)) = dialled else {
            continue;
        };
        let peer = connection.remote_id();
        match gossip.handle_connection(connection).await {
            Ok(()) => connected.push(peer),
            Err(error) => {
                tracing::warn!(%error, "gossip would not take a connection; it may be shutting down");
            }
        }
    }
    connected
}
