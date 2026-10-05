//! The frame every gossip topic shares, at its full size (phase 4's 4b-1).

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

mod common;

use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use common::Member;
use distlib_core::{NodeAddr, SignedAddress, availability::HEARTBEAT_MAX};
use distlib_net::{AllowlistHooks, Transport, endpoint::configure, spawn_gossip};
use futures_lite::StreamExt as _;
use iroh::{
    Endpoint,
    endpoint::{RelayMode, presets},
    protocol::Router,
};
use iroh_gossip::{
    api::{Event, GossipReceiver},
    net::GOSSIP_ALPN,
    proto::TopicId,
};

/// Long past the few milliseconds a message takes on loopback.
const SOON: Duration = Duration::from_secs(10);

/// One member's transport, serving gossip and admitting `peer`.
async fn gossiping(member: &Member, peer: &Member) -> (Transport, Router) {
    // The writer can go: the allowlist keeps the last set it was given.
    let (_, allowed) = member.admitting([peer.id]);
    let endpoint = configure(
        Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled),
        member.secret.clone(),
        AllowlistHooks::new(allowed),
        vec![GOSSIP_ALPN.to_vec()],
    )
    .bind_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
    .unwrap()
    .bind()
    .await
    .unwrap();
    let transport = Transport::new(endpoint.clone(), spawn_gossip(&endpoint)).unwrap();
    let router = Router::builder(endpoint)
        .accept(GOSSIP_ALPN, transport.gossip.clone())
        .spawn();
    (transport, router)
}

async fn next_message(receiver: &mut GossipReceiver) -> Vec<u8> {
    tokio::time::timeout(SOON, async {
        loop {
            match receiver.next().await.unwrap().unwrap() {
                Event::Received(message) => return message.content.to_vec(),
                Event::NeighborDown(peer) => panic!("the connection to {peer} dropped"),
                _ => {}
            }
        }
    })
    .await
    .expect("the message never arrived")
}

/// **The size heartbeats are allowed crosses, and the connection survives it.**
///
/// Built through `spawn_gossip` on both ends, as every node is. On
/// iroh-gossip's default 4 KiB the sender's frame is refused and the
/// connection — every topic on it — goes with it, which is the failure the one
/// shared builder is there to rule out.
#[tokio::test]
async fn a_full_size_message_crosses_without_dropping_the_connection() {
    let (alice, bob) = (Member::generate(), Member::generate());
    let (alice_side, _alice_router) = gossiping(&alice, &bob).await;
    let (bob_side, _bob_router) = gossiping(&bob, &alice).await;

    // Bob dials alice, so he has to know where she is.
    let at = NodeAddr {
        relay: None,
        direct: alice_side.endpoint.bound_sockets().into_iter().collect(),
    };
    bob_side
        .directory
        .learn(&SignedAddress::sign(&alice.secret, at, 0).unwrap())
        .unwrap();

    let topic = TopicId::from_bytes([4; 32]);
    let mut alice_hears = alice_side
        .gossip
        .subscribe(topic, Vec::new())
        .await
        .unwrap();
    let mut bob_speaks = bob_side
        .gossip
        .subscribe(topic, vec![alice.id.endpoint_id()])
        .await
        .unwrap();
    tokio::time::timeout(SOON, bob_speaks.joined())
        .await
        .expect("bob never joined alice")
        .unwrap();
    tokio::time::timeout(SOON, alice_hears.joined())
        .await
        .expect("alice never saw bob join")
        .unwrap();
    let (sender, _bob_hears) = bob_speaks.split();
    let (_, mut alice_hears) = alice_hears.split();

    let full = vec![0xab; HEARTBEAT_MAX];
    sender.broadcast(full.clone().into()).await.unwrap();
    assert_eq!(next_message(&mut alice_hears).await, full);

    // And the same connection still carries the next one.
    let after = b"still here".to_vec();
    sender.broadcast(after.clone().into()).await.unwrap();
    assert_eq!(next_message(&mut alice_hears).await, after);
}
