//! The connections a node holds open.
//!
//! Two properties matter and neither is obvious from the type: a connection
//! carries exactly one protocol, so the same peer needs one per protocol; and a
//! cached connection can be closed underneath us by expulsion, so being in the
//! map does not mean being usable.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

mod common;

use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use common::{Member, direct_addr, direct_endpoint};
use distlib_net::{AllowlistHooks, Connections, alpn, endpoint::configure, ping::PingProtocol};
use iroh::{
    Endpoint, EndpointAddr, SecretKey,
    endpoint::{Connection, RelayMode, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};

/// A second protocol, so the ALPN keying can be tested for real.
///
/// `distlib-net` only serves ping; raft lives in `distlib-consensus`, which
/// cannot be depended on from here. Rather than dial an ALPN nobody serves —
/// which fails at the handshake with "peer doesn't support any known protocol",
/// as the first version of this test discovered — the server below genuinely
/// offers two.
const OTHER_ALPN: &[u8] = b"distlib/test-only/0";

/// Accepts a connection and does nothing with it.
#[derive(Debug, Clone)]
struct Quiet;

impl ProtocolHandler for Quiet {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        connection.closed().await;
        Ok(())
    }
}

/// A running peer serving both protocols, and a client endpoint that admits it.
async fn peers() -> (Member, Router, iroh::Endpoint) {
    let server = Member::generate();
    let client = Member::generate();
    let (_keep_server, server_list) = server.admitting([client.id]);
    let (_keep_client, client_list) = client.admitting([server.id]);

    let mut alpns = alpn::registered();
    alpns.push(OTHER_ALPN.to_vec());
    let endpoint = configure(
        Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled),
        server.secret.clone(),
        AllowlistHooks::new(server_list),
        alpns,
    )
    .bind_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
    .unwrap()
    .bind()
    .await
    .unwrap();

    // Advertises exactly what it serves, as `alpn::registered` requires.
    let router = Router::builder(endpoint)
        .accept(alpn::PING, PingProtocol)
        .accept(OTHER_ALPN, Quiet)
        .spawn();

    let client_endpoint = direct_endpoint(&client, client_list).await;
    (server, router, client_endpoint)
}

#[tokio::test]
async fn the_same_protocol_reuses_one_connection() {
    let (server, router, endpoint) = peers().await;
    let addr = direct_addr(router.endpoint());
    let connections = Connections::new();

    for _ in 0..3 {
        connections
            .get_or_connect(&endpoint, server.id, addr.clone(), alpn::PING)
            .await
            .unwrap();
    }

    assert_eq!(
        connections.len(),
        1,
        "asking three times for the same protocol should dial once"
    );
    let _ = router.shutdown().await;
}

#[tokio::test]
async fn different_protocols_get_separate_connections() {
    // The reason the key includes the ALPN. A connection negotiates one
    // protocol in its handshake, and the remote binds the whole connection to
    // that handler — so handing a raft connection to a ping caller would send
    // its streams somewhere that cannot read them.
    let (server, router, endpoint) = peers().await;
    let addr = direct_addr(router.endpoint());
    let connections = Connections::new();

    let ping = connections
        .get_or_connect(&endpoint, server.id, addr.clone(), alpn::PING)
        .await
        .unwrap();
    let raft = connections
        .get_or_connect(&endpoint, server.id, addr, OTHER_ALPN)
        .await
        .unwrap();

    assert_eq!(connections.len(), 2, "one connection per protocol");
    assert_eq!(ping.alpn(), alpn::PING);
    assert_eq!(raft.alpn(), OTHER_ALPN);
    let _ = router.shutdown().await;
}

#[tokio::test]
async fn a_closed_connection_is_replaced() {
    // Expulsion closes connections without telling whoever opened them (§4.4),
    // so "in the map" does not mean "usable".
    let (server, router, endpoint) = peers().await;
    let addr = direct_addr(router.endpoint());
    let connections = Connections::new();

    let first = connections
        .get_or_connect(&endpoint, server.id, addr.clone(), alpn::PING)
        .await
        .unwrap();
    first.close(0u32.into(), b"closed underneath the pool");

    let second = connections
        .get_or_connect(&endpoint, server.id, addr, alpn::PING)
        .await
        .unwrap();

    assert!(
        second.close_reason().is_none(),
        "a closed connection must be replaced, not handed out again"
    );
    assert_eq!(connections.len(), 1, "the dead one is not kept alongside");
    let _ = router.shutdown().await;
}

#[tokio::test]
async fn an_unreachable_peer_does_not_block_dialling_another() {
    // One mutex covers every peer. Holding it across a dial would make a single
    // unreachable member stall everyone else until its connect timed out.
    //
    // The `std::sync::Mutex` makes that a compile error rather than a bug — a
    // blocking guard held across an await is not `Send`, and this dial is
    // spawned. So this test documents the property rather than being its only
    // guard; it would catch a regression to an async mutex, which compiles.
    let (server, router, endpoint) = peers().await;
    let reachable = direct_addr(router.endpoint());
    let connections = Connections::new();

    // Nothing is listening here, so this dial hangs until it gives up.
    let absent = MemberIdAndAddr::nowhere();
    let stalling = {
        let connections = connections.clone();
        let endpoint = endpoint.clone();
        tokio::spawn(async move {
            let _ = connections
                .get_or_connect(&endpoint, absent.id, absent.addr, alpn::PING)
                .await;
        })
    };
    // Give it a moment to be mid-dial, holding whatever it holds.
    tokio::time::sleep(Duration::from_millis(100)).await;

    tokio::time::timeout(
        Duration::from_secs(5),
        connections.get_or_connect(&endpoint, server.id, reachable, alpn::PING),
    )
    .await
    .expect("a live peer must not wait behind an unreachable one")
    .unwrap();

    stalling.abort();
    let _ = router.shutdown().await;
}

/// A member at an address nothing answers on.
struct MemberIdAndAddr {
    id: distlib_core::MemberId,
    addr: EndpointAddr,
}

impl MemberIdAndAddr {
    fn nowhere() -> Self {
        let id = distlib_core::MemberId::from(SecretKey::generate().public());
        // Port 1 on loopback: reserved, and nothing binds it.
        let addr = EndpointAddr::new(id.endpoint_id())
            .with_ip_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 1)));
        Self { id, addr }
    }
}
