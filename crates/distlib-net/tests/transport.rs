//! Two nodes exchanging a ping, over a direct path and over a relay.
//!
//! No test here may reach n0's public relay or DNS infrastructure. Every
//! endpoint is built from `presets::Minimal`, which configures no address
//! lookup, and is given either explicit socket addresses or an in-process
//! relay. The requirement is determinism: a suite that depends on third-party
//! infrastructure is flaky by construction.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::net::{Ipv4Addr, SocketAddr};

use distlib_net::{Node, endpoint::configure, ping};
use iroh::{
    Endpoint, EndpointAddr, RelayUrl, SecretKey, TransportAddr,
    endpoint::{RelayMode, presets},
};
use iroh_relay::tls::CaTlsConfig;

/// An endpoint with no relays and no address lookup: reachable only at the
/// socket addresses it is bound to.
async fn direct_only_endpoint() -> Endpoint {
    let builder = Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled);
    configure(builder, SecretKey::generate())
        .bind_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .unwrap()
        .bind()
        .await
        .unwrap()
}

/// An endpoint that can *only* use the given relay: `clear_ip_transports`
/// removes the IP transports entirely, so no direct path can exist.
///
/// The certificate check is disabled because the in-process relay from
/// `iroh::test_utils` serves self-signed certificates. It is scoped to this
/// helper and never reaches production code, which uses real relays.
async fn relay_only_endpoint(relays: RelayMode) -> Endpoint {
    let builder = Endpoint::builder(presets::Minimal)
        .relay_mode(relays)
        .clear_ip_transports()
        .ca_tls_config(CaTlsConfig::insecure_skip_verify());
    configure(builder, SecretKey::generate())
        .bind()
        .await
        .unwrap()
}

/// The address of an endpoint reachable only over IP.
fn direct_addr(endpoint: &Endpoint) -> EndpointAddr {
    let addr = EndpointAddr::new(endpoint.id())
        .with_addrs(endpoint.bound_sockets().into_iter().map(TransportAddr::Ip));
    assert!(!addr.is_empty(), "endpoint reported no bound sockets");
    addr
}

/// The address of an endpoint reachable only through `relay`.
///
/// `online()` waits until the endpoint has actually registered with the relay;
/// dialling before the home relay is established is a race, not a failure.
async fn relay_addr(endpoint: &Endpoint, relay: &RelayUrl) -> EndpointAddr {
    endpoint.online().await;
    EndpointAddr::new(endpoint.id()).with_relay_url(relay.clone())
}

#[tokio::test]
async fn two_nodes_direct_ping() {
    let server = Node::spawn(direct_only_endpoint().await);
    let client = direct_only_endpoint().await;

    let reply = ping::ping(&client, direct_addr(server.endpoint()), b"hello")
        .await
        .unwrap();

    assert_eq!(reply, b"hello");
    server.shutdown().await;
}

#[tokio::test]
async fn a_ping_carries_arbitrary_bytes() {
    let server = Node::spawn(direct_only_endpoint().await);
    let client = direct_only_endpoint().await;
    let payload: Vec<u8> = (0..=255u8).cycle().take(ping::MAX_PAYLOAD).collect();

    let reply = ping::ping(&client, direct_addr(server.endpoint()), &payload)
        .await
        .unwrap();

    assert_eq!(reply, payload, "payload must round-trip unaltered");
    server.shutdown().await;
}

#[tokio::test]
async fn an_oversized_payload_is_refused_before_dialling() {
    let client = direct_only_endpoint().await;
    let payload = vec![0u8; ping::MAX_PAYLOAD + 1];
    // Nothing is listening: reaching the network at all would be the bug.
    let nowhere = EndpointAddr::new(SecretKey::generate().public());

    let error = ping::ping(&client, nowhere, &payload).await.unwrap_err();

    assert!(
        matches!(error, distlib_net::NetError::PayloadTooLarge { .. }),
        "expected PayloadTooLarge, got {error:?}"
    );
}

#[tokio::test]
async fn relay_only_ping() {
    // The hermetic stand-in for "two nodes on separate networks": with the IP
    // transports removed, the relay is the only path that can carry this.
    let (relay_map, relay_url, _relay_guard) = iroh::test_utils::run_relay_server().await.unwrap();

    let server = Node::spawn(relay_only_endpoint(RelayMode::Custom(relay_map.clone())).await);
    let client = relay_only_endpoint(RelayMode::Custom(relay_map)).await;

    assert!(
        server.endpoint().bound_sockets().is_empty(),
        "an IP transport survived clear_ip_transports; this test would prove nothing"
    );

    let addr = relay_addr(server.endpoint(), &relay_url).await;
    client.online().await;

    let reply = ping::ping(&client, addr, b"over the relay").await.unwrap();

    assert_eq!(reply, b"over the relay");
    server.shutdown().await;
}
