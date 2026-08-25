//! Two nodes exchanging a ping, over a direct path and over a relay.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

mod common;

use common::{Member, direct_addr, direct_endpoint, relay_addr, relay_endpoint};
use distlib_net::{Node, ping};
use iroh::{EndpointAddr, SecretKey, endpoint::RelayMode};

/// Two members who admit each other, with endpoints already built.
async fn direct_pair() -> (Member, Node, Member, iroh::Endpoint) {
    let server = Member::generate();
    let client = Member::generate();

    let (_keep_server, server_list) = server.admitting([client.id]);
    let (_keep_client, client_list) = client.admitting([server.id]);

    let server_endpoint = direct_endpoint(&server, server_list).await;
    let client_endpoint = direct_endpoint(&client, client_list).await;

    (
        server,
        Node::spawn(server_endpoint),
        client,
        client_endpoint,
    )
}

#[tokio::test]
async fn two_nodes_direct_ping() {
    let (_server, node, _client, client_endpoint) = direct_pair().await;

    let reply = ping::ping(&client_endpoint, direct_addr(node.endpoint()), b"hello")
        .await
        .unwrap();

    assert_eq!(reply, b"hello");
    node.shutdown().await;
}

#[tokio::test]
async fn a_ping_carries_arbitrary_bytes() {
    let (_server, node, _client, client_endpoint) = direct_pair().await;
    let payload: Vec<u8> = (0..=255u8).cycle().take(ping::MAX_PAYLOAD).collect();

    let reply = ping::ping(&client_endpoint, direct_addr(node.endpoint()), &payload)
        .await
        .unwrap();

    assert_eq!(reply, payload, "payload must round-trip unaltered");
    node.shutdown().await;
}

#[tokio::test]
async fn an_oversized_payload_is_refused_before_dialling() {
    let client = Member::generate();
    let (_keep, list) = client.admitting([]);
    let endpoint = direct_endpoint(&client, list).await;
    let payload = vec![0u8; ping::MAX_PAYLOAD + 1];
    // Nothing is listening: reaching the network at all would be the bug.
    let nowhere = EndpointAddr::new(SecretKey::generate().public());

    let error = ping::ping(&endpoint, nowhere, &payload).await.unwrap_err();

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

    let server = Member::generate();
    let client = Member::generate();
    let (_keep_server, server_list) = server.admitting([client.id]);
    let (_keep_client, client_list) = client.admitting([server.id]);

    let server_endpoint =
        relay_endpoint(&server, server_list, RelayMode::Custom(relay_map.clone())).await;
    let client_endpoint = relay_endpoint(&client, client_list, RelayMode::Custom(relay_map)).await;
    let node = Node::spawn(server_endpoint);

    assert!(
        node.endpoint().bound_sockets().is_empty(),
        "an IP transport survived clear_ip_transports; this test would prove nothing"
    );

    let addr = relay_addr(node.endpoint(), &relay_url).await;
    client_endpoint.online().await;

    let reply = ping::ping(&client_endpoint, addr, b"over the relay")
        .await
        .unwrap();

    assert_eq!(reply, b"over the relay");
    node.shutdown().await;
}
