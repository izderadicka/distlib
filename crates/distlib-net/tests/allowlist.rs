//! Membership enforcement: who is refused, in which direction, and when a
//! change takes effect.
//!
//! These are the tests behind the phase 0 acceptance criterion "an unknown peer
//! is refused". Each asserts on [`NetError::Rejected`] rather than on which
//! call returns it: the hook closes the connection *after* the handshake, so
//! the initiator may see `connect` succeed and fail on its first stream
//! operation instead. Pinning that would encode an iroh internal.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

mod common;

use std::time::Duration;

use common::{Member, direct_addr, direct_endpoint, direct_endpoint_with};
use distlib_net::{AllowlistHooks, NetError, Node, allowlist, ping};
use iroh::EndpointAddr;

#[tokio::test]
async fn unknown_peer_refused() {
    let server = Member::generate();
    let stranger = Member::generate();

    // The server admits somebody else entirely.
    let (_keep_server, server_list) = server.admitting([Member::generate().id]);
    // The stranger is perfectly willing to talk to the server.
    let (_keep_stranger, stranger_list) = stranger.admitting([server.id]);

    let node = Node::spawn(direct_endpoint(&server, server_list).await);
    let stranger_endpoint = direct_endpoint(&stranger, stranger_list).await;

    let error = ping::ping(
        &stranger_endpoint,
        direct_addr(node.endpoint()),
        b"let me in",
    )
    .await
    .unwrap_err();

    assert!(
        matches!(error, NetError::Rejected { .. }),
        "a non-member must be refused, not merely time out; got {error:?}"
    );
    node.shutdown().await;
}

#[tokio::test]
async fn outgoing_to_unknown_refused() {
    // The hook gates our own dialling too. Correct for "do not talk to expelled
    // members", and the constraint the phase 1 join flow has to design around:
    // a joiner contacts a core node that is not yet in its log. This test pins
    // the behaviour so that exemption is a deliberate change, not a surprise.
    let server = Member::generate();
    let client = Member::generate();

    let (_keep_server, server_list) = server.admitting([client.id]);
    // The client admits nobody, so its own hook must stop it dialling.
    let (_keep_client, client_list) = client.admitting([]);

    let node = Node::spawn(direct_endpoint(&server, server_list).await);
    let client_endpoint = direct_endpoint(&client, client_list).await;

    let error = ping::ping(&client_endpoint, direct_addr(node.endpoint()), b"hello")
        .await
        .unwrap_err();

    assert!(
        matches!(error, NetError::Rejected { .. }),
        "expected our own allowlist to refuse the dial; got {error:?}"
    );
    node.shutdown().await;
}

#[tokio::test]
async fn an_ordinary_failure_is_not_reported_as_a_rejection() {
    // Guards the error-chain matching in `rejection()`: if it over-matched,
    // every one of the tests above would pass whether or not the hook exists.
    let client = Member::generate();
    let unreachable = Member::generate();
    let (_keep, list) = client.admitting([unreachable.id]);
    let endpoint = direct_endpoint(&client, list).await;

    // Admitted by policy, but with no address to dial.
    let error = ping::ping(
        &endpoint,
        EndpointAddr::new(unreachable.id.into()),
        b"hello",
    )
    .await
    .unwrap_err();

    assert!(
        !matches!(error, NetError::Rejected { .. }),
        "an unreachable-but-admitted peer is a connection failure, not a policy refusal; got {error:?}"
    );
}

#[tokio::test]
async fn a_node_may_always_reach_itself() {
    // A node excluded from its own allowlist would be unable to reach its own
    // services, and in phase 1 an expelled member must still be able to observe
    // the log entry that removed it.
    let member = Member::generate();
    let (_keep, list) = member.admitting([]);

    assert!(list.is_allowed(&member.id));
    assert!(list.is_empty(), "self-membership is implicit, not stored");
}

#[tokio::test]
async fn allowlist_change_takes_effect_without_a_restart() {
    // The seam phase 1 replaces: the Raft state machine will drive the writer
    // while the endpoint keeps running.
    let server = Member::generate();
    let client = Member::generate();

    let (server_writer, server_list) = server.admitting([client.id]);
    let (_keep_client, client_list) = client.admitting([server.id]);

    let node = Node::spawn(direct_endpoint(&server, server_list).await);
    let client_endpoint = direct_endpoint(&client, client_list).await;
    let addr = direct_addr(node.endpoint());

    let reply = ping::ping(&client_endpoint, addr.clone(), b"before")
        .await
        .unwrap();
    assert_eq!(reply, b"before", "the client should start out admitted");

    // Expel the client. No restart, no rebind.
    server_writer.replace([]);

    let error = ping::ping(&client_endpoint, addr, b"after")
        .await
        .unwrap_err();

    assert!(
        matches!(error, NetError::Rejected { .. }),
        "an expelled member must be refused on its next attempt; got {error:?}"
    );
    node.shutdown().await;
}

#[tokio::test]
async fn dropping_the_writer_freezes_the_set() {
    // A dropped writer must not silently empty the allowlist and lock a node
    // out of its own group.
    let server = Member::generate();
    let client = Member::generate();
    let (writer, list) = allowlist(server.id, [client.id]);

    drop(writer);

    assert!(list.is_allowed(&client.id));
    assert_eq!(list.len(), 1);
}

// --- eviction (§4.4) --------------------------------------------------------

#[tokio::test]
async fn expulsion_closes_a_connection_that_is_already_open() {
    // The requirement Phase 0 could not meet. Refusing an expelled member's
    // *next* attempt leaves one expelled mid-transfer carrying on over the
    // connection they already had.
    let server = Member::generate();
    let client = Member::generate();
    let (server_writer, server_list) = server.admitting([client.id]);
    let (_keep_client, client_list) = client.admitting([server.id]);

    let hooks = AllowlistHooks::new(server_list);
    let node = Node::spawn(direct_endpoint_with(&server, hooks.clone()).await);
    let client_endpoint = direct_endpoint(&client, client_list).await;
    let evictions = tokio::spawn(hooks.evict_expelled());

    // A live connection, held open rather than completed and dropped.
    let connection = client_endpoint
        .connect(direct_addr(node.endpoint()), distlib_net::alpn::PING)
        .await
        .unwrap();

    server_writer.replace([]);

    // The close arrives without the client doing anything to provoke it.
    let reason = tokio::time::timeout(Duration::from_secs(10), connection.closed())
        .await
        .expect("an expelled member's open connection must be closed, not left running");

    assert!(
        format!("{reason:?}").contains("not a member"),
        "the peer should be told why it was closed; got {reason:?}"
    );

    evictions.abort();
    node.shutdown().await;
}

#[tokio::test]
async fn a_member_who_stays_admitted_keeps_its_connection() {
    // The other half: eviction must not close connections to members who are
    // still in the group when the set changes for some other reason.
    let server = Member::generate();
    let client = Member::generate();
    let other = Member::generate();
    let (server_writer, server_list) = server.admitting([client.id]);
    let (_keep_client, client_list) = client.admitting([server.id]);

    let hooks = AllowlistHooks::new(server_list);
    let node = Node::spawn(direct_endpoint_with(&server, hooks.clone()).await);
    let client_endpoint = direct_endpoint(&client, client_list).await;
    let evictions = tokio::spawn(hooks.evict_expelled());

    let connection = client_endpoint
        .connect(direct_addr(node.endpoint()), distlib_net::alpn::PING)
        .await
        .unwrap();

    // Someone else joins; the client is still a member.
    server_writer.replace([client.id, other.id]);

    let still_open = tokio::time::timeout(Duration::from_secs(2), connection.closed()).await;
    assert!(
        still_open.is_err(),
        "an unrelated membership change must not disturb a current member"
    );

    evictions.abort();
    node.shutdown().await;
}

#[tokio::test]
async fn the_eviction_loop_stops_when_membership_can_no_longer_change() {
    // Dropping the writer means nothing can ever change again, so the loop has
    // nothing left to react to and should return rather than spin.
    let member = Member::generate();
    let (writer, list) = member.admitting([]);
    let hooks = AllowlistHooks::new(list);

    let evictions = tokio::spawn(hooks.evict_expelled());
    drop(writer);

    tokio::time::timeout(Duration::from_secs(5), evictions)
        .await
        .expect("the loop must end once the allowlist is frozen")
        .unwrap();
}
