//! Dialling a member by id alone.
//!
//! Our own protocols never need this: every one of them carries a `NodeAddr`
//! beside the member id. iroh-gossip does — it subscribes with bare endpoint
//! ids — and with no relay and no address lookup there is nothing to resolve
//! one against. That is what the address book is for.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

mod common;

use common::{Member, direct_endpoint};
use distlib_core::NodeAddr;
use distlib_net::{AddressBook, Node, ping};
use iroh::EndpointAddr;

/// Where an endpoint actually is, in the shape the group passes around.
fn where_it_is(endpoint: &iroh::Endpoint) -> NodeAddr {
    NodeAddr {
        relay: None,
        direct: endpoint.bound_sockets().into_iter().collect(),
    }
}

#[tokio::test]
async fn a_member_the_book_knows_can_be_dialled_by_id_alone() {
    let server = Member::generate();
    let client = Member::generate();
    let (_keep_server, server_list) = server.admitting([client.id]);
    let (_keep_client, client_list) = client.admitting([server.id]);

    let served = direct_endpoint(&server, server_list).await;
    let where_server_is = where_it_is(&served);
    let node = Node::spawn(served);
    let dialling = direct_endpoint(&client, client_list).await;

    // The id and nothing else — exactly what iroh-gossip has to work with.
    let by_id_alone = || EndpointAddr::new(server.id.endpoint_id());

    // Without the address, unreachable. Not a refusal: the client cannot work
    // out where to send a packet, and says so before sending one.
    ping::ping(&dialling, by_id_alone(), b"before")
        .await
        .expect_err("an id with no address and no lookup is not dialable");

    let book = AddressBook::install(&dialling).unwrap();
    book.learn(server.id, &where_server_is);

    let reply = ping::ping(&dialling, by_id_alone(), b"after")
        .await
        .expect("the book resolves the id the caller could not");
    assert_eq!(reply, b"after");

    node.shutdown().await;
}

#[tokio::test]
async fn an_address_with_nothing_in_it_is_not_recorded() {
    // "Find them some other way" — which is what happens anyway when the book
    // has nothing to say. Storing it would claim knowledge we do not have.
    let member = Member::generate();
    let (_keep, list) = member.admitting([]);
    let endpoint = direct_endpoint(&member, list).await;
    let book = AddressBook::install(&endpoint).unwrap();

    book.learn(member.id, &NodeAddr::default());

    let other = Member::generate();
    ping::ping(&endpoint, EndpointAddr::new(other.id.endpoint_id()), b"?")
        .await
        .expect_err("an empty entry teaches nothing");
    endpoint.close().await;
}
