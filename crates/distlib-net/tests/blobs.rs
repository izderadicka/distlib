//! 2b-1's acceptance: a media blob added on one node is fetched by another
//! through `distlib_net::blobs`, not through iroh-docs' own machinery.
//!
//! **Why a docs entry could not stand in for this.** Blobs already moved
//! between two nodes in 2a-2 — but that was iroh-docs fetching its own entry
//! *values* through its own engine and downloader, machinery this crate does
//! not call. Proving the same bytes arrive by adding one as a docs entry
//! would pin that path, not this one. So the blob here is added straight to
//! a store with [`iroh_blobs::api::blobs::Blobs::add_bytes`] — never a
//! document entry's value — and only `distlib_net::Blobs::fetch` ever asks
//! for it.
//!
//! No consensus, no gossip, no docs: two bare `iroh::Endpoint`s, `relay_mode
//! = "disabled"`, and nothing else of ours — the same level `moved.rs` pins
//! its own behaviour at.
//!
//! **Mutation check:** with A's router left unspawned (so nothing answers
//! `iroh_blobs::ALPN`), `a_media_blob_added_on_one_node_is_fetched_by_another`
//! fails with `NetError::Fetch` rather than passing some other way — checked
//! by hand rather than left as an assumption, since a test that passed
//! without A ever serving the blob would be proving nothing about the fetch.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use distlib_core::{MemberId, NodeAddr};
use distlib_net::{AddressBook, Blobs};
use iroh::{
    Endpoint, SecretKey,
    endpoint::{RelayMode, presets},
    protocol::Router,
};
use iroh_blobs::{BlobsProtocol, store::mem::MemStore};

/// A bare endpoint: loopback only, no relay, no address lookup of its own.
async fn endpoint(secret: SecretKey, alpns: Vec<Vec<u8>>) -> Endpoint {
    Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .secret_key(secret)
        .alpns(alpns)
        .bind_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .unwrap()
        .bind()
        .await
        .unwrap()
}

#[tokio::test]
async fn a_media_blob_added_on_one_node_is_fetched_by_another() {
    let a_secret = SecretKey::generate();
    let a_id = MemberId::from(a_secret.public());
    let a_store = MemStore::new();
    let a_endpoint = endpoint(a_secret, vec![iroh_blobs::ALPN.to_vec()]).await;
    let a_router = Router::builder(a_endpoint.clone())
        .accept(iroh_blobs::ALPN, BlobsProtocol::new(&a_store, None))
        .spawn();
    let a_addr = NodeAddr::default().with_direct(
        a_endpoint
            .bound_sockets()
            .into_iter()
            .next()
            .expect("a bound endpoint reports a socket"),
    );

    // A file `library.add` might have hashed and stored, never a document
    // entry's value.
    let hash = a_store
        .add_bytes(&b"a whole audiobook, or near enough"[..])
        .await
        .unwrap()
        .hash;

    // B serves nothing — fetching is a client role — and is given no
    // address for A anywhere in its configuration.
    let b_store = MemStore::new();
    let b_endpoint = endpoint(SecretKey::generate(), Vec::new()).await;

    // The one place A's address is written down at all: an address book B
    // installs on its own endpoint. `Blobs::fetch` itself is handed only
    // `a_id.endpoint_id()`, and it is iroh's own resolution — the same
    // mechanism `distlib_net::Directory`/`AddressBook` give every other
    // protocol — that turns that bare id into this socket, not anything
    // passed at the call site.
    let book = AddressBook::install(&b_endpoint).unwrap();
    book.learn(a_id, &a_addr);

    let blobs = Blobs::new(&b_store, &b_endpoint);
    blobs.fetch(hash, vec![a_id.endpoint_id()]).await.unwrap();

    assert_eq!(
        b_store.get_bytes(hash).await.unwrap().as_ref(),
        b"a whole audiobook, or near enough"
    );

    let _ = a_router.shutdown().await;
}

/// `Blobs::fetch` carries no deadline of its own — see its doc comment for
/// why — so this pins the other half of that decision: a provider that
/// cannot be reached is a fetch that fails, not one that hangs, and a caller
/// wrapping the call in its own `tokio::time::timeout` is a caller guarding
/// against something that can still happen, not the only thing standing
/// between a bad provider and a request that never returns.
#[tokio::test]
async fn fetching_from_an_unreachable_provider_fails_rather_than_hangs() {
    let stranger = MemberId::from(SecretKey::generate().public());
    let b_store = MemStore::new();
    let b_endpoint = endpoint(SecretKey::generate(), Vec::new()).await;

    // Not `Hash::EMPTY`: every store answers for the empty hash locally, with
    // no network call at all, which would pass this test without exercising
    // the failure path it exists to pin.
    let hash = iroh_blobs::Hash::from_bytes([0xab; 32]);

    // Nothing is ever written to an address book for `stranger`: iroh's own
    // resolution has nothing to go on, the same as a bare id nobody
    // announced.
    let blobs = Blobs::new(&b_store, &b_endpoint);
    let fetched = tokio::time::timeout(
        Duration::from_secs(10),
        blobs.fetch(hash, vec![stranger.endpoint_id()]),
    )
    .await
    .expect("an unresolvable provider should fail quickly, not hang");

    assert!(fetched.is_err(), "nobody offered this content");
}
