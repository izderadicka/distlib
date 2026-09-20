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

use distlib_core::{ContentHash, MemberId, NodeAddr};
use distlib_net::{AddressBook, Blobs};
use iroh::{
    Endpoint, SecretKey,
    endpoint::{RelayMode, presets},
    protocol::Router,
};
use iroh_blobs::{
    BlobsProtocol,
    store::{fs::FsStore, mem::MemStore},
};

/// Puts bytes in a store and says what the catalogue would call them.
///
/// The conversion is this crate's own seam in reverse: iroh-blobs answers
/// with its `Hash`, an item's file record names a [`ContentHash`], and both
/// are the same 32 bytes.
async fn added(store: &MemStore, bytes: &'static [u8]) -> ContentHash {
    let hash = store.add_bytes(bytes).await.unwrap().hash;
    ContentHash::from_bytes(*hash.as_bytes())
}

/// The same seam the other way, for reading a store back by hand.
fn to_blobs(hash: ContentHash) -> iroh_blobs::Hash {
    iroh_blobs::Hash::from_bytes(*hash.as_bytes())
}

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
    let hash = added(&a_store, &b"a whole audiobook, or near enough"[..]).await;

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
    blobs.fetch(hash, vec![a_id]).await.unwrap();

    assert_eq!(
        b_store.get_bytes(to_blobs(hash)).await.unwrap().as_ref(),
        b"a whole audiobook, or near enough"
    );

    let _ = a_router.shutdown().await;
}

/// A provider list that names members who do not hold the blob still gets
/// the blob, from the one member who does.
///
/// **This is what `library.download`'s v1 provider selection rests on.**
/// §5.6's availability index is phase 4, so 2b-3 has nothing to ask *who*
/// holds a hash and offers the whole membership instead — which is only
/// implementable if a miss is a provider skipped rather than a fetch failed.
/// Checked rather than assumed, because the two shapes of `library.download`
/// that follow from the answer are entirely different pieces of work.
///
/// Both kinds of miss are in the list, and the holder is last in it so that
/// neither can be stepped over: one member serving `iroh_blobs::ALPN` and
/// answering that it has nothing, and one that cannot be resolved to an
/// address at all — the shapes an expelled member and an offline one take.
#[tokio::test]
async fn a_provider_that_does_not_hold_the_blob_is_skipped_rather_than_fatal() {
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
    let hash = added(&a_store, &b"the only copy in the group"[..]).await;

    // A member running, reachable, serving blobs — and holding none of this.
    let c_secret = SecretKey::generate();
    let c_id = MemberId::from(c_secret.public());
    let c_store = MemStore::new();
    let c_endpoint = endpoint(c_secret, vec![iroh_blobs::ALPN.to_vec()]).await;
    let c_router = Router::builder(c_endpoint.clone())
        .accept(iroh_blobs::ALPN, BlobsProtocol::new(&c_store, None))
        .spawn();
    let c_addr = NodeAddr::default().with_direct(
        c_endpoint
            .bound_sockets()
            .into_iter()
            .next()
            .expect("a bound endpoint reports a socket"),
    );

    // A member nothing can resolve.
    let stranger = MemberId::from(SecretKey::generate().public());

    let b_store = MemStore::new();
    let b_endpoint = endpoint(SecretKey::generate(), Vec::new()).await;
    let book = AddressBook::install(&b_endpoint).unwrap();
    book.learn(a_id, &a_addr);
    book.learn(c_id, &c_addr);

    let blobs = Blobs::new(&b_store, &b_endpoint);
    blobs.fetch(hash, vec![c_id, stranger, a_id]).await.unwrap();

    assert_eq!(
        b_store.get_bytes(to_blobs(hash)).await.unwrap().as_ref(),
        b"the only copy in the group"
    );

    let _ = a_router.shutdown().await;
    let _ = c_router.shutdown().await;
}

/// A blob that is here is written out to a file, and the store goes on
/// holding it.
///
/// **The second half is the one worth a test.** `library.download`'s claim
/// that a node which downloads something becomes a provider of it rests
/// entirely on `export` being a copy: the file is the operator's to move or
/// delete, and the store's copy is what keeps answering
/// `iroh_blobs::ALPN` afterwards. `ExportMode::Reference` would hand out a
/// path into the store's own data and make deleting the downloaded file a
/// node quietly ceasing to be a holder, which nothing else here would notice.
///
/// **An `FsStore`, where every other test here uses a `MemStore`**, because
/// the distinction this is about only exists on disk: `ExportMode` is
/// explicitly something "stores are allowed to ignore", and a store that
/// keeps its data in memory has nothing to reference and copies either way.
/// A test that could not fail is worse than no test.
///
/// **A megabyte, not a sentence.** `FsStore` inlines a small blob into its
/// database, where there is no file to reference and the mode cannot make a
/// difference — checked by running the mutation below at both sizes, since a
/// twenty-six-byte version of this test passes whatever mode it is given.
///
/// **Mutation check:** exporting with `ExportMode::TryReference` turns this
/// red on the *last* assertion, with "poisoned storage" — the store moved its
/// only copy out to `target` and then that copy was deleted. The bytes
/// assertion above still passes, which is what says the tail of this test is
/// the part carrying it.
///
/// **And `has` alone does not catch it**, which is why the last assertion
/// reads the bytes back rather than asking again: after a `TryReference`
/// export the store still answers `Complete` for a blob it can no longer
/// produce, because that status is its bookkeeping rather than a stat of the
/// file. Nothing here can reach that state — this crate only ever exports as
/// a copy, and `Catalogue::add_file` only ever imports as one — but it is the
/// difference between the two assertions and worth saying once.
#[tokio::test]
async fn an_exported_blob_is_a_copy_and_the_store_still_holds_it() {
    let dir = tempfile::TempDir::new().unwrap();
    let store = FsStore::load(dir.path().join("blobs")).await.unwrap();
    let endpoint = endpoint(SecretKey::generate(), Vec::new()).await;
    let blobs = Blobs::new(&store, &endpoint);

    let bytes = vec![b'x'; 1 << 20];
    let hash = store.add_bytes(bytes.clone()).await.unwrap().hash;
    let hash = ContentHash::from_bytes(*hash.as_bytes());
    assert!(blobs.has(hash).await.unwrap(), "just added");

    let target = dir.path().join("chapter-1.mp3");
    blobs.export(hash, &target).await.unwrap();

    assert_eq!(std::fs::read(&target).unwrap(), bytes);
    std::fs::remove_file(&target).unwrap();
    assert!(
        blobs.has(hash).await.unwrap(),
        "the export was a copy, so deleting it must not unmake this node a holder"
    );
    assert_eq!(
        store.get_bytes(to_blobs(hash)).await.unwrap().as_ref(),
        bytes,
        "and the store must still be able to hand those bytes to a peer"
    );

    store.shutdown().await.unwrap();
}

/// A hash the store has never seen is `has() == false` rather than an error,
/// and exporting it fails rather than writing an empty file.
///
/// Both halves matter to `library.download`: the first is how it decides
/// whether to go to the network at all, and the second is what stands between
/// a failed fetch and a plausible-looking file of the wrong length.
#[tokio::test]
async fn a_blob_that_is_not_here_is_neither_held_nor_exportable() {
    let store = MemStore::new();
    let endpoint = endpoint(SecretKey::generate(), Vec::new()).await;
    let blobs = Blobs::new(&store, &endpoint);
    let hash = ContentHash::from_bytes([0xcd; 32]);

    assert!(!blobs.has(hash).await.unwrap());

    let dir = tempfile::TempDir::new().unwrap();
    let target = dir.path().join("nothing.epub");
    assert!(blobs.export(hash, &target).await.is_err());
    assert!(
        !target.exists(),
        "a failed export must not leave a file somebody could mistake for the download"
    );
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
    let hash = ContentHash::from_bytes([0xab; 32]);

    // Nothing is ever written to an address book for `stranger`: iroh's own
    // resolution has nothing to go on, the same as a bare id nobody
    // announced.
    let blobs = Blobs::new(&b_store, &b_endpoint);
    let fetched = tokio::time::timeout(Duration::from_secs(10), blobs.fetch(hash, vec![stranger]))
        .await
        .expect("an unresolvable provider should fail quickly, not hang");

    assert!(fetched.is_err(), "nobody offered this content");
}
