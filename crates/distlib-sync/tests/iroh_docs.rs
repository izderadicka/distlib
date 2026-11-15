//! iroh-docs behaviour the catalogue relies on, pinned so that an upgrade
//! changing it fails here rather than in a live group.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::time::Duration;

use iroh_docs::{Author, ContentStatus, InsertError, NamespaceSecret, PeerIdBytes, store::Store};

const PEER: PeerIdBytes = [3; 32];

/// **An older entry from a peer undoes a delete, never a newer value (C58).**
/// iroh-docs 0.101 deletes by writing an empty entry, and before taking a
/// peer's entry asks whether a newer one is already here, reading without
/// empty entries: it misses the delete, and the older entry replaces it. So
/// the catalogue clears an entry by writing a newer value instead.
///
/// The first half pins iroh-docs's bug: once it is fixed, it fails here, and
/// deleting is safe again.
#[tokio::test]
async fn an_older_entry_from_a_peer_undoes_a_delete_but_not_a_newer_value() {
    let mut store = Store::memory();
    let namespace = NamespaceSecret::from_bytes(&[1; 32]);
    let author = Author::from_bytes(&[2; 32]);
    let id = namespace.id();
    store.new_replica(namespace).unwrap();

    for key in [b"deleted".as_slice(), b"cleared"] {
        store
            .open_replica(&id)
            .unwrap()
            .hash_and_insert(key, &author, b"what a peer still has")
            .await
            .unwrap();
    }
    let older = |store: &mut Store, key: &[u8]| {
        store
            .get_exact(id, author.id(), key, false)
            .unwrap()
            .expect("written")
    };
    let (deleted_before, cleared_before) =
        (older(&mut store, b"deleted"), older(&mut store, b"cleared"));
    // Timestamps are in microseconds: the next write is strictly newer.
    tokio::time::sleep(Duration::from_millis(2)).await;

    let mut replica = store.open_replica(&id).unwrap();
    replica.delete_prefix(b"deleted", &author).await.unwrap();
    replica
        .insert_remote_entry(deleted_before.clone(), PEER, ContentStatus::Complete)
        .await
        .expect("iroh-docs takes the older entry over the delete");

    replica
        .hash_and_insert(b"cleared", &author, b"null")
        .await
        .unwrap();
    let refused = replica
        .insert_remote_entry(cleared_before, PEER, ContentStatus::Complete)
        .await;
    assert!(
        matches!(refused, Err(InsertError::NewerEntryExists)),
        "{refused:?}"
    );
    drop(replica);

    assert_eq!(
        store.get_exact(id, author.id(), b"deleted", false).unwrap(),
        Some(deleted_before),
        "the deleted entry is back"
    );
    let cleared = store
        .get_exact(id, author.id(), b"cleared", false)
        .unwrap()
        .unwrap();
    assert_eq!(
        *cleared.content_hash().as_bytes(),
        *blake3::hash(b"null").as_bytes()
    );
}
