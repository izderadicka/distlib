//! A collection pass and the catalogue's own writes.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use distlib_consensus::{MemberProfile, MembershipEvent, MembershipState, SignedEvent, Timestamp};
use distlib_core::{MemberId, NodeAddr};
use distlib_net::Transport;
use distlib_sync::{Catalogue, CatalogueProtection, CatalogueStorage};
use iroh::{
    Endpoint, SecretKey,
    endpoint::{RelayMode, presets},
};
use iroh_blobs::store::{
    GcConfig, ProtectCb,
    mem::{MemStore, Options},
};
use tokio::sync::watch;

const KEY: &str = "item/1/title";
const PASS_EVERY: Duration = Duration::from_millis(100);

/// **A write that lands while a pass runs keeps its content.** A pass asks the
/// documents what they reference, then lists tags and temporary tags, then
/// deletes the rest: a write in between, whose value was held by a temporary
/// tag only until its entry was in, is in neither list. Here the write is
/// made at exactly that moment, once, from the pass's own protect callback.
#[tokio::test]
async fn a_write_made_while_a_pass_runs_keeps_its_content() {
    let secret = SecretKey::generate();
    let me = MemberId::from(secret.public());
    let (protection, protected_by_catalogue) = CatalogueProtection::new();
    let catalogue_once_open: Arc<OnceLock<Catalogue>> = Arc::default();
    let passes = Arc::new(AtomicU64::new(0));
    let protected = write_once_mid_pass(
        protected_by_catalogue,
        Arc::clone(&catalogue_once_open),
        Arc::clone(&passes),
    );
    let blobs = MemStore::new_with_opts(Options {
        gc_config: Some(GcConfig {
            interval: PASS_EVERY,
            add_protected: Some(protected),
        }),
    });

    let endpoint = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .secret_key(secret.clone())
        .alpns(distlib_sync::alpns())
        .bind_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .unwrap()
        .bind()
        .await
        .unwrap();
    let addr = NodeAddr {
        relay: None,
        direct: endpoint.bound_sockets().into_iter().collect(),
    };
    let gossip = distlib_net::spawn_gossip(&endpoint);
    let transport = Transport::new(endpoint, gossip).unwrap();
    let (_log, membership) = watch::channel(founded_alone(&secret, me, addr));
    let storage = CatalogueStorage {
        blobs: (*blobs).clone(),
        documents: None,
        protect: Some(protection),
        serving_events: None,
    };
    let catalogue = Catalogue::start(transport, storage, &secret, membership)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), catalogue.ready())
        .await
        .unwrap();
    catalogue_once_open.set(catalogue.clone()).unwrap();

    // The pass that writes, and two whole passes after it.
    let until = passes.load(Ordering::SeqCst) + 3;
    while passes.load(Ordering::SeqCst) < until {
        tokio::time::sleep(PASS_EVERY).await;
    }
    let read = catalogue.get(KEY).await;
    assert!(
        matches!(&read, Ok(Some(value)) if value.as_ref() == b"Dune"),
        "the value written mid-pass is gone: {read:?}"
    );
    catalogue.shutdown();
}

/// The catalogue's protect callback, which once the catalogue is open writes
/// [`KEY`] after the documents have said what they reference, before the pass
/// lists tags; and counts the passes.
fn write_once_mid_pass(
    protected_by_catalogue: ProtectCb,
    catalogue_once_open: Arc<OnceLock<Catalogue>>,
    passes: Arc<AtomicU64>,
) -> ProtectCb {
    let written = Arc::new(AtomicBool::new(false));
    Arc::new(move |live| {
        let referenced = protected_by_catalogue(live);
        let write = catalogue_once_open
            .get()
            .filter(|_| !written.swap(true, Ordering::SeqCst))
            .cloned();
        let passes = Arc::clone(&passes);
        Box::pin(async move {
            let outcome = referenced.await;
            if let Some(catalogue) = write {
                // Spawned and awaited: the callback's future must be `Sync`.
                tokio::spawn(async move { catalogue.put(KEY, "Dune").await.unwrap() })
                    .await
                    .unwrap();
            }
            passes.fetch_add(1, Ordering::SeqCst);
            outcome
        })
    })
}

/// A group of one, founded by hand.
fn founded_alone(secret: &SecretKey, me: MemberId, addr: NodeAddr) -> MembershipState {
    let founder = MemberProfile {
        member_id: me,
        display_name: "alice".to_owned(),
    };
    let event = MembershipEvent::found(
        vec![(founder, addr)],
        Default::default(),
        Timestamp::from_millis(1),
    )
    .unwrap();
    let signed = SignedEvent::sign(secret, event, Timestamp::from_millis(1), 0).unwrap();
    let mut state = MembershipState::new();
    state.apply(1, &signed).unwrap();
    state
}
