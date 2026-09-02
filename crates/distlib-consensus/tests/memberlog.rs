//! Fetching the membership log over `distlib/memberlog/0`.
//!
//! The half of §4.2 that lets a non-core member hold the log: it asks a core
//! node for everything since its cursor, verifies each event and folds it with
//! the same function core nodes use. These tests drive that exchange directly,
//! before there is a follower node to run it in a loop.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{
    collections::BTreeSet,
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use distlib_consensus::{
    Fetched, MemberRecord, MemberlogClient, MembershipEvent, MembershipNode, MembershipState,
};
use distlib_core::{MemberId, NodeAddr};
use distlib_net::{AllowlistHooks, Connections, allowlist, endpoint::configure};
use iroh::{
    Endpoint, SecretKey,
    endpoint::{RelayMode, presets},
};
use tempfile::TempDir;

/// A founded group of one, and somebody outside it who may ask for the log.
struct Group {
    node: MembershipNode,
    id: MemberId,
    addr: NodeAddr,
    secret: SecretKey,
    _dir: TempDir,
}

impl Group {
    async fn found() -> Self {
        let secret = SecretKey::generate();
        let id = MemberId::from(secret.public());
        let dir = TempDir::new().unwrap();

        let (writer, reader) = allowlist(id, []);
        let hooks = AllowlistHooks::new(reader);
        let endpoint = configure(
            Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled),
            secret.clone(),
            hooks.clone(),
            distlib_consensus::alpns(),
        )
        .bind_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .unwrap()
        .bind()
        .await
        .unwrap();

        let addr = NodeAddr {
            relay: None,
            direct: endpoint.bound_sockets().into_iter().collect(),
        };
        let node = MembershipNode::start(endpoint, hooks, writer, dir.path(), BTreeSet::from([id]))
            .await
            .unwrap();

        node.init_group(vec![(record(id, "founder"), addr.clone())], &secret)
            .await
            .unwrap();

        Self {
            node,
            id,
            addr,
            secret,
            _dir: dir,
        }
    }

    /// Admits `member`, so the log has something beyond its founding entry.
    async fn admit(&self, member: MemberId, name: &str) {
        self.node
            .propose(
                MembershipEvent::MemberAdded {
                    member: record(member, name),
                },
                &self.secret,
            )
            .await
            .unwrap();
    }

    /// A member of the group, with a client to ask it with.
    ///
    /// Admitted first, because the allowlist refuses a stranger's connection
    /// long before this protocol is reached.
    async fn admitted_asker(&self) -> (MemberId, MemberlogClient) {
        let secret = SecretKey::generate();
        let id = MemberId::from(secret.public());
        self.admit(id, "asker").await;

        let (_writer, reader) = allowlist(id, [self.id]);
        let endpoint = configure(
            Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled),
            secret,
            AllowlistHooks::new(reader),
            distlib_net::alpn::registered(),
        )
        .bind_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .unwrap()
        .bind()
        .await
        .unwrap();

        (id, MemberlogClient::new(endpoint, Connections::new()))
    }
}

fn record(member_id: MemberId, name: &str) -> MemberRecord {
    MemberRecord {
        member_id,
        display_name: name.to_owned(),
        pledge_bytes: 0,
    }
}

/// Folds fetched events the way a follower will.
fn fold(events: &[(u64, distlib_consensus::SignedEvent)]) -> MembershipState {
    let mut state = MembershipState::new();
    for (index, event) in events {
        state.apply(*index, event).unwrap();
    }
    state
}

#[tokio::test]
async fn the_fetched_log_folds_to_the_same_membership() {
    // The claim the whole of follower mode rests on: a member who was told
    // nothing can rebuild the group from the log alone, and arrive at exactly
    // what the core node holds.
    let group = Group::found().await;
    let (_asker, client) = group.admitted_asker().await;
    let bob = MemberId::from(SecretKey::generate().public());
    group.admit(bob, "bob").await;

    let Fetched::Entries { up_to, events, .. } =
        client.fetch(group.id, &group.addr, 0).await.unwrap()
    else {
        panic!("a founded group must hand over its log");
    };

    assert!(up_to > 0, "the founding entry is applied");
    let rebuilt = fold(&events);
    assert_eq!(
        rebuilt,
        group.node.membership(),
        "a follower that folds the log must reach the same membership"
    );
    assert!(rebuilt.is_member(&bob));

    group.node.shutdown().await;
}

#[tokio::test]
async fn a_cursor_only_advances_over_what_it_has_seen() {
    // Fetching twice must not replay: the second answer covers only what
    // happened after the first, and the two folded in sequence match one fetch
    // of everything.
    let group = Group::found().await;
    let (_asker, client) = group.admitted_asker().await;

    let Fetched::Entries { up_to, events, .. } =
        client.fetch(group.id, &group.addr, 0).await.unwrap()
    else {
        panic!("expected entries");
    };
    let mut state = fold(&events);

    let bob = MemberId::from(SecretKey::generate().public());
    group.admit(bob, "bob").await;

    let Fetched::Entries {
        events: rest,
        up_to: further,
        ..
    } = client.fetch(group.id, &group.addr, up_to).await.unwrap()
    else {
        panic!("expected entries");
    };

    assert!(further > up_to, "the log moved");
    assert!(
        rest.iter().all(|(index, _)| *index > up_to),
        "nothing already seen should come back: {rest:?}"
    );
    for (index, event) in &rest {
        state.apply(*index, event).unwrap();
    }
    assert_eq!(state, group.node.membership());

    group.node.shutdown().await;
}

#[tokio::test]
async fn the_answer_says_where_to_ask_next() {
    // §4.5 has `CoreGroupChanged` tell followers where to fetch from, but the
    // event carries ids and no addresses — so the addresses travel with the log
    // instead, and a follower can rotate to another core node without being
    // configured with one.
    let group = Group::found().await;
    let (_asker, client) = group.admitted_asker().await;

    let Fetched::Entries { source, .. } = client.fetch(group.id, &group.addr, 0).await.unwrap()
    else {
        panic!("expected entries");
    };

    assert_eq!(source.leader, Some(group.id), "the founder holds the term");
    assert!(
        source
            .core
            .iter()
            .any(|(member, addr)| *member == group.id && !addr.direct.is_empty()),
        "the core group must arrive with somewhere to reach it: {:?}",
        source.core
    );

    group.node.shutdown().await;
}

#[tokio::test]
async fn a_node_with_no_group_hands_over_nothing() {
    // Rather than an empty log, which a follower would take as "the group is
    // empty" and enforce — evicting the group it was trying to join.
    let secret = SecretKey::generate();
    let id = MemberId::from(secret.public());
    let asker_secret = SecretKey::generate();
    let asker = MemberId::from(asker_secret.public());
    let dir = TempDir::new().unwrap();

    // Seeded with the asker, since an unfounded node has no log to admit
    // anybody from — the bootstrap allowlist is all it has.
    let (writer, reader) = allowlist(id, [asker]);
    let hooks = AllowlistHooks::new(reader);
    let endpoint = configure(
        Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled),
        secret,
        hooks.clone(),
        distlib_consensus::alpns(),
    )
    .bind_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
    .unwrap()
    .bind()
    .await
    .unwrap();
    let addr = NodeAddr {
        relay: None,
        direct: endpoint.bound_sockets().into_iter().collect(),
    };
    let unfounded =
        MembershipNode::start(endpoint, hooks, writer, dir.path(), BTreeSet::from([id]))
            .await
            .unwrap();

    let asking = configure(
        Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled),
        asker_secret,
        AllowlistHooks::new(allowlist(asker, [id]).1),
        distlib_net::alpn::registered(),
    )
    .bind_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
    .unwrap()
    .bind()
    .await
    .unwrap();

    let fetched = MemberlogClient::new(asking, Connections::new())
        .fetch(id, &addr, 0)
        .await
        .unwrap();

    assert!(
        matches!(fetched, Fetched::NoGroup),
        "expected NoGroup; got {fetched:?}"
    );

    unfounded.shutdown().await;
}

#[tokio::test]
async fn an_unreachable_node_is_a_failure_rather_than_an_answer() {
    // A follower has to tell "this node is down, ask another" apart from "this
    // node says there is nothing", and only one of those is worth rotating on.
    let group = Group::found().await;
    let (_asker, client) = group.admitted_asker().await;

    let absent = MemberId::from(SecretKey::generate().public());
    let nowhere = NodeAddr {
        relay: None,
        direct: [SocketAddr::from((Ipv4Addr::LOCALHOST, 1))]
            .into_iter()
            .collect(),
    };

    let failed = tokio::time::timeout(Duration::from_secs(30), client.fetch(absent, &nowhere, 0))
        .await
        .expect("dialling nothing must fail rather than hang")
        .expect_err("there is nobody there");

    assert_eq!(failed.member, absent);

    group.node.shutdown().await;
}
