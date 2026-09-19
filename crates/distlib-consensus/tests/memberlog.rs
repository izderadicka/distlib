//! Fetching the membership log over `distlib/memberlog/0`.
//!
//! The half of §4.2 that lets a non-core member hold the log: it asks a core
//! node for everything since its cursor, verifies each event and folds it with
//! the same function core nodes use. These tests drive that exchange directly,
//! before there is a follower node to run it in a loop.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use distlib_consensus::{
    Fetched, MemberRecord, MemberlogClient, MembershipEvent, MembershipNode, MembershipState,
};
use distlib_core::{MemberId, NodeAddr, SignedAddress};
use distlib_net::{
    AddressBook, AllowlistHooks, Connections, Directory, Transport, allowlist, endpoint::configure,
};
use iroh::{
    Endpoint, SecretKey,
    endpoint::{RelayMode, presets},
    protocol::Router,
};
use iroh_gossip::net::Gossip;
use tempfile::TempDir;

/// A founded group of one, and somebody outside it who may ask for the log.
struct Group {
    node: MembershipNode,
    id: MemberId,
    addr: NodeAddr,
    secret: SecretKey,
    router: Router,
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
        let swarm = Gossip::builder().spawn(endpoint.clone());
        let node = MembershipNode::start(
            Transport::new(endpoint.clone(), swarm).unwrap(),
            hooks,
            writer,
            dir.path(),
            vec![(id, NodeAddr::default())],
        )
        .await
        .unwrap();
        let router = distlib_net::serve(endpoint, node.protocols());

        node.init_group(vec![(record(id, "founder"), addr.clone())], &secret)
            .await
            .unwrap();

        Self {
            node,
            id,
            addr,
            secret,
            router,
            _dir: dir,
        }
    }

    /// Stops this node and the transport under it, in production's order.
    async fn shutdown(&self) {
        self.node.shutdown().await;
        let _ = self.router.shutdown().await;
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
        let (id, client, _directory) = self.admitted_asker_with_directory().await;
        (id, client)
    }

    /// The same, keeping a handle on the directory the client records into.
    async fn admitted_asker_with_directory(&self) -> (MemberId, MemberlogClient, Directory) {
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

        let directory = Directory::default();
        (
            id,
            MemberlogClient::new(
                endpoint,
                Connections::new(),
                AddressBook::default(),
                directory.clone(),
            ),
            directory,
        )
    }

    /// Expels `member`, so the log no longer admits them.
    async fn expel(&self, member: MemberId) {
        self.node
            .propose(
                MembershipEvent::MemberExpelled {
                    member,
                    reason: "test".to_owned(),
                },
                &self.secret,
            )
            .await
            .unwrap();
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

    group.shutdown().await;
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

    group.shutdown().await;
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

    group.shutdown().await;
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
    let swarm = Gossip::builder().spawn(endpoint.clone());
    let unfounded = MembershipNode::start(
        Transport::new(endpoint.clone(), swarm).unwrap(),
        hooks,
        writer,
        dir.path(),
        vec![(id, NodeAddr::default())],
    )
    .await
    .unwrap();
    let serving = distlib_net::serve(endpoint, unfounded.protocols());

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

    let fetched = MemberlogClient::new(
        asking,
        Connections::new(),
        AddressBook::default(),
        Directory::default(),
    )
    .fetch(id, &addr, 0)
    .await
    .unwrap();

    assert!(
        matches!(fetched, Fetched::NoGroup),
        "expected NoGroup; got {fetched:?}"
    );

    unfounded.shutdown().await;
    let _ = serving.shutdown().await;
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

    group.shutdown().await;
}

/// A reachable address for a test member, distinct per port.
fn somewhere(port: u16) -> NodeAddr {
    NodeAddr {
        relay: None,
        direct: [SocketAddr::from((Ipv4Addr::LOCALHOST, port))]
            .into_iter()
            .collect(),
    }
}

/// 2a-5: a core node answers for the group, and what it answers with stands on
/// its own.
///
/// Bob announced on the gossip topic; the asker was not there to hear it and
/// never will be, because an announcement is made once. So the only way it
/// resolves bob is by asking somebody who was listening.
#[tokio::test]
async fn a_core_node_says_where_a_member_it_heard_from_is() {
    let group = Group::found().await;
    let (_asker, client, directory) = group.admitted_asker_with_directory().await;

    let bob_key = SecretKey::generate();
    let bob = MemberId::from(bob_key.public());
    group.admit(bob, "bob").await;

    // What the founder's gossip listener would have done with bob's
    // announcement. Reaching into the node's directory rather than standing up
    // a topic: this test is about the answer, not about how it got there.
    let announced = SignedAddress::sign(&bob_key, somewhere(6101), 1).unwrap();
    assert!(group.node.known_addresses().learn(&announced).unwrap());

    assert_eq!(directory.address_of(bob), None, "nothing is known yet");
    assert_eq!(client.directory(group.id, &group.addr).await.unwrap(), 1);
    assert_eq!(directory.address_of(bob), Some(somewhere(6101)));

    group.shutdown().await;
}

/// The filter that stands in for forgetting: what the log no longer admits does
/// not go on the wire, even though the directory still holds it.
#[tokio::test]
async fn an_expelled_members_address_is_not_relayed() {
    let group = Group::found().await;

    let bob_key = SecretKey::generate();
    let bob = MemberId::from(bob_key.public());
    group.admit(bob, "bob").await;
    let announced = SignedAddress::sign(&bob_key, somewhere(6102), 1).unwrap();
    assert!(group.node.known_addresses().learn(&announced).unwrap());

    group.expel(bob).await;
    assert!(
        group.node.known_addresses().address_of(bob).is_some(),
        "the directory records what was said; expulsion is the log's business"
    );

    // Admitted after the expulsion, so it hears the group as it stands now.
    let (_asker, client, directory) = group.admitted_asker_with_directory().await;
    assert_eq!(client.directory(group.id, &group.addr).await.unwrap(), 0);
    assert_eq!(directory.address_of(bob), None);

    group.shutdown().await;
}
