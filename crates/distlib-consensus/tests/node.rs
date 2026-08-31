//! A group whose allowlist comes from the log rather than from configuration.
//!
//! This is the Phase 1 claim end to end: found a group, admit a member, expel
//! one, and watch what each node will talk to follow the committed log without
//! anybody editing a config file.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point
#![allow(clippy::result_large_err)] // openraft's error types, in its own signatures

use std::{
    collections::BTreeSet,
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use distlib_consensus::{MemberRecord, MembershipEvent, MembershipNode, NodeAddr};
use distlib_core::MemberId;
use distlib_net::{AllowlistHooks, allowlist, endpoint::configure};
use iroh::{
    Endpoint, SecretKey,
    endpoint::{RelayMode, presets},
};
use tempfile::TempDir;

/// A member, its endpoint and its running consensus.
struct Peer {
    secret: SecretKey,
    id: MemberId,
    node: MembershipNode,
    addr: NodeAddr,
    /// Kept so a test can ask what this node would actually admit, which is the
    /// thing being enforced — not just what the log says.
    hooks: AllowlistHooks,
    _dir: TempDir,
}

impl Peer {
    /// Starts a node whose allowlist is seeded with `bootstrap`.
    /// Starts a node seeded with `bootstrap` that will found with `bootstrap`.
    ///
    /// The common case: everyone this node talks to before there is a log is
    /// somebody it is founding with.
    async fn start(secret: SecretKey, bootstrap: Vec<MemberId>) -> Self {
        let id = MemberId::from(secret.public());
        let founding_core = bootstrap.iter().copied().chain([id]).collect();
        Self::start_with(secret, bootstrap, founding_core).await
    }

    /// Starts a node whose founding core group is stated separately.
    ///
    /// The two sets are not the same thing, and a node that conflates them
    /// serves consensus to anyone it would talk to. A member with an empty
    /// founding core is one that is not founding anything — it should speak
    /// `distlib/raft/0` with nobody.
    async fn start_with(
        secret: SecretKey,
        bootstrap: Vec<MemberId>,
        founding_core: BTreeSet<MemberId>,
    ) -> Self {
        let id = MemberId::from(secret.public());
        let dir = TempDir::new().unwrap();

        // The bootstrap seed and the hooks share one channel; the node gets the
        // write half. Exactly the arrangement production uses.
        let (writer, reader) = allowlist(id, bootstrap);
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
        let node =
            MembershipNode::start(endpoint, hooks.clone(), writer, dir.path(), founding_core)
                .await
                .unwrap();

        Self {
            secret,
            id,
            node,
            addr,
            hooks,
            _dir: dir,
        }
    }

    fn record(&self, name: &str) -> MemberRecord {
        MemberRecord {
            member_id: self.id,
            display_name: name.to_owned(),
            pledge_bytes: 0,
        }
    }
}

/// Waits for a node's derived membership to satisfy `predicate`.
async fn wait_for(
    peer: &Peer,
    what: &str,
    predicate: impl Fn(&distlib_consensus::MembershipState) -> bool,
) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if predicate(&peer.node.membership()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

#[tokio::test]
async fn a_founded_group_derives_its_membership_from_the_log() {
    let founder = Peer::start(SecretKey::generate(), vec![]).await;

    founder
        .node
        .init_group(
            vec![(founder.record("founder"), founder.addr.clone())],
            &founder.secret,
        )
        .await
        .unwrap();

    wait_for(&founder, "the founding event to apply", |membership| {
        membership.group_id().is_some()
    })
    .await;

    let membership = founder.node.membership();
    assert!(membership.is_member(&founder.id));
    assert!(
        membership.core().contains(&founder.id),
        "founders are the initial voters"
    );
    founder.node.shutdown().await;
}

#[tokio::test]
async fn admitting_a_member_reaches_every_node() {
    // Two core nodes, each seeded with the other so they can replicate at all.
    let one = SecretKey::generate();
    let two = SecretKey::generate();
    let (one_id, two_id) = (MemberId::from(one.public()), MemberId::from(two.public()));

    let first = Peer::start(one, vec![two_id]).await;
    let second = Peer::start(two, vec![one_id]).await;

    first
        .node
        .init_group(
            vec![
                (first.record("first"), first.addr.clone()),
                (second.record("second"), second.addr.clone()),
            ],
            &first.secret,
        )
        .await
        .unwrap();

    wait_for(&second, "the founding event to replicate", |membership| {
        membership.group_id().is_some()
    })
    .await;

    // A third member, admitted through the log rather than a config file.
    let newcomer = MemberId::from(SecretKey::generate().public());
    first
        .node
        .propose(
            MembershipEvent::MemberAdded {
                member: MemberRecord {
                    member_id: newcomer,
                    display_name: "newcomer".to_owned(),
                    pledge_bytes: 0,
                },
            },
            &first.secret,
        )
        .await
        .unwrap();

    for peer in [&first, &second] {
        wait_for(peer, "the new member to reach every node", |membership| {
            membership.is_member(&newcomer)
        })
        .await;
    }

    first.node.shutdown().await;
    second.node.shutdown().await;
}

#[tokio::test]
async fn the_bootstrap_seed_survives_until_a_group_exists() {
    // The circular-start problem, pinned. Core nodes cannot replicate the
    // founding entry without connecting to each other, and cannot connect
    // without an allowlist — so an unfounded node must keep the seed rather
    // than adopt the log's empty membership.
    let peer = SecretKey::generate();
    let seeded = MemberId::from(SecretKey::generate().public());
    let node = Peer::start(peer, vec![seeded]).await;

    // Long enough for the follow task to have run and, if it were wrong,
    // overwritten the seed with the log's empty set.
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert!(
        node.node.membership().group_id().is_none(),
        "no group has been founded, so the log says nothing about membership"
    );
    assert!(
        node.hooks.allowlist().is_allowed(&seeded),
        "the seed must still be enforced; without it core nodes could never \
         reach each other to replicate the founding entry"
    );
    node.node.shutdown().await;
}

#[tokio::test]
async fn founding_replaces_the_seed_with_the_log() {
    // The other half of the rule. Once `GroupFounded` is applied the log is
    // authoritative, so a member who was only ever in the seed stops being
    // admitted — otherwise a stale config would keep granting access forever.
    let founder_key = SecretKey::generate();
    let stale = MemberId::from(SecretKey::generate().public());
    let founder = Peer::start(founder_key, vec![stale]).await;

    assert!(
        founder.hooks.allowlist().is_allowed(&stale),
        "seeded before founding"
    );

    founder
        .node
        .init_group(
            vec![(founder.record("founder"), founder.addr.clone())],
            &founder.secret,
        )
        .await
        .unwrap();

    wait_for(&founder, "the founding event to apply", |membership| {
        membership.group_id().is_some()
    })
    .await;

    // The bridge publishes on the next change; give it a moment to land.
    tokio::time::timeout(Duration::from_secs(5), async {
        while founder.hooks.allowlist().is_allowed(&stale) {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("a member only ever in the seed must stop being admitted once the log speaks");

    assert!(founder.hooks.allowlist().is_allowed(&founder.id));
    founder.node.shutdown().await;
}

#[tokio::test]
async fn a_follower_can_propose() {
    // §4.3 has a member submit a proposal to *any* core node, and most core
    // nodes are not the leader: one admitted to the core group later is a
    // follower, and so is a node that restarted while somebody else held the
    // term. Only the leader can commit, so a follower has to hand it on.
    let one = SecretKey::generate();
    let two = SecretKey::generate();
    let (one_id, two_id) = (MemberId::from(one.public()), MemberId::from(two.public()));

    let first = Peer::start(one, vec![two_id]).await;
    let second = Peer::start(two, vec![one_id]).await;

    // `first` founds, so `first` is the leader and `second` is not.
    first
        .node
        .init_group(
            vec![
                (first.record("first"), first.addr.clone()),
                (second.record("second"), second.addr.clone()),
            ],
            &first.secret,
        )
        .await
        .unwrap();
    wait_for(&second, "the founding event to replicate", |membership| {
        membership.group_id().is_some()
    })
    .await;

    assert_ne!(
        second.node.raft().metrics().borrow().current_leader,
        Some(distlib_core::RawMemberId::from(second.id)),
        "this test is only meaningful while `second` is a follower"
    );

    // The follower proposes. Without forwarding this fails with
    // ForwardToLeader and the group can never be grown by anyone but its
    // founder.
    let newcomer = MemberId::from(SecretKey::generate().public());
    second
        .node
        .propose(
            MembershipEvent::MemberAdded {
                member: MemberRecord {
                    member_id: newcomer,
                    display_name: "admitted by a follower".to_owned(),
                    pledge_bytes: 0,
                },
            },
            &second.secret,
        )
        .await
        .expect("a follower must be able to propose");

    for peer in [&first, &second] {
        wait_for(peer, "the proposal to reach every node", |membership| {
            membership.is_member(&newcomer)
        })
        .await;
    }

    first.node.shutdown().await;
    second.node.shutdown().await;
}

#[tokio::test]
async fn a_proposal_the_rules_refuse_is_reported_as_refused() {
    // Committing and applying are different things. A committed event whose
    // rules do not hold is skipped rather than fatal, so returning Ok on the
    // strength of the commit would tell a caller their change took effect when
    // the membership never moved.
    let founder = Peer::start(SecretKey::generate(), vec![]).await;
    founder
        .node
        .init_group(
            vec![(founder.record("founder"), founder.addr.clone())],
            &founder.secret,
        )
        .await
        .unwrap();
    wait_for(&founder, "the founding event to apply", |membership| {
        membership.group_id().is_some()
    })
    .await;

    // Expelling somebody who is not a member: it commits, and every state
    // machine refuses it.
    let stranger = MemberId::from(SecretKey::generate().public());
    let error = founder
        .node
        .propose(
            MembershipEvent::MemberExpelled {
                member: stranger,
                reason: "never joined".to_owned(),
            },
            &founder.secret,
        )
        .await
        .expect_err("a refused event must not be reported as success");

    assert!(
        format!("{error}").contains(&stranger.to_string()),
        "the caller should learn which member was not found; got {error}"
    );
    assert!(founder.node.membership().is_member(&founder.id));
    founder.node.shutdown().await;
}

#[tokio::test]
async fn three_founders_converge_on_one_group() {
    // The shape people actually start with: a few friends who all want a say
    // from the beginning. Worth its own test because quorum stops being trivial
    // here — with three voters the founder needs one of the other two to grant
    // its vote before it can commit anything, where with one it needed nobody.
    let keys: Vec<SecretKey> = (0..3).map(|_| SecretKey::generate()).collect();
    let ids: Vec<MemberId> = keys
        .iter()
        .map(|key| MemberId::from(key.public()))
        .collect();

    let mut peers = Vec::new();
    for (index, key) in keys.iter().enumerate() {
        let others = ids
            .iter()
            .enumerate()
            .filter(|(other, _)| *other != index)
            .map(|(_, id)| *id)
            .collect();
        peers.push(Peer::start(key.clone(), others).await);
    }

    let founders = peers
        .iter()
        .enumerate()
        .map(|(index, peer)| (peer.record(&format!("founder-{index}")), peer.addr.clone()))
        .collect();
    peers[0]
        .node
        .init_group(founders, &peers[0].secret)
        .await
        .unwrap();

    for peer in &peers {
        wait_for(
            peer,
            "the founding event to reach every founder",
            |membership| membership.group_id().is_some(),
        )
        .await;
    }

    let group = peers[0].node.membership().group_id();
    for peer in &peers {
        let membership = peer.node.membership();
        assert_eq!(membership.group_id(), group, "one group, not three");
        assert_eq!(membership.core().len(), 3, "all three founders are voters");
        for id in &ids {
            assert!(membership.is_member(id));
        }
    }

    for peer in peers {
        peer.node.shutdown().await;
    }
}

#[tokio::test]
async fn a_member_who_is_not_a_voter_is_refused_raft_but_may_propose() {
    // The reason `distlib/raft/0` and `distlib/memberlog/0` are two protocols.
    // Being in the allowlist proves you are a member; it is not licence to take
    // part in consensus, because a `Vote` from a non-voter can disrupt a term.
    // Proposing is the opposite: §4.3 and §4.4 open it to every member.
    let founder = Peer::start(SecretKey::generate(), vec![]).await;
    founder
        .node
        .init_group(
            vec![(founder.record("founder"), founder.addr.clone())],
            &founder.secret,
        )
        .await
        .unwrap();
    wait_for(&founder, "the founding event to apply", |membership| {
        membership.group_id().is_some()
    })
    .await;

    // A member of the group who was never made a voter.
    let bystander =
        Peer::start_with(SecretKey::generate(), vec![founder.id], BTreeSet::new()).await;
    founder
        .node
        .propose(
            MembershipEvent::MemberAdded {
                member: bystander.record("bystander"),
            },
            &founder.secret,
        )
        .await
        .unwrap();
    wait_for(&founder, "the bystander to be admitted", |membership| {
        membership.is_member(&bystander.id)
    })
    .await;
    assert!(
        !founder.node.membership().core().contains(&bystander.id),
        "this test is only meaningful while the bystander is not a voter"
    );

    let founder_addr = distlib_consensus::NodeAddr {
        relay: None,
        direct: founder.addr.direct.clone(),
    };

    // Raft: refused. The ALPN is advertised, so the connection is established
    // and then closed — which is the earliest point the peer's identity is
    // proven and the voter set can be consulted.
    let raft = bystander
        .node
        .endpoint()
        .connect(
            founder_addr.to_endpoint_addr(founder.id).unwrap(),
            distlib_net::alpn::RAFT,
        )
        .await
        .expect("a member may open the connection; it is the RPCs that are refused");
    let closed = tokio::time::timeout(Duration::from_secs(10), raft.closed())
        .await
        .expect("the core node must close a raft connection from a non-voter");
    assert!(
        format!("{closed}").contains("not a voter"),
        "closed for the wrong reason: {closed}"
    );

    // Memberlog: served. Same peer, same node, different conversation.
    let newcomer = MemberId::from(SecretKey::generate().public());
    let event = distlib_consensus::SignedEvent::sign(
        &bystander.secret,
        MembershipEvent::MemberAdded {
            member: MemberRecord {
                member_id: newcomer,
                display_name: "invited by a non-voter".to_owned(),
                pledge_bytes: 0,
            },
        },
        distlib_consensus::Timestamp::now(),
        founder.node.membership().changed_at(),
    )
    .unwrap();

    distlib_consensus::MemberlogClient::new(
        bystander.node.endpoint().clone(),
        bystander.node.connections().clone(),
    )
    .propose(founder.id, &founder_addr, event)
    .await
    .expect("every member may propose, voter or not");

    wait_for(&founder, "the proposal to be committed", |membership| {
        membership.is_member(&newcomer)
    })
    .await;

    founder.node.shutdown().await;
    bystander.node.shutdown().await;
}

#[tokio::test]
async fn a_node_that_founds_nothing_serves_raft_to_nobody() {
    // The other direction of the gate, and the one that matters most. A node
    // that is never initialised has an empty Raft voter set for its whole life,
    // so "empty means we are founding" would leave it serving consensus to
    // every member in its allowlist forever. A member could then send it a
    // `Vote`, then an `AppendEntries` carrying a `GroupFounded` naming only
    // themselves — validly signed, `changed_at` 0 against an empty state — and
    // the victim would apply it, rebuild its allowlist from that log and evict
    // the real group.
    let founder = Peer::start(SecretKey::generate(), vec![]).await;
    founder
        .node
        .init_group(
            vec![(founder.record("founder"), founder.addr.clone())],
            &founder.secret,
        )
        .await
        .unwrap();
    wait_for(&founder, "the founding event to apply", |membership| {
        membership.group_id().is_some()
    })
    .await;

    // A node that talks to the founder but founds nothing with anybody.
    let bystander =
        Peer::start_with(SecretKey::generate(), vec![founder.id], BTreeSet::new()).await;
    founder
        .node
        .propose(
            MembershipEvent::MemberAdded {
                member: bystander.record("bystander"),
            },
            &founder.secret,
        )
        .await
        .unwrap();

    let bystander_addr = distlib_consensus::NodeAddr {
        relay: None,
        direct: bystander.addr.direct.clone(),
    };

    // Even the founder — a real voter of a real group, and the only member the
    // bystander will talk to — gets nothing here. Being a voter somewhere else
    // is not being a voter of a Raft this node does not run.
    let raft = founder
        .node
        .endpoint()
        .connect(
            bystander_addr.to_endpoint_addr(bystander.id).unwrap(),
            distlib_net::alpn::RAFT,
        )
        .await
        .expect("the allowlist admits the connection; the RPCs are what is refused");
    let closed = tokio::time::timeout(Duration::from_secs(10), raft.closed())
        .await
        .expect("a node that founds nothing must close a raft connection");
    assert!(
        format!("{closed}").contains("not a voter"),
        "closed for the wrong reason: {closed}"
    );

    founder.node.shutdown().await;
    bystander.node.shutdown().await;
}
