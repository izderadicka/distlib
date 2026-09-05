//! A group whose allowlist comes from the log rather than from configuration.
//!
//! This is the Phase 1 claim end to end: found a group, admit a member, expel
//! one, and watch what each node will talk to follow the committed log without
//! anybody editing a config file.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point
#![allow(clippy::result_large_err)] // openraft's error types, in its own signatures

use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use distlib_consensus::{MemberRecord, MembershipEvent};
use distlib_core::{MemberId, NodeAddr};
use iroh::SecretKey;

mod common;
use common::{Peer, wait_for};

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
        second
            .node
            .raft()
            .unwrap()
            .metrics()
            .borrow()
            .current_leader,
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
    let bystander = Peer::start_with(SecretKey::generate(), vec![founder.id], Vec::new()).await;
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

    let founder_addr = NodeAddr {
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
        distlib_net::AddressBook::default(),
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
    let bystander = Peer::start_with(SecretKey::generate(), vec![founder.id], Vec::new()).await;
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

    let bystander_addr = NodeAddr {
        relay: None,
        direct: bystander.addr.direct.clone(),
    };

    // Even the founder — a real voter of a real group, and the only member the
    // bystander will talk to — gets nothing here. Being a voter somewhere else
    // is not being a voter of a Raft this node does not run.
    //
    // Refused during the handshake rather than after it: a node that follows
    // rather than votes serves ping and nothing else, and iroh's router is what
    // an endpoint advertises, so the ALPN is not on offer in the first place.
    // Stronger than the connect-then-close the voter gate gives, and the reason
    // both are worth having — a core node has to advertise raft, so its
    // refusals happen a step later.
    let refused = founder
        .node
        .endpoint()
        .connect(
            bystander_addr.to_endpoint_addr(bystander.id).unwrap(),
            distlib_net::alpn::RAFT,
        )
        .await
        .expect_err("a node that founds nothing must not serve raft at all");
    assert!(
        format!("{refused}").contains("known protocol"),
        "refused for the wrong reason: {refused}"
    );

    founder.node.shutdown().await;
    bystander.node.shutdown().await;
}

#[tokio::test]
async fn a_follower_catches_up_on_the_log_it_was_never_pushed() {
    // The claim of follower mode: a member who votes on nothing still ends up
    // enforcing exactly what the group decided, by fetching it.
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

    // Not in the core group, so it follows rather than votes. It is given the
    // founder's address because it has no log yet to find one in.
    let follower_key = SecretKey::generate();
    let follower_id = MemberId::from(follower_key.public());
    founder
        .node
        .propose(
            MembershipEvent::MemberAdded {
                member: MemberRecord {
                    member_id: follower_id,
                    display_name: "follower".to_owned(),
                    pledge_bytes: 0,
                },
            },
            &founder.secret,
        )
        .await
        .unwrap();

    let follower = Peer::start_with(
        follower_key,
        vec![founder.id],
        vec![(founder.id, founder.addr.clone())],
    )
    .await;
    assert!(!follower.node.is_core(), "not in the core group");
    assert!(follower.node.raft().is_none(), "and so runs no raft");

    wait_for(&follower, "the group to arrive", |membership| {
        membership.group_id() == founder.node.membership().group_id()
    })
    .await;
    assert_eq!(
        follower.node.membership(),
        founder.node.membership(),
        "a follower must reach exactly the membership the group decided"
    );

    // And it keeps up: a change made after it caught up reaches it too.
    let newcomer = MemberId::from(SecretKey::generate().public());
    founder
        .node
        .propose(
            MembershipEvent::MemberAdded {
                member: MemberRecord {
                    member_id: newcomer,
                    display_name: "later".to_owned(),
                    pledge_bytes: 0,
                },
            },
            &founder.secret,
        )
        .await
        .unwrap();

    wait_for(&follower, "a later change to arrive", |membership| {
        membership.is_member(&newcomer)
    })
    .await;

    // And what it *enforces* follows from that, which is the point of holding
    // the log at all. The bridge from the projection to the allowlist is its
    // own task, so this is waited for rather than asserted outright.
    tokio::time::timeout(Duration::from_secs(5), async {
        while !follower.hooks.allowlist().is_allowed(&newcomer) {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("a follower must admit whoever the log says is a member");

    follower.node.shutdown().await;
    founder.node.shutdown().await;
}

#[tokio::test]
async fn a_follower_proposes_through_a_core_node() {
    // §4.3 opens proposing to any member, and a follower has no Raft to commit
    // with — so it hands the event to a core node, which commits it as if the
    // proposal had originated there.
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

    let follower_key = SecretKey::generate();
    let follower_id = MemberId::from(follower_key.public());
    founder
        .node
        .propose(
            MembershipEvent::MemberAdded {
                member: MemberRecord {
                    member_id: follower_id,
                    display_name: "follower".to_owned(),
                    pledge_bytes: 0,
                },
            },
            &founder.secret,
        )
        .await
        .unwrap();

    let follower = Peer::start_with(
        follower_key,
        vec![founder.id],
        vec![(founder.id, founder.addr.clone())],
    )
    .await;
    wait_for(&follower, "the group to arrive", |membership| {
        membership.is_member(&follower.id)
    })
    .await;

    // A pledge, because that is the one thing only its owner may propose — so
    // this could not have been committed by anybody else on its behalf.
    follower
        .node
        .propose(
            MembershipEvent::PledgeChanged {
                member: follower.id,
                pledge_bytes: 4096,
            },
            &follower.secret,
        )
        .await
        .expect("a follower must be able to propose");

    wait_for(&founder, "the follower's pledge to commit", |membership| {
        membership
            .member(&follower.id)
            .is_some_and(|record| record.pledge_bytes == 4096)
    })
    .await;

    follower.node.shutdown().await;
    founder.node.shutdown().await;
}

#[tokio::test]
async fn a_follower_moves_on_from_a_source_that_does_not_answer() {
    // A follower that gave up on the first core node it could not reach would
    // sit frozen at whatever it last saw — still enforcing it. So an
    // unreachable source is a reason to ask somebody else, not to stop.
    //
    // The dead source is first in the list and stays dead, which makes this
    // deterministic: the same rotation runs when a live source is killed
    // mid-follow, but that would need an election to finish before the group
    // could move again.
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

    let absent = MemberId::from(SecretKey::generate().public());
    let follower_key = SecretKey::generate();
    let follower_id = MemberId::from(follower_key.public());
    founder
        .node
        .propose(
            MembershipEvent::MemberAdded {
                member: MemberRecord {
                    member_id: follower_id,
                    display_name: "follower".to_owned(),
                    pledge_bytes: 0,
                },
            },
            &founder.secret,
        )
        .await
        .unwrap();

    let follower = Peer::start_with(
        follower_key,
        vec![founder.id, absent],
        vec![
            // Nothing is listening here, and nothing ever will be.
            (
                absent,
                NodeAddr {
                    relay: None,
                    direct: [SocketAddr::from((Ipv4Addr::LOCALHOST, 1))]
                        .into_iter()
                        .collect(),
                },
            ),
            (founder.id, founder.addr.clone()),
        ],
    )
    .await;

    wait_for(&follower, "the log to arrive from the second source", |m| {
        m.group_id() == founder.node.membership().group_id()
    })
    .await;
    assert_eq!(follower.node.membership(), founder.node.membership());

    follower.node.shutdown().await;
    founder.node.shutdown().await;
}

#[tokio::test]
async fn a_change_reaches_a_follower_without_waiting_for_its_timer() {
    // What gossip buys. The follow loop's idle timer is 30 seconds — long on
    // purpose, since thousands of members polling three to seven core nodes is
    // what gossip exists to avoid — so a change that arrives in a second or two
    // cannot have come from the timer.
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

    let follower_key = SecretKey::generate();
    let follower_id = MemberId::from(follower_key.public());
    founder
        .node
        .propose(
            MembershipEvent::MemberAdded {
                member: MemberRecord {
                    member_id: follower_id,
                    display_name: "follower".to_owned(),
                    pledge_bytes: 0,
                },
            },
            &founder.secret,
        )
        .await
        .unwrap();

    let follower = Peer::start_with(
        follower_key,
        vec![founder.id],
        vec![(founder.id, founder.addr.clone())],
    )
    .await;
    wait_for(&follower, "the first catch-up", |membership| {
        membership.is_member(&follower.id)
    })
    .await;

    // Let the gossip swarm form before making the change. Catching up says the
    // follow loop works, not that the topic is connected — and an announcement
    // made before this node can hear it is simply lost, since gossip is
    // best-effort and does not replay. That is by design, and it is what the
    // 30-second timer covers; without this pause the test would be asserting
    // promptness across a window where the design promises none.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Caught up and now idle, so its next scheduled fetch is 30 seconds away.
    let newcomer = MemberId::from(SecretKey::generate().public());
    let announced = std::time::Instant::now();
    founder
        .node
        .propose(
            MembershipEvent::MemberAdded {
                member: MemberRecord {
                    member_id: newcomer,
                    display_name: "newcomer".to_owned(),
                    pledge_bytes: 0,
                },
            },
            &founder.secret,
        )
        .await
        .unwrap();

    wait_for(&follower, "the change to be announced and fetched", |m| {
        m.is_member(&newcomer)
    })
    .await;

    let took = announced.elapsed();
    assert!(
        took < Duration::from_secs(10),
        "the timer is 30s, so {took:?} means this waited for it rather than being told"
    );

    follower.node.shutdown().await;
    founder.node.shutdown().await;
}

#[tokio::test]
async fn voters_that_never_spoke_can_still_be_dialled_by_id() {
    // What iroh-gossip needs and cannot ask for. It subscribes with bare
    // endpoint ids, so a member is reachable to it only if the endpoint can
    // resolve one — and with no relay and no address lookup, the only ids that
    // resolve are those of peers already spoken to. Raft connects the leader to
    // each voter and never one voter to another, so without an address book two
    // non-leading voters can never become gossip neighbours, and the mesh
    // collapses to a star centred on the leader.
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
        .map(|(index, peer)| (peer.record(&format!("voter-{index}")), peer.addr.clone()))
        .collect();
    peers[0]
        .node
        .init_group(founders, &peers[0].secret)
        .await
        .unwrap();
    for peer in &peers {
        wait_for(peer, "the group to be founded", |m| m.group_id().is_some()).await;
    }

    // Two voters that are not the leader, so neither has any reason to have
    // dialled the other: Raft replicates leader to voter, never voter to voter.
    let leader = peers
        .iter()
        .position(|peer| {
            peer.node
                .raft()
                .and_then(|raft| raft.metrics().borrow().current_leader)
                .is_some_and(|id| MemberId::try_from(id).is_ok_and(|id| id == peer.id))
        })
        .expect("a founded group has a leader");
    let (dialling, target) = peers
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != leader)
        .map(|(_, peer)| peer)
        .collect::<Vec<_>>()
        .split_first()
        .map(|(first, rest)| (*first, rest[0]))
        .expect("three voters leave two that do not lead");

    // The id and nothing else — no address, no relay, exactly what gossip has.
    let echo = distlib_net::ping::ping(
        dialling.node.endpoint(),
        iroh::EndpointAddr::new(target.id.endpoint_id()),
        b"by id alone",
    )
    .await
    .expect("a voter must be reachable by id, or gossip cannot reach it either");
    assert_eq!(echo, b"by id alone");

    for peer in &peers {
        peer.node.shutdown().await;
    }
}

#[tokio::test]
async fn a_follower_learns_the_rest_of_the_core_group_from_the_one_it_asks() {
    // A follower holds no `StoredMembership`, so the core group it is told
    // about when it fetches is its whole picture of where the group lives. Here
    // it starts knowing one address — a ticket naming a single node, or the
    // only one still at the address it was founded with — and has to end up
    // able to reach the others, which is what rotating off a dead source and
    // joining the gossip mesh both need.
    let keys: Vec<SecretKey> = (0..2).map(|_| SecretKey::generate()).collect();
    let ids: Vec<MemberId> = keys
        .iter()
        .map(|key| MemberId::from(key.public()))
        .collect();

    let mut core = Vec::new();
    for (index, key) in keys.iter().enumerate() {
        let others = ids
            .iter()
            .enumerate()
            .filter(|(other, _)| *other != index)
            .map(|(_, id)| *id)
            .collect();
        core.push(Peer::start(key.clone(), others).await);
    }

    let founders = core
        .iter()
        .enumerate()
        .map(|(index, peer)| (peer.record(&format!("voter-{index}")), peer.addr.clone()))
        .collect();
    core[0]
        .node
        .init_group(founders, &core[0].secret)
        .await
        .unwrap();
    for peer in &core {
        wait_for(peer, "the group to be founded", |m| m.group_id().is_some()).await;
    }

    let key = SecretKey::generate();
    let joiner = MemberId::from(key.public());
    core[0]
        .node
        .propose(
            MembershipEvent::MemberAdded {
                member: MemberRecord {
                    member_id: joiner,
                    display_name: "late arrival".to_owned(),
                    pledge_bytes: 0,
                },
            },
            &core[0].secret,
        )
        .await
        .unwrap();

    // One address, not two: everything else has to come from the log's own
    // answers.
    let follower =
        Peer::start_with(key, ids.clone(), vec![(core[0].id, core[0].addr.clone())]).await;
    wait_for(&follower, "the log to reach the follower", |m| {
        m.is_member(&joiner)
    })
    .await;

    let echo = distlib_net::ping::ping(
        follower.node.endpoint(),
        iroh::EndpointAddr::new(core[1].id.endpoint_id()),
        b"never introduced",
    )
    .await
    .expect("a follower must learn where the other core nodes are from the ones it asks");
    assert_eq!(echo, b"never introduced");

    follower.node.shutdown().await;
    for peer in &core {
        peer.node.shutdown().await;
    }
}

#[tokio::test]
async fn an_expelled_follower_stops_asking_and_says_so() {
    // §4.4 from the outside. An expelled member is refused by every core node,
    // so it never receives the entry expelling it — the only way it can find
    // out is by being turned away. Before this it kept asking about once a
    // second for the life of the process, filling every core node's log with
    // refusals, which is a denial of service against the group by a node that
    // is no longer in it.
    let core_key = SecretKey::generate();
    let core_id = MemberId::from(core_key.public());
    let follower_key = SecretKey::generate();
    let follower_id = MemberId::from(follower_key.public());

    let core = Peer::start(core_key, vec![follower_id]).await;
    core.node
        .init_group(vec![(core.record("core"), core.addr.clone())], &core.secret)
        .await
        .unwrap();
    core.node
        .propose(
            MembershipEvent::MemberAdded {
                member: MemberRecord {
                    member_id: follower_id,
                    display_name: "for now".to_owned(),
                    pledge_bytes: 0,
                },
            },
            &core.secret,
        )
        .await
        .unwrap();

    let follower = Peer::start_with(
        follower_key,
        vec![core_id],
        vec![(core.id, core.addr.clone())],
    )
    .await;
    wait_for(&follower, "the log to reach the follower", |m| {
        m.is_member(&follower_id)
    })
    .await;

    // Let the gossip swarm form before taking it away. Joining the topic
    // happens after the first fetch, so without this the expulsion can land
    // while the follower has no neighbour to lose — which is a slower path
    // rather than a broken one, and not the one being measured here.
    tokio::time::sleep(Duration::from_secs(2)).await;

    core.node
        .propose(
            MembershipEvent::MemberExpelled {
                member: follower_id,
                reason: "the test says so".to_owned(),
            },
            &core.secret,
        )
        .await
        .unwrap();

    // Generous, because two mechanisms can deliver this and only one is quick:
    // a core node closing its connections shows up as a gossip neighbour going
    // away, which wakes the loop at once, and failing that its own timer comes
    // round within thirty seconds. What is being asserted is that it works out
    // it has been expelled at all — before this it never did.
    tokio::time::timeout(Duration::from_secs(45), follower.node.expelled())
        .await
        .expect("an expelled follower must work out that it has been expelled");

    // And it still holds the log it had: expulsion is not amnesia, and §4.4 is
    // explicit that an expelled member keeps what it already has.
    assert!(
        follower.node.membership().group_id().is_some(),
        "the node knows which group threw it out"
    );

    follower.node.shutdown().await;
    core.node.shutdown().await;
}
