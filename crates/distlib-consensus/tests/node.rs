//! A group whose allowlist comes from the log rather than from configuration.
//!
//! This is the Phase 1 claim end to end: found a group, admit a member, expel
//! one, and watch what each node will talk to follow the committed log without
//! anybody editing a config file.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point
#![allow(clippy::result_large_err)] // openraft's error types, in its own signatures

use std::time::Duration;

use std::net::{Ipv4Addr, SocketAddr};

use distlib_consensus::{MemberRecord, MembershipEvent};
use distlib_core::{MemberId, NodeAddr};
use iroh::SecretKey;

mod common;
use common::{PATIENTLY, Peer, pending_on, until, until_upto, wait_for, wait_for_upto};

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
        membership.is_core(&founder.id),
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

/// Three founders, all voters, with the group founded and settled.
async fn a_founded_trio() -> Vec<Peer> {
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
        wait_for(peer, "the group to be founded", |m| m.group_id().is_some()).await;
    }
    peers
}

/// What openraft — not the projection — currently holds for `peer`.
///
/// The distinction is the whole point of these two tests: the projection is
/// what the log says, and openraft's membership is what consensus actually
/// uses. P1-23 was that the second never moved.
fn raft_voters(peer: &Peer) -> Vec<(MemberId, NodeAddr)> {
    let raft = peer.node.raft().expect("a voter has a raft");
    let metrics = raft.server_metrics();
    let membership = metrics.borrow().membership_config.clone();
    let voters: Vec<_> = membership.voter_ids().collect();
    membership
        .nodes()
        .filter(|(id, _)| voters.contains(id))
        .map(|(id, addr)| {
            (
                MemberId::try_from(*id).expect("a voter id is a member id"),
                addr.clone(),
            )
        })
        .collect()
}

#[tokio::test]
async fn a_core_node_that_moves_is_dialled_at_its_new_address() {
    // P1-23, the half that bites first: `raft.initialize` writes the voter map
    // once and nothing rewrote it, so with `relay_mode = "disabled"` a core node
    // that changed IP or port was out of its own group for good. Machines get
    // renumbered far more often than founders get replaced.
    let peers = a_founded_trio().await;

    // A second address for the third node, keeping the one that works — the
    // realistic shape of a machine gaining an interface, and it leaves the
    // cluster able to carry on while the change commits.
    let mut moved = peers[2].addr.clone();
    moved
        .direct
        .insert(SocketAddr::from((Ipv4Addr::LOCALHOST, 11299)));

    let core: Vec<(MemberId, NodeAddr)> = peers
        .iter()
        .map(|peer| {
            if peer.id == peers[2].id {
                (peer.id, moved.clone())
            } else {
                (peer.id, peer.addr.clone())
            }
        })
        .collect();

    peers[0]
        .node
        .propose(MembershipEvent::CoreGroupChanged { core }, &peers[0].secret)
        .await
        .unwrap();

    // The projection is what the log says; openraft's membership is what
    // consensus dials. Before this, only the first of the two ever moved.
    //
    // Every voter, not just the leader. Only the leader can *submit* a
    // membership change — it is a write — but openraft carries it as log
    // entries, so the others receive it by ordinary replication rather than by
    // doing anything themselves. That is the whole reason the reconciliation
    // loop does nothing on a non-leader, so it is worth a test rather than a
    // comment.
    for peer in &peers {
        until("every voter's raft to be told the new address", || {
            raft_voters(peer).contains(&(peers[2].id, moved.clone()))
        })
        .await;
    }

    for peer in &peers {
        wait_for(peer, "every node to record the new address", |m| {
            m.core().get(&peers[2].id) == Some(&moved)
        })
        .await;
    }

    for peer in peers {
        peer.node.shutdown().await;
    }
}

#[tokio::test]
async fn a_core_node_the_log_drops_stops_voting() {
    // Also the fix for a bug that was quietly there all along: expulsion
    // already removed a member from the projection's core group, and openraft
    // kept counting them as a voter — a vote the group could never collect, so
    // expelling core members silently ate the fault tolerance they provided.
    let peers = a_founded_trio().await;
    assert_eq!(raft_voters(&peers[0]).len(), 3, "all three founders vote");

    peers[0]
        .node
        .propose(
            MembershipEvent::MemberExpelled {
                member: peers[2].id,
                reason: "went away".to_owned(),
            },
            &peers[0].secret,
        )
        .await
        .unwrap();

    // Removing a voter takes a majority of the voters (§4.4), so one core node
    // saying so is not enough — and this is the first place that rule meets
    // real consensus rather than a folded log. The second approval comes from
    // the other survivor; the third node is the one being removed and could not
    // approve it anyway.
    //
    // Waited for on the node about to approve, not read off the proposer: an
    // approval names a log index, and signing one against a membership this
    // node has not applied yet is the race that has broken tests here before.
    let proposal = pending_on(&peers[1], "the expulsion to be pending on the second voter").await;
    assert_eq!(
        raft_voters(&peers[0]).len(),
        3,
        "one core node's word must not remove a voter"
    );
    peers[1]
        .node
        .propose(MembershipEvent::Approved { proposal }, &peers[1].secret)
        .await
        .unwrap();

    // Both survivors, for the same reason: the change replicates, it is not
    // re-derived independently. The expelled node is not asked — the group has
    // stopped talking to it, which is the point of expelling it.
    for peer in &peers[..2] {
        until(
            "every surviving voter to stop counting the expelled node",
            || {
                let voters = raft_voters(peer);
                voters.len() == 2 && !voters.iter().any(|(member, _)| *member == peers[2].id)
            },
        )
        .await;
    }

    for peer in peers {
        peer.node.shutdown().await;
    }
}

#[tokio::test]
async fn a_follower_proposing_a_core_expulsion_needs_a_majority_of_the_core() {
    // §4.4 end to end, and the sub-phase's own acceptance: any member may
    // *submit* a change, and a quorum of core nodes decides it. Before this,
    // one signed proposal from anybody at all removed a voter — so a handful of
    // them shrank the group's ability to commit anything, and a mistyped id in
    // `distlib expel` did it by accident.
    let peers = a_founded_trio().await;

    // A member who does not vote. Admitted by a core node, so that step is one
    // step and this test is about the expulsion rather than the admission.
    let follower_key = SecretKey::generate();
    let follower_id = MemberId::from(follower_key.public());
    peers[0]
        .node
        .propose(
            MembershipEvent::MemberAdded {
                member: MemberRecord {
                    member_id: follower_id,
                    display_name: "the proposer".to_owned(),
                    pledge_bytes: 0,
                },
            },
            &peers[0].secret,
        )
        .await
        .unwrap();
    let follower = Peer::start_with(
        follower_key,
        peers.iter().map(|peer| peer.id).collect(),
        peers
            .iter()
            .map(|peer| (peer.id, peer.addr.clone()))
            .collect(),
    )
    .await;
    wait_for(&follower, "the log to reach the follower", |m| {
        m.is_member(&follower_id)
    })
    .await;

    // The follower proposes removing a voter. It commits — submitting is open
    // to every member — and decides nothing.
    let proposal = follower
        .node
        .propose(
            MembershipEvent::MemberExpelled {
                member: peers[2].id,
                reason: "proposed by somebody who does not vote".to_owned(),
            },
            &follower.secret,
        )
        .await
        .unwrap();
    assert_eq!(
        raft_voters(&peers[0]).len(),
        3,
        "a follower's proposal must not remove a voter by itself"
    );

    // One core member agreeing is still not enough: three voters, so it takes
    // two, and the third is the one being removed and gets no say.
    pending_on(&peers[0], "the proposal to reach the first voter").await;
    peers[0]
        .node
        .propose(MembershipEvent::Approved { proposal }, &peers[0].secret)
        .await
        .unwrap();
    assert!(
        peers[0].node.membership().is_member(&peers[2].id),
        "one of three voters is not a majority"
    );
    assert_eq!(
        peers[0].node.membership().pending().count(),
        1,
        "and the proposal is still waiting"
    );

    // The second decides it, and 2.1-2's reconciliation loop takes the voter
    // out of openraft rather than leaving a vote the group can never collect.
    pending_on(&peers[1], "the proposal to reach the second voter").await;
    peers[1]
        .node
        .propose(MembershipEvent::Approved { proposal }, &peers[1].secret)
        .await
        .unwrap();

    for peer in &peers[..2] {
        until("every surviving voter to drop the expelled one", || {
            let voters = raft_voters(peer);
            voters.len() == 2 && !voters.iter().any(|(member, _)| *member == peers[2].id)
        })
        .await;
        // Waited for rather than asserted, and the two waits are on different
        // mechanisms on purpose. openraft puts a membership change into effect
        // when the entry is *appended*, not when it is applied — which is what
        // `server_metrics` reports and what the reconciliation loop relies on —
        // so a node can already be down to two voters while its state machine
        // has yet to apply the approval that caused it. Asserting the
        // projection the instant the voter set moves is reading a second
        // mechanism through the first, which is how this failed about one fast
        // lane run in nine.
        wait_for(peer, "a decided proposal to stop pending", |m| {
            m.pending().count() == 0
        })
        .await;
    }

    follower.node.shutdown().await;
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
        !founder.node.membership().is_core(&bystander.id),
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

    // Committed — as a *proposal*. §4.4 opens submitting to every member and
    // gives the decision to the core group, so a non-voter's admission waits
    // for a core member to agree to it. That is the half this test now proves
    // in both directions: the entry reached the log, and it did not take effect
    // on its own.
    let proposal = pending_on(&founder, "the non-voter's proposal to be committed").await;
    assert!(
        !founder.node.membership().is_member(&newcomer),
        "a non-voter's proposal must not admit anybody by itself"
    );

    founder
        .node
        .propose(MembershipEvent::Approved { proposal }, &founder.secret)
        .await
        .unwrap();
    wait_for(
        &founder,
        "the approved proposal to take effect",
        |membership| membership.is_member(&newcomer),
    )
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
    // **Refused after the handshake rather than during it, which changed in
    // 2.3-2.** A follower used not to advertise `distlib/raft/0` at all, so the
    // connection failed with "no known protocol" — earlier, and stronger. That
    // is no longer available: a router's protocols are fixed when it spawns, so
    // a node that may be promoted has to be listening before it has a Raft to
    // listen with. What replaces it is this: the connection opens and is closed
    // with a reason, and the RPC underneath is never answered.
    //
    // The claim in the comment above is unchanged and is what this checks. The
    // bystander's stand-in voter set is its own `[consensus] core`, which is
    // empty — it was configured to found nothing — so there is nobody it will
    // accept consensus from, the founder included.
    let opened = founder
        .node
        .endpoint()
        .connect(
            bystander_addr.to_endpoint_addr(bystander.id).unwrap(),
            distlib_net::alpn::RAFT,
        )
        .await
        .expect("the alpn is advertised on every node now");
    let closed = tokio::time::timeout(Duration::from_secs(10), opened.closed())
        .await
        .expect("a node that founds nothing must close a raft connection");
    assert!(
        format!("{closed}").contains("not a voter"),
        "refused for the wrong reason: {closed}"
    );
    assert!(
        !bystander.node.is_core(),
        "and it is still not a voter of anything"
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

// Six seconds, and on its own the reason this binary took six: it waits out a
// connect timeout against a source that is deliberately dead.
#[cfg(feature = "slow-tests")]
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

    // Seeded into every allowlist and into no founding set, so it is admitted
    // before the group exists and never after. That makes it a barrier: the
    // moment a node stops allowing it, that node's log-derived membership has
    // been published — and with it, in the same pass and just before, the
    // addresses of the core group. Without something to wait on, this test
    // dials while the task that fills the address book has not run, which is
    // how it came to fail one run in three under load.
    let stale = MemberId::from(SecretKey::generate().public());
    let core: Vec<(MemberId, NodeAddr)> = ids.iter().map(|id| (*id, NodeAddr::default())).collect();

    let mut peers = Vec::new();
    for (index, key) in keys.iter().enumerate() {
        let bootstrap = ids
            .iter()
            .enumerate()
            .filter(|(other, _)| *other != index)
            .map(|(_, id)| *id)
            .chain([stale])
            .collect();
        peers.push(Peer::start_with(key.clone(), bootstrap, core.clone()).await);
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

    // Wait for the address book to have been filled, then dial *once*. Retrying
    // the dial instead would pass without it: given a few seconds, gossip
    // distributes peer addresses of its own accord, so a retry loop here
    // quietly stopped testing the thing it names — removing the address book's
    // only writer left it passing.
    until("the log-derived membership to be published", || {
        !dialling.hooks.allowlist().is_allowed(&stale)
    })
    .await;

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
    // A follower holds no `StoredMembership`. It learns the founding addresses
    // from the log itself — see the test below — but a node that joined the
    // core group later is not in `GroupFounded`, and the addresses it is told
    // about when it fetches are what fill that in. Here
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

    // Not retried, unlike the test above, and the difference is the point: a
    // follower's addresses arrive inside `MemberlogClient::fetch`, *before* the
    // entries it fetched are applied — so by the time the membership shows the
    // joiner, the address book has already been told. Nothing to wait for.
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
async fn a_follower_reports_where_the_core_group_is() {
    // `core_addresses` is what a join ticket is made of and what a follower
    // rotates around, and it used to read openraft's `StoredMembership` — which
    // a follower never has, running no Raft at all. So a follower answered "no
    // core nodes" about the group it was following, and could hand out a ticket
    // naming nobody. It reads the projection now, which is built from the log,
    // and the log carries the founding addresses.
    let founder_key = SecretKey::generate();
    let founder_id = MemberId::from(founder_key.public());
    let joiner_key = SecretKey::generate();
    let joiner = MemberId::from(joiner_key.public());

    let founder = Peer::start(founder_key, vec![]).await;
    founder
        .node
        .init_group(
            vec![(founder.record("the founder"), founder.addr.clone())],
            &founder.secret,
        )
        .await
        .unwrap();
    wait_for(&founder, "the group to be founded", |m| {
        m.group_id().is_some()
    })
    .await;

    founder
        .node
        .propose(
            MembershipEvent::MemberAdded {
                member: MemberRecord {
                    member_id: joiner,
                    display_name: "a follower".to_owned(),
                    pledge_bytes: 0,
                },
            },
            &founder.secret,
        )
        .await
        .unwrap();

    let follower = Peer::start_with(
        joiner_key,
        vec![founder_id],
        vec![(founder_id, founder.addr.clone())],
    )
    .await;
    wait_for(&follower, "the log to reach the follower", |m| {
        m.is_member(&joiner)
    })
    .await;

    assert_eq!(
        follower.node.core_addresses(),
        vec![(founder_id, founder.addr.clone())],
        "a follower must be able to say where the group's core nodes are"
    );

    follower.node.shutdown().await;
    founder.node.shutdown().await;
}

// Two seconds, most of it letting the gossip swarm form before taking it away.
#[cfg(feature = "slow-tests")]
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

#[cfg(feature = "slow-tests")]
#[tokio::test]
async fn a_follower_promoted_by_the_log_starts_voting_without_a_restart() {
    // 2.3-2, and the claim P1-30 deferred. Two halves have to meet without
    // either going first: openraft will not make a node a voter until it is a
    // learner it can replicate to, and it cannot replicate to a node with no
    // Raft. The log is the rendezvous — the leader and the promoted node read
    // the same committed `CoreGroupChanged`.
    //
    // **The assertion that matters is the last one**, not that the node votes.
    // Taking a seat means blanking the state machine so the log folds exactly
    // once, and the risk in doing that is ending up with a membership subtly
    // unlike everybody else's. So this compares the whole projection against a
    // founder's.
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

    // A member who follows: not in anybody's founding core group, so it starts
    // with no Raft at all.
    //
    // Admitted before it is started, as `acceptance.rs` does and for the same
    // reason: a node the log does not name yet is refused by every core node,
    // and a follower that has never held the log backs off a full minute
    // before asking again.
    let key = SecretKey::generate();
    let joining = MemberId::from(key.public());
    founder
        .node
        .propose(
            MembershipEvent::MemberAdded {
                member: MemberRecord {
                    member_id: joining,
                    display_name: "joiner".to_owned(),
                    pledge_bytes: 0,
                },
            },
            &founder.secret,
        )
        .await
        .unwrap();

    let joiner = Peer::start_with(
        key,
        vec![founder.id],
        vec![(founder.id, founder.addr.clone())],
    )
    .await;
    // Patient, because a follower learns by fetching: gossip makes it prompt
    // but promises nothing, and the guarantee underneath is its own 30-second
    // timer. Every wait in this test that depends on the joiner noticing
    // something is bounded the same way, for the same reason.
    wait_for_upto(
        &joiner,
        PATIENTLY,
        "the joiner to catch up as a follower",
        |m| m.is_member(&joiner.id),
    )
    .await;
    assert!(
        !joiner.node.is_core() && joiner.node.raft().is_none(),
        "a follower holds the log without voting on it"
    );

    // Something for it to have missed while it is being promoted, so the
    // comparison at the end is against a log with more than founding in it.
    let absentee = MemberId::from(SecretKey::generate().public());
    founder
        .node
        .propose(
            MembershipEvent::MemberAdded {
                member: MemberRecord {
                    member_id: absentee,
                    display_name: "absentee".to_owned(),
                    pledge_bytes: 0,
                },
            },
            &founder.secret,
        )
        .await
        .unwrap();

    // The one core member is a majority of one, so this applies at once.
    founder
        .node
        .propose(
            MembershipEvent::CoreGroupChanged {
                core: vec![
                    (founder.id, founder.addr.clone()),
                    (joiner.id, joiner.addr.clone()),
                ],
            },
            &founder.secret,
        )
        .await
        .unwrap();

    // Both halves, in the order they can only happen in: the joiner reads the
    // entry, stops following, blanks itself and sits down; the leader adds it
    // as a learner, catches it up, and then makes it a voter.
    until_upto(PATIENTLY, "the joiner to take a seat in consensus", || {
        joiner.node.raft().is_some()
    })
    .await;
    until("openraft to make the joiner a voter", || {
        founder.node.raft().is_some_and(|raft| {
            raft.metrics()
                .borrow()
                .membership_config
                .voter_ids()
                .count()
                == 2
        })
    })
    .await;

    // And it is caught up, identically. `changed_at` included: it is what
    // every later proposal is checked against, so a promoted node that agreed
    // about the membership but not about *when* it last moved would refuse
    // proposals the rest of the group accepts.
    wait_for_upto(
        &joiner,
        PATIENTLY,
        "the promoted node to be caught up",
        |membership| *membership == founder.node.membership(),
    )
    .await;

    let promoted = joiner.node.membership();
    assert!(promoted.is_core(&joiner.id), "and it says so itself");
    assert!(
        promoted.is_member(&absentee),
        "including the entry it was never served as a follower"
    );
    assert_eq!(
        promoted.pending().count(),
        0,
        "a log folded twice would leave proposals behind that were already decided"
    );

    founder.node.shutdown().await;
    joiner.node.shutdown().await;
}
