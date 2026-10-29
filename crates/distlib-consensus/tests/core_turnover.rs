//! Voter retirement, core turnover, re-promotion and expulsion cleanup.

#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)]
#![allow(clippy::too_many_lines)]

use std::collections::BTreeSet;

use distlib_consensus::{MemberProfile, MembershipEvent};
use distlib_core::{MemberId, RawMemberId};
use iroh::SecretKey;

mod common;
use common::{PATIENTLY, Peer, pending_on, until_upto, wait_for, wait_for_upto};

async fn a_founded_trio() -> Vec<Peer> {
    let keys: Vec<SecretKey> = (0..3).map(|_| SecretKey::generate()).collect();
    let ids: Vec<MemberId> = keys.iter().map(|k| MemberId::from(k.public())).collect();
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
        .map(|(i, p)| (p.profile(&format!("founder-{i}")), p.addr.clone()))
        .collect();
    peers[0]
        .node
        .init_group(founders, Default::default(), &peers[0].secret)
        .await
        .unwrap();
    for peer in &peers {
        wait_for(peer, "founded", |m| m.group_id().is_some()).await;
    }
    peers
}

/// The uniform voter set `peer`'s Raft holds, or None while joint / no Raft.
fn uniform_voters(peer: &Peer) -> Option<BTreeSet<RawMemberId>> {
    let raft = peer.node.raft()?;
    let metrics = raft.metrics();
    let seen = metrics.borrow();
    let configs = seen.membership_config.membership().get_joint_config();
    (configs.len() == 1).then(|| configs[0].clone())
}

fn nodes_known(peer: &Peer) -> usize {
    peer.node.raft().map_or(0, |raft| {
        raft.metrics().borrow().membership_config.nodes().count()
    })
}

/// C demoted; the core moves wholly to D; C promoted again under D.
#[tokio::test]
async fn re_promotion_after_the_core_turned_over() {
    core_turnover(true).await;
}

#[tokio::test]
async fn a_retired_follower_discovers_a_disjoint_core_without_restart() {
    core_turnover(false).await;
}

async fn core_turnover(restart: bool) {
    let mut peers = a_founded_trio().await;
    let c_peer = peers.pop().unwrap();
    let (a, b) = (&peers[0], &peers[1]);
    let c = &c_peer;

    // 1. Demote c.
    a.node
        .propose(
            MembershipEvent::CoreGroupChanged {
                core: vec![(a.id, a.addr.clone()), (b.id, b.addr.clone())],
            },
            &a.secret,
        )
        .await
        .unwrap();
    let proposal = pending_on(b, "c's demotion pending").await;
    b.node
        .propose(MembershipEvent::Approved { proposal }, &b.secret)
        .await
        .unwrap();
    until_upto(PATIENTLY, "c retires", || c.node.raft().is_none()).await;

    if restart {
        c.shutdown().await;
    }

    let d_key = SecretKey::generate();
    let d_id = MemberId::from(d_key.public());
    a.node
        .propose(
            MembershipEvent::MemberAdded {
                member: MemberProfile {
                    member_id: d_id,
                    display_name: "d".into(),
                },
            },
            &a.secret,
        )
        .await
        .unwrap();
    let d = Peer::start_with(
        d_key,
        peers.iter().map(|p| p.id).collect(),
        peers.iter().map(|p| (p.id, p.addr.clone())).collect(),
    )
    .await;
    wait_for_upto(&d, PATIENTLY, "d follows", |m| m.is_member(&d_id)).await;

    // 2. The core moves wholly to d.
    a.node
        .propose(
            MembershipEvent::CoreGroupChanged {
                core: vec![(d.id, d.addr.clone())],
            },
            &a.secret,
        )
        .await
        .unwrap();
    let proposal = pending_on(b, "the move to d pending").await;
    b.node
        .propose(MembershipEvent::Approved { proposal }, &b.secret)
        .await
        .unwrap();
    until_upto(PATIENTLY * 2, "d votes alone, a and b retired", || {
        uniform_voters(&d) == Some(BTreeSet::from([RawMemberId::from(d.id)]))
            && a.node.raft().is_none()
            && b.node.raft().is_none()
    })
    .await;

    // 3. d promotes c again; one approval of one is enough.
    d.node
        .propose(
            MembershipEvent::CoreGroupChanged {
                core: vec![(c.id, c.addr.clone()), (d.id, d.addr.clone())],
            },
            &d.secret,
        )
        .await
        .unwrap();
    // C's remembered sources A/B have retired. Even after restart the persisted
    // sources win over configured D, so C must follow their history/referrals.
    let c_peer = if restart {
        c_peer.restart_with_core(vec![(d.id, d.addr.clone())]).await
    } else {
        c_peer
    };
    let c = &c_peer;
    until_upto(PATIENTLY * 2, "c takes a seat again", || {
        c.node.raft().is_some()
    })
    .await;
    until_upto(PATIENTLY * 2, "raft on d counts c a voter", || {
        uniform_voters(&d).is_some_and(|v| v.contains(&RawMemberId::from(c.id)))
    })
    .await;

    for peer in peers.iter().chain([&d, c]) {
        peer.shutdown().await;
    }
}

/// An expelled core member that exits as soon as it knows, as the CLI does.
#[tokio::test]
async fn an_expelled_voter_that_exits_is_forgotten_by_raft() {
    let peers = a_founded_trio().await;
    let (a, b, c) = (&peers[0], &peers[1], &peers[2]);
    a.node
        .propose(
            MembershipEvent::MemberExpelled {
                member: c.id,
                reason: "review".into(),
            },
            &a.secret,
        )
        .await
        .unwrap();
    let proposal = pending_on(b, "c's expulsion pending").await;
    b.node
        .propose(MembershipEvent::Approved { proposal }, &b.secret)
        .await
        .unwrap();

    // The CLI's `wait_for_stop_or_expulsion`, then shutdown.
    tokio::time::timeout(PATIENTLY, c.node.expelled())
        .await
        .expect("c learns it was expelled");
    c.shutdown().await;

    let result = tokio::time::timeout(PATIENTLY, async {
        loop {
            if [a, b]
                .iter()
                .all(|p| nodes_known(p) == 2 || p.node.raft().is_none())
                && [a, b].iter().any(|p| nodes_known(p) == 2)
            {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    })
    .await;
    assert!(result.is_ok(), "the expelled voter lingers as a learner");

    // And the group still commits.
    let stranger = MemberId::from(SecretKey::generate().public());
    a.node
        .propose(
            MembershipEvent::MemberAdded {
                member: MemberProfile {
                    member_id: stranger,
                    display_name: "after".into(),
                },
            },
            &a.secret,
        )
        .await
        .unwrap();
    a.shutdown().await;
    b.shutdown().await;
}

/// Demote C, add D without replacing A/B, elect D, then re-promote C.
/// D was never in C's old Raft voter set.
#[tokio::test]
async fn re_promotion_accepts_a_newly_elected_leader() {
    let peers = a_founded_trio().await;
    let (a, b, c) = (&peers[0], &peers[1], &peers[2]);
    change_core(a, &[b], &[a, b]).await;
    until_upto(PATIENTLY, "c retires", || c.node.raft().is_none()).await;

    let key = SecretKey::generate();
    let id = MemberId::from(key.public());
    a.node
        .propose(
            MembershipEvent::MemberAdded {
                member: MemberProfile {
                    member_id: id,
                    display_name: "new leader".into(),
                },
            },
            &a.secret,
        )
        .await
        .unwrap();
    let d = Peer::start_with(
        key,
        vec![a.id, b.id],
        vec![(a.id, a.addr.clone()), (b.id, b.addr.clone())],
    )
    .await;
    wait_for_upto(&d, PATIENTLY, "d follows", |m| m.is_member(&id)).await;
    change_core(a, &[b], &[a, b, &d]).await;
    until_upto(PATIENTLY * 2, "d becomes a voter", || {
        uniform_voters(&d).is_some_and(|v| v.contains(&RawMemberId::from(d.id)))
    })
    .await;
    until_leading(&d, &[a, b]).await;
    change_core(&d, &[a], &[a, b, c, &d]).await;
    until_upto(PATIENTLY * 2, "c votes under its new leader", || {
        uniform_voters(&d).is_some_and(|v| v.len() == 4)
            && uniform_voters(c).is_some_and(|v| v.len() == 4)
    })
    .await;
    d.node
        .propose(
            MembershipEvent::PledgeChanged {
                member: d.id,
                pledge_bytes: 100_000_000_000,
            },
            &d.secret,
        )
        .await
        .unwrap();
    wait_for_upto(c, PATIENTLY, "c applies later writes", |m| {
        m.member(&d.id)
            .is_some_and(|member| member.pledge_bytes == 100_000_000_000)
    })
    .await;
    for peer in peers.iter().chain([&d]) {
        peer.shutdown().await;
    }
}

/// Makes `peer` the leader, the others having to stop calling elections of
/// their own while it asks.
///
/// openraft 0.9 has no leadership transfer, and an election a voter calls is
/// not one it is sure to win:
/// - the others still hear from their leader and refuse it (the leader lease),
///   though its higher term unseats that leader, and the election after goes
///   to whichever voter's timer fires first;
/// - while its term is the higher one it refuses the leader's appends, so it
///   misses what a new leader writes, and is then refused for its shorter log
///   as well.
///
/// Retrying on a timer fell into step with another voter's election timer and
/// lost every time; retrying after each election settled still lost about one
/// run in five (both seen under load). So: once `peer` holds all the leader
/// has written, the others' elections are switched off, `peer` asks, and its
/// own timer asks again once their leases run out. An attempt that has not won
/// within ten seconds — a write slipped in and `peer` is behind — switches them
/// back on and starts again.
async fn until_leading(peer: &Peer, others: &[&Peer]) {
    let me = RawMemberId::from(peer.id);
    let leads = || peer.node.raft().unwrap().metrics().borrow().current_leader == Some(me);
    let elections = |enabled: bool| {
        for other in others {
            other.node.raft().unwrap().runtime_config().elect(enabled);
        }
    };
    tokio::time::timeout(PATIENTLY * 2, async {
        while !leads() {
            until_upto(PATIENTLY, "a leader peer has caught up with", || {
                caught_up(peer, others)
            })
            .await;
            elections(false);
            peer.node.raft().unwrap().trigger().elect().await.unwrap();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(10), async {
                while !leads() {
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
            })
            .await;
            elections(true);
        }
    })
    .await
    .expect("peer leads");
}

/// Whether `peer` follows a leader, among `others` or itself, and holds every
/// entry that leader has written.
fn caught_up(peer: &Peer, others: &[&Peer]) -> bool {
    let mine = peer.node.raft().unwrap().metrics().borrow().clone();
    let Some(leader) = mine.current_leader else {
        return false;
    };
    others
        .iter()
        .chain([&peer])
        .find(|voter| RawMemberId::from(voter.id) == leader)
        .and_then(|voter| voter.node.raft())
        .is_some_and(|raft| {
            let theirs = raft.metrics().borrow().clone();
            theirs.current_leader == Some(leader)
                && theirs.current_term == mine.current_term
                && theirs.last_log_index == mine.last_log_index
        })
}

async fn change_core(proposer: &Peer, approvers: &[&Peer], core: &[&Peer]) {
    proposer
        .node
        .propose(
            MembershipEvent::CoreGroupChanged {
                core: core.iter().map(|p| (p.id, p.addr.clone())).collect(),
            },
            &proposer.secret,
        )
        .await
        .unwrap();
    for peer in approvers {
        let proposal = pending_on(peer, "core change pending").await;
        peer.node
            .propose(MembershipEvent::Approved { proposal }, &peer.secret)
            .await
            .unwrap();
    }
}
