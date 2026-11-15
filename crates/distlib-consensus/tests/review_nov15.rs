//! Evidence for review of master ed88649. Passing reproduces the defect.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use distlib_consensus::MembershipEvent;
use distlib_core::{MemberId, RawMemberId};
use iroh::SecretKey;

mod common;
use common::{PATIENTLY, Peer, until_upto, wait_for};

async fn says_retired(caller: &Peer, target: &Peer) -> bool {
    let connection = caller
        .node
        .endpoint()
        .connect(
            target.addr.to_endpoint_addr(target.id).unwrap(),
            distlib_net::alpn::RAFT,
        )
        .await
        .unwrap();
    let (mut send, mut receive) = connection.open_bi().await.unwrap();
    // The golden wire encodings of Request::Retired and Response::Retired.
    send.write_all(&[3]).await.unwrap();
    send.finish().unwrap();
    let answer = receive.read_to_end(16).await.unwrap();
    assert!(answer == [3, 0] || answer == [3, 1]);
    answer[1] == 1
}

#[tokio::test]
async fn proof_shutdown_acknowledges_retirement_without_durable_voter_removal() {
    let a_key = SecretKey::generate();
    let b_key = SecretKey::generate();
    let a = Peer::start(a_key.clone(), vec![MemberId::from(b_key.public())]).await;
    let b = Peer::start(b_key, vec![a.id]).await;
    a.node
        .init_group(
            vec![
                (a.profile("a"), a.addr.clone()),
                (b.profile("b"), b.addr.clone()),
            ],
            Default::default(),
            &a.secret,
        )
        .await
        .unwrap();
    wait_for(&b, "founded", |m| m.group_id().is_some()).await;
    let raft = a.node.raft().unwrap();
    let before = raft.metrics().borrow().membership_config.clone();
    assert!(before.voter_ids().any(|id| id == RawMemberId::from(a.id)));
    assert!(raft.metrics().borrow().last_applied >= *before.log_id());
    assert!(!says_retired(&b, &a).await);

    // Runtime calls this while its router and endpoint are still serving.
    a.node.stop_consensus().await;
    assert!(
        says_retired(&b, &a).await,
        "shutdown now looks like retirement"
    );
    assert_eq!(raft.metrics().borrow().membership_config, before);
    assert!(a.node.membership().is_core(&a.id));
    b.shutdown().await;
    a.shutdown().await;
}

#[tokio::test]
async fn proof_shutdown_during_removal_is_forgotten_then_restarts_as_a_stale_voter() {
    let keys: Vec<_> = (0..3).map(|_| SecretKey::generate()).collect();
    let ids: Vec<_> = keys
        .iter()
        .map(|key| MemberId::from(key.public()))
        .collect();
    let mut peers = Vec::new();
    for key in keys {
        let me = MemberId::from(key.public());
        peers.push(Peer::start(key, ids.iter().copied().filter(|id| *id != me).collect()).await);
    }
    let c = peers.pop().unwrap();
    let (a, b) = (&peers[0], &peers[1]);
    a.node
        .init_group(
            vec![
                (a.profile("a"), a.addr.clone()),
                (b.profile("b"), b.addr.clone()),
                (c.profile("c"), c.addr.clone()),
            ],
            Default::default(),
            &a.secret,
        )
        .await
        .unwrap();
    wait_for(&c, "all three founded", |m| m.core().len() == 3).await;
    assert!(!says_retired(b, &c).await);

    // Hold the real shutdown window open: consensus has stopped, but the
    // endpoint has not closed. C can no longer apply its own removal.
    c.node.stop_consensus().await;
    let proposal = a
        .node
        .propose(
            MembershipEvent::CoreGroupChanged {
                core: vec![(a.id, a.addr.clone()), (b.id, b.addr.clone())],
            },
            &a.secret,
        )
        .await
        .unwrap();
    b.node
        .propose(MembershipEvent::Approved { proposal }, &b.secret)
        .await
        .unwrap();
    until_upto(
        PATIENTLY,
        "C incorrectly forgotten without learning removal",
        || {
            [a, b].iter().all(|peer| {
                !peer
                    .node
                    .raft()
                    .unwrap()
                    .metrics()
                    .borrow()
                    .membership_config
                    .nodes()
                    .any(|(id, _)| *id == RawMemberId::from(c.id))
            })
        },
    )
    .await;
    assert!(c.node.membership().is_core(&c.id));

    let c = c.restart_in_place(vec![a.id, b.id]).await;
    assert!(c.node.raft().is_some(), "persisted state still seats C");
    // More than two read-ahead intervals: the new catch-up task only opens
    // authorization; A and B no longer replicate to the forgotten C.
    tokio::time::sleep(Duration::from_secs(13)).await;
    assert!(
        c.node.membership().is_core(&c.id),
        "C never followed the removal"
    );
    assert!(c.node.raft().is_some());
    c.shutdown().await;
    for peer in peers {
        peer.shutdown().await;
    }
}
