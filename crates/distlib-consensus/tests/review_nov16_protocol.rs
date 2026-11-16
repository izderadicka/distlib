//! Review evidence: passing reproduces a protocol capability left by a newer build.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use distlib_consensus::{MembershipEvent, PROTOCOL_MAX, announce_protocol_support};
use iroh::SecretKey;

mod common;
use common::{Peer, wait_for};

#[tokio::test]
async fn proof_a_rolled_back_voter_keeps_its_newer_builds_capability_and_accepts_a_raise() {
    let founder = Peer::start(SecretKey::generate(), vec![]).await;
    founder
        .node
        .init_group(
            vec![(founder.profile("founder"), founder.addr.clone())],
            Default::default(),
            &founder.secret,
        )
        .await
        .unwrap();
    wait_for(&founder, "founded", |m| m.group_id().is_some()).await;

    // Simulate the prior newer build using the same support advertisement
    // mechanism that production runs. The group itself has not upgraded yet.
    tokio::select! {
        () = announce_protocol_support(&founder.node, &founder.secret, PROTOCOL_MAX + 1) => unreachable!(),
        () = wait_for(&founder, "newer build advertised", |m| m.speaks(&founder.id, PROTOCOL_MAX + 1)) => {}
    }
    assert_eq!(founder.node.membership().protocol(), PROTOCOL_MAX);

    // Restart the older build from that durable database. No forged RPCs,
    // changed state-machine implementation or changed protocol constant.
    let founder = founder.restart(vec![]).await;
    assert!(founder.node.raft().is_some());
    assert_eq!(founder.node.upgrade_required(), None);
    tokio::select! {
        () = announce_protocol_support(&founder.node, &founder.secret, PROTOCOL_MAX) => unreachable!(),
        () = tokio::time::sleep(Duration::from_millis(200)) => {}
    }
    assert!(
        founder
            .node
            .membership()
            .speaks(&founder.id, PROTOCOL_MAX + 1)
    );

    founder
        .node
        .propose(
            MembershipEvent::ProtocolRaised {
                to: PROTOCOL_MAX + 1,
            },
            &founder.secret,
        )
        .await
        .unwrap();
    assert_eq!(founder.node.membership().protocol(), PROTOCOL_MAX + 1);
    assert!(founder.node.raft().is_some());
    assert_eq!(founder.node.upgrade_required(), None);
    founder.shutdown().await;
}
