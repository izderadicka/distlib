//! Review evidence: these tests assert defects observed on master 8a7827f.
//! They are not regression tests asserting the desired, corrected behaviour.
#![allow(clippy::unwrap_used)]
#![cfg(feature = "slow-tests")]

use distlib_consensus::{
    MemberProfile, MembershipEvent, PROTOCOL_MAX, SignedEvent, StateMachineStore, Timestamp,
    TypeConfig, UpgradeRequired,
};
use distlib_core::{MemberId, NodeAddr, RawMemberId};
use iroh::SecretKey;
use openraft::{
    CommittedLeaderId, Entry, EntryPayload, LogId, Membership, storage::RaftStateMachine,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

mod common;
use common::{Peer, wait_for};

fn id(key: &SecretKey) -> MemberId {
    MemberId::from(key.public())
}
fn profile(key: &SecretKey) -> MemberProfile {
    MemberProfile {
        member_id: id(key),
        display_name: "review".into(),
    }
}

async fn apply(store: &mut StateMachineStore, key: &SecretKey, index: u64, event: MembershipEvent) {
    let signed = SignedEvent::sign(
        key,
        event,
        Timestamp::from_millis(index),
        store.membership().changed_at(),
    )
    .unwrap();
    let leader = store.applied_state().await.unwrap().0.unwrap().leader_id;
    let verdicts = store
        .apply([Entry::<TypeConfig> {
            log_id: LogId::new(leader, index),
            payload: EntryPayload::Normal(signed),
        }])
        .await
        .unwrap();
    for result in verdicts {
        result.unwrap();
    }
}

#[tokio::test]
async fn proof_raise_can_overtake_removal_of_an_unsupported_raft_voter() {
    let a = SecretKey::generate();
    let b = SecretKey::generate();
    let dir = tempfile::tempdir().unwrap();
    let mut store = StateMachineStore::open(dir.path().join("review.redb")).unwrap();
    // A valid interleaving: the signed desired-core change commits before the
    // asynchronous reconciler has written Raft's removal configurations.
    let voters = BTreeSet::from([RawMemberId::from(id(&a)), RawMemberId::from(id(&b))]);
    let nodes: BTreeMap<_, _> = voters.iter().map(|id| (*id, NodeAddr::default())).collect();
    store
        .apply([Entry::<TypeConfig> {
            log_id: LogId::new(CommittedLeaderId::new(1, RawMemberId::from(id(&a))), 0),
            payload: EntryPayload::Membership(Membership::new(vec![voters], nodes)),
        }])
        .await
        .unwrap();
    apply(
        &mut store,
        &a,
        1,
        MembershipEvent::found(
            vec![
                (profile(&a), NodeAddr::default()),
                (profile(&b), NodeAddr::default()),
            ],
            Default::default(),
            Timestamp::from_millis(1),
        )
        .unwrap(),
    )
    .await;
    // Simulate upgraded B as the existing protocol-upgrade tests do. A has
    // never advertised support above this build's protocol 1.
    apply(
        &mut store,
        &b,
        2,
        MembershipEvent::ProtocolSupported {
            member: id(&b),
            max: PROTOCOL_MAX + 1,
        },
    )
    .await;
    apply(
        &mut store,
        &a,
        3,
        MembershipEvent::CoreGroupChanged {
            core: vec![(id(&b), NodeAddr::default())],
        },
    )
    .await;
    apply(&mut store, &b, 4, MembershipEvent::Approved { proposal: 3 }).await;
    assert!(!store.membership().is_core(&id(&a)));
    apply(
        &mut store,
        &b,
        5,
        MembershipEvent::ProtocolRaised {
            to: PROTOCOL_MAX + 1,
        },
    )
    .await;
    let (_, actual) = store.applied_state().await.unwrap();
    assert!(actual.voter_ids().any(|v| v == RawMemberId::from(id(&a))));
    assert!(!store.membership().speaks(&id(&a), PROTOCOL_MAX + 1));
    assert_eq!(store.membership().protocol(), PROTOCOL_MAX + 1);
    // The Raft apply path itself also keeps interpreting later entries.
    apply(
        &mut store,
        &b,
        6,
        MembershipEvent::MemberAdded {
            member: profile(&SecretKey::generate()),
        },
    )
    .await;
    assert_eq!(store.membership().changed_at(), 6);
}

#[tokio::test]
async fn proof_restart_temporarily_clears_a_durable_upgrade_requirement() {
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
    let key = SecretKey::generate();
    founder
        .node
        .propose(
            MembershipEvent::MemberAdded {
                member: profile(&key),
            },
            &founder.secret,
        )
        .await
        .unwrap();
    let follower = Peer::start_with(
        key,
        vec![founder.id],
        vec![(founder.id, founder.addr.clone())],
    )
    .await;
    wait_for(&follower, "admitted", |m| m.is_member(&follower.id)).await;
    founder
        .node
        .propose(
            MembershipEvent::ProtocolSupported {
                member: founder.id,
                max: PROTOCOL_MAX + 1,
            },
            &founder.secret,
        )
        .await
        .unwrap();
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
    let mut watch = follower.node.subscribe_upgrade_required();
    tokio::time::timeout(Duration::from_secs(10), watch.wait_for(Option::is_some))
        .await
        .unwrap()
        .unwrap();
    founder.shutdown().await;
    let follower = follower
        .restart_with_core(vec![(founder.id, founder.addr.clone())])
        .await;
    assert_eq!(follower.node.membership().protocol(), PROTOCOL_MAX + 1);
    // This is the signal wired into PinManager::release_if_safe. The durable
    // protocol is already too new, yet the guard is absent until networking ends.
    assert_eq!(follower.node.upgrade_required(), None);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(follower.node.upgrade_required(), None);
    let mut watch = follower.node.subscribe_upgrade_required();
    tokio::time::timeout(Duration::from_secs(10), watch.wait_for(Option::is_some))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        follower.node.upgrade_required(),
        Some(UpgradeRequired::Protocol { .. })
    ));
    follower.shutdown().await;
}
