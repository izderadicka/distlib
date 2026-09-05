//! The Phase 1 acceptance criteria, run as one test.
//!
//! §9 states them in a sentence:
//!
//! > 3-core-node cluster + 2 follower nodes; add a member → it can connect;
//! > expel it → open connection drops, reconnect refused; kill one core node →
//! > group still admits members.
//!
//! Deliberately one test rather than four. Each clause is a claim about a
//! *group* that has been running for a while — the fourth one only means
//! anything if the first three already happened to the same cluster — and
//! splitting them would be four setups testing four fresh groups, which is a
//! weaker thing than the sentence promises.
//!
//! In process, for the speed and determinism that lets it run on every commit.
//! The binary-level counterpart is `three_friends_found_a_group` in the
//! `distlib` crate, which covers the part a library test cannot: the procedure
//! a human follows.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point
#![allow(clippy::result_large_err)] // openraft's error types, in its own signatures

use std::time::Duration;

use distlib_consensus::{MemberRecord, MembershipEvent};
use distlib_core::{MemberId, NodeAddr};
use iroh::SecretKey;

mod common;
use common::{Peer, wait_for};

/// How long to wait for something that should happen without prompting.
const SOON: Duration = Duration::from_secs(15);

#[tokio::test]
async fn a_group_of_three_voters_and_two_followers_meets_phase_one() {
    // --- a 3-core-node cluster ------------------------------------------
    let keys: Vec<SecretKey> = (0..3).map(|_| SecretKey::generate()).collect();
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
        .map(|(index, peer)| (peer.record(&format!("core-{index}")), peer.addr.clone()))
        .collect();
    core[0]
        .node
        .init_group(founders, &core[0].secret)
        .await
        .unwrap();
    for peer in &core {
        wait_for(peer, "the group to be founded", |m| m.group_id().is_some()).await;
    }

    // Where a follower is told to look, before it has a log to find it in.
    let sources: Vec<(MemberId, NodeAddr)> = core
        .iter()
        .map(|peer| (peer.id, peer.addr.clone()))
        .collect();

    // --- add a member → it can connect ----------------------------------
    let mut followers = Vec::new();
    for name in ["follower-0", "follower-1"] {
        let key = SecretKey::generate();
        let id = MemberId::from(key.public());

        // Admitted first: until the log says so, nothing will talk to them.
        admit(&core[0], id, name).await;

        let follower = Peer::start_with(key, ids.clone(), sources.clone()).await;
        wait_for(&follower, "the log to reach a new follower", |m| {
            m.is_member(&id)
        })
        .await;
        followers.push(follower);
    }

    // "It can connect" means what it says: a real connection, not just an entry
    // in somebody's table. Ping is the smallest thing that proves one.
    for follower in &followers {
        let echo = distlib_net::ping::ping(
            follower.node.endpoint(),
            core[1].addr.to_endpoint_addr(core[1].id).unwrap(),
            b"admitted",
        )
        .await
        .expect("an admitted member must be able to reach a core node");
        assert_eq!(echo, b"admitted");
    }

    // Every node agrees who is in the group: three voters and two who are not.
    for peer in core.iter().chain(&followers) {
        let membership = peer.node.membership();
        assert_eq!(membership.len(), 5, "three core plus two followers");
        assert_eq!(membership.core().len(), 3, "only the founders vote");
    }
    assert!(
        !followers[0].node.is_core() && followers[0].node.raft().is_none(),
        "a follower holds the log without voting on it"
    );

    // --- expel it → open connection drops, reconnect refused -------------
    let expelled = &followers[1];

    // A connection held open, so the close is something the group does rather
    // than something this test provokes by asking again.
    let held = expelled
        .node
        .endpoint()
        .connect(
            core[1].addr.to_endpoint_addr(core[1].id).unwrap(),
            distlib_net::alpn::PING,
        )
        .await
        .expect("a member may connect while it is still a member");

    admit_or_expel(
        &core[0],
        MembershipEvent::MemberExpelled {
            member: expelled.id,
            reason: "the acceptance criteria say so".to_owned(),
        },
    )
    .await;
    wait_for(
        &core[1],
        "the expulsion to reach the node holding it",
        |m| !m.is_member(&expelled.id),
    )
    .await;

    let reason = tokio::time::timeout(SOON, held.closed())
        .await
        .expect("an expelled member's open connection must be closed, not left running");
    assert!(
        format!("{reason:?}").contains("not a member"),
        "the peer should be told why; got {reason:?}"
    );

    // And the next attempt is refused rather than merely dropped.
    let refused = distlib_net::ping::ping(
        expelled.node.endpoint(),
        core[1].addr.to_endpoint_addr(core[1].id).unwrap(),
        b"still here?",
    )
    .await
    .expect_err("an expelled member must not be able to reconnect");
    assert!(
        format!("{refused}").contains("not a member") || format!("{refused:?}").contains("refused"),
        "the refusal should say what it was; got {refused}"
    );

    // --- kill one core node → group still admits members ----------------
    // Three voters, so one can go and the remaining two are still a quorum.
    // Losing the *leader* is the case worth testing: the group has to elect
    // another before it can commit anything at all.
    let leader = core
        .iter()
        .position(|peer| {
            peer.node
                .raft()
                .and_then(|raft| raft.metrics().borrow().current_leader)
                .is_some_and(|id| MemberId::try_from(id).is_ok_and(|id| id == peer.id))
        })
        .expect("a founded group has a leader");
    let dead = core.remove(leader);
    dead.node.shutdown().await;

    // No pause: the group is asked to commit while it is still working out who
    // leads it. That is the realistic shape of losing a leader, and it is what
    // exposed a forward to the dead one blocking for forty-five seconds.
    let newcomer = MemberId::from(SecretKey::generate().public());
    tokio::time::timeout(SOON, admit(&core[0], newcomer, "after the funeral"))
        .await
        .expect("two of three voters are a quorum; the group must still commit");

    for peer in core.iter().chain(&followers[..1]) {
        wait_for(peer, "the group to keep working without its leader", |m| {
            m.is_member(&newcomer)
        })
        .await;
    }

    for peer in core.iter().chain(&followers) {
        peer.node.shutdown().await;
    }
}

/// Admits `member` through `by`, and waits for it to take effect there.
async fn admit(by: &Peer, member: MemberId, name: &str) {
    admit_or_expel(
        by,
        MembershipEvent::MemberAdded {
            member: MemberRecord {
                member_id: member,
                display_name: name.to_owned(),
                pledge_bytes: 0,
            },
        },
    )
    .await;
}

/// Commits `event` through `by`.
async fn admit_or_expel(by: &Peer, event: MembershipEvent) {
    by.node
        .propose(event, &by.secret)
        .await
        .expect("the group must accept a proposal from one of its own core nodes");
}
