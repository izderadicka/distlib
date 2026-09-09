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

// Thirteen seconds: five nodes, a real election, and a leader killed in the
// middle of one. Skipped by `--no-default-features`; see the `slow-tests`
// feature in Cargo.toml.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point
#![allow(clippy::result_large_err)] // openraft's error types, in its own signatures

use std::time::Duration;

use distlib_consensus::{MemberRecord, MembershipEvent};
use distlib_core::{MemberId, NodeAddr};
use iroh::SecretKey;

mod common;
use common::{PATIENTLY, Peer, until, wait_for, wait_for_upto};

/// How long to wait for something that should happen without prompting.
const SOON: Duration = Duration::from_secs(15);

/// What a proposal can cost when the leader has just been killed.
///
/// Arithmetic rather than a guess. `propose` makes `PROPOSE_ATTEMPTS` = 3
/// attempts; an attempt that forwards to the dead leader spends
/// `CONNECT_TIMEOUT` = 3s discovering it is gone, with `FORWARD_RETRY_DELAY`
/// between them — about 9.5 seconds before an election has been decided, never
/// mind a commit replicated. `SOON` left no room for either half, which is how
/// this clause failed under a parallel runner.
const AFTER_A_DEATH: Duration = Duration::from_secs(45);

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
    //
    // The wait is on the node being asked. The loop above waits on the
    // *follower* seeing itself admitted, and this asks a *core node* to accept
    // it — a node whose allowlist is published by a task of its own, so on a
    // loaded machine it lags, and this failed exactly that way under a parallel
    // test runner. Waiting on the allowlist itself, rather than retrying the
    // ping, keeps the ping a single attempt that has to work.
    for follower in &followers {
        until("a core node to enforce the admission", || {
            core[1].hooks.allowlist().is_allowed(&follower.id)
        })
        .await;

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
    //
    // Waited for, not asserted outright. Nothing above makes every node current:
    // the loop waits for each *follower* to see *itself* admitted, and the ping
    // waits for `core[1]`'s allowlist — so `core[2]` is never waited on at all,
    // and neither is follower-0 learning about follower-1, which reaches it by
    // gossip or by its own timer. Asserting a count against that is asserting
    // that replication has already happened, and under a parallel test runner it
    // sometimes had not: `left: 4, right: 5`. Agreement is the claim §9 makes,
    // so waiting for it is the test; the bound turns a node that never converges
    // into a failure that says which one.
    for peer in core.iter().chain(&followers) {
        // Patiently, because of *what* is being waited on rather than how slow
        // the machine is. follower-0 was admitted before follower-1 existed, so
        // it learns about follower-1 either from a gossip announcement — which
        // is best-effort, and lost outright if it arrives before the
        // subscription is live (P1-33) — or from its own thirty-second timer.
        // A fifteen-second bound is below the guarantee, and failed exactly
        // that way under a parallel runner.
        wait_for_upto(peer, PATIENTLY, "every node to see the whole group", |m| {
            m.len() == 5
        })
        .await;
        assert_eq!(
            peer.node.membership().core().len(),
            3,
            "only the founders vote"
        );
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
    //
    // Waited for, not read once. `current_leader` is `None` while a term is
    // being decided, and an expulsion committed immediately above is exactly
    // the churn that can start one — so a single pass over the three peers can
    // legitimately find nobody. Every other read of node state in this file
    // waits for what it needs; this one did not, and its failure mode was a
    // bare panic 7 seconds in, with no bound to name and nothing to retry.
    let leader = leader_of(&core).await;
    let dead = core.remove(leader);
    dead.node.shutdown().await;

    // No pause: the group is asked to commit while it is still working out who
    // leads it. That is the realistic shape of losing a leader, and it is what
    // exposed a forward to the dead one blocking for forty-five seconds.
    let newcomer = MemberId::from(SecretKey::generate().public());
    tokio::time::timeout(
        AFTER_A_DEATH,
        admit(&core[0], newcomer, "after the funeral"),
    )
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

/// Which of `core` currently believes it is the leader.
///
/// Retried, because a founded group has a leader *eventually*: between one
/// term ending and the next being decided there is a window in which every node
/// answers `None`, and the group is asked to commit right before this.
async fn leader_of(core: &[Peer]) -> usize {
    tokio::time::timeout(SOON, async {
        loop {
            let found = core.iter().position(|peer| {
                peer.node
                    .raft()
                    .and_then(|raft| raft.metrics().borrow().current_leader)
                    .is_some_and(|id| MemberId::try_from(id).is_ok_and(|id| id == peer.id))
            });
            if let Some(leader) = found {
                return leader;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("a founded group must settle on a leader")
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
