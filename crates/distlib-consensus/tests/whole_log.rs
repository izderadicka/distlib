//! 5c-0's acceptance: the log is never purged (C43), so a member joining after
//! the leader has taken a snapshot reads every entry from the founding one
//! onward, a page at a time. And, run by hand, the measurement of a log of
//! 100,000 entries: its size on disk, and how long a joiner takes to catch up.

#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{
    num::NonZeroU8,
    time::{Duration, Instant},
};

use distlib_consensus::{EVENTS_PER_FETCH, MemberProfile, MembershipEvent, RAFT_DB, TypeConfig};
use distlib_core::{GroupSettings, MemberId};
use iroh::SecretKey;
use openraft::Raft;

mod common;
use common::{Peer, wait_for_upto};

/// A group of one core node, founded with settings that are not the defaults,
/// so a joiner that reads them shows it read the founding entry.
async fn founded() -> Peer {
    let founder = Peer::start(SecretKey::generate(), Vec::new()).await;
    let settings = GroupSettings {
        replicas: NonZeroU8::new(2).unwrap(),
        ..GroupSettings::default()
    };
    founder
        .node
        .init_group(
            vec![(founder.profile("founder"), founder.addr.clone())],
            settings,
            &founder.secret,
        )
        .await
        .unwrap();
    founder
}

/// Admits `count` members, so the log has that many more entries.
async fn admit_many(founder: &Peer, count: usize) {
    for n in 0..count {
        let member = MemberProfile {
            member_id: MemberId::from(SecretKey::generate().public()),
            display_name: format!("member-{n}"),
        };
        founder
            .node
            .propose(MembershipEvent::MemberAdded { member }, &founder.secret)
            .await
            .unwrap();
    }
}

/// Changes the founder's pledge `count` times: what a long-lived group's log
/// is mostly made of, rather than ever more members.
async fn change_pledge_many(founder: &Peer, count: usize) {
    const GB: u64 = 1_000_000_000;
    for n in 0..count as u64 {
        let event = MembershipEvent::PledgeChanged {
            member: founder.id,
            pledge_bytes: (100 + n % 2) * GB,
        };
        founder.node.propose(event, &founder.secret).await.unwrap();
    }
}

/// Snapshots `raft` now, and waits until the snapshot is taken.
async fn snapshot(raft: &Raft<TypeConfig>) {
    raft.trigger().snapshot().await.unwrap();
    raft.wait(Some(Duration::from_secs(30)))
        .metrics(|metrics| metrics.snapshot.is_some(), "a snapshot")
        .await
        .unwrap();
}

/// Starts a member admitted by `founder`, following its log.
async fn joiner(founder: &Peer) -> Peer {
    let key = SecretKey::generate();
    let member = MemberProfile {
        member_id: MemberId::from(key.public()),
        display_name: "joiner".to_owned(),
    };
    founder
        .node
        .propose(MembershipEvent::MemberAdded { member }, &founder.secret)
        .await
        .unwrap();
    Peer::start_with(
        key,
        vec![founder.id],
        vec![(founder.id, founder.addr.clone())],
    )
    .await
}

/// More entries than openraft's default of 1,000 kept behind a snapshot, with
/// which the founding entry would be purged. And three pages of them: gossip's
/// first word after joining wakes a follower once, so only the third shows it
/// asks again at once after a page short of the end.
#[tokio::test(flavor = "multi_thread")]
async fn a_member_joining_after_a_snapshot_reads_every_entry_from_the_founding_one() {
    let founder = founded().await;
    admit_many(&founder, 2 * EVENTS_PER_FETCH + 100).await;
    let raft = founder.node.raft().unwrap();
    snapshot(&raft).await;

    let joiner = joiner(&founder).await;
    // Well within the follower's idle poll of thirty seconds.
    let expected = founder.node.membership();
    wait_for_upto(
        &joiner,
        Duration::from_secs(15),
        "the whole log",
        |membership| *membership == expected,
    )
    .await;
    assert_eq!(joiner.node.membership().settings().replicas.get(), 2);
    assert_eq!(raft.metrics().borrow().purged, None, "nothing purged");

    joiner.shutdown().await;
    founder.shutdown().await;
}

/// The measurement 5c-0 repeats, by hand:
/// `cargo nextest run -p distlib-consensus --test whole_log --run-ignored only --no-capture`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, most of an hour long"]
async fn measure_a_log_of_100_000_entries() {
    const ENTRIES: usize = 100_000;
    const MEMBERS: usize = 1_000;
    let founder = founded().await;
    let started = Instant::now();
    admit_many(&founder, MEMBERS).await;
    change_pledge_many(&founder, ENTRIES - MEMBERS).await;
    eprintln!("{ENTRIES} entries committed in {:?}", started.elapsed());
    snapshot(&founder.node.raft().unwrap()).await;
    let size = std::fs::metadata(founder.dir().join(RAFT_DB))
        .unwrap()
        .len();
    eprintln!(
        "raft database: {size} bytes, {} bytes an entry",
        size / ENTRIES as u64
    );

    let started = Instant::now();
    let joiner = joiner(&founder).await;
    let expected = founder.node.membership();
    wait_for_upto(
        &joiner,
        Duration::from_secs(600),
        "the whole log",
        |membership| *membership == expected,
    )
    .await;
    eprintln!("a joiner caught up in {:?}", started.elapsed());

    joiner.shutdown().await;
    founder.shutdown().await;
}
