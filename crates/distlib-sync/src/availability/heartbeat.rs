//! The heartbeat: telling the group this node is online, and hearing who else
//! is (phase 4's D4).
//!
//! One gossip topic per group, separate from the membership topic and the
//! catalogue's, on the process's one swarm. Every node beats on it and listens
//! to it. A beat carries the sender's signed address, so a member that missed
//! an address announcement learns it here, by the next beat (C5). And it says
//! what the sender holds — see [`super::statement`]; nobody reads that yet
//! (4b-4's second half).

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use bytes::Bytes;
use distlib_consensus::{MembershipState, gossip};
use distlib_core::{GroupId, Heartbeat, MemberId, SignedAddress, SignedHeartbeat};
use distlib_net::{Directory, Transport};
use futures_lite::StreamExt as _;
use iroh::SecretKey;
use iroh_gossip::{
    api::{Event, GossipReceiver, GossipSender},
    proto::TopicId,
};
use tokio::{
    sync::{Notify, watch},
    task::JoinHandle,
    time::Instant,
};
use tracing::Instrument as _;

use super::{
    Holdings,
    online::{MAX_INTERVAL, Online},
    statement::Statement,
};
use crate::error::{Result, SyncError};

/// The domain the topic is derived under. A wire fact, like the catalogue's:
/// two nodes deriving it differently hear nothing from each other, and nothing
/// says so.
const TOPIC_TAG: &[u8] = b"distlib.availability.v1";

/// Above this many members, each sender stretches its interval in proportion,
/// so every node still receives about this many beats per interval (D4's
/// budget — Ivan's call).
const BUDGET: usize = 50;

/// How long a goodbye is given to leave before the endpoint closes under it.
///
/// Broadcasting only hands the beat to the gossip actor; closing a connection
/// discards whatever it has not yet written. Milliseconds on any network this
/// is sent over, and a goodbye that is lost costs its peers a TTL, not more.
const GOODBYE_GRACE: Duration = Duration::from_millis(250);

/// The least time between a beat and one a change to the held set prompts,
/// so a burst of changes — a download of many items — is one beat, not many
/// (D4).
const CHANGE_FLOOR: Duration = Duration::from_secs(10);

/// The availability topic of `group`.
pub fn topic_for(group: GroupId) -> TopicId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(TOPIC_TAG);
    hasher.update(group.as_bytes());
    TopicId::from_bytes(*hasher.finalize().as_bytes())
}

/// What a heartbeat is made from, each as it changes.
pub struct Sources {
    /// The group, to wait for and to count.
    pub membership: watch::Receiver<MembershipState>,
    /// Where this node says it is — `MembershipNode::own_address`.
    pub own_address: watch::Receiver<Option<SignedAddress>>,
    /// What this node holds — `Catalogue::holdings`.
    pub holdings: Holdings,
}

/// This node's heartbeat, and who else is online.
pub struct Availability {
    secret: SecretKey,
    online: watch::Receiver<BTreeSet<MemberId>>,
    /// The last beat sent, and where — what a goodbye follows. `None` until
    /// the first beat.
    said: Arc<Mutex<Option<Said>>>,
    /// Waiting for the group, then beating and listening.
    task: Mutex<Option<JoinHandle<()>>>,
}

/// The last beat this node sent, and the topic it went to.
#[derive(Debug, Clone)]
struct Said {
    sender: GossipSender,
    group: GroupId,
    beat: Heartbeat,
}

impl Availability {
    /// Starts beating every `interval` once this node is in a group and
    /// knows where it is.
    ///
    /// A beat goes out at once, then every interval — stretched in groups
    /// over fifty, give or take a tenth so the group does not beat in step —
    /// and again whenever this node's address statement changes, a neighbour
    /// comes up, or what it holds changes (at most once per ten seconds).
    pub fn start(
        transport: &Transport,
        secret: &SecretKey,
        sources: Sources,
        interval: Duration,
    ) -> Result<Self> {
        let Sources {
            membership,
            own_address,
            holdings,
        } = sources;
        // Which run of this node a beat is from: a restart is told apart from
        // a replay by this, not by a clock.
        let epoch = getrandom::u64().map_err(|error| SyncError::Random {
            message: error.to_string(),
        })?;
        let me = MemberId::from(secret.public());
        let (published, online) = watch::channel(BTreeSet::new());
        let said = Arc::new(Mutex::new(None));
        let span = tracing::debug_span!(parent: None, "availability", %me);
        let task = tokio::spawn(
            join_when_founded(Joining {
                transport: transport.clone(),
                secret: secret.clone(),
                membership,
                own_address,
                holdings,
                interval,
                epoch,
                published,
                said: said.clone(),
            })
            .instrument(span),
        );
        Ok(Self {
            secret: secret.clone(),
            online,
            said,
            task: Mutex::new(Some(task)),
        })
    }

    /// The other members online now, by their heartbeats.
    pub fn online(&self) -> watch::Receiver<BTreeSet<MemberId>> {
        self.online.clone()
    }

    /// Stops beating and tells the group this node is going — before the
    /// endpoint closes, which a goodbye cannot outlive.
    ///
    /// Without it the group counts this node online until its last beat runs
    /// out. A node that never beat has nobody to tell.
    pub async fn leave(&self) {
        // Stopped, not merely asked to: no beat may follow the goodbye.
        if let Some(task) = self.take_task() {
            task.abort();
            let _ = task.await;
        }
        let said = self
            .said
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let Some(Said {
            sender,
            group,
            mut beat,
        }) = said
        else {
            return;
        };
        beat.seq += 1;
        beat.leaving = true;
        say(&sender, &self.secret, &group, beat).await;
        tokio::time::sleep(GOODBYE_GRACE).await;
    }

    /// Stops beating without a word, as a crash would.
    pub fn shutdown(&self) {
        if let Some(task) = self.take_task() {
            task.abort();
        }
    }

    fn take_task(&self) -> Option<JoinHandle<()>> {
        self.task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }
}

/// What the task needs; see [`Availability::start`].
struct Joining {
    transport: Transport,
    secret: SecretKey,
    membership: watch::Receiver<MembershipState>,
    own_address: watch::Receiver<Option<SignedAddress>>,
    holdings: Holdings,
    interval: Duration,
    epoch: u64,
    published: watch::Sender<BTreeSet<MemberId>>,
    said: Arc<Mutex<Option<Said>>>,
}

/// Waits for the group, joins its topic, then beats and listens until
/// aborted.
async fn join_when_founded(joining: Joining) {
    let Joining {
        transport,
        secret,
        mut membership,
        own_address,
        holdings,
        interval,
        epoch,
        published,
        said,
    } = joining;
    let me = MemberId::from(secret.public());
    let group = loop {
        if let Some(group) = membership.borrow_and_update().group_id() {
            break group;
        }
        if membership.changed().await.is_err() {
            tracing::error!("this node is no longer in a group; availability task ends");
            return;
        }
    };

    // Only those this node can reach, for the reason the membership topic
    // names: a member named here with no address strands itself (C30).
    let bootstrap = gossip::reachable(&membership.borrow_and_update(), me, &transport.directory)
        .map(|(member, _)| member.endpoint_id())
        .collect();
    let topic = match transport
        .gossip
        .subscribe(topic_for(group), bootstrap)
        .await
    {
        Ok(topic) => topic,
        Err(error) => {
            tracing::warn!(%error, "could not join the availability topic");
            return;
        }
    };
    tracing::debug!(%group, "joined the availability topic");
    let (sender, receiver) = topic.split();
    let neighbour_up = Notify::new();

    tokio::join!(
        beat(
            Beating {
                sender,
                secret,
                group,
                epoch,
                interval,
                membership: membership.clone(),
                own_address,
                holdings,
                said,
            },
            &neighbour_up,
        ),
        listen(
            Listening {
                receiver,
                me,
                group,
                membership,
                directory: transport.directory.clone(),
                published,
            },
            &neighbour_up,
        ),
    );
}

/// What the beating half needs.
struct Beating {
    sender: GossipSender,
    secret: SecretKey,
    group: GroupId,
    epoch: u64,
    interval: Duration,
    membership: watch::Receiver<MembershipState>,
    own_address: watch::Receiver<Option<SignedAddress>>,
    holdings: Holdings,
    said: Arc<Mutex<Option<Said>>>,
}

/// Beats until aborted, or until this node stops saying where it is.
///
/// Also whenever a neighbour comes up, which has not heard this node yet —
/// the first beat of all goes out before anyone is connected, to nobody.
async fn beat(beating: Beating, neighbour_up: &Notify) {
    let Beating {
        sender,
        secret,
        group,
        epoch,
        interval,
        membership,
        mut own_address,
        holdings,
        said,
    } = beating;
    let mut statement = Statement::new(holdings.blobs.clone());
    let mut changes = holdings.changes();
    let mut seq = 0;
    let mut last_beat = Instant::now();
    loop {
        let stretched = stretch(interval, membership.borrow().allowlist().count());
        // Nothing to say until consensus has said where this node is; its
        // first statement wakes the loop below.
        let address = own_address.borrow_and_update().clone();
        if let Some(address) = address {
            let changed_at = *changes.borrow_and_update();
            match statement
                .holdings(&holdings.held(), changed_at, Instant::now())
                .await
            {
                Ok(stated) => {
                    seq += 1;
                    let beat = Heartbeat {
                        address,
                        epoch,
                        seq,
                        interval_secs: whole_seconds(stretched),
                        holdings: stated,
                        leaving: false,
                    };
                    // Recorded before it is sent, so a goodbye always counts
                    // past it.
                    *said.lock().unwrap_or_else(PoisonError::into_inner) = Some(Said {
                        sender: sender.clone(),
                        group,
                        beat: beat.clone(),
                    });
                    say(&sender, &secret, &group, beat).await;
                    last_beat = Instant::now();
                }
                // The next beat tries again; one that stated less than the
                // truth would be worse than one that is late.
                Err(error) => tracing::error!(%error, "could not state what this node holds"),
            }
        }

        tokio::select! {
            () = tokio::time::sleep(jittered(stretched)) => {}
            () = neighbour_up.notified() => {}
            moved = own_address.changed() => if moved.is_err() {
                tracing::error!("this node no longer says where it is; beating no more");
                return;
            },
            // Cannot fail: `holdings` here keeps the sending half alive.
            _ = changes.changed() => tokio::time::sleep_until(last_beat + CHANGE_FLOOR).await,
        }
    }
}

/// D4's budget: `interval` once per fifty members, or part of fifty — and
/// never more than [`MAX_INTERVAL`], however large the group or the setting.
fn stretch(interval: Duration, members: usize) -> Duration {
    let times = u32::try_from(members.div_ceil(BUDGET).max(1)).unwrap_or(u32::MAX);
    interval.saturating_mul(times).min(MAX_INTERVAL)
}

/// `interval` as the beat states it: whole seconds, rounded up, never zero.
fn whole_seconds(interval: Duration) -> u32 {
    u32::try_from(interval.as_millis().div_ceil(1_000))
        .unwrap_or(u32::MAX)
        .max(1)
}

/// `interval`, give or take up to a tenth.
///
/// Randomness that fails — it does not, on any platform this runs on — costs
/// only the spread, so it is not worth stopping the heartbeat for.
fn jittered(interval: Duration) -> Duration {
    let draw = getrandom::u32().unwrap_or(u32::MAX / 2);
    interval.mul_f64(0.9 + 0.2 * f64::from(draw) / f64::from(u32::MAX))
}

/// Signs, encodes and sends one beat, saying so if it cannot. Never retried:
/// the next beat says the same and more.
async fn say(sender: &GossipSender, secret: &SecretKey, group: &GroupId, beat: Heartbeat) {
    let encoded =
        match SignedHeartbeat::sign(secret, group, beat).and_then(|signed| signed.encode()) {
            Ok(encoded) => encoded,
            Err(error) => {
                tracing::error!(%error, "could not sign this node's heartbeat");
                return;
            }
        };
    if let Err(error) = sender.broadcast(Bytes::from(encoded)).await {
        tracing::warn!(%error, "could not send a heartbeat");
    }
}

/// What the listening half needs.
struct Listening {
    receiver: GossipReceiver,
    me: MemberId,
    group: GroupId,
    membership: watch::Receiver<MembershipState>,
    directory: Directory,
    published: watch::Sender<BTreeSet<MemberId>>,
}

/// Hears the group's beats, and publishes who is online after each change.
async fn listen(listening: Listening, neighbour_up: &Notify) {
    let Listening {
        mut receiver,
        me,
        group,
        mut membership,
        directory,
        published,
    } = listening;
    let mut online = Online::new(me);
    loop {
        let expiry = online.next_expiry();
        tokio::select! {
            event = receiver.next() => match event {
                Some(Ok(Event::Received(message))) => {
                    let heard = hear(
                        &mut online,
                        &directory,
                        &group,
                        &membership.borrow(),
                        &message.content,
                    );
                    if let Err(error) = heard {
                        tracing::warn!(
                            %error,
                            from = %message.delivered_from,
                            "ignoring a heartbeat that did not verify"
                        );
                    }
                }
                Some(Ok(Event::NeighborUp(_))) => neighbour_up.notify_one(),
                // Neighbours going, and beats dropped before they were read:
                // every beat is whole on its own, so the next one makes up for
                // whatever was missed.
                Some(Ok(_)) => {}
                Some(Err(error)) => {
                    tracing::warn!(%error, "the availability topic failed; hearing no more");
                    return;
                }
                None => return,
            },
            () = until(expiry) => online.expire(Instant::now()),
            changed = membership.changed() => {
                if changed.is_err() {
                    return;
                }
                let now = membership.borrow_and_update();
                online.retain_members(|member| now.is_member(member));
            }
        }
        let now = online.online();
        published.send_if_modified(|held| {
            let news = *held != now;
            if news {
                *held = now;
            }
            news
        });
    }
}

/// Sleeps until `deadline`, or for ever without one.
async fn until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// D4's receive rule for one message: decoded within the size, both
/// signatures verified, then news only from another member and only if newer
/// than what is held — and then its address goes to the directory.
fn hear(
    online: &mut Online,
    directory: &Directory,
    group: &GroupId,
    membership: &MembershipState,
    content: &[u8],
) -> distlib_core::error::Result<()> {
    let signed = SignedHeartbeat::decode(content)?;
    let beat = signed.open(group)?;
    if online.heard(beat, Instant::now(), |member| membership.is_member(member))
        && let Err(error) = directory.learn(&beat.address)
    {
        // `open` has verified this address already, so this is not a
        // stranger's doing but a disagreement between two checks.
        tracing::warn!(%error, "a verified heartbeat's address was refused");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::availability::online::ttl;

    const SECOND: Duration = Duration::from_secs(1);

    #[test]
    fn the_interval_stretches_once_per_fifty_members() {
        for (members, times) in [(0, 1), (1, 1), (50, 1), (51, 2), (100, 2), (101, 3)] {
            assert_eq!(
                stretch(60 * SECOND, members),
                times * 60 * SECOND,
                "{members}"
            );
        }
    }

    #[test]
    fn the_interval_stops_stretching_at_twenty_minutes() {
        assert_eq!(stretch(60 * SECOND, 1_000), 20 * 60 * SECOND);
        assert_eq!(stretch(60 * SECOND, 1_001), MAX_INTERVAL);
        assert_eq!(stretch(60 * SECOND, usize::MAX), MAX_INTERVAL);
        assert_eq!(
            stretch(2 * 60 * 60 * SECOND, 1),
            MAX_INTERVAL,
            "nor the setting"
        );
    }

    /// The TTL and the stretch are two clamps on one number: a receiver must
    /// believe every beat for three of its sender's intervals, however large
    /// the group or the setting.
    #[test]
    fn a_beat_is_always_believed_for_three_of_its_intervals() {
        for interval in [SECOND, 60 * SECOND, 60 * 60 * SECOND] {
            for members in [1, 50, 1_000, 1_001, 100_000, usize::MAX] {
                let stretched = stretch(interval, members);
                assert!(
                    ttl(whole_seconds(stretched)) >= 3 * stretched,
                    "{interval:?} at {members} members"
                );
            }
        }
    }

    #[test]
    fn a_beat_states_its_interval_in_whole_seconds_rounded_up() {
        assert_eq!(whole_seconds(Duration::from_millis(300)), 1);
        assert_eq!(whole_seconds(Duration::ZERO), 1);
        assert_eq!(whole_seconds(SECOND), 1);
        assert_eq!(whole_seconds(Duration::from_millis(1_001)), 2);
        assert_eq!(whole_seconds(Duration::MAX), u32::MAX);
    }

    #[test]
    fn the_jitter_is_within_a_tenth() {
        for _ in 0..1_000 {
            let jittered = jittered(10 * SECOND);
            assert!(
                (9 * SECOND..=11 * SECOND).contains(&jittered),
                "{jittered:?}"
            );
        }
    }

    /// The topic is a wire fact: pinned, like the catalogue key.
    #[test]
    fn the_topic_derivation_is_fixed() {
        let topic = topic_for(GroupId::from_bytes([7; 32]));
        assert_eq!(
            topic.to_string(),
            "61b5a95a79e274f7f343348c7b1d2b7fea93bd253d000efea04649fce712c683",
            "changing this splits every group's heartbeats in two"
        );
    }
}
