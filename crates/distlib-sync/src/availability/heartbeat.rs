//! The heartbeat: telling the group this node is online, and hearing who else
//! is (phase 4's D4).
//!
//! One gossip topic per group, separate from the membership topic and the
//! catalogue's, on the process's one swarm. Every node beats on it and listens
//! to it. A beat carries the sender's signed address, so a member that missed
//! an address announcement learns it here, by the next beat (C5). And it says
//! what the sender holds — see [`super::statement`] — which every listener
//! reads into [`super::holders`], fetching a base list from its sender when a
//! beat names one it does not have.

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use bytes::Bytes;
use distlib_consensus::{MembershipState, gossip};
use distlib_core::{
    ContentHash, Event, GroupId, Heartbeat, ItemId, MemberId, SignedAddress, SignedHeartbeat,
    availability::{BASE_MAX, decode_base},
};
use distlib_net::{Directory, Transport};
use futures_lite::StreamExt as _;
use iroh::{Endpoint, SecretKey};
use iroh_blobs::{
    Hash,
    get::request::{get_blob, get_verified_size},
};
use iroh_gossip::{
    api::{Event as GossipEvent, GossipReceiver, GossipSender},
    proto::TopicId,
};
use tokio::{
    sync::{Notify, broadcast, watch},
    task::{JoinHandle, JoinSet},
    time::Instant,
};
use tracing::Instrument as _;

use super::{
    Holdings,
    holders::{Heard, Holders},
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

/// The largest base list fetched: the format byte and [`BASE_MAX`] ids.
const BASE_BYTES_MAX: u64 = 1 + 32 * BASE_MAX;

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

/// This node's heartbeat, who else is online, and what they hold.
///
/// Cheap to clone; every clone is the same heartbeat.
#[derive(Clone)]
pub struct Availability {
    secret: SecretKey,
    online: watch::Receiver<BTreeSet<MemberId>>,
    holders: Arc<Mutex<Holders>>,
    /// The last beat sent, and where — what a goodbye follows. `None` until
    /// the first beat.
    said: Arc<Mutex<Option<Said>>>,
    /// Waiting for the group, then beating and listening.
    task: Arc<Mutex<Option<JoinHandle<()>>>>,
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
    ///
    /// Says on `events` whenever what it can tell about a member changes —
    /// `availability.changed`, never awaited.
    pub fn start(
        transport: &Transport,
        secret: &SecretKey,
        sources: Sources,
        interval: Duration,
        events: broadcast::Sender<Event>,
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
        let holders = Arc::new(Mutex::new(Holders::default()));
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
                holders: holders.clone(),
                events,
                said: said.clone(),
            })
            .instrument(span),
        );
        Ok(Self {
            secret: secret.clone(),
            online,
            holders,
            said,
            task: Arc::new(Mutex::new(Some(task))),
        })
    }

    /// The other members online now, by their heartbeats.
    pub fn online(&self) -> watch::Receiver<BTreeSet<MemberId>> {
        self.online.clone()
    }

    /// The other members known to hold `item`, online or not — by their
    /// beats, and the base lists those name.
    pub fn holders(&self, item: &ItemId) -> Vec<MemberId> {
        lock(&self.holders).holders(item).to_vec()
    }

    /// Whether what `member` holds is known: until its base list has arrived,
    /// it is unknown rather than "holds nothing" (D5).
    pub fn knows_holdings_of(&self, member: &MemberId) -> bool {
        lock(&self.holders).is_known(member)
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
    holders: Arc<Mutex<Holders>>,
    events: broadcast::Sender<Event>,
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
        holders,
        events,
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
                endpoint: transport.endpoint.clone(),
                published,
                holders,
                events,
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

        let due = Instant::now() + jittered(stretched);
        tokio::select! {
            () = tokio::time::sleep_until(due) => {}
            () = neighbour_up.notified() => {}
            moved = own_address.changed() => if moved.is_err() {
                tracing::error!("this node no longer says where it is; beating no more");
                return;
            },
            // Cannot fail: `holdings` here keeps the sending half alive. The
            // floor holds back only the beat the change prompts: never past
            // the one already due, or a busy node would beat less often than
            // it promised, and outlive its TTL.
            _ = changes.changed() => {
                tokio::time::sleep_until((last_beat + CHANGE_FLOOR).min(due)).await;
            }
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
    endpoint: Endpoint,
    published: watch::Sender<BTreeSet<MemberId>>,
    holders: Arc<Mutex<Holders>>,
    events: broadcast::Sender<Event>,
}

/// Hears the group's beats, and publishes who is online after each change.
///
/// Base lists are fetched, and taken into the map, by tasks of their own, never
/// in this loop: a slow or unreachable sender must not hold up every other beat
/// and every expiry. The tasks end with the loop.
async fn listen(listening: Listening, neighbour_up: &Notify) {
    let Listening {
        mut receiver,
        me,
        group,
        mut membership,
        directory,
        endpoint,
        published,
        holders,
        events,
    } = listening;
    let mut online = Online::new(me);
    let mut fetches = JoinSet::new();
    loop {
        let expiry = online.next_expiry();
        tokio::select! {
            event = receiver.next() => match event {
                Some(Ok(GossipEvent::Received(message))) => {
                    let heard = hear(
                        Hearing {
                            online: &mut online,
                            holders: &mut lock(&holders),
                            directory: &directory,
                            group: &group,
                            membership: &membership.borrow(),
                            events: &events,
                        },
                        &message.content,
                    );
                    match heard {
                        Ok(Some(fetch)) => {
                            reap(&mut fetches);
                            fetches.spawn(learn(
                                endpoint.clone(),
                                holders.clone(),
                                events.clone(),
                                fetch,
                            ));
                        }
                        Ok(None) => {}
                        Err(error) => tracing::warn!(
                            %error,
                            from = %message.delivered_from,
                            "ignoring a heartbeat that did not verify"
                        ),
                    }
                }
                Some(Ok(GossipEvent::NeighborUp(_))) => neighbour_up.notify_one(),
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
                lock(&holders).retain_members(|member| now.is_member(member));
            }
        }
        let now = online.online();
        published.send_if_modified(|was| {
            if *was == now {
                return false;
            }
            was.symmetric_difference(&now)
                .for_each(|member| tell(&events, *member));
            *was = now;
            true
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

fn lock(holders: &Mutex<Holders>) -> MutexGuard<'_, Holders> {
    holders.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What hearing a beat changes.
struct Hearing<'a> {
    online: &'a mut Online,
    holders: &'a mut Holders,
    directory: &'a Directory,
    group: &'a GroupId,
    membership: &'a MembershipState,
    events: &'a broadcast::Sender<Event>,
}

/// A member's base list, to be fetched from it.
struct Fetch {
    member: MemberId,
    base: ContentHash,
}

/// Says that what this node can tell about `member` changed. Never awaited:
/// nobody watching is not a failure (phase 3's rule).
fn tell(events: &broadcast::Sender<Event>, member: MemberId) {
    let _ = events.send(Event::AvailabilityChanged { member_id: member });
}

/// D4's receive rule for one message: decoded within the size, both
/// signatures verified, then news only from another member and only if newer
/// than what is held — and then its address goes to the directory, and what
/// it holds to the map. The base list to fetch, if the beat names one this
/// node lacks.
fn hear(hearing: Hearing<'_>, content: &[u8]) -> distlib_core::error::Result<Option<Fetch>> {
    let Hearing {
        online,
        holders,
        directory,
        group,
        membership,
        events,
    } = hearing;
    let signed = SignedHeartbeat::decode(content)?;
    let beat = signed.open(group)?;
    if !online.heard(beat, Instant::now(), |member| membership.is_member(member)) {
        return Ok(None);
    }
    if let Err(error) = directory.learn(&beat.address) {
        // `open` has verified this address already, so this is not a
        // stranger's doing but a disagreement between two checks.
        tracing::warn!(%error, "a verified heartbeat's address was refused");
    }
    let member = beat.address.member();
    let Heard { fetch, changed } = holders.heard(member, &beat.holdings);
    if changed {
        tell(events, member);
    }
    Ok(fetch.map(|base| Fetch { member, base }))
}

/// Forgets the fetches that have ended, so the set holds only those running.
fn reap(fetches: &mut JoinSet<()>) {
    while let Some(ended) = fetches.try_join_next() {
        if let Err(error) = ended {
            // A panic, and the member it was for stays unknown: a bug.
            tracing::error!(%error, "a base list fetch failed outright");
        }
    }
}

/// Whether fetching the same list again could end differently. Only reaching
/// the member can: the hash fixes the bytes, so a list too big or unreadable
/// once is so every time.
fn worth_retrying(error: &SyncError) -> bool {
    matches!(error, SyncError::BaseFetch { .. })
}

/// Fetches a member's base and takes it into the map.
async fn learn(
    endpoint: Endpoint,
    holders: Arc<Mutex<Holders>>,
    events: broadcast::Sender<Event>,
    wanted: Fetch,
) {
    let fetched = fetch(&endpoint, wanted.member, wanted.base).await;
    take_in(&mut lock(&holders), &events, wanted, fetched);
}

/// Fetches `member`'s `base` into memory, never into this node's store — so a
/// receiver leaves nothing behind (D5). Its size is asked first, verified
/// against the hash, so a member naming some huge blob as its list is refused
/// before a byte of it is read.
async fn fetch(endpoint: &Endpoint, member: MemberId, base: ContentHash) -> Result<Bytes> {
    let failed =
        |source: Box<dyn std::error::Error + Send + Sync>| SyncError::BaseFetch { member, source };
    let hash = Hash::from_bytes(*base.as_bytes());
    let connection = endpoint
        .connect(member.endpoint_id(), iroh_blobs::ALPN)
        .await
        .map_err(|error| failed(error.into()))?;
    let (size, _) = get_verified_size(&connection, &hash)
        .await
        .map_err(|error| failed(error.into()))?;
    if size > BASE_BYTES_MAX {
        return Err(SyncError::BaseTooBig { member, size });
    }
    get_blob(connection, hash)
        .bytes()
        .await
        .map_err(|error| failed(error.into()))
}

/// Takes a member's fetched base into the map, and says so — or, failing,
/// lets the next beat that names it try again, if trying again could help.
fn take_in(
    holders: &mut Holders,
    events: &broadcast::Sender<Event>,
    Fetch { member, base }: Fetch,
    fetched: Result<Bytes>,
) {
    let error = match fetched {
        Ok(bytes) => match decode_base(&bytes) {
            Ok(items) => {
                if holders.arrived(member, base, items) {
                    tell(events, member);
                }
                return;
            }
            Err(source) => SyncError::BadBase { member, source },
        },
        Err(error) => error,
    };
    tracing::warn!(%error, "could not read what a member holds");
    holders.failed(member, base, worth_retrying(&error));
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

    fn bob_names_a_base() -> (Holders, MemberId, ContentHash) {
        let bob = MemberId::from(SecretKey::generate().public());
        let base = ContentHash::from_bytes([9; 32]);
        let mut holders = Holders::default();
        let stated = distlib_core::Holdings {
            count: 1,
            base: Some(base),
            ..Default::default()
        };
        assert_eq!(holders.heard(bob, &stated).fetch, Some(base));
        (holders, bob, base)
    }

    fn named_again(holders: &mut Holders, bob: MemberId, base: ContentHash) -> bool {
        let stated = distlib_core::Holdings {
            count: 1,
            base: Some(base),
            ..Default::default()
        };
        holders.heard(bob, &stated).fetch.is_some()
    }

    #[test]
    fn a_member_that_could_not_be_reached_is_asked_again() {
        let (mut holders, bob, base) = bob_names_a_base();
        let unreachable = SyncError::BaseFetch {
            member: bob,
            source: "no route".into(),
        };
        let (events, mut told) = broadcast::channel(4);
        take_in(
            &mut holders,
            &events,
            Fetch { member: bob, base },
            Err(unreachable),
        );
        assert!(named_again(&mut holders, bob, base));
        assert!(told.try_recv().is_err(), "nothing new to tell");
    }

    /// The hash fixes the bytes: what was wrong with them once always is.
    #[test]
    fn a_list_that_is_not_one_is_not_fetched_again() {
        for fetched in [
            Ok(Bytes::from_static(b"\x07not a list")),
            Err(SyncError::BaseTooBig {
                member: MemberId::from(SecretKey::generate().public()),
                size: BASE_BYTES_MAX + 1,
            }),
        ] {
            let (mut holders, bob, base) = bob_names_a_base();
            let (events, mut told) = broadcast::channel(4);
            take_in(&mut holders, &events, Fetch { member: bob, base }, fetched);
            assert!(!named_again(&mut holders, bob, base));
            assert!(!holders.is_known(&bob));
            assert!(told.try_recv().is_err(), "nothing new to tell");
        }
    }

    #[test]
    fn a_list_that_arrives_is_read_into_the_map_and_told() {
        let (mut holders, bob, base) = bob_names_a_base();
        let item = ItemId::from_bytes([1; 32]);
        let list = distlib_core::availability::encode_base([item]);
        let (events, mut told) = broadcast::channel(4);
        take_in(
            &mut holders,
            &events,
            Fetch { member: bob, base },
            Ok(Bytes::from(list)),
        );
        assert!(holders.is_known(&bob));
        assert_eq!(holders.holders(&item), [bob]);
        assert_eq!(
            told.try_recv().ok(),
            Some(Event::AvailabilityChanged { member_id: bob }),
            "what bob holds is known now"
        );
    }
}
