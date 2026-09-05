//! Following the membership log without voting on it.
//!
//! §4.2's other kind of member: it holds the whole log and derives the same
//! things a core node does — the allowlist, the pledge table — but takes no
//! part in consensus. It pulls rather than being pushed to, which is what makes
//! thousands of members possible: Raft would have the leader replicate to every
//! one of them.
//!
//! The cost of pulling is that **a follower is eventually consistent, and
//! nothing here promises otherwise**. Raft's guarantee stops at the voters. A
//! follower's staleness is bounded by its own liveness and by how often it
//! asks, and it can be arbitrarily stale after being offline. That matters
//! because it *enforces* the allowlist from its copy: a stale follower keeps
//! talking to somebody expelled, and refuses somebody newly admitted.
//!
//! What is guaranteed instead:
//!
//! * **Monotonicity.** The cursor only advances, entries apply in order.
//! * **Integrity.** Every event is signed, and folded with the same
//!   [`crate::MembershipState::apply`] a core node uses — so no source can talk a
//!   follower into a membership no core node would accept. A source can still
//!   *withhold*, which is a different thing and not defended against here.
//! * **It announces itself.** A proposal carries the membership it was made
//!   against, so a follower that has fallen behind finds out the moment it
//!   tries to act rather than proposing into a group that has moved on.
//!
//! Deliberately not done: failing closed. A follower that has not heard for
//! some time could refuse everything, but a network blip would then take a node
//! off the air, and §4.4 already sets the precedent the other way — an expelled
//! member keeps what they downloaded, by design.

use std::{
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use tokio::{sync::watch, time::Instant};

use distlib_core::{MemberId, NodeAddr};

use crate::{
    gossip::{Hint, Hints},
    raft::{
        memberlog::{Fetched, MemberlogClient},
        state_machine::StateMachineStore,
    },
};

/// How long to wait before asking again when nothing has changed.
///
/// A backstop rather than the main mechanism: [`crate::gossip`] announces each
/// advance and wakes this loop, and the timer is what catches a follower that
/// missed the announcement. Long, because that is the point — thousands of
/// members polling briskly at three to seven core nodes is the thing gossip
/// exists to avoid.
const IDLE_POLL: Duration = Duration::from_secs(30);

/// How long to wait after every known source has failed.
///
/// Shorter than [`IDLE_POLL`]: being unable to reach anybody is a state worth
/// leaving quickly, where having nothing new to fetch is not.
const RETRY_DELAY: Duration = Duration::from_secs(1);

/// How long to wait when catching up is impossible.
///
/// Long, because nothing this loop does will change the answer: the entries
/// this node needs have been purged everywhere, and it stays stuck until it can
/// be given the state itself. Retrying at [`RETRY_DELAY`] would fill the log
/// with a message that is already as loud as it needs to be.
const STRANDED_BACKOFF: Duration = Duration::from_secs(60);

/// How long to wait after being refused by everyone who answered.
///
/// Only reached by a node that has never held the log — one waiting to be
/// admitted. What it is waiting for is a person proposing it, so a brisk retry
/// buys nothing and costs every core node a refused connection to log.
const REFUSED_BACKOFF: Duration = Duration::from_secs(60);

/// Where a follower asks for the log, and who it prefers.
#[derive(Debug, Clone, Default)]
pub struct Sources {
    /// The core group, each with somewhere to reach them.
    ///
    /// Seeded from `[consensus] core` and replaced by what the log's own
    /// answers carry, so a follower keeps up with a core group that changes
    /// without being reconfigured.
    pub core: Vec<(MemberId, NodeAddr)>,

    /// The leader, when a source last named one.
    pub leader: Option<MemberId>,
}

impl Sources {
    /// Who to try, in order, leaving this node out.
    ///
    /// The leader first: Raft guarantees it holds every committed entry, so it
    /// is the most current answer available. The rest follow in whatever order
    /// they arrived — they are all voters, so any of them is correct, only
    /// possibly behind.
    pub fn candidates(&self, me: MemberId) -> Vec<(MemberId, NodeAddr)> {
        let mut candidates: Vec<_> = self
            .core
            .iter()
            .filter(|(member, _)| *member != me)
            .cloned()
            .collect();
        candidates.sort_by_key(|(member, _)| Some(*member) != self.leader);
        candidates
    }
}

/// Everything the follow loop needs to keep this node's copy of the log fresh.
///
/// A struct rather than five arguments, and it earns its place: the loop is one
/// job — ask somebody for the log — and these are the parts of the node it does
/// it with.
pub struct Following {
    /// This node, so it does not ask itself.
    pub me: MemberId,
    pub state_machine: StateMachineStore,
    pub client: MemberlogClient,
    pub sources: SharedSources,

    /// Set once, if this node discovers it has been expelled.
    ///
    /// The loop stops there, and whoever started the node decides what to do
    /// about it — there is nothing useful left for a node outside the group to
    /// do, and plenty of harm in carrying on asking.
    pub expelled: watch::Sender<bool>,
}

/// Sources shared between the follow task and whoever forwards a proposal.
pub type SharedSources = Arc<Mutex<Sources>>;

/// A poison-tolerant read of the shared sources.
///
/// Nothing panics while holding this lock — it covers a clone and nothing else
/// — so a poisoned lock means an unrelated panic, and refusing to follow
/// afterwards would turn that into a second failure.
pub fn read(sources: &SharedSources) -> Sources {
    sources
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone()
}

/// Keeps this node's copy of the log up to date, forever.
///
/// Runs until aborted. Every failure is a reason to ask somebody else rather
/// than to stop: a follower that gave up on an unreachable core node would stay
/// frozen at whatever it last saw, still enforcing it.
pub async fn follow(following: Following, mut hints: Hints) {
    let Following {
        me,
        state_machine,
        client,
        sources,
        expelled,
    } = following;

    loop {
        let delay = match ask_around(me, &state_machine, &client, &sources).await {
            // A source hands over everything it has applied in one answer, so
            // there is nothing waiting behind a successful fetch. Asking again
            // at once would buy an empty round trip per change.
            Progress::Fetched | Progress::UpToDate => IDLE_POLL,

            // Somebody else may be reachable, or this one may come back.
            Progress::Unreachable => RETRY_DELAY,

            // The source was fine; this node's storage was not. Asking a
            // different one cannot help, so try the same thing again shortly.
            Progress::NotStored => RETRY_DELAY,

            // Nothing this loop does will fix either of these.
            Progress::NoSources => IDLE_POLL,
            Progress::Stranded => STRANDED_BACKOFF,

            // Every core node that answered says this node is not in the
            // group. Which of the two things that means depends on whether
            // this node has ever held the log.
            Progress::Refused => {
                if state_machine.membership().group_id().is_some() {
                    // It has. So it was in the group and is not now: §4.4,
                    // from the outside. Stopping is the point — a loop that
                    // kept asking would hammer every core node roughly once a
                    // second for as long as the process lived, filling their
                    // logs with refusals and doing itself no good.
                    tracing::error!(
                        "this node has been expelled from the group; it will stop following"
                    );
                    let _ = expelled.send(true);
                    return;
                }

                // It has not, so this is a node whose admission has not been
                // committed yet — or was never proposed. Waiting is right, but
                // not briskly: the answer changes when somebody admits it,
                // which is not a thing that happens within a second.
                tracing::warn!(
                    "no core node will talk to this node yet; it has to be admitted first                      with `distlib admit`"
                );
                REFUSED_BACKOFF
            }
        };
        wait_for_a_reason(delay, &mut hints, state_machine.followed_upto()).await;
    }
}

/// Waits until there is something worth asking about.
///
/// The timer is the guarantee — gossip is best-effort, and a node that missed
/// an announcement must not wait for the next change. An announcement is what
/// makes the common case prompt, and it carries an index: one that this node
/// has already reached is not a reason to do anything, so it goes back to
/// waiting rather than spending a round trip to be told it is up to date.
async fn wait_for_a_reason(delay: Duration, hints: &mut Hints, cursor: u64) {
    // A deadline rather than a fresh sleep each time round, so a stream of
    // announcements this node already has cannot hold the timer off forever.
    let deadline = Instant::now() + delay;

    loop {
        tokio::select! {
            () = tokio::time::sleep_until(deadline) => return,
            changed = hints.changed() => {
                if changed.is_err() {
                    // Nothing will announce anything again — the listener is
                    // gone, or this node never had one. The timer is all that
                    // is left, so wait it out rather than spinning.
                    tokio::time::sleep_until(deadline).await;
                    return;
                }
                match *hints.borrow_and_update() {
                    Hint::Reaches(up_to) if up_to <= cursor => {}
                    Hint::Reaches(_) | Hint::MayHaveMissed => return,
                }
            }
        }
    }
}

/// What one pass over the known sources achieved.
///
/// Named for what actually happened rather than lumped into a single failure,
/// because the right thing to do next differs for each: retry soon, retry
/// slowly, or accept that retrying will not help.
enum Progress {
    /// Entries arrived and were applied.
    Fetched,

    /// A source answered and had nothing this node has not already got.
    UpToDate,

    /// Every source that answered refused us at the allowlist.
    ///
    /// Separate from [`Progress::Unreachable`] because it is an answer rather
    /// than a silence, and the answer does not change by asking again.
    Refused,

    /// Every known source failed to answer.
    Unreachable,

    /// A source answered, and this node could not store what it sent.
    ///
    /// Its own case because it says nothing about the source: rotating away
    /// from a node that is behaving perfectly would be the wrong response to
    /// local storage failing.
    NotStored,

    /// This node is behind what the log still holds.
    ///
    /// Nothing here recovers from this — see [`Fetched::TooFarBehind`]. It
    /// needs the state itself, which no protocol serves yet.
    ///
    /// [`Fetched::TooFarBehind`]: crate::Fetched::TooFarBehind
    Stranded,

    /// There is nowhere to ask.
    ///
    /// Configuration, not the network: `[consensus] core` named nobody
    /// reachable, or named only this node.
    NoSources,
}

/// Tries each known source until one answers.
async fn ask_around(
    me: MemberId,
    state_machine: &StateMachineStore,
    client: &MemberlogClient,
    sources: &SharedSources,
) -> Progress {
    // Bandwidth and noise, not correctness: asking from zero every time would
    // still converge, because the fold refuses a replayed event — the founding
    // one as `AlreadyFounded`, the rest as proposals against a membership that
    // has moved on — and refusals are skipped. It would just re-transfer the
    // whole log every poll and log a refusal for each entry. Worth saying
    // because mutating this to zero breaks no test.
    let cursor = state_machine.followed_upto();
    let candidates = read(sources).candidates(me);

    if candidates.is_empty() {
        tracing::warn!("no core node to follow; check `[consensus] core`");
        return Progress::NoSources;
    }

    // Refusals are counted rather than acted on one at a time: one core node
    // refusing could be one core node that is wrong, and the useful question is
    // whether *everybody who answered* refused.
    let mut refusals = 0usize;
    let mut answered_otherwise = 0usize;

    for (member, addr) in candidates {
        match client.fetch(member, &addr, cursor).await {
            Ok(Fetched::Entries {
                up_to,
                events,
                source,
            }) => {
                // Take the new view of the group before applying, so that even
                // a batch that fails to store leaves us knowing where to ask.
                *sources.lock().unwrap_or_else(PoisonError::into_inner) = Sources {
                    core: source.core,
                    leader: source.leader,
                };

                if up_to <= cursor {
                    return Progress::UpToDate;
                }
                if let Err(error) = state_machine.apply_followed(up_to, &events).await {
                    tracing::error!(%error, "could not store followed entries");
                    return Progress::NotStored;
                }
                tracing::debug!(count = events.len(), up_to, from = %member, "followed the log");
                return Progress::Fetched;
            }

            // Reachable, but with nothing to give: a core node that has not
            // itself received the founding entry yet. Somebody else may have.
            Ok(Fetched::NoGroup) => {
                answered_otherwise += 1;
                tracing::debug!(from = %member, "asked a node that has no group yet");
            }

            // Unrecoverable from entries alone, and asking another core node
            // will not help — they purge at the same watermark.
            Ok(Fetched::TooFarBehind { first_available }) => {
                tracing::error!(
                    cursor,
                    first_available,
                    from = %member,
                    "this node is behind what the log still holds; it needs the membership \
                     state itself, which nothing serves yet — delete this node's data \
                     directory and rejoin, or wait for snapshot transfer"
                );
                return Progress::Stranded;
            }

            Err(error) if error.refused => {
                refusals += 1;
                tracing::debug!(%error, from = %member, "a core node will not talk to this node");
            }

            Err(error) => {
                tracing::debug!(%error, "could not reach a core node; trying another");
            }
        }
    }

    // Unreachable nodes get no vote: a core node that is down says nothing
    // about who belongs, so one that is down while the others refuse must not
    // keep this node asking forever.
    if refusals > 0 && answered_otherwise == 0 {
        return Progress::Refused;
    }

    // Every candidate was tried. `NoGroup` answers land here too: a core node
    // that has not yet received the founding entry has nothing to give, and
    // waiting is the only thing to do about it.
    Progress::Unreachable
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

    use super::*;

    /// How long a test waits before calling something "did not return".
    ///
    /// Far below the timer it is distinguishing from, so a pass means the hint
    /// decided it rather than the clock.
    const PROMPTLY: Duration = Duration::from_millis(200);
    const A_LONG_TIMER: Duration = Duration::from_secs(30);

    #[tokio::test]
    async fn an_announcement_this_node_already_has_costs_nothing() {
        // The reason the hint carries an index at all. A core node announcing
        // index 5 to a follower that has read to 10 is telling it nothing, and
        // a round trip to discover that is a round trip per announcement per
        // follower — which is the load gossip was brought in to remove.
        let (hints, mut listens) = tokio::sync::watch::channel(Hint::MayHaveMissed);
        listens.borrow_and_update();

        hints.send(Hint::Reaches(5)).unwrap();
        let waited =
            tokio::time::timeout(PROMPTLY, wait_for_a_reason(A_LONG_TIMER, &mut listens, 10)).await;

        assert!(
            waited.is_err(),
            "an index already reached must not wake the loop"
        );
    }

    #[tokio::test]
    async fn an_announcement_beyond_this_node_wakes_it() {
        let (hints, mut listens) = tokio::sync::watch::channel(Hint::MayHaveMissed);
        listens.borrow_and_update();

        hints.send(Hint::Reaches(11)).unwrap();
        tokio::time::timeout(PROMPTLY, wait_for_a_reason(A_LONG_TIMER, &mut listens, 10))
            .await
            .expect("a longer log must wake the loop rather than waiting for the timer");
    }

    #[tokio::test]
    async fn a_gap_this_node_cannot_measure_wakes_it() {
        // No index to compare, so the only safe reading is to go and look.
        let (hints, mut listens) = tokio::sync::watch::channel(Hint::Reaches(0));
        listens.borrow_and_update();

        hints.send(Hint::MayHaveMissed).unwrap();
        tokio::time::timeout(PROMPTLY, wait_for_a_reason(A_LONG_TIMER, &mut listens, 10))
            .await
            .expect("a possible gap must wake the loop");
    }

    #[tokio::test]
    async fn the_timer_still_fires_when_nothing_announces() {
        // The guarantee behind the optimisation: gossip is best-effort, so a
        // follower that hears nothing must still look.
        let (_hints, mut listens) = tokio::sync::watch::channel(Hint::Reaches(0));
        listens.borrow_and_update();

        tokio::time::timeout(
            Duration::from_secs(5),
            wait_for_a_reason(Duration::from_millis(50), &mut listens, 10),
        )
        .await
        .expect("the timer must fire even with nothing to hear");
    }

    #[tokio::test]
    async fn a_stream_of_stale_announcements_cannot_hold_the_timer_off() {
        // The deadline is fixed rather than restarted per hint, so a peer
        // announcing an index this node already has — over and over — cannot
        // keep it from its scheduled look.
        let (hints, mut listens) = tokio::sync::watch::channel(Hint::MayHaveMissed);
        listens.borrow_and_update();

        let chatter = tokio::spawn(async move {
            loop {
                if hints.send(Hint::Reaches(1)).is_err() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });

        tokio::time::timeout(
            Duration::from_secs(5),
            wait_for_a_reason(Duration::from_millis(100), &mut listens, 10),
        )
        .await
        .expect("the deadline must survive a chatty peer");
        chatter.abort();
    }
}
