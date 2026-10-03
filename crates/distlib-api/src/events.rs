//! `GET /events`: §7.2's event stream, as server-sent events.
//!
//! **One bus, many producers, and none of them waits.** Everything that has
//! news publishes into a `tokio::sync::broadcast` with `let _ = send(..)`: the
//! send is synchronous, never awaited, and succeeds whether or not anybody is
//! listening — which is most of a node's life. A watcher that reads too slowly
//! is skipped past rather than waited for, and told so with `resync`. That is
//! the whole reason for a lossy channel here: the producers are the membership
//! log, the catalogue's projection and the downloads (3a-5), and a browser tab
//! left in the background must never be able to hold any of them up.
//!
//! Replacing the bus with something that "never drops an event" — a bounded
//! `mpsc` per watcher, say — would bring that stall straight back, and would
//! look like an improvement while doing it. Events carry ids and a page
//! refetches (D2), so a dropped event costs one refetch, not correctness.

use std::{convert::Infallible, time::Duration};

use axum::response::sse::Event as Frame;
use distlib_core::Event;
use futures_lite::{Stream, StreamExt as _, stream};
use tokio::sync::{
    broadcast::{self, error::RecvError},
    watch,
};

/// How far a watcher may fall behind before it is skipped ahead and told to
/// resync.
///
/// Generous for what flows through it: membership changes are rare, and a
/// catalogue sync that lands hundreds of items at once is exactly the case
/// where a page should refetch its list rather than replay every item.
pub const CAPACITY: usize = 256;

/// How often an idle stream says so, with a comment line.
///
/// Also how a watcher tells a quiet stream from a dead one: a connection that
/// has said nothing at all for a good deal longer than this is broken, even
/// if nothing has closed it — see [`crate::client::Events`].
pub const KEEP_ALIVE: Duration = Duration::from_secs(15);

/// A new bus.
///
/// The binary makes one and hands it to the download registry
/// ([`crate::tasks::Tasks`]), which every other producer takes a clone from —
/// so none has to reach another through its API.
pub fn bus() -> broadcast::Sender<Event> {
    broadcast::channel(CAPACITY).0
}

/// The frames one watcher reads: a comment, `first`, then everything on the
/// bus until it closes.
///
/// **The comment is for Firefox**, whose `fetch` resolves only once the body
/// has sent something, where Chromium's resolves on the headers. With nothing
/// to say, the first bytes were the keep-alive [`KEEP_ALIVE`] later, so the
/// page read "Connecting…" for fifteen seconds while it worked (found by hand
/// after phase 3, measured as 15.0 s in Firefox against 3 ms in Chromium). A
/// comment is what a keep-alive is, so every watcher already skips it.
///
/// `first` is what the watcher would have heard had it been connected sooner
/// — the downloads already running, as `GET /events` uses it.
///
/// `Lagged` is not the end of the stream — the receiver has been moved past
/// what it missed and carries on — so it becomes a `resync` frame, which a
/// page answers with the same refetch as any other event.
pub(crate) fn frames(
    first: Vec<Event>,
    receiver: broadcast::Receiver<Event>,
) -> impl Stream<Item = Result<Frame, Infallible>> {
    let opening = Frame::default().comment("connected");
    let first = std::iter::once(opening)
        .chain(first.into_iter().map(|event| frame(event.name(), &event)))
        .map(Ok);
    stream::iter(first).chain(stream::unfold(receiver, |mut receiver| async move {
        let frame = match receiver.recv().await {
            Ok(event) => frame(event.name(), &event),
            Err(RecvError::Lagged(missed)) => {
                tracing::debug!(missed, "an event watcher fell behind; telling it to resync");
                frame("resync", &serde_json::json!({ "type": "resync" }))
            }
            Err(RecvError::Closed) => return None,
        };
        Some((Ok(frame), receiver))
    }))
}

fn frame(name: &str, data: &impl serde::Serialize) -> Frame {
    Frame::default()
        .event(name)
        .json_data(data)
        .expect("an event is plain data and always serialises")
}

/// Publishes `event` whenever `changes` moves — `membership.changed` for the
/// membership, `sync.status` for the catalogue's swarm.
///
/// Never borrows the value: the events have no payload, and a borrow of a
/// `watch` is a read lock that its writer's next update — the Raft state
/// machine's, or the catalogue's pump — would have to wait behind.
pub(crate) async fn publish_whenever<T>(
    mut changes: watch::Receiver<T>,
    events: broadcast::Sender<Event>,
    event: Event,
) {
    while changes.changed().await.is_ok() {
        // Nobody watching is the ordinary case, not a failure — see the module
        // docs.
        let _ = events.send(event.clone());
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

    use std::pin::pin;

    use super::*;

    #[tokio::test]
    async fn a_watcher_hears_something_before_anything_happens() {
        let (_events, receiver) = broadcast::channel(1);
        let mut frames = pin!(frames(Vec::new(), receiver));

        // Nothing is sent, and the first frame is there anyway: a browser that
        // waits for body bytes must not be kept waiting for the keep-alive.
        let opening = tokio::time::timeout(Duration::from_millis(100), frames.next())
            .await
            .expect("not at the keep-alive")
            .unwrap()
            .unwrap();
        assert!(
            format!("{opening:?}").contains(": connected"),
            "got {opening:?}"
        );
    }

    #[tokio::test]
    async fn a_watcher_that_fell_behind_is_told_to_resync_and_carries_on() {
        let (events, receiver) = broadcast::channel(1);
        let mut frames = pin!(frames(Vec::new(), receiver));
        let _opening = frames.next().await.unwrap().unwrap();

        // Two into a channel of one: the first is overwritten before anybody
        // reads it.
        events.send(Event::MembershipChanged).unwrap();
        events.send(Event::MembershipChanged).unwrap();

        let first = format!("{:?}", frames.next().await.unwrap().unwrap());
        assert!(first.contains("resync"), "got {first}");

        // Not the end: the watcher was moved past what it missed.
        let second = format!("{:?}", frames.next().await.unwrap().unwrap());
        assert!(second.contains("membership.changed"), "got {second}");
    }

    /// Every change to the watched value is one event, and the publisher stops
    /// when the value's writer goes — which is how `sync.status` follows the
    /// catalogue's swarm.
    #[tokio::test]
    async fn a_change_is_announced_and_the_publisher_stops_with_its_source() {
        let (source, watched) = watch::channel(0_u32);
        let (events, mut heard) = broadcast::channel(4);
        let publisher = tokio::spawn(publish_whenever(watched, events, Event::SyncStatus));

        source.send_replace(1);
        let event = tokio::time::timeout(Duration::from_secs(5), heard.recv())
            .await
            .expect("the change is announced")
            .unwrap();
        assert_eq!(event, Event::SyncStatus);

        drop(source);
        tokio::time::timeout(Duration::from_secs(5), publisher)
            .await
            .expect("the publisher stops once nothing can change")
            .unwrap();
    }
}
