//! `GET /events`: §7.2's event stream, as server-sent events.
//!
//! **One bus, many producers, and none of them waits.** Everything that has
//! news publishes into a `tokio::sync::broadcast` with `let _ = send(..)`: the
//! send is synchronous, never awaited, and succeeds whether or not anybody is
//! listening — which is most of a node's life. A watcher that reads too slowly
//! is skipped past rather than waited for, and told so with `resync`. That is
//! the whole reason for a lossy channel here: the producers are the membership
//! log and, from 3a-2, the catalogue's projection, and a browser tab left in
//! the background must never be able to hold either of them up.
//!
//! Replacing the bus with something that "never drops an event" — a bounded
//! `mpsc` per watcher, say — would bring that stall straight back, and would
//! look like an improvement while doing it. Events carry ids and a page
//! refetches (D2), so a dropped event costs one refetch, not correctness.

use std::convert::Infallible;

use axum::response::sse::Event as Frame;
use distlib_consensus::MembershipState;
use distlib_core::Event;
use futures_lite::{Stream, stream};
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

/// A new bus.
///
/// The binary makes one and hands a clone to every producer, so that no
/// producer owns it and none has to reach another through its API.
pub fn bus() -> broadcast::Sender<Event> {
    broadcast::channel(CAPACITY).0
}

/// The frames one watcher reads, until the bus closes.
///
/// `Lagged` is not the end of the stream — the receiver has been moved past
/// what it missed and carries on — so it becomes a `resync` frame, which a
/// page answers with the same refetch as any other event.
pub(crate) fn frames(
    receiver: broadcast::Receiver<Event>,
) -> impl Stream<Item = Result<Frame, Infallible>> {
    stream::unfold(receiver, |mut receiver| async move {
        let frame = match receiver.recv().await {
            Ok(event) => frame(event.name(), &event),
            Err(RecvError::Lagged(missed)) => {
                tracing::debug!(missed, "an event watcher fell behind; telling it to resync");
                frame("resync", &serde_json::json!({ "type": "resync" }))
            }
            Err(RecvError::Closed) => return None,
        };
        Some((Ok(frame), receiver))
    })
}

fn frame(name: &str, data: &impl serde::Serialize) -> Frame {
    Frame::default()
        .event(name)
        .json_data(data)
        .expect("an event is plain data and always serialises")
}

/// Publishes `membership.changed` whenever the membership moves.
///
/// Never borrows the membership: the event has no payload, and a borrow of a
/// `watch` is a read lock that the Raft state machine's next `send_replace`
/// would have to wait behind.
pub(crate) async fn publish_membership(
    mut memberships: watch::Receiver<MembershipState>,
    events: broadcast::Sender<Event>,
) {
    while memberships.changed().await.is_ok() {
        // Nobody watching is the ordinary case, not a failure — see the module
        // docs.
        let _ = events.send(Event::MembershipChanged);
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

    use std::pin::pin;

    use futures_lite::StreamExt as _;

    use super::*;

    #[tokio::test]
    async fn a_watcher_that_fell_behind_is_told_to_resync_and_carries_on() {
        let (events, receiver) = broadcast::channel(1);
        let mut frames = pin!(frames(receiver));

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
}
