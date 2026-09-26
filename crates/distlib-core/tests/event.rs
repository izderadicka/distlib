//! An event's name is written twice — in `Event::name` and in its serde tag —
//! and a page sees both: the SSE `event:` field and the `type` in its data.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use distlib_core::{Event, ItemId, TaskId};

/// Every variant, once.
///
/// The match has no wildcard, so a new variant does not compile until somebody
/// has looked at this function — which is the moment to add it to the list.
fn every_event() -> Vec<Event> {
    let item_id = ItemId::from_content_hashes(&[[7; 32]]);
    let task_id = TaskId(1);
    let all = vec![
        Event::MembershipChanged,
        Event::ItemAdded { item_id },
        Event::ItemChanged { item_id },
        Event::DownloadProgress {
            task_id,
            item_id,
            done: 1,
            total: 2,
        },
        Event::DownloadFinished { task_id, item_id },
        Event::DownloadFailed { task_id, item_id },
    ];
    for event in &all {
        match event {
            Event::MembershipChanged
            | Event::ItemAdded { .. }
            | Event::ItemChanged { .. }
            | Event::DownloadProgress { .. }
            | Event::DownloadFinished { .. }
            | Event::DownloadFailed { .. } => {}
        }
    }
    all
}

#[test]
fn an_events_name_and_its_type_agree() {
    for event in every_event() {
        let data = serde_json::to_value(&event).unwrap();
        assert_eq!(
            data["type"],
            event.name(),
            "the SSE event name and the data's type must be the same string"
        );
    }
}
