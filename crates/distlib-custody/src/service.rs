//! The task: settles the membership, runs the whole pass, and follows items.

use std::time::Duration;

use distlib_consensus::MembershipState;
use distlib_core::{CustodyGroup, Event, ItemId, MemberId};
use distlib_store::Store;
use tokio::{
    sync::{broadcast::error::RecvError, watch},
    time::{Instant, sleep_until},
};

use crate::{CustodySources, settling::Settling, view::CustodyView};

pub(crate) async fn run(
    me: MemberId,
    sources: CustodySources,
    view: watch::Sender<Option<CustodyView>>,
) {
    let CustodySources {
        mut membership,
        store,
        mut events,
        mut replays,
    } = sources;
    // `None` until the group is known. The first group seen is adopted at
    // once: at a start, what the log already holds counts as settled.
    let mut settling: Option<Settling<CustodyGroup>> = None;
    let mut replays_open = true;
    loop {
        if settling.is_none() {
            settling = first_settled(&membership.borrow_and_update());
            if let Some(first) = &settling {
                whole_pass(&store, first.adopted().clone(), me, &view).await;
            }
        }
        let deadline = settling.as_ref().and_then(Settling::deadline);
        tokio::select! {
            changed = membership.changed() => {
                if changed.is_err() {
                    tracing::debug!("the membership log is gone; custody stops");
                    return;
                }
                let group = custody_group(&membership.borrow_and_update());
                if let (Some(settling), Some(group)) = (&mut settling, group) {
                    settling.change(group, Instant::now());
                }
            }
            () = sleep_until(deadline.unwrap_or_else(Instant::now)), if deadline.is_some() => {
                if let Some(group) = settling.as_mut().and_then(|settling| settling.settle(Instant::now())) {
                    tracing::info!("the membership has settled; recomputing custody");
                    whole_pass(&store, group.clone(), me, &view).await;
                }
            }
            replayed = replays.changed(), if replays_open => {
                if replayed.is_err() {
                    replays_open = false;
                } else if let Some(settling) = &settling {
                    whole_pass(&store, settling.adopted().clone(), me, &view).await;
                }
            }
            event = events.recv() => match event {
                Ok(Event::ItemAdded { item_id } | Event::ItemChanged { item_id }) => {
                    follow_item(&store, item_id, &view).await;
                }
                Ok(_) => {}
                Err(RecvError::Lagged(missed)) => {
                    tracing::warn!(missed, "custody missed item events; recomputing");
                    if let Some(settling) = &settling {
                        whole_pass(&store, settling.adopted().clone(), me, &view).await;
                    }
                }
                Err(RecvError::Closed) => {
                    tracing::error!("the event bus is gone; custody stops");
                    return;
                }
            },
        }
    }
}

/// The group as it is now, taken as settled, with the group's settle periods.
fn first_settled(membership: &MembershipState) -> Option<Settling<CustodyGroup>> {
    let group = custody_group(membership)?;
    let settings = membership.settings();
    Some(Settling::new(
        group,
        Duration::from_secs(settings.settle_secs.into()),
        Duration::from_secs(settings.max_settle_secs.into()),
    ))
}

/// The members and pledges custody is computed from, once there is a group.
fn custody_group(membership: &MembershipState) -> Option<CustodyGroup> {
    membership.group_id()?;
    let settings = membership.settings();
    Some(CustodyGroup::new(
        settings.replicas.get(),
        settings.weight_rule(),
        membership
            .members()
            .map(|member| (member.member_id, member.pledge_bytes)),
    ))
}

/// Custody of every item, off the async workers: about two seconds a million
/// items on one core.
async fn whole_pass(
    store: &Store,
    group: CustodyGroup,
    me: MemberId,
    view: &watch::Sender<Option<CustodyView>>,
) {
    let items = match store.custody_items().await {
        Ok(items) => items,
        Err(error) => {
            tracing::warn!(%error, "could not read the items to compute custody");
            return;
        }
    };
    match tokio::task::spawn_blocking(move || CustodyView::compute(group, me, &items)).await {
        Ok(computed) => {
            tracing::info!(custody = ?computed.status(), "custody computed");
            view.send_replace(Some(computed));
        }
        Err(error) => tracing::error!(%error, "the custody pass failed"),
    }
}

/// One item added or changed: whether this node keeps it, at once.
async fn follow_item(store: &Store, item_id: ItemId, view: &watch::Sender<Option<CustodyView>>) {
    if view.borrow().is_none() {
        return;
    }
    match store.item(item_id).await {
        Ok(Some(stored)) => {
            let item = stored.custody_item();
            view.send_modify(|view| {
                if let Some(view) = view {
                    view.update(&item);
                }
            });
        }
        Ok(None) => {}
        Err(error) => tracing::warn!(%item_id, %error, "could not read an item for custody"),
    }
}
