//! The task: settles the membership, runs the whole pass, and follows items.

use std::time::Duration;

use distlib_consensus::MembershipState;
use distlib_core::{CustodyGroup, Event, ItemId, MemberId};
use distlib_store::Store;
use tokio::{
    sync::{
        broadcast::{self, error::RecvError},
        watch,
    },
    time::{Instant, sleep_until},
};

use crate::{CustodyChange, CustodySources, settling::Settling, view::CustodyView};

pub(crate) async fn run(me: MemberId, sources: CustodySources, published: Published) {
    let CustodySources {
        mut membership,
        store,
        mut events,
        mut replays,
        ..
    } = sources;
    let Some(mut settling) = first_group(&mut membership).await else {
        tracing::error!("the membership log is gone before a group was founded; custody stops");
        return;
    };
    // Item events from before the group are in the first pass. That pass is
    // the one after the read model's replay, which runs at every start once
    // the group is known: a replay already finished still counts as changed.
    events = events.resubscribe();
    let mut replays_open = true;
    loop {
        let deadline = settling.deadline();
        tokio::select! {
            changed = membership.changed() => {
                if changed.is_err() {
                    tracing::error!("the membership log is gone; custody stops");
                    return;
                }
                let group = custody_group(&membership.borrow_and_update());
                settling.change(group, Instant::now());
            }
            () = sleep_until(deadline.unwrap_or_else(Instant::now)), if deadline.is_some() => {
                if let Some(group) = settling.settle(Instant::now()) {
                    tracing::info!("the membership has settled; recomputing custody");
                    whole_pass(&store, group.clone(), me, &published).await;
                }
            }
            replayed = replays.changed(), if replays_open => {
                if replayed.is_err() {
                    replays_open = false;
                } else {
                    whole_pass(&store, settling.adopted().clone(), me, &published).await;
                }
            }
            event = events.recv() => match event {
                Ok(Event::ItemAdded { item_id } | Event::ItemChanged { item_id }) => {
                    follow_item(&store, item_id, &published).await;
                }
                Ok(_) => {}
                Err(RecvError::Lagged(missed)) => {
                    tracing::warn!(missed, "custody missed item events; recomputing");
                    whole_pass(&store, settling.adopted().clone(), me, &published).await;
                }
                Err(RecvError::Closed) => {
                    tracing::error!("the event bus is gone; custody stops");
                    return;
                }
            },
        }
    }
}

/// Waits for the group to be founded, and takes the first group seen as
/// settled: at a start, what the log already holds counts as settled. `None`
/// if the log is gone first.
async fn first_group(
    membership: &mut watch::Receiver<MembershipState>,
) -> Option<Settling<CustodyGroup>> {
    loop {
        {
            let current = membership.borrow_and_update();
            if current.group_id().is_some() {
                let settings = current.settings();
                return Some(Settling::new(
                    custody_group(&current),
                    Duration::from_secs(settings.settle_secs.into()),
                    Duration::from_secs(settings.max_settle_secs.into()),
                ));
            }
        }
        membership.changed().await.ok()?;
    }
}

/// The members and pledges custody is computed from.
pub(crate) fn custody_group(membership: &MembershipState) -> CustodyGroup {
    let settings = membership.settings();
    CustodyGroup::new(
        settings.replicas.get(),
        settings.weight_rule(),
        membership
            .members()
            .map(|member| (member.member_id, member.pledge_bytes)),
    )
}

/// What the task publishes: the view, and each change to this node's items.
pub(crate) struct Published {
    pub(crate) view: watch::Sender<Option<CustodyView>>,
    pub(crate) changes: broadcast::Sender<CustodyChange>,
    pub(crate) over_pledge_items: watch::Sender<u64>,
}

impl Published {
    /// Says, for the heartbeat, how many items the view leaves over this
    /// node's pledge.
    fn tell_over_pledge(&self) {
        let items = self.view.borrow().as_ref().map_or(0, |view| {
            u64::try_from(view.status().over_pledge.items).unwrap_or(u64::MAX)
        });
        self.over_pledge_items.send_if_modified(|told| {
            let changed = *told != items;
            *told = items;
            changed
        });
    }
}

/// Custody of every item, off the async workers: about two seconds a million
/// items on one core.
async fn whole_pass(store: &Store, group: CustodyGroup, me: MemberId, published: &Published) {
    let items = match store.custody_items().await {
        Ok(items) => items,
        Err(error) => {
            tracing::error!(%error, "could not read the items to compute custody");
            return;
        }
    };
    match tokio::task::spawn_blocking(move || CustodyView::compute(group, me, &items)).await {
        Ok(computed) => {
            tracing::info!(custody = ?computed.status(), "custody computed");
            published.view.send_replace(Some(computed));
            published.tell_over_pledge();
            // Nobody listening is fine: whoever starts later reads the view.
            let _ = published.changes.send(CustodyChange::Recomputed);
        }
        Err(error) => tracing::error!(%error, "the custody pass failed"),
    }
}

/// One item added or changed: whether this node keeps it, at once.
async fn follow_item(store: &Store, item_id: ItemId, published: &Published) {
    // Before the first pass, which will read it anyway.
    if published.view.borrow().is_none() {
        return;
    }
    match store.item(item_id).await {
        Ok(Some(stored)) => {
            let item = stored.custody_item();
            let mut changes = None;
            published.view.send_modify(|view| {
                changes = view.as_mut().map(|view| view.update(&item));
            });
            published.tell_over_pledge();
            for change in changes.unwrap_or_default() {
                let _ = published.changes.send(change);
            }
        }
        Ok(None) => {}
        Err(error) => tracing::warn!(%item_id, %error, "could not read an item for custody"),
    }
}
