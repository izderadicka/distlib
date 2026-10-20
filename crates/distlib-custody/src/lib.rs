//! This node's custody (phase 5's D1, D4, D9): which items it keeps for the
//! group, recomputed in the background once the membership has settled.
//!
//! Nothing here is stored: custody is a function of the log's members and
//! pledges and the catalogue's items, computed again at every start.

mod pins;
mod service;
mod settling;
mod storage;
mod view;

use std::{
    collections::HashSet,
    sync::{Arc, Mutex, PoisonError},
};

use distlib_consensus::MembershipState;
use distlib_core::{CustodyItem, Event, ItemId, MemberId};
use distlib_store::Store;
use tokio::{
    sync::{broadcast, watch},
    task::JoinHandle,
};

pub use pins::{PinCounts, PinManager, PinSources};
use service::Published;
pub use storage::{IfShort, Room, Storage, StorageError};
pub use view::CustodyStatus;
use view::CustodyView;

/// What custody is computed from.
#[derive(Debug)]
pub struct CustodySources {
    pub membership: watch::Receiver<MembershipState>,
    /// The items, with their creators and sizes.
    pub store: Store,
    /// Where items added or changed are announced.
    pub events: broadcast::Receiver<Event>,
    /// Bumped by each replay of the read model, which announces nothing.
    pub replays: watch::Receiver<u64>,
}

/// This node's custody, kept up to date by a background task.
#[derive(Debug, Clone)]
pub struct Custody {
    inner: Arc<Inner>,
}

/// A change to which items this node keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CustodyChange {
    /// A whole pass: read [`Custody::my_items`] again.
    Recomputed,
    /// One item added or changed, and whether this node keeps it.
    Item { item: ItemId, mine: bool },
}

/// How many changes may wait for a slow listener before it has to read every
/// item again.
const CHANGES_WAITING: usize = 1024;

#[derive(Debug)]
struct Inner {
    /// `None` until the first pass after the group is known.
    view: watch::Receiver<Option<CustodyView>>,
    changes: broadcast::Sender<CustodyChange>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(task) = self.task.get_mut().unwrap_or_else(PoisonError::into_inner) {
            task.abort();
        }
    }
}

impl Custody {
    /// Starts following `me`'s custody.
    pub fn start(me: MemberId, sources: CustodySources) -> Self {
        let (view_tx, view) = watch::channel(None);
        let (changes, _) = broadcast::channel(CHANGES_WAITING);
        let published = Published {
            view: view_tx,
            changes: changes.clone(),
        };
        let task = tokio::spawn(service::run(me, sources, published));
        Self {
            inner: Arc::new(Inner {
                view,
                changes,
                task: Mutex::new(Some(task)),
            }),
        }
    }

    /// `item`'s custodians, highest score first, under the settled
    /// membership. `None` until custody has been computed.
    pub fn custodians(&self, item: &CustodyItem) -> Option<Vec<MemberId>> {
        self.inner
            .view
            .borrow()
            .as_ref()
            .map(|view| view.custodians(item))
    }

    /// The items this node keeps. `None` until custody has been computed.
    pub(crate) fn my_items(&self) -> Option<HashSet<ItemId>> {
        self.inner.view.borrow().as_ref().map(CustodyView::my_items)
    }

    /// Whether this node keeps `item`. `false` until custody has been
    /// computed.
    pub(crate) fn keeps(&self, item: ItemId) -> bool {
        self.inner
            .view
            .borrow()
            .as_ref()
            .is_some_and(|view| view.is_mine(item))
    }

    /// Each change to which items this node keeps, from now on.
    pub(crate) fn changes(&self) -> broadcast::Receiver<CustodyChange> {
        self.inner.changes.subscribe()
    }

    /// This node's custody and the group's load. `None` until computed.
    pub fn status(&self) -> Option<CustodyStatus> {
        self.inner.view.borrow().as_ref().map(CustodyView::status)
    }

    /// Stops the task. Dropping the last handle does the same.
    pub fn shutdown(&self) {
        let task = self
            .inner
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(task) = task {
            task.abort();
        }
    }
}
