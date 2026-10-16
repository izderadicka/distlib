//! This node's custody (phase 5's D1, D4, D9): which items it keeps for the
//! group, recomputed in the background once the membership has settled.
//!
//! Nothing here is stored: custody is a function of the log's members and
//! pledges and the catalogue's items, computed again at every start.

mod service;
mod settling;
mod view;

use std::sync::{Arc, Mutex, PoisonError};

use distlib_consensus::MembershipState;
use distlib_core::{CustodyItem, Event, MemberId};
use distlib_store::Store;
use tokio::{
    sync::{broadcast, watch},
    task::JoinHandle,
};

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

#[derive(Debug)]
struct Inner {
    /// `None` until the first pass after the group is known.
    view: watch::Receiver<Option<CustodyView>>,
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
        let task = tokio::spawn(service::run(me, sources, view_tx));
        Self {
            inner: Arc::new(Inner {
                view,
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
