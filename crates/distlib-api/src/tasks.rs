//! Work this node is doing on somebody's behalf, and how it is going.
//!
//! A download is the only kind today (phase 3's D4). It outlives the request
//! that started it — axum drops a handler when its client goes away, and a
//! page reload must not kill a download — so it needs a name the caller can
//! ask about afterwards, and somewhere its progress can be read from by a page
//! that was not watching when it began. That is this registry.
//!
//! **In memory, and bounded.** Nothing here survives a restart, and nothing
//! needs to: a download interrupted by one is gone either way, and its files
//! were either written or not. Finished and failed downloads are kept for a
//! while so that a caller who asks late still gets an answer, then pruned
//! oldest first; running ones are never pruned.
//!
//! **One terminal state, reached once.** [`Download::finish`] and
//! [`Download::fail`] consume the handle, so a download can end only one way,
//! and a handle dropped without either — a panic, an abort, the node shutting
//! down under it — marks it failed rather than leaving it "running" for ever
//! in every page that connects afterwards. The stored state changes before
//! the event goes out, so a page that hears `download.finished` and asks
//! `library.task` is already told `finished`.

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, Instant},
};

use distlib_core::{ContentHash, Event, ItemId, Progress, TaskId};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

/// How many finished or failed downloads are kept for `library.task`.
const KEEP_ENDED: usize = 64;

/// How often a download's progress is published, at most.
///
/// iroh-blobs reports every chunk it verifies, which for a large file is tens
/// of thousands of reports — enough to overrun the bus and put every watcher
/// into `resync`. A bar redrawn four times a second loses nothing a person can
/// see. The stored state is updated on every report regardless.
const PUBLISH_EVERY: Duration = Duration::from_millis(250);

/// The downloads this node is running or has recently run.
///
/// Cheap to clone; every clone is the same registry. Holds the node's event
/// bus as well, since what the registry records is exactly what it publishes.
#[derive(Debug, Clone)]
pub struct Tasks {
    registry: Arc<Mutex<Registry>>,
    events: broadcast::Sender<Event>,
}

#[derive(Debug, Default)]
struct Registry {
    next: u64,
    /// Ordered by id, which is the order they started in — so the first ended
    /// ones found are the oldest, which is what pruning wants.
    tasks: BTreeMap<TaskId, TaskState>,
}

/// What `library.task` answers with.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskState {
    pub task_id: TaskId,
    pub item_id: ItemId,
    pub title: Option<String>,
    #[serde(flatten)]
    pub progress: Progress,
    #[serde(flatten)]
    pub outcome: Outcome,
}

/// Where a download has got to.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum Outcome {
    Running,
    Finished { files: Vec<Downloaded> },
    Failed { error: String },
}

/// One file a finished download wrote.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Downloaded {
    pub file: ContentHash,
    pub filename: String,
    pub path: PathBuf,
    pub from: Source,
}

/// Where a downloaded file came from. A caller cannot act on it, but an
/// operator watching a download of an item half of which was already here can
/// read it — and one who expected the group to be asked can tell it was not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    /// Fetched from another member.
    Network,
    /// Already in this node's store, and written out from there.
    Store,
    /// Already at its destination, byte for byte — an earlier download of
    /// the same item wrote it — so nothing was fetched or written.
    Destination,
}

impl Tasks {
    /// A registry publishing to `events`.
    pub fn new(events: broadcast::Sender<Event>) -> Self {
        Self {
            registry: Arc::default(),
            events,
        }
    }

    /// The bus this registry publishes to, for whoever streams it.
    pub fn events(&self) -> &broadcast::Sender<Event> {
        &self.events
    }

    /// Registers a download of `files_total` files and `bytes_total` bytes of
    /// `item_id` as running.
    ///
    /// Registered here, before any work starts, so that the id a caller is
    /// handed back can be asked about at once.
    pub fn start_download(
        &self,
        item_id: ItemId,
        title: Option<String>,
        files_total: u64,
        bytes_total: u64,
    ) -> Download {
        let progress = Progress {
            bytes_total,
            files_total,
            ..Progress::default()
        };
        let mut registry = self.lock();
        registry.next += 1;
        let task_id = TaskId(registry.next);
        registry.tasks.insert(
            task_id,
            TaskState {
                task_id,
                item_id,
                title,
                progress,
                outcome: Outcome::Running,
            },
        );
        Download {
            tasks: self.clone(),
            task_id,
            item_id,
            progress,
            published: None,
            ended: false,
        }
    }

    /// One task, if this node started it and has not pruned it since.
    pub fn get(&self, task_id: TaskId) -> Option<TaskState> {
        self.lock().tasks.get(&task_id).cloned()
    }

    /// Every download still running, as the progress a page would have heard
    /// had it been watching — what a page that has just connected is sent
    /// first, so that a reload keeps its progress bars.
    pub fn running(&self) -> Vec<Event> {
        self.lock()
            .tasks
            .values()
            .filter(|task| task.outcome == Outcome::Running)
            .map(|task| Event::DownloadProgress {
                task_id: task.task_id,
                item_id: task.item_id,
                progress: task.progress,
            })
            .collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Registry> {
        self.registry.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records how far `task_id` has got.
    fn record(&self, task_id: TaskId, progress: Progress) {
        if let Some(task) = self.lock().tasks.get_mut(&task_id) {
            task.progress = progress;
        }
    }

    /// Moves `task_id` to `outcome`, and prunes the oldest ended tasks past
    /// [`KEEP_ENDED`].
    fn end(&self, task_id: TaskId, outcome: Outcome) {
        let mut registry = self.lock();
        if let Some(task) = registry.tasks.get_mut(&task_id) {
            task.outcome = outcome;
        }
        let ended: Vec<TaskId> = registry
            .tasks
            .values()
            .filter(|task| task.outcome != Outcome::Running)
            .map(|task| task.task_id)
            .collect();
        for old in ended.iter().take(ended.len().saturating_sub(KEEP_ENDED)) {
            registry.tasks.remove(old);
        }
    }

    fn publish(&self, event: Event) {
        // Nobody watching is the ordinary case — see `crate::events`.
        let _ = self.events.send(event);
    }
}

/// One running download: how its work reports progress, and how it ends.
#[derive(Debug)]
pub struct Download {
    tasks: Tasks,
    task_id: TaskId,
    item_id: ItemId,
    progress: Progress,
    published: Option<Instant>,
    ended: bool,
}

impl Download {
    /// The id the caller was handed.
    pub fn id(&self) -> TaskId {
        self.task_id
    }

    /// Records that `bytes_done` bytes of the total are here, and publishes
    /// that if the last publication was long enough ago.
    pub fn progress(&mut self, bytes_done: u64) {
        self.progress.bytes_done = bytes_done;
        self.tasks.record(self.task_id, self.progress);
        if self
            .published
            .is_none_or(|at| at.elapsed() >= PUBLISH_EVERY)
        {
            self.publish_progress();
        }
    }

    /// Records that one more file is written, `bytes_done` bytes into the
    /// download, and publishes it whatever the throttle says: a file becoming
    /// something that can be opened happens once per file, and is worth
    /// hearing about at once.
    pub fn file_written(&mut self, bytes_done: u64) {
        self.progress.bytes_done = bytes_done;
        self.progress.files_done += 1;
        self.tasks.record(self.task_id, self.progress);
        self.publish_progress();
    }

    /// Ends it as done, with the files it wrote.
    ///
    /// A last progress of the whole total goes out first, whatever was
    /// published before: throttling may have held the last one back, a file
    /// already held reports no bytes at all, and a page's bar should end full
    /// rather than wherever the last throttled report left it.
    pub fn finish(mut self, files: Vec<Downloaded>) {
        self.progress.bytes_done = self.progress.bytes_total;
        self.progress.files_done = self.progress.files_total;
        self.tasks.record(self.task_id, self.progress);
        self.publish_progress();
        let (task_id, item_id) = (self.task_id, self.item_id);
        self.end(
            Outcome::Finished { files },
            Event::DownloadFinished { task_id, item_id },
        );
    }

    /// Ends it as failed, saying why.
    pub fn fail(mut self, error: String) {
        self.failed(error);
    }

    fn publish_progress(&mut self) {
        self.published = Some(Instant::now());
        self.tasks.publish(Event::DownloadProgress {
            task_id: self.task_id,
            item_id: self.item_id,
            progress: self.progress,
        });
    }

    fn failed(&mut self, error: String) {
        let (task_id, item_id) = (self.task_id, self.item_id);
        self.end(
            Outcome::Failed { error },
            Event::DownloadFailed { task_id, item_id },
        );
    }

    /// Stores `outcome`, then publishes `event` — in that order, so a watcher
    /// that hears it and asks is told the same.
    fn end(&mut self, outcome: Outcome, event: Event) {
        self.ended = true;
        self.tasks.end(self.task_id, outcome);
        self.tasks.publish(event);
    }
}

impl Drop for Download {
    fn drop(&mut self) {
        if !self.ended {
            self.failed("interrupted before it finished".to_owned());
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

    use super::*;

    const ITEM: ItemId = ItemId::from_bytes([1; 32]);

    fn registry() -> (Tasks, broadcast::Receiver<Event>) {
        let events = crate::events::bus();
        let watching = events.subscribe();
        (Tasks::new(events), watching)
    }

    /// The progress of a download of two files and a hundred bytes.
    fn progress_of(bytes_done: u64, files_done: u64) -> Progress {
        Progress {
            bytes_done,
            bytes_total: 100,
            files_done,
            files_total: 2,
        }
    }

    /// Everything published so far.
    fn heard(watching: &mut broadcast::Receiver<Event>) -> Vec<Event> {
        std::iter::from_fn(|| watching.try_recv().ok()).collect()
    }

    #[test]
    fn a_finished_download_ends_on_a_full_bar_and_one_ending() {
        let (tasks, mut watching) = registry();
        let mut download = tasks.start_download(ITEM, None, 2, 100);
        let task_id = download.id();
        download.progress(10);
        // Inside the throttle window, so held back — the case the final
        // report is there for.
        download.progress(60);
        download.finish(Vec::new());

        let progress = |bytes_done, files_done| Event::DownloadProgress {
            task_id,
            item_id: ITEM,
            progress: Progress {
                bytes_done,
                bytes_total: 100,
                files_done,
                files_total: 2,
            },
        };
        assert_eq!(
            heard(&mut watching),
            [
                progress(10, 0),
                progress(100, 2),
                Event::DownloadFinished {
                    task_id,
                    item_id: ITEM
                },
            ]
        );
        let state = tasks.get(task_id).unwrap();
        assert_eq!(state.progress, progress_of(100, 2));
        assert_eq!(state.outcome, Outcome::Finished { files: Vec::new() });
    }

    #[test]
    fn a_written_file_is_counted_and_heard_at_once() {
        let (tasks, mut watching) = registry();
        let mut download = tasks.start_download(ITEM, None, 2, 100);
        download.progress(10);
        // Inside the throttle window: a byte count here would be held back.
        download.file_written(50);

        let heard = heard(&mut watching);
        assert_eq!(heard.len(), 2, "{heard:?}");
        assert_eq!(
            heard[1],
            Event::DownloadProgress {
                task_id: download.id(),
                item_id: ITEM,
                progress: progress_of(50, 1),
            }
        );
        assert_eq!(tasks.get(download.id()).unwrap().progress.files_done, 1);
    }

    #[test]
    fn a_download_is_not_heard_to_end_before_it_has() {
        // Whoever hears the ending and asks must be told it has ended. So
        // with the registry held, an ending cannot be recorded — and must
        // therefore not be published either.
        let (tasks, mut watching) = registry();
        let download = tasks.start_download(ITEM, None, 2, 100);
        let held = tasks.lock();
        let ending = std::thread::spawn(move || download.fail("nobody had it".to_owned()));
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            watching.try_recv(),
            Err(broadcast::error::TryRecvError::Empty),
            "published before it was recorded"
        );
        drop(held);
        ending.join().unwrap();
        assert!(matches!(
            watching.try_recv(),
            Ok(Event::DownloadFailed { .. })
        ));
    }

    #[test]
    fn progress_is_recorded_every_time_and_published_at_most_every_so_often() {
        let (tasks, mut watching) = registry();
        let mut download = tasks.start_download(ITEM, None, 2, 1_000);
        for done in 1..=500 {
            download.progress(done);
        }
        assert_eq!(
            heard(&mut watching).len(),
            1,
            "one report, not five hundred"
        );
        assert_eq!(
            tasks.get(download.id()).unwrap().progress.bytes_done,
            500,
            "a page that asks is told the latest, throttled or not"
        );
    }

    #[test]
    fn a_download_dropped_before_it_ended_is_failed_not_running_for_ever() {
        let (tasks, mut watching) = registry();
        let download = tasks.start_download(ITEM, None, 2, 100);
        let task_id = download.id();
        drop(download);

        assert_eq!(
            heard(&mut watching),
            [Event::DownloadFailed {
                task_id,
                item_id: ITEM
            }]
        );
        assert!(matches!(
            tasks.get(task_id).unwrap().outcome,
            Outcome::Failed { .. }
        ));
        assert!(tasks.running().is_empty());
    }

    #[test]
    fn a_failed_download_ends_once() {
        let (tasks, mut watching) = registry();
        let download = tasks.start_download(ITEM, None, 2, 100);
        let task_id = download.id();
        download.fail("nobody had it".to_owned());

        // Consuming `fail` drops the handle; its guard must not end it again.
        assert_eq!(
            heard(&mut watching),
            [Event::DownloadFailed {
                task_id,
                item_id: ITEM
            }]
        );
        assert_eq!(
            tasks.get(task_id).unwrap().outcome,
            Outcome::Failed {
                error: "nobody had it".to_owned()
            }
        );
    }

    #[test]
    fn only_running_downloads_are_replayed_to_a_new_watcher() {
        let (tasks, _watching) = registry();
        let mut running = tasks.start_download(ITEM, None, 2, 100);
        running.progress(40);
        tasks.start_download(ITEM, None, 2, 100).finish(Vec::new());

        assert_eq!(
            tasks.running(),
            [Event::DownloadProgress {
                task_id: running.id(),
                item_id: ITEM,
                progress: progress_of(40, 0),
            }]
        );
    }

    #[test]
    fn ended_downloads_are_pruned_oldest_first_and_running_ones_never() {
        let (tasks, _watching) = registry();
        let running = tasks.start_download(ITEM, None, 2, 100);
        let ended: Vec<TaskId> = (0..KEEP_ENDED + 3)
            .map(|_| {
                let download = tasks.start_download(ITEM, None, 2, 100);
                let task_id = download.id();
                download.finish(Vec::new());
                task_id
            })
            .collect();

        assert!(
            tasks.get(running.id()).is_some(),
            "older, but still running"
        );
        assert!(ended[..3].iter().all(|id| tasks.get(*id).is_none()));
        assert!(ended[3..].iter().all(|id| tasks.get(*id).is_some()));
    }
}
