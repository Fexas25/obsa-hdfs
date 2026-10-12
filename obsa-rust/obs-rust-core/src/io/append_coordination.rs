use crate::error::{ObsError, ObsResult};
use std::sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex, Weak};
use tokio::sync::Notify;
use std::collections::HashMap;

/// State locks never span an await or an object-store request. Reservations
/// serialize overlapping paths, while disjoint renames and appends proceed.
#[derive(Default)]
pub(crate) struct AppendCoordinator {
    state: Mutex<CoordinatorState>,
}

#[derive(Default)]
struct CoordinatorState {
    targets: Vec<TargetEntry>,
    // Built only for exact-file renames. Directory updates/compaction invalidate
    // it rather than rebuilding thousands of buckets on every directory move.
    exact_index: Option<HashMap<String, TargetSlots>>,
    cleanup_at: usize,
    queue: Vec<QueueEntry>,
}

// Most paths have one live stream. Keep that slot inline rather than allocating
// a separate Vec for every path while holding the coordinator mutex.
#[derive(Default)]
enum TargetSlots {
    #[default]
    Empty,
    One(usize),
    Many(Vec<usize>),
}
impl TargetSlots {
    fn push(&mut self, slot: usize) {
        match self {
            Self::Empty => *self = Self::One(slot),
            Self::One(first) => *self = Self::Many(vec![*first, slot]),
            Self::Many(slots) => slots.push(slot),
        }
    }
    fn as_slice(&self) -> &[usize] {
        match self {
            Self::Empty => &[],
            Self::One(slot) => std::slice::from_ref(slot),
            Self::Many(slots) => slots,
        }
    }
    fn is_empty(&self) -> bool { self.as_slice().is_empty() }
}

enum QueueEntry {
    Rename(Arc<RenameScope>),
    Access(Arc<AccessEntry>),
}

enum AccessTarget {
    Stream(Arc<AppendTarget>),
    Path(String),
}
impl AccessTarget {
    fn conflicts(&self, scope: &RenameScope) -> bool {
        match self {
            Self::Stream(target) => target.state.lock().unwrap().key.as_ref().is_ok_and(|key| scope.contains(key)),
            Self::Path(key) => scope.contains(key),
        }
    }
}

// A completion is sticky: removing a blocker before its dependent starts
// waiting must not lose the wake-up. Register before checking the state.
#[derive(Default)]
struct Completion {
    done: AtomicBool,
    changed: Notify,
}
impl Completion {
    fn complete(&self) {
        self.done.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }
    async fn wait(&self) {
        let changed = self.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if !self.done.load(Ordering::Acquire) { changed.await; }
    }
}
struct AccessEntry {
    target: AccessTarget,
    completed: Completion,
}
enum WaitFor {
    Rename(Arc<RenameScope>),
    Access(Arc<AccessEntry>),
    Active(Arc<AppendTarget>),
}
impl WaitFor {
    async fn wait(self) {
        match self {
            Self::Rename(scope) => scope.completed.wait().await,
            Self::Access(entry) => entry.completed.wait().await,
            Self::Active(target) => {
                let drained = target.drained.notified();
                tokio::pin!(drained);
                drained.as_mut().enable();
                let waiting = {
                    let mut state = target.state.lock().unwrap();
                    if state.active == 0 { false }
                    else { state.drain_waiters += 1; true }
                };
                if waiting {
                    let _subscription = DrainSubscription(&target);
                    drained.await;
                }
            }
        }
    }
}

// Only contended access allocates a queue entry. Identity follows the stream's
// current path, so an earlier rename cannot strand a waiter on an obsolete key.
struct AccessGuard {
    coordinator: Arc<AppendCoordinator>,
    target: Arc<AccessEntry>,
    queued: bool,
}
impl AccessGuard {
    fn enqueue(coordinator: &Arc<AppendCoordinator>, state: &mut CoordinatorState, target: AccessTarget) -> Self {
        let target = Arc::new(AccessEntry { target, completed: Completion::default() });
        state.queue.push(QueueEntry::Access(target.clone()));
        Self { coordinator: coordinator.clone(), target, queued: true }
    }
    fn complete(&mut self, state: &mut CoordinatorState) {
        state.queue.retain(|entry| !matches!(entry, QueueEntry::Access(target) if Arc::ptr_eq(target, &self.target)));
        self.queued = false;
    }
}
impl Drop for AccessGuard {
    fn drop(&mut self) {
        // Successful append admission transfers this completion to its permit.
        let notify = self.queued || matches!(&self.target.target, AccessTarget::Path(_));
        if self.queued {
            let coordinator = self.coordinator.clone();
            self.complete(&mut coordinator.state.lock().unwrap());
        }
        if notify { self.target.completed.complete(); }
    }
}

// Cache the path under the coordinator lock: unrelated targets require neither
// an Arc upgrade nor a target lock during rename. Retarget both copies together.
struct TargetEntry {
    key: String,
    target: Weak<AppendTarget>,
}
impl CoordinatorState {
    fn access_blocked(&self, key: &str, waiter: Option<&AccessGuard>) -> Option<WaitFor> {
        self.queue.iter().take_while(|entry| {
            !matches!((entry, waiter), (QueueEntry::Access(target), Some(waiter)) if Arc::ptr_eq(target, &waiter.target))
        }).find_map(|entry| match entry {
            QueueEntry::Rename(scope) if scope.contains(key) => Some(WaitFor::Rename(scope.clone())),
            _ => None,
        })
    }
    fn ensure_exact_index(&mut self) {
        if self.exact_index.is_none() {
            let mut index: HashMap<String, TargetSlots> = HashMap::new();
            for (slot, entry) in self.targets.iter().enumerate() {
                if entry.target.strong_count() > 0 {
                    index.entry(entry.key.clone()).or_default().push(slot);
                }
            }
            self.exact_index = Some(index);
        }
    }
    fn scoped_targets<'a>(&'a self, scope: &'a RenameScope) -> impl Iterator<Item = &'a Weak<AppendTarget>> {
        let exact = self.exact_index.as_ref().into_iter().flat_map(move |index| {
            [index.get(&scope.source), index.get(&scope.destination)].into_iter().flatten().flat_map(TargetSlots::as_slice)
        }).map(|slot| &self.targets[*slot].target).take(if scope.directory { 0 } else { usize::MAX });
        let directories = self.targets.iter().filter(move |entry| scope.contains(&entry.key))
            .map(|entry| &entry.target).take(if scope.directory { usize::MAX } else { 0 });
        exact.chain(directories)
    }
    fn prune_targets(&mut self) {
        self.exact_index = None;
        self.targets.retain(|entry| entry.target.strong_count() > 0);
        self.cleanup_at = self.targets.len().saturating_mul(2).max(64);
    }
}

struct TargetState {
    key: ObsResult<String>,
    active: usize,
    drain_waiters: usize,
}
struct DrainSubscription<'a>(&'a AppendTarget);
impl Drop for DrainSubscription<'_> {
    fn drop(&mut self) {
        self.0.state.lock().unwrap().drain_waiters -= 1;
    }
}
pub(crate) struct AppendTarget {
    state: Mutex<TargetState>,
    drained: Notify,
}
impl AppendTarget {
    pub fn key(&self) -> ObsResult<String> {
        self.state.lock().unwrap().key.clone()
    }
}

struct RenameScope {
    source: String,
    destination: String,
    directory: bool,
    completed: Completion,
}
impl RenameScope {
    fn contains(&self, key: &str) -> bool {
        matches(key, &self.source, self.directory) || matches(key, &self.destination, self.directory)
    }
    fn overlaps(&self, other: &Self) -> bool {
        self.contains(&other.source) || self.contains(&other.destination)
            || other.contains(&self.source) || other.contains(&self.destination)
    }
}

impl AppendCoordinator {
    pub async fn register(self: &Arc<Self>, key: String) -> Arc<AppendTarget> {
        let mut waiter: Option<AccessGuard> = None;
        loop {
            let blocker = {
                let mut state = self.state.lock().unwrap();
                match state.access_blocked(&key, waiter.as_ref()) {
                    None => {
                        if state.targets.len() >= state.cleanup_at { state.prune_targets(); }
                        let target = Arc::new(AppendTarget {
                            state: Mutex::new(TargetState { key: Ok(key.clone()), active: 0, drain_waiters: 0 }),
                            drained: Notify::new(),
                        });
                        let slot = state.targets.len();
                        if let Some(index) = &mut state.exact_index {
                            index.entry(key.clone()).or_default().push(slot);
                        }
                        state.targets.push(TargetEntry { key, target: Arc::downgrade(&target) });
                        if let Some(waiter) = &mut waiter { waiter.complete(&mut state); }
                        return target;
                    }
                    Some(blocker) => {
                        if waiter.is_none() {
                            waiter = Some(AccessGuard::enqueue(self, &mut state, AccessTarget::Path(key.clone())));
                        }
                        blocker
                    }
                }
            };
            blocker.wait().await;
        }
    }

    pub async fn append(self: &Arc<Self>, target: &Arc<AppendTarget>) -> ObsResult<(AppendPermit, String)> {
        let mut waiter: Option<AccessGuard> = None;
        loop {
            let blocker = {
                // Always acquire coordinator before target when both are needed.
                let mut state = self.state.lock().unwrap();
                let mut current = target.state.lock().unwrap();
                let key = current.key.as_ref().map_err(Clone::clone)?;
                match state.access_blocked(key, waiter.as_ref()) {
                    None => {
                        let key = key.clone();
                        current.active += 1;
                        let admission = waiter.as_mut().map(|waiter| {
                            waiter.complete(&mut state);
                            waiter.target.clone()
                        });
                        return Ok((AppendPermit { target: target.clone(), admission }, key));
                    }
                    Some(blocker) => {
                        if waiter.is_none() {
                            waiter = Some(AccessGuard::enqueue(self, &mut state, AccessTarget::Stream(target.clone())));
                        }
                        blocker
                    }
                }
            };
            blocker.wait().await;
        }
    }

    pub async fn rename(self: &Arc<Self>, source: &str, destination: &str, directory: bool) -> RenameGuard {
        let scope = Arc::new(RenameScope {
            source: source.into(), destination: destination.into(), directory,
            completed: Completion::default(),
        });
        self.state.lock().unwrap().queue.push(QueueEntry::Rename(scope.clone()));
        let mut guard = RenameGuard { coordinator: self.clone(), scope, armed: false };
        loop {
            let blocker = {
                let mut state = self.state.lock().unwrap();
                let earlier = state.queue.iter()
                    .take_while(|entry| !matches!(entry, QueueEntry::Rename(scope) if Arc::ptr_eq(scope, &guard.scope)))
                    .find_map(|entry| match entry {
                        QueueEntry::Rename(scope) if scope.overlaps(&guard.scope) => Some(WaitFor::Rename(scope.clone())),
                        QueueEntry::Access(entry) if entry.target.conflicts(&guard.scope) => Some(WaitFor::Access(entry.clone())),
                        _ => None,
                    });
                // A queued conflict already prevents admission; avoid scanning
                // the registry until that dependency actually completes.
                earlier.or_else(|| {
                    if !guard.scope.directory { state.ensure_exact_index(); }
                    state.scoped_targets(&guard.scope).filter_map(Weak::upgrade)
                        .find(|target| target.state.lock().unwrap().active > 0)
                        .map(WaitFor::Active)
                })
            };
            match blocker {
                Some(blocker) => blocker.wait().await,
                None => { guard.armed = true; return guard; }
            }
        }
    }
}

/// Retains the request's target until completion, even after its stream is dropped.
pub(crate) struct AppendPermit {
    target: Arc<AppendTarget>,
    admission: Option<Arc<AccessEntry>>,
}
impl Drop for AppendPermit {
    fn drop(&mut self) {
        // Admission still takes coordinator -> target. Completion needs only the
        // target lock, and only the last active request wakes a draining rename.
        let drained = {
            let mut state = self.target.state.lock().unwrap();
            state.active -= 1;
            state.active == 0 && state.drain_waiters > 0
        };
        if drained { self.target.drained.notify_waiters(); }
        if let Some(entry) = &self.admission { entry.completed.complete(); }
    }
}

pub(crate) struct RenameGuard {
    coordinator: Arc<AppendCoordinator>,
    scope: Arc<RenameScope>,
    armed: bool,
}

fn matches(key: &str, prefix: &str, directory: bool) -> bool {
    key == prefix || (directory && key.strip_prefix(prefix).is_some_and(|tail| tail.starts_with('/')))
}

impl RenameGuard {
    pub fn finish(mut self, result: &ObsResult<()>) {
        match result {
            Ok(()) => self.update(true),
            Err(error) if error.is_not_found() || error.is_conflict_with_code("FileAlreadyExists") => {},
            Err(_) => self.update(false),
        }
        self.armed = false;
    }

    fn update(&self, committed: bool) {
        let mut registry = self.coordinator.state.lock().unwrap();
        if self.scope.directory {
            // Keep the contiguous directory path; do not maintain the optional
            // file index through a bulk rename or through Vec compaction.
            registry.exact_index = None;
            registry.targets.retain_mut(|entry| self.update_entry(entry, committed));
            return;
        }
        registry.ensure_exact_index();
        // Remove both old buckets before reindexing any moved entry, so moved
        // source streams cannot be processed again as overwritten destinations.
        let index = registry.exact_index.as_mut().unwrap();
        let source = index.remove(&self.scope.source).unwrap_or_default();
        let destination = index.remove(&self.scope.destination).unwrap_or_default();
        let mut source_remaining = TargetSlots::default();
        let mut destination_remaining = TargetSlots::default();
        for slot in source.as_slice().iter().chain(destination.as_slice()).copied() {
            let entry = &mut registry.targets[slot];
            if self.update_entry(entry, committed) {
                if entry.key == self.scope.source { source_remaining.push(slot); }
                else { destination_remaining.push(slot); }
            }
        }
        let index = registry.exact_index.as_mut().unwrap();
        if !source_remaining.is_empty() { index.insert(self.scope.source.clone(), source_remaining); }
        if !destination_remaining.is_empty() { index.insert(self.scope.destination.clone(), destination_remaining); }
        // Dead slots stay in the Vec until geometric registration cleanup or a
        // directory update. Exact rename never scans/compacts unrelated slots.
    }

    fn update_entry(&self, entry: &mut TargetEntry, committed: bool) -> bool {
        if entry.target.strong_count() == 0 { return false; }
        if !self.scope.contains(&entry.key) { return true; }
        let Some(target) = entry.target.upgrade() else { return false; };
        let mut state = target.state.lock().unwrap();
        if let Ok(key) = &state.key {
            if committed && matches(key, &self.scope.source, self.scope.directory) {
                let new_key = format!("{}{}", self.scope.destination, &key[self.scope.source.len()..]);
                entry.key.clone_from(&new_key);
                state.key = Ok(new_key);
            } else if self.scope.contains(key) {
                state.key = Err(ObsError::Io("append target was replaced or rename outcome is uncertain; reopen the stream".into()));
            }
        }
        true
    }
}

impl Drop for RenameGuard {
    fn drop(&mut self) {
        if self.armed { self.update(false); }
        self.coordinator.state.lock().unwrap().queue.retain(|entry| {
            !matches!(entry, QueueEntry::Rename(scope) if Arc::ptr_eq(scope, &self.scope))
        });
        self.scope.completed.complete();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn indexed_registration_and_directory_move_invalidate_cached_slots() {
        let c = Arc::new(AppendCoordinator::default());
        let first = c.register("a/file".into()).await;
        c.rename("a/file", "b/file", false).await.finish(&Ok(()));
        assert!(c.state.lock().unwrap().exact_index.is_some());
        let second = c.register("b/file".into()).await;
        let (active, _) = c.append(&second).await.unwrap();
        let mut rename = Box::pin(c.rename("b/file", "b/new", false));
        assert!(futures::poll!(rename.as_mut()).is_pending());
        drop(active);
        rename.await.finish(&Ok(()));
        c.rename("b", "c", true).await.finish(&Ok(()));
        assert!(c.state.lock().unwrap().exact_index.is_none());
        c.rename("c/new", "last", false).await.finish(&Ok(()));
        assert_eq!(first.key().unwrap(), "last");
        assert_eq!(second.key().unwrap(), "last");
    }

    #[tokio::test]
    async fn indexed_compaction_rebuilds_shifted_slots() {
        let c = Arc::new(AppendCoordinator::default());
        let dead = c.register("dead".into()).await;
        let live = c.register("a".into()).await;
        c.rename("a", "b", false).await.finish(&Ok(()));
        drop(dead);
        c.state.lock().unwrap().prune_targets();
        assert!(c.state.lock().unwrap().exact_index.is_none());
        let other = c.register("other".into()).await;
        c.rename("b", "c", false).await.finish(&Ok(()));
        assert_eq!(live.key().unwrap(), "c");
        assert_eq!(other.key().unwrap(), "other");
    }

    #[tokio::test]
    async fn indexed_same_path_streams_move_together_and_destination_stays_poisoned() {
        let c = Arc::new(AppendCoordinator::default());
        let a = c.register("a".into()).await;
        let a2 = c.register("a".into()).await;
        let b = c.register("b".into()).await;
        let unrelated = c.register("ab".into()).await;
        c.rename("a", "b", false).await.finish(&Ok(()));
        assert!(b.key().is_err());
        assert_eq!(a.key().unwrap(), "b");
        assert_eq!(a2.key().unwrap(), "b");
        let (active, _) = c.append(&a2).await.unwrap();
        let mut next = Box::pin(c.rename("b", "c", false));
        assert!(futures::poll!(next.as_mut()).is_pending());
        drop(active);
        next.await.finish(&Ok(()));
        assert_eq!(a.key().unwrap(), "c");
        assert_eq!(a2.key().unwrap(), "c");
        assert!(b.key().is_err());
        assert_eq!(unrelated.key().unwrap(), "ab");
        assert_eq!(c.state.lock().unwrap().targets.len(), 4);
    }

    #[tokio::test]
    async fn indexed_late_same_path_registration_waits_for_all_live_streams() {
        let c = Arc::new(AppendCoordinator::default());
        let first = c.register("a".into()).await;
        c.rename("a", "b", false).await.finish(&Ok(()));
        // Add a second stream to a path that was already indexed with one slot.
        let second = c.register("b".into()).await;
        let (active, _) = c.append(&second).await.unwrap();
        let mut rename = Box::pin(c.rename("b", "c", false));
        assert!(futures::poll!(rename.as_mut()).is_pending());
        drop(active);
        rename.await.finish(&Ok(()));
        assert_eq!(first.key().unwrap(), "c");
        assert_eq!(second.key().unwrap(), "c");
        drop(second);
        c.rename("c", "d", false).await.finish(&Ok(()));
        assert_eq!(first.key().unwrap(), "d");
        let third = c.register("d".into()).await;
        c.rename("d", "e", false).await.finish(&Ok(()));
        assert_eq!(third.key().unwrap(), "e");
        assert_eq!(first.key().unwrap(), "e");
    }

    #[tokio::test]
    async fn indexed_same_key_churn_is_bounded_and_reclaims_dead_buckets() {
        let c = Arc::new(AppendCoordinator::default());
        let survivor = c.register("keep".into()).await;
        for _ in 0..2048 { drop(c.register("temporary".into()).await); }
        {
            let state = c.state.lock().unwrap();
            assert!(state.targets.len() <= 64);
        }
        c.rename("temporary", "moved", false).await.finish(&Ok(()));
        let mut state = c.state.lock().unwrap();
        assert!(!state.exact_index.as_ref().unwrap().contains_key("temporary"));
        state.prune_targets();
        assert_eq!(state.targets.len(), 1);
        assert_eq!(survivor.key().unwrap(), "keep");
    }

    #[tokio::test]
    async fn indexed_directory_collision_does_not_poison_moved_source() {
        let c = Arc::new(AppendCoordinator::default());
        let a = c.register("a/child".into()).await;
        let b = c.register("b/child".into()).await;
        let sibling = c.register("ab/child".into()).await;
        c.rename("a", "b", true).await.finish(&Ok(()));
        c.rename("b", "c", true).await.finish(&Ok(()));
        assert_eq!(a.key().unwrap(), "c/child");
        assert!(b.key().is_err());
        assert_eq!(sibling.key().unwrap(), "ab/child");
        assert_eq!(c.state.lock().unwrap().targets.len(), 3);
    }






    #[tokio::test]
    async fn targeted_wake_admission_waits_for_completion_or_cancellation() {
        use std::future::Future;
        for cancel in [false, true] {
            let c = Arc::new(AppendCoordinator::default());
            let a = c.register("a".into()).await;
            let first = c.rename("a", "b", false).await;
            let mut append = Box::pin(c.append(&a));
            assert!(futures::poll!(append.as_mut()).is_pending());
            first.finish(&Ok(()));
            let mut later = Box::pin(c.rename("b", "c", false));
            let wakes = Arc::new(WakeCount::default());
            let waker = futures::task::waker(wakes.clone());
            let mut cx = std::task::Context::from_waker(&waker);
            assert!(later.as_mut().poll(&mut cx).is_pending());
            if cancel {
                drop(append);
            } else {
                let (permit, key) = append.await.unwrap();
                assert_eq!(key, "b");
                assert_eq!(wakes.count(), 0, "admission alone must not wake the rename");
                drop(permit);
            }
            assert_eq!(wakes.count(), 1);
            later.await.finish(&Ok(()));
            assert!(c.state.lock().unwrap().queue.is_empty());
        }
    }

    #[tokio::test]
    async fn targeted_wake_only_last_active_permit_notifies() {
        use std::future::Future;
        let c = Arc::new(AppendCoordinator::default());
        let a = c.register("a".into()).await;
        let p1 = c.append(&a).await.unwrap();
        let p2 = c.append(&a).await.unwrap();
        let mut rename = Box::pin(c.rename("a", "b", false));
        let wakes = Arc::new(WakeCount::default());
        let waker = futures::task::waker(wakes.clone());
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(rename.as_mut().poll(&mut cx).is_pending());
        drop(p1);
        assert_eq!(wakes.count(), 0);
        drop(p2);
        assert_eq!(wakes.count(), 1);
        rename.await.finish(&Ok(()));
    }

    #[tokio::test]
    async fn targeted_wake_completion_before_wait_is_not_lost() {
        let c = Arc::new(AppendCoordinator::default());
        let a = c.register("a".into()).await;
        let first = c.rename("a", "b", false).await;
        let wait = WaitFor::Rename(first.scope.clone());
        first.finish(&Ok(()));
        assert!(futures::poll!(Box::pin(wait.wait())).is_ready());
        let (permit, _) = c.append(&a).await.unwrap();
        let wait = WaitFor::Active(a.clone());
        drop(permit);
        assert!(futures::poll!(Box::pin(wait.wait())).is_ready());
        let first = c.rename("b", "c", false).await;
        let mut append = Box::pin(c.append(&a));
        assert!(futures::poll!(append.as_mut()).is_pending());
        let wait = {
            let state = c.state.lock().unwrap();
            let Some(QueueEntry::Access(entry)) = state.queue.last() else { panic!("missing access"); };
            WaitFor::Access(entry.clone())
        };
        drop(append);
        assert!(futures::poll!(Box::pin(wait.wait())).is_ready());
        first.finish(&Ok(()));
    }


    #[tokio::test]
    async fn targeted_wake_cancelled_drain_releases_subscription() {
        let c = Arc::new(AppendCoordinator::default());
        let a = c.register("a".into()).await;
        let (permit, _) = c.append(&a).await.unwrap();
        let mut rename = Box::pin(c.rename("a", "b", false));
        assert!(futures::poll!(rename.as_mut()).is_pending());
        assert_eq!(a.state.lock().unwrap().drain_waiters, 1);
        drop(rename);
        assert_eq!(a.state.lock().unwrap().drain_waiters, 0);
        drop(permit);
        c.rename("a", "b", false).await.finish(&Ok(()));
    }

    #[derive(Default)]
    struct WakeCount(std::sync::atomic::AtomicUsize);
    impl futures::task::ArcWake for WakeCount {
        fn wake_by_ref(this: &Arc<Self>) {
            this.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
    impl WakeCount {
        fn count(&self) -> usize { self.0.load(std::sync::atomic::Ordering::Relaxed) }
    }

    #[tokio::test]
    async fn targeted_wake_append_ignores_unrelated_completion() {
        use std::future::Future;
        let c = Arc::new(AppendCoordinator::default());
        let a = c.register("a".into()).await;
        let b = c.register("other".into()).await;
        let first = c.rename("a", "b", false).await;
        let mut append = Box::pin(c.append(&a));
        let wakes = Arc::new(WakeCount::default());
        let waker = futures::task::waker(wakes.clone());
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(append.as_mut().poll(&mut cx).is_pending());
        drop(c.append(&b).await.unwrap());
        assert_eq!(wakes.count(), 0);
        first.finish(&Ok(()));
        assert_eq!(wakes.count(), 1);
        drop(append.await.unwrap());
    }

    #[tokio::test]
    async fn targeted_wake_rename_ignores_unrelated_drain() {
        use std::future::Future;
        let c = Arc::new(AppendCoordinator::default());
        let a = c.register("a".into()).await;
        let b = c.register("other".into()).await;
        let pa = c.append(&a).await.unwrap();
        let pb = c.append(&b).await.unwrap();
        let mut rename = Box::pin(c.rename("a", "b", false));
        let wakes = Arc::new(WakeCount::default());
        let waker = futures::task::waker(wakes.clone());
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(rename.as_mut().poll(&mut cx).is_pending());
        drop(pb);
        assert_eq!(wakes.count(), 0);
        drop(pa);
        assert_eq!(wakes.count(), 1);
        rename.await.finish(&Ok(()));
    }


    #[tokio::test]
    async fn directory_rename_updates_descendants_only_and_prunes_dropped_streams() {
        let coordinator = Arc::new(AppendCoordinator::default());
        let child = coordinator.register("dir/child/文件".into()).await;
        let sibling = coordinator.register("directory/file".into()).await;
        let replaced = coordinator.register("new/child/文件".into()).await;
        let dropped = coordinator.register("dir/dropped".into()).await;
        drop(dropped);
        coordinator.rename("dir", "new", true).await.finish(&Ok(()));
        assert_eq!(child.key().unwrap(), "new/child/文件");
        assert_eq!(sibling.key().unwrap(), "directory/file");
        assert!(replaced.key().is_err());
        assert_eq!(coordinator.state.lock().unwrap().targets.len(), 3);
        coordinator.rename("new", "again", true).await.finish(&Ok(()));
        assert_eq!(child.key().unwrap(), "again/child/文件");
    }

    #[tokio::test]
    async fn cancelled_or_uncertain_rename_poison_affected_streams() {
        for cancelled in [true, false] {
            let coordinator = Arc::new(AppendCoordinator::default());
            let source = coordinator.register("source".into()).await;
            let destination = coordinator.register("destination".into()).await;
            let unrelated = coordinator.register("other".into()).await;
            let guard = coordinator.rename("source", "destination", false).await;
            if cancelled { drop(guard); }
            else { guard.finish(&Err(ObsError::Transport("lost response".into()))); }
            assert!(source.key().is_err());
            assert!(destination.key().is_err());
            assert_eq!(unrelated.key().unwrap(), "other");
            assert!(coordinator.state.lock().unwrap().queue.is_empty());
        }
    }

    #[tokio::test]
    async fn definitive_rejection_keeps_streams_usable() {
        let coordinator = Arc::new(AppendCoordinator::default());
        let source = coordinator.register("source".into()).await;
        let destination = coordinator.register("destination".into()).await;
        coordinator.rename("source", "destination", false).await.finish(&Err(ObsError::Conflict {
            code: "FileAlreadyExists".into(), message: String::new(), request_id: String::new(),
        }));
        assert_eq!(source.key().unwrap(), "source");
        assert_eq!(destination.key().unwrap(), "destination");
    }

    #[tokio::test]
    async fn queued_rename_blocks_only_its_paths_and_cancellation_keeps_stream_usable() {
        let coordinator = Arc::new(AppendCoordinator::default());
        let source = coordinator.register("a".into()).await;
        let other = coordinator.register("ab".into()).await;
        let (active, _) = coordinator.append(&source).await.unwrap();
        let mut rename = Box::pin(coordinator.rename("a", "b", false));
        assert!(futures::poll!(rename.as_mut()).is_pending());
        let mut append = Box::pin(coordinator.append(&source));
        assert!(futures::poll!(append.as_mut()).is_pending());
        let mut registration = Box::pin(coordinator.register("b".into()));
        assert!(futures::poll!(registration.as_mut()).is_pending());
        let (unrelated, key) = coordinator.append(&other).await.unwrap();
        assert_eq!(key, "ab");
        coordinator.rename("x", "y", false).await.finish(&Ok(()));
        drop(rename);
        assert_eq!(source.key().unwrap(), "a");
        let (resumed, key) = append.await.unwrap();
        assert_eq!(key, "a");
        assert_eq!(registration.await.key().unwrap(), "b");
        drop((active, unrelated, resumed));
        assert!(coordinator.state.lock().unwrap().queue.is_empty());
    }

    #[tokio::test]
    async fn overlapping_renames_queue_while_disjoint_directory_names_proceed() {
        let coordinator = Arc::new(AppendCoordinator::default());
        let child = coordinator.register("a/file".into()).await;
        let first = coordinator.rename("a", "b", true).await;
        let mut second = Box::pin(coordinator.rename("b/file", "c", false));
        assert!(futures::poll!(second.as_mut()).is_pending());
        coordinator.rename("ab", "bb", true).await.finish(&Ok(()));
        let mut registration = Box::pin(coordinator.register("b/new".into()));
        assert!(futures::poll!(registration.as_mut()).is_pending());
        first.finish(&Ok(()));
        second.await.finish(&Ok(()));
        assert_eq!(child.key().unwrap(), "c");
        assert_eq!(registration.await.key().unwrap(), "b/new");
    }

    #[tokio::test]
    async fn destination_append_must_drain_and_permit_retains_dropped_stream() {
        let coordinator = Arc::new(AppendCoordinator::default());
        let destination = coordinator.register("b/file".into()).await;
        let (permit, _) = coordinator.append(&destination).await.unwrap();
        drop(destination);
        let mut rename = Box::pin(coordinator.rename("a", "b", true));
        assert!(futures::poll!(rename.as_mut()).is_pending());
        drop(permit);
        rename.await.finish(&Ok(()));
        assert!(coordinator.state.lock().unwrap().targets.is_empty());
    }


    #[tokio::test]
    async fn registration_churn_reclaims_expired_targets_and_retains_live_streams() {
        let coordinator = Arc::new(AppendCoordinator::default());
        let survivor = coordinator.register("live".into()).await;
        let mut held = Vec::new();
        for index in 0..128 {
            held.push(coordinator.register(format!("held/{index}")).await);
        }
        drop(held);
        for index in 0..1024 {
            drop(coordinator.register(format!("temporary/{index}")).await);
        }
        assert!(coordinator.state.lock().unwrap().targets.len() <= 64);
        coordinator.rename("live", "moved", false).await.finish(&Ok(()));
        let (permit, key) = coordinator.append(&survivor).await.unwrap();
        assert_eq!(key, "moved");
        drop(permit);
        coordinator.rename("moved", "again", false).await.finish(&Ok(()));
        assert_eq!(survivor.key().unwrap(), "again");
        let mut state = coordinator.state.lock().unwrap();
        assert!(state.targets.len() <= 64);
        state.prune_targets();
        assert_eq!(state.targets.len(), 1);
    }


    #[tokio::test]
    async fn fair_append_cannot_be_overtaken_by_later_rename() {
        let coordinator = Arc::new(AppendCoordinator::default());
        let target = coordinator.register("a".into()).await;
        let first = coordinator.rename("a", "b", false).await;
        let mut append = Box::pin(coordinator.append(&target));
        assert!(futures::poll!(append.as_mut()).is_pending());
        first.finish(&Ok(()));
        // Poll the new rename first: waking the append is not sufficient fairness.
        let mut later = Box::pin(coordinator.rename("b", "c", false));
        assert!(futures::poll!(later.as_mut()).is_pending());
        let (permit, key) = append.await.unwrap();
        assert_eq!(key, "b");
        assert!(futures::poll!(later.as_mut()).is_pending());
        drop(permit);
        later.await.finish(&Ok(()));
        assert_eq!(target.key().unwrap(), "c");
    }

    #[tokio::test]
    async fn fair_rename_keeps_priority_over_later_append() {
        let coordinator = Arc::new(AppendCoordinator::default());
        let target = coordinator.register("a".into()).await;
        let first = coordinator.rename("a", "b", false).await;
        let mut second = Box::pin(coordinator.rename("b", "c", false));
        assert!(futures::poll!(second.as_mut()).is_pending());
        let mut append = Box::pin(coordinator.append(&target));
        assert!(futures::poll!(append.as_mut()).is_pending());
        first.finish(&Ok(()));
        assert!(futures::poll!(append.as_mut()).is_pending());
        second.await.finish(&Ok(()));
        let mut third = Box::pin(coordinator.rename("c", "d", false));
        assert!(futures::poll!(third.as_mut()).is_pending());
        let (permit, key) = append.await.unwrap();
        assert_eq!(key, "c");
        drop(permit);
        third.await.finish(&Ok(()));
    }

    #[tokio::test]
    async fn fair_cancelled_append_releases_its_queue_position() {
        let coordinator = Arc::new(AppendCoordinator::default());
        let target = coordinator.register("a".into()).await;
        let first = coordinator.rename("a", "b", false).await;
        let mut append = Box::pin(coordinator.append(&target));
        assert!(futures::poll!(append.as_mut()).is_pending());
        let mut later = Box::pin(coordinator.rename("b", "c", false));
        assert!(futures::poll!(later.as_mut()).is_pending());
        first.finish(&Ok(()));
        assert!(futures::poll!(later.as_mut()).is_pending());
        drop(append);
        later.await.finish(&Ok(()));
        let (permit, key) = coordinator.append(&target).await.unwrap();
        assert_eq!(key, "c");
        drop(permit);
    }


    #[tokio::test]
    async fn fair_directory_rename_drains_all_earlier_append_waiters() {
        let coordinator = Arc::new(AppendCoordinator::default());
        let a = coordinator.register("dir/a".into()).await;
        let b = coordinator.register("dir/b".into()).await;
        let first = coordinator.rename("dir", "moved", true).await;
        let mut wa = Box::pin(coordinator.append(&a));
        let mut wb = Box::pin(coordinator.append(&b));
        assert!(futures::poll!(wa.as_mut()).is_pending());
        assert!(futures::poll!(wb.as_mut()).is_pending());
        let mut later = Box::pin(coordinator.rename("moved", "last", true));
        assert!(futures::poll!(later.as_mut()).is_pending());
        // No head-of-line blocking for another directory.
        coordinator.rename("unrelated", "other", true).await.finish(&Ok(()));
        first.finish(&Ok(()));
        assert!(futures::poll!(later.as_mut()).is_pending());
        let (pa, ka) = wa.await.unwrap();
        let (pb, kb) = wb.await.unwrap();
        assert_eq!((ka.as_str(), kb.as_str()), ("moved/a", "moved/b"));
        drop(pa);
        assert!(futures::poll!(later.as_mut()).is_pending());
        drop(pb);
        later.await.finish(&Ok(()));
        assert_eq!(a.key().unwrap(), "last/a");
        assert_eq!(b.key().unwrap(), "last/b");
    }

    #[tokio::test]
    async fn fair_registration_and_cancellation_release_queue_positions() {
        for cancelled in [false, true] {
            let coordinator = Arc::new(AppendCoordinator::default());
            let first = coordinator.rename("a", "b", false).await;
            let mut registration = Box::pin(coordinator.register("b".into()));
            assert!(futures::poll!(registration.as_mut()).is_pending());
            first.finish(&Ok(()));
            let mut later = Box::pin(coordinator.rename("b", "c", false));
            assert!(futures::poll!(later.as_mut()).is_pending());
            if cancelled {
                drop(registration);
                later.await.finish(&Ok(()));
            } else {
                let target = registration.await;
                later.await.finish(&Ok(()));
                assert_eq!(target.key().unwrap(), "c");
            }
            assert!(coordinator.state.lock().unwrap().queue.is_empty());
        }
    }

    #[tokio::test]
    async fn fair_poisoned_waiter_releases_its_queue_entry() {
        let coordinator = Arc::new(AppendCoordinator::default());
        let target = coordinator.register("a".into()).await;
        let first = coordinator.rename("a", "b", false).await;
        let mut append = Box::pin(coordinator.append(&target));
        assert!(futures::poll!(append.as_mut()).is_pending());
        drop(first); // Remote outcome unknown: the queued stream must fail.
        assert!(append.await.is_err());
        assert!(coordinator.state.lock().unwrap().queue.is_empty());
        coordinator.rename("a", "b", false).await.finish(&Ok(()));
    }

}

#[cfg(test)]
#[path = "append_benchmark.rs"]
mod benchmark;
