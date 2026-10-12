//! Opt-in, bounded task timing for local diagnosis; absent from default builds.
use std::sync::{atomic::{AtomicUsize, AtomicU64, Ordering}, Arc, Mutex};
use std::time::Instant;

#[derive(Clone)]
pub struct AppendTaskTiming {
    pub position: u64,
    pub queued: Instant,
    pub started: Option<Instant>,
    pub store_finished: Option<Instant>,
    pub completed: Instant,
    pub polls: u64,
    pub wake_to_poll_ns: u64,
}

/// Each stream can have its own sink. At most `limit` records are retained.
/// Snapshot outside timed work; enabling this feature adds observation overhead.
pub struct AppendTaskDiagnostics {
    records: Mutex<Vec<AppendTaskTiming>>,
    limit: usize,
    dropped: AtomicUsize,
    scans: [AtomicU64; 6],
}
impl AppendTaskDiagnostics {
    pub fn new(limit: usize) -> Self {
        Self { records: Mutex::new(Vec::with_capacity(limit.min(4096))), limit, dropped: AtomicUsize::new(0), scans: Default::default() }
    }
    /// Aggregate stream-lifetime scans: reap calls/handles/ns, join polls/handles/ns.
    /// Durations include the local timer overhead, exclude counter updates, and
    /// overlap operation timings. Read after the stream has closed for a stable snapshot.
    pub fn completion_scans(&self) -> [u64; 6] {
        std::array::from_fn(|index| self.scans[index].load(Ordering::Relaxed))
    }
    pub(crate) fn record_scan(&self, join: bool, handles: usize, elapsed: u64) {
        let offset = if join { 3 } else { 0 };
        for (index, value) in [1, handles as u64, elapsed].into_iter().enumerate() {
            self.scans[offset + index].fetch_add(value, Ordering::Relaxed);
        }
    }
    pub fn snapshot(&self) -> Vec<AppendTaskTiming> { self.records.lock().unwrap().clone() }
    pub fn dropped(&self) -> usize { self.dropped.load(Ordering::Relaxed) }
}

pub(crate) struct TaskObservation {
    sink: Arc<AppendTaskDiagnostics>,
    timing: AppendTaskTiming,
}
impl TaskObservation {
    pub fn new(sink: Arc<AppendTaskDiagnostics>, position: u64) -> Self {
        let queued = Instant::now();
        Self { sink, timing: AppendTaskTiming { position, queued, started: None, store_finished: None, completed: queued, polls: 0, wake_to_poll_ns: 0 } }
    }
    pub async fn observe<F: std::future::Future>(&mut self, future: F) -> F::Output {
        let signaled = Arc::new(Mutex::new(None::<Instant>));
        tokio::pin!(future);
        futures::future::poll_fn(|cx| {
            self.timing.polls += 1;
            if let Some(woken) = signaled.lock().unwrap().take() {
                self.timing.wake_to_poll_ns += woken.elapsed().as_nanos() as u64;
            }
            let waker = futures::task::waker(Arc::new(WakeProbe { parent: cx.waker().clone(), signaled: signaled.clone() }));
            future.as_mut().poll(&mut std::task::Context::from_waker(&waker))
        }).await
    }
    pub fn start(&mut self) { self.timing.started = Some(Instant::now()); }
    pub fn store_finished(&mut self) { self.timing.store_finished = Some(Instant::now()); }
}
struct WakeProbe { parent: std::task::Waker, signaled: Arc<Mutex<Option<Instant>>> }
impl futures::task::ArcWake for WakeProbe {
    fn wake_by_ref(probe: &Arc<Self>) {
        probe.signaled.lock().unwrap().get_or_insert_with(Instant::now);
        probe.parent.wake_by_ref();
    }
}
impl Drop for TaskObservation {
    fn drop(&mut self) {
        self.timing.completed = Instant::now();
        let mut records = self.sink.records.lock().unwrap();
        if records.len() < self.sink.limit { records.push(self.timing.clone()); }
        else { self.sink.dropped.fetch_add(1, Ordering::Relaxed); }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_sink_records_cancellation_before_first_poll() {
        let sink = Arc::new(AppendTaskDiagnostics::new(1));
        drop(TaskObservation::new(sink.clone(), 4096));
        drop(TaskObservation::new(sink.clone(), 8192));
        let rows = sink.snapshot();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].position, 4096);
        assert!(rows[0].started.is_none() && rows[0].store_finished.is_none());
        assert!(rows[0].queued <= rows[0].completed);
        assert_eq!(sink.dropped(), 1);
        assert_eq!(sink.completion_scans(), [0; 6]);
        sink.record_scan(false, 0, 10);
        sink.record_scan(false, 3, 20);
        sink.record_scan(true, 2, 30);
        assert_eq!(sink.completion_scans(), [2, 3, 30, 1, 2, 30]);
    }
}
