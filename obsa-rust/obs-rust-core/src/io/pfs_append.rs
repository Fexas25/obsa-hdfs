use super::append_coordination::{AppendCoordinator, AppendTarget};
use super::budget::{BufferBudget, BudgetBuffer};
use crate::{
    config::ObsConfig,
    error::{ObsError, ObsResult},
    store::ObjectStore,
};
use futures::FutureExt;
use std::{collections::VecDeque, sync::Arc, task::Poll};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
};

pub(crate) struct AppendBudget {
    gate: Arc<Semaphore>,
    capacity: usize,
}
impl AppendBudget {
    pub(crate) fn new(config: &ObsConfig) -> Arc<Self> {
        let capacity = config
            .pfs_append_total_bytes
            .clamp(1, u32::MAX as u64)
            .min(Semaphore::MAX_PERMITS as u64) as usize;
        Arc::new(Self {
            gate: Arc::new(Semaphore::new(capacity)),
            capacity,
        })
    }
}

struct InvalidateOnDrop {
    internal: Option<Arc<dyn Fn() + Send + Sync>>,
    user: Option<Arc<dyn Fn() + Send + Sync>>,
}
impl Drop for InvalidateOnDrop {
    fn drop(&mut self) {
        if let Some(callback) = &self.internal {
            callback();
        }
        if let Some(callback) = &self.user {
            callback();
        }
    }
}

/// Bounded positioned writes. Errors are sticky and flush always drains all tasks.
pub struct PfsAppendStream {
    #[cfg(feature = "append-diagnostics")]
    task_diagnostics: Option<Arc<super::append_diagnostics::AppendTaskDiagnostics>>,
    store: Arc<dyn ObjectStore>,
    key: String,
    coordination: Option<(Arc<AppendCoordinator>, Arc<AppendTarget>)>,
    position: u64,
    concurrency: usize,
    chunk_size: usize,
    budget: Arc<AppendBudget>,
    buffer_budget: Arc<BufferBudget>,
    write_in_progress: bool,
    pending: VecDeque<JoinHandle<ObsResult<()>>>,
    failure: Option<ObsError>,
    on_flush: Option<Arc<dyn Fn() + Send + Sync>>,
    on_invalidate: Option<Arc<dyn Fn() + Send + Sync>>,
}
impl PfsAppendStream {
    /// Next positioned write offset, including scheduled but uncommitted data.
    pub fn position(&self) -> u64 {
        self.position
    }
    pub fn new(
        store: Arc<dyn ObjectStore>,
        config: &ObsConfig,
        key: String,
        position: u64,
    ) -> Self {
        Self::with_budget(store, config, key, position, AppendBudget::new(config))
    }
    pub(crate) fn with_budget(
        store: Arc<dyn ObjectStore>,
        config: &ObsConfig,
        key: String,
        position: u64,
        budget: Arc<AppendBudget>,
    ) -> Self {
        Self {
            chunk_size: config
                .pfs_append_chunk_size
                .max(1)
                .min(budget.capacity as u64) as usize,
            budget,
            buffer_budget: store.buffer_budget().unwrap_or_else(|| BufferBudget::new(config.buffer_total_bytes)),
            coordination: None,
            #[cfg(feature = "append-diagnostics")]
            task_diagnostics: None,
            write_in_progress: false,
            store,
            key,
            position,
            concurrency: config.pfs_append_concurrency.max(1),
            pending: VecDeque::new(),
            failure: None,
            on_flush: None,
            on_invalidate: None,
        }
    }
    pub(crate) fn with_coordination(mut self, coordinator: Arc<AppendCoordinator>, target: Arc<AppendTarget>) -> Self {
        self.coordination = Some((coordinator, target));
        self
    }
    pub(crate) fn with_buffer_budget(mut self, budget: Arc<BufferBudget>) -> Self {
        self.buffer_budget = budget;
        self
    }
    #[cfg(feature = "append-diagnostics")]
    pub fn with_task_diagnostics(mut self, sink: Arc<super::append_diagnostics::AppendTaskDiagnostics>) -> Self {
        self.task_diagnostics = Some(sink);
        self
    }
    pub fn on_flush(mut self, callback: impl Fn() + Send + Sync + 'static) -> Self {
        self.on_flush = Some(Arc::new(callback));
        self
    }
    pub(crate) fn on_invalidate(mut self, callback: impl Fn() + Send + Sync + 'static) -> Self {
        self.on_invalidate = Some(Arc::new(callback));
        self
    }
    fn check_write_state(&mut self) -> ObsResult<()> {
        if let Some((_, target)) = &self.coordination {
            if let Err(error) = target.key() {
                self.failure.get_or_insert(error);
            }
        }
        if self.write_in_progress {
            self.failure.get_or_insert_with(|| {
                ObsError::Io("append write was cancelled; discard this stream".into())
            });
        }
        if self.failure.is_some() {
            self.abort_pending();
        }
        self.failure.clone().map_or(Ok(()), Err)
    }
    fn abort_pending(&self) {
        // Retain ownership for flush/close to drain; task exit releases its
        // byte lease and invalidates metadata, even while this stream is kept.
        for task in &self.pending {
            task.abort();
        }
    }
    pub async fn write(&mut self, data: &[u8]) -> ObsResult<usize> {
        self.check_write_state()?;
        if data.is_empty() {
            // Surface completed failures without waiting for unfinished writes.
            self.reap_finished().await?;
            return Ok(0);
        }
        // Validate the entire call before scheduling any positioned requests.
        self.position
            .checked_add(data.len() as u64)
            .ok_or_else(|| ObsError::InvalidArgument("append offset overflow".into()))?;
        self.write_in_progress = true;
        let result = self.write_inner(data).await;
        self.write_in_progress = false;
        if let Err(error) = &result {
            self.failure.get_or_insert_with(|| error.clone());
            self.abort_pending();
        }
        result
    }
    async fn write_inner(&mut self, data: &[u8]) -> ObsResult<usize> {
        for chunk in data.chunks(self.chunk_size) {
            self.reap_finished().await?;
            if self.pending.len() >= self.concurrency {
                self.join_one().await?;
            }
            // Reserve bytes before copying. The lease lives in the task until
            // its body is released, including failure or cancellation.
            let permit = self.acquire_budget(chunk.len() as u32).await?;
            self.reap_finished().await?;
            let store = self.store.clone();
            // Acquire before spawning: even queued requests must finish before rename.
            let (namespace_permit, key) = if let Some((coordinator, target)) = &self.coordination {
                let (permit, key) = coordinator.append(target).await?;
                (Some(permit), key)
            } else {
                (None, self.key.clone())
            };
            let position = self.position;
            let mut buffer = BudgetBuffer::new(self.buffer_budget.clone());
            buffer.append(chunk, chunk.len())?;
            let body = buffer.take_bytes();
            let invalidation = InvalidateOnDrop {
                internal: self.on_invalidate.clone(), user: self.on_flush.clone(),
            };
            #[cfg(feature = "append-diagnostics")]
            let observation = self.task_diagnostics.as_ref().map(|sink|
                super::append_diagnostics::TaskObservation::new(sink.clone(), position));
            self.pending.push_back(tokio::spawn(async move {
                // Declared first so its final timestamp follows permit release
                // and cache/user callbacks; cancellation also drops this guard.
                #[cfg(feature = "append-diagnostics")]
                let mut observation = observation;
                #[cfg(feature = "append-diagnostics")]
                if let Some(observation) = &mut observation { observation.start(); }
                // Locals drop in reverse order: after the request future/body
                // is released, return its byte lease before invoking callbacks.
                // A slow synchronous user callback must not hold other streams' budget.
                let _invalidation = invalidation;
                let _namespace_permit = namespace_permit;
                let _permit = permit;
                let request = store.posix_append(&key, position, body);
                #[cfg(feature = "append-diagnostics")]
                let result = match &mut observation {
                    Some(observation) => observation.observe(request).await,
                    None => request.await,
                };
                #[cfg(not(feature = "append-diagnostics"))]
                let result = request.await;
                #[cfg(feature = "append-diagnostics")]
                if let Some(observation) = &mut observation { observation.store_finished(); }
                result
            }));
            self.position += chunk.len() as u64;
        }
        Ok(data.len())
    }
    async fn acquire_budget(&mut self, bytes: u32) -> ObsResult<OwnedSemaphorePermit> {
        let acquisition = self.budget.gate.clone().acquire_many_owned(bytes);
        tokio::pin!(acquisition);
        loop {
            let has_tasks = !self.pending.is_empty();
            tokio::select! {
                biased;
                result = self.join_one(), if has_tasks => { result?; }
                permit = &mut acquisition => {
                    return permit.map_err(|error| ObsError::Io(error.to_string()));
                }
            }
            // Preserve queue position while processing completed requests.
        }
    }
    async fn reap_finished(&mut self) -> ObsResult<()> {
        while {
            #[cfg(feature = "append-diagnostics")]
            let started = self.task_diagnostics.as_ref().map(|_| std::time::Instant::now());
            #[cfg(feature = "append-diagnostics")]
            let mut scanned = 0;
            let finished = self.pending.iter().any(|task| {
                #[cfg(feature = "append-diagnostics")]
                { scanned += 1; }
                task.is_finished()
            });
            #[cfg(feature = "append-diagnostics")]
            if let (Some(sink), Some(started)) = (&self.task_diagnostics, started) {
                sink.record_scan(false, scanned, started.elapsed().as_nanos() as u64);
            }
            finished
        } {
            self.join_one().await?;
        }
        Ok(())
    }
    async fn join_one(&mut self) -> ObsResult<()> {
        // Completion order matters: a stalled early request must not hide a
        // later failure. Keep every handle in the stream across cancellation.
        let (result, index) = futures::future::poll_fn(|cx| {
            #[cfg(feature = "append-diagnostics")]
            let started = self.task_diagnostics.as_ref().map(|_| std::time::Instant::now());
            for (index, task) in self.pending.iter_mut().enumerate() {
                if let Poll::Ready(result) = task.poll_unpin(cx) {
                    #[cfg(feature = "append-diagnostics")]
                    if let (Some(sink), Some(started)) = (&self.task_diagnostics, started) {
                        sink.record_scan(true, index + 1, started.elapsed().as_nanos() as u64);
                    }
                    return Poll::Ready((result, index));
                }
            }
            #[cfg(feature = "append-diagnostics")]
            if let (Some(sink), Some(started)) = (&self.task_diagnostics, started) {
                sink.record_scan(true, self.pending.len(), started.elapsed().as_nanos() as u64);
            }
            Poll::Pending
        })
        .await;
        self.pending.remove(index);
        let result = result
            .map_err(|error| ObsError::Io(error.to_string()))
            .and_then(|r| r);
        if let Err(error) = result {
            self.failure.get_or_insert(error);
            self.abort_pending();
        }
        self.failure.clone().map_or(Ok(()), Err)
    }
    pub async fn flush(&mut self) -> ObsResult<()> {
        let _ = self.check_write_state();
        while !self.pending.is_empty() {
            let _ = self.join_one().await;
        }
        if let Some(callback) = &self.on_invalidate {
            callback();
        }
        if let Some(callback) = &self.on_flush {
            callback();
        }
        self.failure.clone().map_or(Ok(()), Err)
    }
    pub async fn close(mut self) -> ObsResult<()> {
        self.flush().await
    }
}
impl Drop for PfsAppendStream {
    fn drop(&mut self) {
        self.abort_pending();
        if let Some(callback) = &self.on_invalidate {
            callback();
        }
        if let Some(callback) = &self.on_flush {
            callback();
        }
    }
}
