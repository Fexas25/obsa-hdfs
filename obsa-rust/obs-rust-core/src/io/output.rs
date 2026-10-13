use super::journal::{UploadJournal, JournalEntry};
use super::budget::{BufferBudget, BudgetBuffer};
use crate::config::{bounded_permits, ObsConfig};
use crate::error::{ObsError, ObsResult};
use crate::store::{ObjectStore, PutHeaders};
use crate::types::{CompletedPart, ObjectMeta};
use bytes::Bytes;
use futures::FutureExt;
use std::sync::Arc;
use std::task::Poll;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;

/// Bounded multipart upload. Permits are acquired before spawning, so queued
/// tasks cannot retain an unbounded number of part buffers.
pub struct ObsOutputStream {
    store: Arc<dyn ObjectStore>,
    key: String,
    part_size: usize,
    threshold: usize,
    buffer: BudgetBuffer,
    total_written: u64,
    upload_id: Option<String>,
    journal: Option<Arc<UploadJournal>>,
    journal_entry: Option<JournalEntry>,
    next_part: i32,
    tasks: Vec<JoinHandle<ObsResult<CompletedPart>>>,
    completed: Vec<CompletedPart>,
    stream_gate: Arc<Semaphore>,
    global_gate: Arc<Semaphore>,
    failure: Option<ObsError>,
    write_in_progress: bool,
    headers: PutHeaders,
    on_close: Option<Box<dyn FnOnce(&ObjectMeta) + Send>>,
    on_commit: Option<Box<dyn FnOnce(&ObjectMeta) + Send>>,
}
impl ObsOutputStream {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        config: &ObsConfig,
        key: String,
        global_gate: Arc<Semaphore>,
    ) -> Self {
        // Preserve this infallible API, but poison invalid streams before any
        // buffering or remote mutation. The fallback sizes are never used.
        let (part_size, threshold, failure) = match config.upload_buffer_sizes() {
            Ok((part_size, threshold)) => (part_size, threshold, None),
            Err(error) => (5 * 1024 * 1024, 1, Some(error)),
        };
        Self {
            buffer: BudgetBuffer::new(store.buffer_budget().unwrap_or_else(|| BufferBudget::new(config.buffer_total_bytes))),
            store,
            key,
            part_size,
            threshold,
            total_written: 0,
            upload_id: None,
            journal: None,
            journal_entry: None,
            next_part: 0,
            tasks: Vec::new(),
            completed: Vec::new(),
            stream_gate: Arc::new(Semaphore::new(bounded_permits(config.upload_active_blocks.max(1)))),
            global_gate,
            failure,
            write_in_progress: false,
            headers: PutHeaders::default(),
            on_close: None,
            on_commit: None,
        }
    }
    pub(crate) fn with_buffer_budget(mut self, budget: Arc<BufferBudget>) -> Self {
        self.buffer = BudgetBuffer::new(budget);
        self
    }
    pub(crate) fn with_journal(mut self, journal: Option<Arc<UploadJournal>>) -> Self {
        self.journal = journal;
        self
    }
    async fn clear_journal(&mut self) {
        if let Some(entry) = self.journal_entry.take() {
            if let Err(error) = entry.remove().await { tracing::warn!(%error, "MPU journal cleanup deferred"); }
        }
    }
    pub fn with_headers(mut self, headers: PutHeaders) -> Self {
        self.headers = headers;
        self
    }
    pub fn on_close(mut self, callback: impl FnOnce(&ObjectMeta) + Send + 'static) -> Self {
        self.on_close = Some(Box::new(callback));
        self
    }
    // Filesystem lifecycle hook; user callbacks must not replace its guard.
    pub(crate) fn on_commit(mut self, callback: impl FnOnce(&ObjectMeta) + Send + 'static) -> Self {
        self.on_commit = Some(Box::new(callback));
        self
    }
    fn check_write_state(&mut self) -> ObsResult<()> {
        if self.write_in_progress {
            self.failure.get_or_insert_with(|| {
                ObsError::Io("upload write was cancelled; discard this stream".into())
            });
        }
        if self.failure.is_some() {
            self.release_failed_resources();
        }
        self.failure.clone().map_or(Ok(()), Err)
    }
    fn release_failed_resources(&mut self) {
        // Keep handles and upload_id so close/Drop can drain cancellation and
        // abort the remote MPU. Clearing length alone would retain allocation.
        self.buffer.clear();
        self.completed = Vec::new();
        for task in &self.tasks {
            task.abort();
        }
    }
    pub async fn write(&mut self, data: &[u8]) -> ObsResult<usize> {
        self.check_write_state()?;
        // Reject the whole call before buffering or starting an MPU. Invalid
        // input leaves prior accepted data available for a subsequent write.
        self.total_written
            .checked_add(data.len() as u64)
            .ok_or_else(|| ObsError::InvalidArgument("upload length overflow".into()))?;
        // A dropped write future may already have consumed caller data or
        // initiated remote work. Never allow close to commit an incomplete file.
        self.write_in_progress = true;
        let result = self.write_inner(data).await;
        self.write_in_progress = false;
        if let Err(error) = &result {
            self.failure.get_or_insert_with(|| error.clone());
            self.release_failed_resources();
        }
        result
    }
    async fn write_inner(&mut self, mut data: &[u8]) -> ObsResult<usize> {
        self.reap_finished().await;
        if let Some(e) = &self.failure {
            return Err(e.clone());
        }
        let len = data.len();
        while !data.is_empty() {
            // The caller already supplies enough bytes to cross the threshold.
            // Start MPU before copying them, then buffer only up to a part.
            if self.upload_id.is_none() && data.len() >= self.threshold.saturating_sub(self.buffer.len()) {
                match self
                    .store
                    .initiate_multipart_upload_with_headers(&self.key, self.headers.clone())
                    .await
                {
                    Ok(id) => {
                        self.upload_id = Some(id);
                        if let Some(journal) = &self.journal {
                            // Persist the known ID before admitting any upload part.
                            let journal = journal.clone();
                            let key = self.key.clone();
                            let id = self.upload_id.clone().unwrap();
                            self.journal_entry = Some(tokio::task::spawn_blocking(move || journal.record(&key, &id))
                                .await.map_err(|e| ObsError::Io(format!("upload journal worker: {e}")))??);
                        }
                    },
                    Err(e) => {
                        self.failure = Some(e.clone());
                        return Err(e);
                    }
                }
            }
            let limit = if self.upload_id.is_none() {
                self.threshold
            } else {
                self.part_size
            };
            let n = (limit.saturating_sub(self.buffer.len())).min(data.len());
            let hint = self.buffer.capacity().saturating_mul(2).min(limit);
            self.buffer.append(&data[..n], hint)?;
            self.total_written += n as u64;
            data = &data[n..];
            while self.upload_id.is_some() && self.buffer.len() >= self.part_size {
                let bytes = self.buffer.split_to(self.part_size);
                if let Err(e) = self.upload(bytes).await {
                    self.failure = Some(e.clone());
                    return Err(e);
                }
            }
        }
        Ok(len)
    }
    async fn upload(&mut self, bytes: Bytes) -> ObsResult<()> {
        if self.next_part >= 10_000 {
            return Err(ObsError::InvalidArgument(
                "multipart upload exceeds 10000 parts".into(),
            ));
        }
        // Critical rule: stream permit before global permit.
        let local = self.acquire_upload_permit(self.stream_gate.clone()).await?;
        let global = self.acquire_upload_permit(self.global_gate.clone()).await?;
        // A failed upload releases its permits too. Check it before queuing more data.
        self.reap_finished().await;
        if let Some(e) = &self.failure {
            return Err(e.clone());
        }
        self.next_part += 1;
        let number = self.next_part;
        let store = self.store.clone();
        let key = self.key.clone();
        let id = self
            .upload_id
            .clone()
            .ok_or_else(|| ObsError::Io("no upload id".into()))?;
        self.tasks.push(tokio::spawn(async move {
            let (_local, _global) = (local, global);
            let etag = store.upload_part(&key, &id, number, bytes).await?;
            Ok(CompletedPart {
                part_number: number,
                etag,
            })
        }));
        Ok(())
    }
    async fn acquire_upload_permit(
        &mut self,
        gate: Arc<Semaphore>,
    ) -> ObsResult<OwnedSemaphorePermit> {
        let acquisition = gate.acquire_owned();
        tokio::pin!(acquisition);
        loop {
            if let Some(error) = &self.failure {
                return Err(error.clone());
            }
            let has_tasks = !self.tasks.is_empty();
            tokio::select! {
                // Process a ready failure before admitting another part.
                biased;
                result = self.wait_for_part(), if has_tasks => {
                    match result {
                        Ok(part) => self.completed.push(part),
                        Err(error) => { self.failure.get_or_insert(error); }
                    }
                }
                permit = &mut acquisition => {
                    return permit.map_err(|error| ObsError::Io(error.to_string()));
                }
            }
            // Keep the same acquisition future across successful completions
            // so this upload does not lose its place in the semaphore queue.
        }
    }
    async fn wait_for_part(&mut self) -> ObsResult<CompletedPart> {
        // Poll the bounded handle set without allocating a temporary list.
        // Handles remain owned by the stream if this wait is cancelled.
        let (result, index) = futures::future::poll_fn(|cx| {
            for (index, task) in self.tasks.iter_mut().enumerate() {
                if let Poll::Ready(result) = task.poll_unpin(cx) {
                    return Poll::Ready((result, index));
                }
            }
            Poll::Pending
        })
        .await;
        drop(self.tasks.swap_remove(index));
        result.map_err(|error| ObsError::Io(error.to_string()))?
    }
    async fn reap_finished(&mut self) {
        let mut index = 0;
        while index < self.tasks.len() {
            if self.tasks[index].is_finished() {
                let task = self.tasks.swap_remove(index);
                match task
                    .await
                    .map_err(|e| ObsError::Io(e.to_string()))
                    .and_then(|r| r)
                {
                    Ok(part) => self.completed.push(part),
                    Err(e) => {
                        self.failure.get_or_insert(e);
                    }
                }
            } else {
                index += 1;
            }
        }
    }
    /// Discard buffered data, cancel and drain all parts, then abort a known MPU.
    /// Never commits or invokes success callbacks. Remote cleanup errors are
    /// returned; Drop retains its best-effort fallback if this call fails or is
    /// cancelled. An upload ID lost with an initiation response is not recoverable here.
    pub async fn abort(mut self) -> ObsResult<()> {
        self.release_failed_resources();
        while !self.tasks.is_empty() {
            let _ = self.wait_for_part().await;
        }
        if let Some(id) = self.upload_id.as_deref() {
            super::abort_known_upload(self.store.as_ref(), &self.key, id).await?;
            self.upload_id = None;
            self.clear_journal().await;
        }
        Ok(())
    }
    pub async fn close(mut self) -> ObsResult<ObjectMeta> {
        let result = self.finish().await;
        if result.is_err() {
            // Keep the ID while awaiting abort. If close is cancelled here,
            // Drop must still be able to schedule cleanup. Failed aborts get
            // one best-effort fallback attempt through Drop too.
            if let Some(id) = self.upload_id.as_deref() {
                if super::abort_known_upload(self.store.as_ref(), &self.key, id).await.is_ok()
                {
                    self.upload_id = None;
                    self.clear_journal().await;
                }
            }
        }
        if let Ok(meta) = &result {
            if let Some(callback) = self.on_commit.take() {
                callback(meta);
            }
            if let Some(callback) = self.on_close.take() {
                callback(meta);
            }
        }
        result
    }
    async fn finish(&mut self) -> ObsResult<ObjectMeta> {
        // Still drain any existing upload tasks before aborting a poisoned MPU.
        let _ = self.check_write_state();
        self.reap_finished().await;
        if self.upload_id.is_none() {
            if let Some(e) = &self.failure {
                return Err(e.clone());
            }
            let mut meta = self
                .store
                .put(
                    &self.key,
                    self.buffer.take_bytes(),
                    self.headers.clone(),
                )
                .await?;
            meta.content_length = self.total_written;
            return Ok(meta);
        }
        if self.failure.is_none() && !self.buffer.is_empty() {
            let tail = self.buffer.take_bytes();
            if let Err(e) = self.upload(tail).await {
                self.failure = Some(e);
            }
        }
        let mut parts = std::mem::take(&mut self.completed);
        if self.failure.is_some() {
            for task in &self.tasks {
                task.abort();
            }
        }
        // Wait for whichever part completes first. Waiting in submission order
        // can hide a failed part behind an unrelated stalled request.
        while !self.tasks.is_empty() {
            match self.wait_for_part().await {
                Ok(part) => parts.push(part),
                Err(e) => {
                    self.failure.get_or_insert(e);
                    for task in &self.tasks {
                        task.abort();
                    }
                }
            }
        }
        if let Some(e) = &self.failure {
            return Err(e.clone());
        }
        parts.sort_by_key(|p| p.part_number);
        let mut meta = self
            .store
            .complete_multipart_upload(&self.key, self.upload_id.as_deref().unwrap(), &parts)
            .await?;
        if let Some(content_type) = &self.headers.content_type {
            meta.content_type = Some(content_type.clone());
        }
        meta.content_length = self.total_written;
        self.upload_id = None;
        self.clear_journal().await;
        Ok(meta)
    }
    /// Buffered writes become durable at close; flush never completes an MPU.
    pub async fn flush(&mut self) -> ObsResult<()> {
        self.check_write_state()?;
        self.reap_finished().await;
        self.check_write_state()
    }
}
impl Drop for ObsOutputStream {
    fn drop(&mut self) {
        let tasks = std::mem::take(&mut self.tasks);
        for task in &tasks {
            task.abort();
        }
        if let (Some(id), Ok(rt)) = (self.upload_id.take(), tokio::runtime::Handle::try_current()) {
            let store = self.store.clone();
            let key = self.key.clone();
            let entry = self.journal_entry.take();
            let journal = self.journal.take();
            rt.spawn(async move {
                for task in tasks {
                    let _ = task.await;
                }
                let _journal = journal; // retain namespace lock until cleanup exits
                if super::abort_known_upload(store.as_ref(), &key, &id).await.is_ok() {
                    if let Some(entry) = entry { let _ = entry.remove().await; }
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{CredentialChain, Credentials};
    use crate::store::ObsStore;

    #[tokio::test]
    async fn upload_length_overflow_is_rejected_before_buffering_or_initiation() {
        let config = ObsConfig {
            endpoint: "invalid host with spaces".into(),
            bucket: "bucket".into(),
            multipart_threshold: 4,
            ..Default::default()
        };
        let store = Arc::new(ObsStore::new(config.clone(), CredentialChain::new(Credentials {
            access_key: "test".into(), secret_key: "test".into(), session_token: None,
        })));
        for payload in [&b"xy"[..], &b"xyz"[..]] {
            let mut stream = ObsOutputStream::new(
                store.clone(), &config, "key".into(), Arc::new(Semaphore::new(1)),
            );
            stream.write(b"a").await.unwrap();
            // Inject the boundary without allocating an enormous object. The
            // two payloads cover buffered PUT and threshold-crossing MPU paths.
            stream.total_written = u64::MAX - 1;
            let capacity = stream.buffer.capacity();
            let error = stream.write(payload).await.unwrap_err();
            assert!(matches!(error, ObsError::InvalidArgument(ref message)
                if message.contains("upload length overflow")), "{error}");
            assert_eq!(&stream.buffer[..], b"a");
            assert_eq!(stream.buffer.capacity(), capacity);
            assert_eq!(stream.total_written, u64::MAX - 1);
            assert!(stream.upload_id.is_none());
            assert!(stream.tasks.is_empty());
            assert!(!stream.write_in_progress);
            assert!(stream.failure.is_none());
            // Invalid input does not poison the stream; the exact boundary and
            // an empty write remain valid, but another byte is rejected.
            assert_eq!(stream.write(b"b").await.unwrap(), 1);
            assert_eq!(stream.total_written, u64::MAX);
            assert_eq!(stream.write(b"").await.unwrap(), 0);
            assert!(matches!(stream.write(b"c").await, Err(ObsError::InvalidArgument(_))));
            assert_eq!(&stream.buffer[..], b"ab");
            stream.flush().await.unwrap();
        }
    }

    #[tokio::test]
    async fn crossing_threshold_does_not_copy_input_before_initiation() {
        let config = ObsConfig {
            endpoint: "invalid host with spaces".into(),
            bucket: "bucket".into(),
            multipart_threshold: 64,
            ..Default::default()
        };
        let store = Arc::new(ObsStore::new(config.clone(), CredentialChain::new(Credentials {
            access_key: "test".into(),
            secret_key: "test".into(),
            session_token: None,
        })));
        for prefix in [0, 17] {
            let mut stream = ObsOutputStream::new(
                store.clone(), &config, "key".into(), Arc::new(Semaphore::new(1)),
            );
            stream.write(&vec![1; prefix]).await.unwrap();
            let capacity = stream.buffer.capacity();
            // Invalid URI fails initiation locally, without opening a connection.
            // Inspect staging before public write performs failed-stream cleanup.
            assert!(stream.write_inner(&vec![2; 64 - prefix]).await.is_err());
            assert_eq!(stream.buffer.len(), prefix);
            assert_eq!(stream.buffer.capacity(), capacity);
            assert_eq!(stream.total_written, prefix as u64);
            assert!(stream.flush().await.is_err());
            assert_eq!(stream.buffer.len(), 0);
            assert_eq!(stream.buffer.capacity(), 0);
            assert!(stream.close().await.is_err());
        }
    }
}
