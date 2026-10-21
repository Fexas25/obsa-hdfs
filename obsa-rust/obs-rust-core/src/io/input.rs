use super::budget::{BufferBudget, BudgetBuffer};
use crate::{
    config::{bounded_permits, ObsConfig},
    error::{ObsError, ObsResult},
    store::ObjectStore,
    types::InodeHint,
};
use bytes::Bytes;
use futures::StreamExt;
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

type PendingChunk = JoinHandle<ObsResult<(Bytes, tokio::sync::OwnedSemaphorePermit)>>;
#[derive(Clone, Copy)]
struct ReadCheckpoint {
    position: u64,
    sequential_chunks: u32,
    window: usize,
}

pub struct ObsInputStream {
    store: Arc<dyn ObjectStore>,
    key: String,
    length: u64,
    position: u64,
    read_start: Option<ReadCheckpoint>,
    chunk_size: u64,
    max_window: usize,
    window: usize,
    sequential_chunks: u32,
    pending: HashMap<u64, PendingChunk>,
    gate: Arc<Semaphore>,
    buffer_budget: Arc<BufferBudget>,
    hint: Option<InodeHint>,
    current: Option<(u64, Bytes)>,
    revision: Option<Arc<OnceLock<String>>>,
    failure: Option<ObsError>,
}
impl ObsInputStream {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        config: &ObsConfig,
        key: String,
        length: u64,
        hint: Option<InodeHint>,
    ) -> Self {
        Self::with_gate(
            store,
            config,
            key,
            length,
            hint,
            Arc::new(Semaphore::new(bounded_permits(config.readahead_total_chunks))),
        )
    }
    pub fn with_gate(
        store: Arc<dyn ObjectStore>,
        config: &ObsConfig,
        key: String,
        length: u64,
        hint: Option<InodeHint>,
        gate: Arc<Semaphore>,
    ) -> Self {
        let revision =
            (config.read_change_detection && !store.is_pfs()).then(|| Arc::new(OnceLock::new()));
        Self {
            buffer_budget: store.buffer_budget().unwrap_or_else(|| BufferBudget::new(config.buffer_total_bytes)),
            store,
            key,
            length,
            position: 0,
            read_start: None,
            chunk_size: config.readahead_chunk_size.max(1),
            max_window: config.readahead_max_chunks,
            window: 1,
            sequential_chunks: 0,
            pending: HashMap::new(),
            gate,
            hint,
            current: None,
            revision,
            failure: None,
        }
    }
    pub(crate) fn with_buffer_budget(mut self, budget: Arc<BufferBudget>) -> Self {
        self.buffer_budget = budget;
        self
    }
    /// Seed from an existing HEAD/LIST result without adding a metadata request.
    pub fn with_etag(self, etag: Option<String>) -> Self {
        if let (Some(revision), Some(etag)) = (&self.revision, etag) {
            let etag = etag.trim_matches('"');
            if !etag.is_empty() {
                let _ = revision.set(etag.to_string());
            }
        }
        self
    }
    /// Length captured when this stream was opened; no metadata request is made.
    pub fn content_length(&self) -> u64 {
        self.length
    }
    pub fn position(&self) -> u64 {
        self.read_start
            .map_or(self.position, |start| start.position)
    }
    /// Skip within the opened length without fetching discarded bytes.
    /// Reuse seek's cancelled-read restoration and prefetch cleanup.
    pub fn skip(&mut self, amount: u64) -> ObsResult<u64> {
        let position = self.position();
        let skipped = amount.min(self.length.saturating_sub(position));
        self.seek(position + skipped)?;
        Ok(skipped)
    }
    fn restore_cancelled_read(&mut self) {
        if let Some(start) = self.read_start.take() {
            self.position = start.position;
            self.sequential_chunks = start.sequential_chunks;
            self.window = start.window;
        }
    }
    pub fn seek(&mut self, position: u64) -> ObsResult<()> {
        self.restore_cancelled_read();
        if position != self.position {
            self.sequential_chunks = 0;
            self.window = 1;
            // Reuse the resident range for nearby seeks (e.g. footer parsing).
            if self.current.as_ref().map(|c| c.0) != Some(position / self.chunk_size) {
                self.current = None;
            }
            // Keep the target range, including its permit and version check.
            // Cancelling it would discard ready bytes or repeat an in-flight GET.
            let target = (position < self.length).then_some(position / self.chunk_size);
            self.pending.retain(|index, task| {
                if Some(*index) == target {
                    true
                } else {
                    task.abort();
                    false
                }
            });
        }
        self.position = position;
        Ok(())
    }
    pub async fn read(&mut self, buf: &mut [u8]) -> ObsResult<usize> {
        self.restore_cancelled_read();
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        self.read_start = Some(ReadCheckpoint {
            position: self.position,
            sequential_chunks: self.sequential_chunks,
            window: self.window,
        });
        let result = self.read_inner(buf).await;
        // An Err reports no bytes consumed to JNI/Java. Keep the native cursor
        // aligned too, even if earlier chunks were already copied internally.
        if result.is_err() {
            self.restore_cancelled_read();
            // A caller may retain the stream without retrying. Release both
            // resident bytes and speculative requests on every returned error.
            // Cancellation of the read future still keeps resumable prefetch.
            self.current = None;
            for (_, task) in self.pending.drain() {
                task.abort();
            }
        } else {
            self.read_start = None;
        }
        if let Err(error @ (ObsError::RemoteFileChanged(_) | ObsError::MissingVersion(_))) = &result
        {
            self.failure = Some(error.clone());
        }
        result
    }
    async fn read_inner(&mut self, buf: &mut [u8]) -> ObsResult<usize> {
        if buf.is_empty() || self.position >= self.length {
            return Ok(0);
        }
        let mut read = 0;
        while read < buf.len() && self.position < self.length {
            let index = self.position / self.chunk_size;
            if self.current.as_ref().map(|c| c.0) != Some(index) {
                let had_current = self.current.take().is_some();
                let bytes = if let Some(task) = self.pending.get_mut(&index) {
                    let result = task.await;
                    // Keep ownership across await so cancelling read does not
                    // detach a prefetch task or force a duplicate GET on retry.
                    self.pending.remove(&index);
                    let (bytes, _permit) = result.map_err(|e| ObsError::Io(e.to_string()))??;
                    bytes
                } else {
                    let expected = self.chunk_size.min(self.length - index * self.chunk_size);
                    drop(self.buffer_budget.reserve(expected)?);
                    let bytes = fetch(
                        self.store.as_ref(),
                        &self.key,
                        index,
                        self.chunk_size,
                        self.length,
                        self.hint.as_ref(),
                        self.revision.as_deref(),
                        self.buffer_budget.clone(),
                    )
                    .await?;
                    bytes
                };
                if had_current && self.sequential_chunks >= 3 {
                    self.window = self.window.saturating_mul(2).min(self.max_window);
                }
                self.current = Some((index, bytes));
            }
            let bytes = &self.current.as_ref().unwrap().1;
            let offset = usize::try_from(self.position % self.chunk_size).map_err(|_| {
                ObsError::InvalidArgument("read offset exceeds addressable memory".into())
            })?;
            let available = bytes.len().saturating_sub(offset);
            if available == 0 {
                return Err(ObsError::Io("short ranged GET before EOF".into()));
            }
            let count =
                bounded_read_count(available, buf.len() - read, self.length - self.position);
            buf[read..read + count].copy_from_slice(&bytes[offset..offset + count]);
            read += count;
            self.position += count as u64;
            // Refill inside the loop: a single large read must pipeline its
            // ranges too, rather than waiting until the caller's buffer is full.
            // Tiny reads within a resident block are not evidence that more
            // blocks will be consumed. Count completed ranges, not read calls.
            if offset + count == bytes.len() {
                self.sequential_chunks = self.sequential_chunks.saturating_add(1);
                if self.sequential_chunks >= 3 {
                    self.prefetch();
                }
            }
        }
        Ok(read)
    }
    fn prefetch(&mut self) {
        let first = self.position / self.chunk_size;
        for index in first..=first.saturating_add(self.window as u64) {
            if self.pending.len() >= self.max_window
                || index.saturating_mul(self.chunk_size) >= self.length
            {
                break;
            }
            if self.pending.contains_key(&index)
                || self.current.as_ref().map(|c| c.0) == Some(index)
            {
                continue;
            }
            let Ok(permit) = self.gate.clone().try_acquire_owned() else {
                break;
            };
            let expected = self.chunk_size.min(self.length - index * self.chunk_size);
            let Ok(admission) = self.buffer_budget.reserve(expected) else { break; };
            drop(admission);
            let budget = self.buffer_budget.clone();
            let store = self.store.clone();
            let key = self.key.clone();
            let hint = self.hint.clone();
            let revision = self.revision.clone();
            let size = self.chunk_size;
            let length = self.length;
            self.pending.insert(
                index,
                tokio::spawn(async move {
                    let bytes = fetch(
                        store.as_ref(),
                        &key,
                        index,
                        size,
                        length,
                        hint.as_ref(),
                        revision.as_deref(),
                        budget,
                    )
                    .await?;
                    // Keep the permit until cached data is consumed, bounding ready data too.
                    Ok((bytes, permit))
                }),
            );
        }
    }
}
impl Drop for ObsInputStream {
    fn drop(&mut self) {
        for (_, task) in self.pending.drain() {
            task.abort();
        }
    }
}

fn bounded_read_count(available: usize, requested: usize, remaining: u64) -> usize {
    // The result is bounded by two addressable buffers before narrowing. In
    // particular, a 4 GiB remainder must not become zero on 32-bit platforms.
    (available.min(requested) as u64).min(remaining) as usize
}

/// Stateless positioned reads over the same object snapshot as its stream.
///
/// Reads never move the stream cursor or disturb its prefetch state, so
/// concurrent callers (Hadoop `PositionedReadable`) need no stream lock. The
/// opened length bounds every read, and object buckets share the stream's
/// pinned ETag: a revision change fails the read instead of mixing versions.
#[derive(Clone)]
pub struct PositionedReader {
    store: Arc<dyn ObjectStore>,
    key: String,
    length: u64,
    chunk_size: u64,
    parallelism: usize,
    hint: Option<InodeHint>,
    revision: Option<Arc<OnceLock<String>>>,
    buffer_budget: Arc<BufferBudget>,
}
impl ObsInputStream {
    pub fn positioned_reader(&self) -> PositionedReader {
        PositionedReader {
            store: self.store.clone(),
            key: self.key.clone(),
            length: self.length,
            chunk_size: self.chunk_size,
            parallelism: self.max_window.max(1),
            hint: self.hint.clone(),
            revision: self.revision.clone(),
            buffer_budget: self.buffer_budget.clone(),
        }
    }
    /// Serve a positioned read from the resident range without any request.
    /// Returns None unless the whole (EOF-clipped) request is resident; the
    /// cursor and prefetch state are left untouched either way.
    pub fn read_resident_at(&self, position: u64, buf: &mut [u8]) -> Option<usize> {
        if self.failure.is_some() {
            return None;
        }
        if buf.is_empty() || position >= self.length {
            return Some(0);
        }
        let (index, bytes) = self.current.as_ref()?;
        let start = index.checked_mul(self.chunk_size)?;
        let offset = usize::try_from(position.checked_sub(start)?).ok()?;
        let count = bounded_read_count(usize::MAX, buf.len(), self.length - position);
        let resident = bytes.get(offset..offset.checked_add(count)?)?;
        buf[..count].copy_from_slice(resident);
        Some(count)
    }
}
impl PositionedReader {
    pub fn content_length(&self) -> u64 {
        self.length
    }
    /// Read up to `buf.len()` bytes at `position`; 0 means EOF (or empty buf).
    /// Large requests are split into chunk-sized ranges fetched concurrently,
    /// at most `readahead_max_chunks` in flight, and copied in order. On error
    /// the buffer may be partially overwritten and no byte count is reported.
    pub async fn read_at(&self, position: u64, buf: &mut [u8]) -> ObsResult<usize> {
        if buf.is_empty() || position >= self.length {
            return Ok(0);
        }
        let total = (buf.len() as u64).min(self.length - position);
        let ranges = (0..total.div_ceil(self.chunk_size)).map(|n| {
            let start = position + n * self.chunk_size;
            (start, start.saturating_add(self.chunk_size).min(position + total) - 1)
        });
        let mut fetched = futures::stream::iter(ranges)
            .map(|(start, end)| async move {
                drop(self.buffer_budget.reserve(end - start + 1)?);
                fetch_range(
                    self.store.as_ref(),
                    &self.key,
                    start,
                    end,
                    self.hint.as_ref(),
                    self.revision.as_deref(),
                    self.buffer_budget.clone(),
                )
                .await
            })
            .buffered(self.parallelism);
        let mut copied = 0;
        while let Some(bytes) = fetched.next().await {
            let bytes = bytes?;
            buf[copied..copied + bytes.len()].copy_from_slice(&bytes);
            copied += bytes.len();
        }
        Ok(copied)
    }
    /// Vectored read: fill every `(position, buffer)` completely.
    ///
    /// Ranges are sorted and coalesced when the gap to the previous span is at
    /// most `merge_gap` and the merged span stays within `max_merged` bytes
    /// (Hadoop's minSeekForVectorReads / maxReadSizeForVectorReads). Overlapping
    /// and duplicate ranges are allowed. Merged spans are fetched as chunk-sized
    /// ranged GETs, at most `readahead_max_chunks` in flight across all spans,
    /// and scattered into the buffers as they complete. Any range extending
    /// past the opened length fails the whole call before any request is sent;
    /// on error, buffers may be partially written.
    pub async fn read_ranges(
        &self,
        ranges: &mut [(u64, &mut [u8])],
        merge_gap: u64,
        max_merged: u64,
    ) -> ObsResult<()> {
        let mut order: Vec<usize> = Vec::with_capacity(ranges.len());
        for (index, (position, buf)) in ranges.iter().enumerate() {
            let end = position.checked_add(buf.len() as u64);
            if end.map_or(true, |end| end > self.length) {
                return Err(ObsError::Io(format!(
                    "vectored range {position}+{} exceeds length {}", buf.len(), self.length)));
            }
            if !buf.is_empty() {
                order.push(index);
            }
        }
        order.sort_by_key(|&index| ranges[index].0);
        // Spans as (start, end exclusive, member range indices).
        let mut spans: Vec<(u64, u64, Vec<usize>)> = Vec::new();
        for index in order {
            let start = ranges[index].0;
            let end = start + ranges[index].1.len() as u64;
            match spans.last_mut() {
                Some(span) if start <= span.1.saturating_add(merge_gap)
                    && end.max(span.1) - span.0 <= max_merged => {
                    span.1 = span.1.max(end);
                    span.2.push(index);
                }
                _ => spans.push((start, end, vec![index])),
            }
        }
        let pieces = spans.iter().enumerate().flat_map(|(span, (start, end, _))| {
            let (start, end) = (*start, *end);
            (0..(end - start).div_ceil(self.chunk_size)).map(move |n| {
                let piece = start + n * self.chunk_size;
                (span, piece, piece.saturating_add(self.chunk_size).min(end) - 1)
            })
        });
        let mut fetched = futures::stream::iter(pieces)
            .map(|(span, start, end)| async move {
                drop(self.buffer_budget.reserve(end - start + 1)?);
                let bytes = fetch_range(
                    self.store.as_ref(),
                    &self.key,
                    start,
                    end,
                    self.hint.as_ref(),
                    self.revision.as_deref(),
                    self.buffer_budget.clone(),
                )
                .await?;
                Ok::<_, ObsError>((span, start, bytes))
            })
            .buffer_unordered(self.parallelism);
        while let Some(result) = fetched.next().await {
            let (span, piece_start, bytes) = result?;
            let piece_end = piece_start + bytes.len() as u64;
            for &index in &spans[span].2 {
                let (position, buf) = &mut ranges[index];
                let start = piece_start.max(*position);
                let end = piece_end.min(*position + buf.len() as u64);
                if start < end {
                    let target = (start - *position) as usize;
                    let source = (start - piece_start) as usize;
                    let count = (end - start) as usize;
                    buf[target..target + count].copy_from_slice(&bytes[source..source + count]);
                }
            }
        }
        Ok(())
    }
}

async fn fetch(
    store: &dyn ObjectStore,
    key: &str,
    index: u64,
    size: u64,
    length: u64,
    hint: Option<&InodeHint>,
    revision: Option<&OnceLock<String>>,
    budget: Arc<BufferBudget>,
) -> ObsResult<Bytes> {
    let start = index * size;
    let end = start.saturating_add(size - 1).min(length - 1);
    fetch_range(store, key, start, end, hint, revision, budget).await
}

async fn fetch_range(
    store: &dyn ObjectStore,
    key: &str,
    start: u64,
    end: u64,
    hint: Option<&InodeHint>,
    revision: Option<&OnceLock<String>>,
    budget: Arc<BufferBudget>,
) -> ObsResult<Bytes> {
    let expected = end - start + 1;
    // Reject impossible buffers before issuing a request. Validate the clipped
    // range, not the configured chunk size, so short tail reads remain valid.
    addressable_range_length(expected)?;
    let result = store
        .get_with_etag(
            key,
            Some((start, end)),
            hint,
            revision.and_then(|r| r.get().map(String::as_str)),
        )
        .await?;
    if let Some(revision) = revision {
        let etag = result
            .meta
            .etag
            .as_deref()
            .map(|s| s.trim_matches('"'))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| ObsError::MissingVersion(key.into()))?;
        let expected = revision.get_or_init(|| etag.to_string());
        if etag != expected {
            return Err(ObsError::RemoteFileChanged(key.into()));
        }
    }
    let stream = result.body;
    collect_range_budgeted(stream, expected, budget, store.buffer_budget().is_some()).await
}

fn addressable_range_length(expected: u64) -> ObsResult<usize> {
    usize::try_from(expected)
        .ok()
        .filter(|size| *size <= isize::MAX as usize)
        .ok_or_else(|| ObsError::InvalidArgument("range exceeds addressable memory".into()))
}

#[cfg(test)]
async fn collect_range(
    stream: std::pin::Pin<Box<dyn futures::Stream<Item = ObsResult<Bytes>> + Send>>,
    expected: u64,
) -> ObsResult<Bytes> {
    collect_range_budgeted(stream, expected, BufferBudget::new(0), false).await
}

async fn collect_range_budgeted(
    mut stream: std::pin::Pin<Box<dyn futures::Stream<Item = ObsResult<Bytes>> + Send>>,
    expected: u64, budget: Arc<BufferBudget>, already_budgeted: bool,
) -> ObsResult<Bytes> {
    let expected = addressable_range_length(expected)?;
    let mut first: Option<Bytes> = None;
    let mut combined = BudgetBuffer::new(budget.clone());
    let mut received = 0usize;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if chunk.len() > expected.saturating_sub(received) {
            return Err(ObsError::Io("server ignored GET range".into()));
        }
        if chunk.is_empty() { continue; }
        let chunk = if already_budgeted { chunk } else {
            budget.reserve(chunk.len() as u64)?.attach(chunk)
        };
        received += chunk.len();
        if first.is_none() && combined.is_empty() {
            first = Some(chunk);
            continue;
        }
        let hint = received.max(combined.capacity().saturating_mul(4))
            .max(expected.min(8 * 1024 * 1024)).min(expected);
        if let Some(initial) = first.take() { combined.append(&initial, hint)?; }
        combined.append(&chunk, hint)?;
    }
    if received != expected { return Err(ObsError::Io("incomplete ranged GET".into())); }
    Ok(first.unwrap_or_else(|| combined.take_bytes()))
}

#[cfg(test)]
mod response_tests {
    use super::*;

    // Reference copied from the implementation before bounded initial reservation.
    async fn eager_range_reference(
        mut stream: std::pin::Pin<Box<dyn futures::Stream<Item = ObsResult<Bytes>> + Send>>,
        expected: u64,
    ) -> ObsResult<Bytes> {
        let expected = addressable_range_length(expected)?;
        let mut first: Option<Bytes> = None;
        let mut combined: Option<Vec<u8>> = None;
        let mut received = 0usize;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if chunk.len() > expected.saturating_sub(received) {
                return Err(ObsError::Io("server ignored GET range".into()));
            }
            if chunk.is_empty() {
                continue;
            }
            received += chunk.len();
            if let Some(buffer) = &mut combined {
                buffer.extend_from_slice(&chunk);
            } else if let Some(initial) = first.take() {
                // Allocate only when coalescing is necessary, at the known bounded
                // range size, avoiding repeated growth and copying earlier frames.
                // Fallible reservation turns capacity/allocation failures into a
                // read error instead of an infallible allocator panic/abort.
                let mut buffer = Vec::new();
                buffer.try_reserve_exact(expected).map_err(|error| {
                    ObsError::Io(format!("cannot allocate {expected}-byte range buffer: {error}"))
                })?;
                buffer.extend_from_slice(&initial);
                buffer.extend_from_slice(&chunk);
                combined = Some(buffer);
            } else {
                first = Some(chunk);
            }
        }
        // Drain through EOF even after receiving the expected bytes: a subsequent
        // transport error or extra frame must not be mistaken for a valid response.
        if received != expected {
            return Err(ObsError::Io("incomplete ranged GET".into()));
        }
        Ok(combined.map(Bytes::from).or(first).unwrap_or_default())
    }

    fn reserve_range_buffer_doubling(buffer: &mut Vec<u8>, needed: usize, expected: usize) -> ObsResult<()> {
        if needed > buffer.capacity() {
            // Preserve one allocation for common ranges, but don't reserve an
            // enormous declared range after only a few response bytes arrived.
            let capacity = needed.max(buffer.capacity().saturating_mul(2))
                .max(expected.min(8 * 1024 * 1024)).min(expected);
            buffer.try_reserve_exact(capacity - buffer.len()).map_err(|error| {
                ObsError::Io(format!("cannot allocate {capacity}-byte range buffer: {error}"))
            })?;
        }
        Ok(())
    }

    async fn doubling_range_reference(
        mut stream: std::pin::Pin<Box<dyn futures::Stream<Item = ObsResult<Bytes>> + Send>>,
        expected: u64,
    ) -> ObsResult<Bytes> {
        let expected = addressable_range_length(expected)?;
        let mut first: Option<Bytes> = None;
        let mut combined: Option<Vec<u8>> = None;
        let mut received = 0usize;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if chunk.len() > expected.saturating_sub(received) {
                return Err(ObsError::Io("server ignored GET range".into()));
            }
            if chunk.is_empty() {
                continue;
            }
            received += chunk.len();
            if let Some(buffer) = &mut combined {
                reserve_range_buffer_doubling(buffer, received, expected)?;
                buffer.extend_from_slice(&chunk);
            } else if let Some(initial) = first.take() {
                // Allocate only when coalescing is necessary. All growth remains
                // fallible and bounded by the validated expected range length.
                let mut buffer = Vec::new();
                reserve_range_buffer_doubling(&mut buffer, received, expected)?;
                buffer.extend_from_slice(&initial);
                buffer.extend_from_slice(&chunk);
                combined = Some(buffer);
            } else {
                first = Some(chunk);
            }
        }
        // Drain through EOF even after receiving the expected bytes: a subsequent
        // transport error or extra frame must not be mistaken for a valid response.
        if received != expected {
            return Err(ObsError::Io("incomplete ranged GET".into()));
        }
        Ok(combined.map(Bytes::from).or(first).unwrap_or_default())
    }

    #[tokio::test]
    #[ignore = "manual isolated release benchmark"]
    async fn benchmark_range_collection() {
        let mut samples = Vec::new();
        for (total, frame_size) in [
            (8 * 1024 * 1024, 65536),
            (32 * 1024 * 1024, 4096),
            (32 * 1024 * 1024, 65536),
            (32 * 1024 * 1024, 1024 * 1024),
            (32 * 1024 * 1024, 32 * 1024 * 1024),
        ] {
            let frames = Arc::new((0..total / frame_size)
                .map(|_| Bytes::from(vec![42; frame_size])).collect::<Vec<_>>());
            for round in 0..6 {
                let mut totals = [0u128; 3];
                for iteration in 0..8 {
                    for slot in 0..3 {
                        let strategy = (slot + round + iteration) % 3;
                        let source = frames.clone();
                        let stream = Box::pin(futures::stream::iter(0..source.len())
                            .map(move |index| Ok(source[index].clone())));
                        let start = std::time::Instant::now();
                        let bytes = if strategy == 0 {
                            eager_range_reference(stream, total as u64).await.unwrap()
                        } else if strategy == 1 {
                            doubling_range_reference(stream, total as u64).await.unwrap()
                        } else {
                            collect_range(stream, total as u64).await.unwrap()
                        };
                        totals[strategy] += start.elapsed().as_nanos();
                        assert_eq!(bytes.len(), total);
                        assert!(bytes.iter().all(|byte| *byte == 42));
                        std::hint::black_box(bytes);
                    }
                }
                if round != 0 {
                    samples.push(serde_json::json!({
                        "total_bytes": total, "frame_bytes": frame_size,
                        "round": round, "iterations": 8,
                        "eager_ns": totals[0], "doubling_ns": totals[1], "bounded_ns": totals[2],
                    }));
                }
            }
        }
        println!("RANGE_COLLECTION_BENCH={}", serde_json::json!({
            "samples": samples, "warmup_rounds": 1,
            "scope": "release cached in-memory frames; source allocation, result verification and drop excluded; no network or RSS"
        }));
    }


    #[test]
    fn copy_count_bounds_large_logical_lengths_before_narrowing() {
        for remaining in [1u64 << 32, (1u64 << 32) + 7, u64::MAX] {
            assert_eq!(bounded_read_count(4096, 128, remaining), 128);
            assert_eq!(bounded_read_count(64, 128, remaining), 64);
        }
        assert_eq!(bounded_read_count(4096, 128, 7), 7);
        assert_eq!(bounded_read_count(4096, 128, 0), 0);
        assert_eq!(bounded_read_count(4096, 0, u64::MAX), 0);
    }

    async fn collect(chunks: Vec<ObsResult<Bytes>>, expected: u64) -> ObsResult<Bytes> {
        collect_range(Box::pin(futures::stream::iter(chunks)), expected).await
    }

    #[tokio::test]
    async fn huge_declared_range_does_not_mask_early_transport_error() {
        let error = collect(
            vec![
                Ok(Bytes::from_static(b"a")),
                Ok(Bytes::from_static(b"b")),
                Err(ObsError::Transport("injected early body failure".into())),
            ],
            isize::MAX as u64,
        ).await.unwrap_err();
        assert!(matches!(error, ObsError::Transport(ref message) if message == "injected early body failure"), "{error}");
    }

    #[tokio::test]
    async fn fragmented_range_grows_beyond_initial_reservation() {
        let size = 3 * 1024 * 1024;
        let result = collect(
            vec![Ok(Bytes::from(vec![1; size])), Ok(Bytes::from(vec![2; size])), Ok(Bytes::from(vec![3; size]))],
            (size * 3) as u64,
        ).await.unwrap();
        assert_eq!(result.len(), size * 3);
        for (index, chunk) in result.chunks(size).enumerate() {
            assert!(chunk.iter().all(|byte| *byte == index as u8 + 1));
        }
    }

    #[tokio::test]
    async fn unaddressable_fragmented_range_returns_error_without_allocating() {
        let error = collect(
            vec![Ok(Bytes::from_static(b"a")), Ok(Bytes::from_static(b"b"))],
            isize::MAX as u64 + 1,
        ).await.unwrap_err();
        assert!(matches!(error, ObsError::InvalidArgument(_)));
        assert!(error.to_string().contains("addressable memory"));
    }

    #[tokio::test]
    async fn single_frame_reuses_allocation_even_with_empty_frames() {
        let original = Bytes::from(vec![42; 4096]);
        let result = collect(
            vec![Ok(Bytes::new()), Ok(original.clone()), Ok(Bytes::new())],
            4096,
        )
        .await
        .unwrap();
        assert_eq!(result.as_ptr(), original.as_ptr());
        assert_eq!(result, original);
    }

    #[tokio::test]
    async fn fragmented_frames_preserve_order_and_contents() {
        let result = collect(
            vec![
                Ok(Bytes::from_static(b"ab")),
                Ok(Bytes::new()),
                Ok(Bytes::from_static(b"c")),
                Ok(Bytes::from_static(b"def")),
            ],
            6,
        )
        .await
        .unwrap();
        assert_eq!(result.as_ref(), b"abcdef");
    }

    #[tokio::test]
    async fn short_response_is_rejected() {
        assert!(collect(vec![Ok(Bytes::from_static(b"ab"))], 3)
            .await
            .is_err());
        assert!(collect(vec![], 1).await.is_err());
    }

    #[tokio::test]
    async fn extra_frames_after_full_range_are_rejected() {
        assert!(collect(
            vec![Ok(Bytes::from_static(b"abc")), Ok(Bytes::from_static(b"d"))],
            3
        )
        .await
        .is_err());
        assert!(collect(vec![Ok(Bytes::from_static(b"abcd"))], 3)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn late_transport_error_is_not_hidden_by_complete_data() {
        let error = collect(
            vec![
                Ok(Bytes::from_static(b"abc")),
                Err(ObsError::Io("broken body".into())),
            ],
            3,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("broken body"));
    }
}
