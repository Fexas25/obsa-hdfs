//! Idle (no-progress) timeouts for one HTTP attempt.
//!
//! `fs.obs.connection.timeout` is the OBS SDK's socket timeout: a transfer
//! fails only when no data moves for that long, not when a large body takes
//! longer in total. Request bodies are sent in chunks and response bodies are
//! read frame by frame; each chunk marks progress.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use hyper::body::{Body, Frame, SizeHint};
use tokio::time::{sleep_until, Instant};

use crate::error::{ObsError, ObsResult};

/// Upload chunk size; hyper buffers a few of these before the socket drains.
const CHUNK: usize = 64 * 1024;

/// Time of the last observed transfer progress.
pub(crate) struct Progress {
    base: Instant,
    last_ms: AtomicU64,
}

impl Progress {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self { base: Instant::now(), last_ms: AtomicU64::new(0) })
    }

    pub(crate) fn touch(&self) {
        let elapsed = self.base.elapsed().as_millis().min(u64::MAX as u128) as u64;
        self.last_ms.fetch_max(elapsed, Ordering::Relaxed);
    }

    fn last(&self) -> Instant {
        self.base + Duration::from_millis(self.last_ms.load(Ordering::Relaxed))
    }
}

/// Runs `future` until it completes, or fails once no progress was made for
/// `idle`.
pub(crate) async fn with_idle_timeout<F: Future>(
    idle: Duration,
    progress: &Progress,
    future: F,
    message: &str,
) -> ObsResult<F::Output> {
    tokio::pin!(future);
    loop {
        let deadline = progress.last().checked_add(idle)
            .ok_or_else(|| ObsError::InvalidArgument("HTTP timeout is too large".into()))?;
        tokio::select! {
            output = &mut future => return Ok(output),
            _ = sleep_until(deadline) => {
                if progress.last() + idle <= Instant::now() {
                    return Err(ObsError::Transport(message.into()));
                }
            }
        }
    }
}

/// A fixed request body sent in chunks, marking progress as hyper takes each.
pub(crate) struct ProgressBody {
    data: Bytes,
    progress: Arc<Progress>,
}

impl ProgressBody {
    pub(crate) fn new(data: Bytes, progress: Arc<Progress>) -> Self {
        Self { data, progress }
    }
}

impl Body for ProgressBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        if self.data.is_empty() {
            return Poll::Ready(None);
        }
        // hyper asks for the next chunk once the previous one was buffered.
        self.progress.touch();
        let n = self.data.len().min(CHUNK);
        let chunk = self.data.split_to(n);
        Poll::Ready(Some(Ok(Frame::data(chunk))))
    }

    fn is_end_stream(&self) -> bool {
        self.data.is_empty()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.data.len() as u64)
    }
}

pin_project_lite::pin_project! {
    /// A response body that marks progress for every received frame.
    pub(crate) struct ProgressRecv<B> {
        #[pin]
        inner: B,
        progress: Arc<Progress>,
    }
}

impl<B> ProgressRecv<B> {
    pub(crate) fn new(inner: B, progress: Arc<Progress>) -> Self {
        Self { inner, progress }
    }
}

impl<B: Body> Body for ProgressRecv<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<B::Data>, B::Error>>> {
        let this = self.project();
        let polled = this.inner.poll_frame(cx);
        if polled.is_ready() {
            this.progress.touch();
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    #[tokio::test(start_paused = true)]
    async fn idle_timeout_resets_on_progress_and_fires_when_stalled() {
        let progress = Progress::new();
        let idle = Duration::from_millis(100);
        // Progress every 60 ms keeps a 300 ms transfer alive.
        let ticking = {
            let progress = progress.clone();
            async move {
                for _ in 0..5 {
                    tokio::time::sleep(Duration::from_millis(60)).await;
                    progress.touch();
                }
                "done"
            }
        };
        assert_eq!(with_idle_timeout(idle, &progress, ticking, "idle").await.unwrap(), "done");
        // No progress: fails after exactly the idle time.
        let start = Instant::now();
        let stalled = with_idle_timeout(idle, &progress, std::future::pending::<()>(), "idle").await;
        assert!(matches!(stalled, Err(ObsError::Transport(ref m)) if m == "idle"));
        assert_eq!(start.elapsed(), idle);
    }

    #[tokio::test]
    async fn progress_body_keeps_exact_length_and_content() {
        let data = Bytes::from(vec![7u8; CHUNK * 2 + 5]);
        let body = ProgressBody::new(data.clone(), Progress::new());
        assert_eq!(body.size_hint().exact(), Some(data.len() as u64));
        assert_eq!(body.collect().await.unwrap().to_bytes(), data);
        assert!(ProgressBody::new(Bytes::new(), Progress::new()).is_end_stream());
    }
}
