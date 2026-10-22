use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::{Method, StatusCode, Uri};
use http_body_util::{BodyExt, LengthLimitError, Limited};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::{timeout_at, Instant as TokioInstant};

use super::progress::{with_idle_timeout, Progress, ProgressBody, ProgressRecv};
use crate::io::budget::{BufferBudget, BudgetBuffer};
use crate::config::{bounded_permits, ObsConfig};
use crate::error::{ObsError, ObsResult};

/// Shared HTTP connection pool (grow-on-demand, never shrink).
pub struct HttpPool {
    client: Client<HttpsConnector<HttpConnector>, ProgressBody>,
    config: ObsConfig,
    gate: Arc<Semaphore>,
    buffer_budget: Arc<BufferBudget>,
}

impl HttpPool {
    pub fn new(config: ObsConfig) -> Self {
        let mut connector = HttpConnector::new();
        connector.set_connect_timeout(Some(Duration::from_millis(
            config.connection_establish_timeout_ms,
        )));
        connector.set_nodelay(true);
        connector.set_keepalive(Some(Duration::from_secs(30)));

        connector.enforce_http(false);
        let connector = HttpsConnectorBuilder::new()
            .with_tls_config((*super::tls::build_tls_config()).clone())
            .https_or_http()
            .enable_http1()
            .wrap_connector(connector);
        let client = Client::builder(TokioExecutor::new())
            .retry_canceled_requests(false)
            .pool_idle_timeout(None)
            .build(connector);
        let gate = Arc::new(Semaphore::new(bounded_permits(config.connection_maximum.max(1))));

        Self {
            client,
            buffer_budget: BufferBudget::new(config.buffer_total_bytes),
            config,
            gate,
        }
    }

    pub fn buffer_budget(&self) -> Arc<BufferBudget> { self.buffer_budget.clone() }

    pub fn config(&self) -> &ObsConfig {
        &self.config
    }

    /// Execute idempotent requests with retry on throttling/transient failures.
    pub async fn execute(
        &self,
        method: Method,
        uri: Uri,
        headers: BTreeMap<String, String>,
        body: Option<Bytes>,
        idempotent: bool,
    ) -> ObsResult<HttpResponse> {
        super::with_request_timeout(self.config.request_timeout_ms, async {
            let policy = super::RetryPolicy::new(&self.config, idempotent);
            for attempt in 0..=policy.max_retries {
                let result = self
                    .execute_once(&method, uri.clone(), &headers, body.clone())
                    .await;
                if !policy.should_retry(attempt, &result) {
                    return result;
                }
                let server_delay = super::response_retry_after(&result);
                drop(result); // Do not retain response buffers during backoff.
                policy.wait(attempt + 1, server_delay).await?;
            }
            unreachable!("retry loop always returns on final attempt")
        })
        .await
    }

    pub(crate) async fn execute_once(
        &self,
        method: &Method,
        uri: Uri,
        headers: &BTreeMap<String, String>,
        body: Option<Bytes>,
    ) -> ObsResult<HttpResponse> {
        // `fs.obs.connection.timeout` bounds the connection queue wait, and
        // then, like the OBS SDK's socket timeout, any stretch without upload or
        // download progress; a slow but moving transfer is not cut off. The
        // optional `fs.obs.request.timeout.ms` bounds the whole attempt.
        let start = Instant::now();
        let idle = Duration::from_millis(self.config.connection_timeout_ms);
        let deadline = TokioInstant::now()
            .checked_add(idle)
            .ok_or_else(|| ObsError::InvalidArgument("HTTP timeout is too large".into()))?;
        let _permit = timeout_at(deadline, self.gate.acquire())
            .await
            .map_err(|_| ObsError::Transport("connection queue timeout".into()))?
            .map_err(|e| ObsError::Transport(e.to_string()))?;
        let path = uri.path().to_string();
        let out_bytes = body.as_ref().map_or(0, |b| b.len());
        let progress = Progress::new();
        let req_body = ProgressBody::new(body.unwrap_or_default(), progress.clone());

        let mut builder = hyper::Request::builder().method(method).uri(uri);
        if let Some(headers_map) = builder.headers_mut() {
            for (k, v) in headers {
                headers_map.insert(
                    http::HeaderName::from_bytes(k.as_bytes())
                        .map_err(|e| ObsError::InvalidArgument(e.to_string()))?,
                    http::HeaderValue::from_str(v)
                        .map_err(|e| ObsError::InvalidArgument(e.to_string()))?,
                );
            }
            // hyper omits Content-Length for an empty body; S3-compatible
            // servers reject such PUT/POST requests (folder markers, empty
            // files, metadata updates) with 411 MissingContentLength.
            if matches!(*method, Method::PUT | Method::POST) {
                headers_map.insert(http::header::CONTENT_LENGTH, http::HeaderValue::from(out_bytes));
            }
        }

        let req = builder
            .body(req_body)
            .map_err(|e| ObsError::Transport(e.to_string()))?;

        let resp = with_idle_timeout(idle, &progress, self.client.request(req), "read timeout")
            .await?
            .map_err(|e| ObsError::Transport(e.to_string()))?;

        let status = resp.status();
        let request_id = resp
            .headers()
            .get("x-obs-request-id")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        let mut resp_headers = BTreeMap::new();
        for (k, v) in resp.headers() {
            if let Ok(vs) = v.to_str() {
                resp_headers.insert(k.as_str().to_string(), vs.to_string());
            }
        }

        let (limit, expected) = response_limit(
            method,
            headers,
            status,
            &resp_headers,
            self.config.http_response_max_bytes,
        )?;
        if method != Method::HEAD {
            if let Some(length) = resp_headers
                .get("content-length")
                .and_then(|v| v.parse::<u64>().ok())
            {
                if length > limit as u64 || expected.is_some_and(|n| n != length) {
                    return Err(ObsError::Io(
                        "invalid or oversized response Content-Length".into(),
                    ));
                }
            }
        }
        let known_length = expected.or_else(|| resp_headers.get("content-length")
            .and_then(|value| value.parse::<u64>().ok()))
            .and_then(|length| usize::try_from(length).ok());
        progress.touch();
        let body = ProgressRecv::new(resp.into_body(), progress.clone());
        let body_bytes = with_idle_timeout(
            idle,
            &progress,
            collect_response_body_budgeted(body, limit, known_length, self.buffer_budget.clone()),
            "response body timeout",
        )
        .await??;
        if expected.is_some_and(|n| n != body_bytes.len() as u64) {
            return Err(ObsError::Io(
                "response body length differs from Content-Range".into(),
            ));
        }

        let in_bytes = body_bytes.len() as u64;
        let duration_us = start.elapsed().as_micros() as u64;

        if self.config.access_log_enabled {
            tracing::info!(
                method = %method,
                status = %status,
                out_bytes,
                duration_us,
                in_bytes,
                path = %path,
                "access"
            );
        }

        Ok(HttpResponse {
            status,
            headers: resp_headers,
            body: body_bytes,
            request_id,
            duration_us,
        })
    }
}

/// HTTP response wrapper.
#[derive(Clone)]
pub struct HttpResponse {
    pub status: StatusCode,
    pub headers: BTreeMap<String, String>,
    pub body: Bytes,
    pub request_id: String,
    pub duration_us: u64,
}

impl std::fmt::Debug for HttpResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpResponse")
            .field("status", &self.status)
            .field("header_count", &self.headers.len())
            .field("body_len", &self.body.len())
            .field("request_id", &self.request_id)
            .field("duration_us", &self.duration_us)
            .finish()
    }
}

impl HttpResponse {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn etag(&self) -> Option<String> {
        crate::types::unquote_etag(self.header("etag")?).map(str::to_owned)
    }

    pub fn content_length(&self) -> Option<u64> {
        self.header("content-length").and_then(|s| s.parse().ok())
    }
}

/// Re-export request types from request module.
pub use super::request::{HttpRequest, RequestBuilder};

#[cfg(test)]
mod timeout_tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn total_deadline_bounds_connection_queue_before_attempt_deadline() {
        let pool = HttpPool::new(ObsConfig {
            connection_maximum: 1,
            connection_timeout_ms: 1000,
            request_timeout_ms: 20,
            retry_max_attempts: 10,
            ..Default::default()
        });
        let occupied = pool.gate.acquire().await.unwrap();
        let start = TokioInstant::now();
        let result = pool
            .execute(
                Method::GET,
                "http://127.0.0.1:1/".parse().unwrap(),
                BTreeMap::new(),
                None,
                true,
            )
            .await;
        assert!(
            matches!(result, Err(ObsError::Transport(ref e)) if e == "HTTP request total timeout")
        );
        assert_eq!(start.elapsed().as_millis(), 20);
        drop(occupied);
        assert!(pool.gate.try_acquire().is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn connection_queue_has_a_deadline_and_drops_waiter() {
        let pool = HttpPool::new(ObsConfig {
            connection_maximum: 1,
            connection_timeout_ms: 100,
            ..Default::default()
        });
        let occupied = pool.gate.acquire().await.unwrap();
        let started = TokioInstant::now();
        // No connection can be made while the sole permit is held.
        let result = pool
            .execute_once(
                &Method::GET,
                "http://127.0.0.1:1/".parse().unwrap(),
                &BTreeMap::new(),
                None,
            )
            .await;
        assert!(
            matches!(result, Err(ObsError::Transport(ref e)) if e == "connection queue timeout")
        );
        assert_eq!(started.elapsed(), Duration::from_millis(100));
        drop(occupied);
        assert_eq!(pool.gate.available_permits(), 1);
        assert!(pool.gate.try_acquire().is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_a_queued_request_drops_waiter() {
        let pool = HttpPool::new(ObsConfig {
            connection_maximum: 1,
            ..Default::default()
        });
        let occupied = pool.gate.acquire().await.unwrap();
        let headers = BTreeMap::new();
        {
            let request = pool.execute_once(
                &Method::GET,
                "http://127.0.0.1:1/".parse().unwrap(),
                &headers,
                None,
            );
            tokio::pin!(request);
            assert!(futures::poll!(&mut request).is_pending());
        }
        drop(occupied);
        assert_eq!(pool.gate.available_permits(), 1);
        assert!(pool.gate.try_acquire().is_ok());
    }
}

/// Validate range semantics before buffering; a server ignoring Range must not
/// turn a small read into a whole-object allocation or return the wrong offset.
fn response_limit(
    method: &Method,
    request: &BTreeMap<String, String>,
    status: StatusCode,
    response: &BTreeMap<String, String>,
    configured: u64,
) -> ObsResult<(usize, Option<u64>)> {
    let mut limit = configured.min(usize::MAX as u64) as usize;
    if method == Method::HEAD {
        return Ok((0, None));
    }
    if !status.is_success() {
        return Ok((limit.min(64 * 1024), None));
    }
    let range = request
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("range"));
    let Some((_, range)) = range.filter(|_| method == Method::GET) else {
        return Ok((limit, None));
    };
    let invalid =
        || ObsError::Io("invalid ranged GET response (expected matching 206 Content-Range)".into());
    let parse = |s: &str| s.parse::<u64>().map_err(|_| invalid());
    let (start, end) = range
        .strip_prefix("bytes=")
        .and_then(|r| r.split_once('-'))
        .ok_or_else(invalid)?;
    let (start, end) = (parse(start)?, parse(end)?);
    if status != StatusCode::PARTIAL_CONTENT || end < start {
        return Err(invalid());
    }
    let value = response.get("content-range").ok_or_else(invalid)?;
    let (actual, total) = value
        .strip_prefix("bytes ")
        .and_then(|r| r.split_once('/'))
        .ok_or_else(invalid)?;
    let (a, b) = actual.split_once('-').ok_or_else(invalid)?;
    let (a, b, total) = (parse(a)?, parse(b)?, parse(total)?);
    if a != start || b < a || b > end || b >= total || b != end.min(total - 1) {
        return Err(invalid());
    }
    let count = b
        .checked_sub(a)
        .and_then(|v| v.checked_add(1))
        .ok_or_else(invalid)?;
    if count > limit as u64 {
        return Err(ObsError::Io(
            "range exceeds fs.obs.http.response.max.bytes".into(),
        ));
    }
    limit = count as usize;
    Ok((limit, Some(count)))
}

async fn collect_response_body_budgeted<B>(body: B, limit: usize, known_length: Option<usize>, budget: Arc<BufferBudget>) -> ObsResult<Bytes>
where
    B: hyper::body::Body<Data = Bytes> + Unpin,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    // Only reserve after a second nonempty frame proves merging is needed.
    // Length was validated by the caller; cap speculative reservation at 8 MiB.
    let initial_reserve = known_length.unwrap_or(0).min(limit).min(8 * 1024 * 1024);
    let mut body = Limited::new(body, limit);
    let mut first: Option<Bytes> = None;
    let mut combined = BudgetBuffer::new(budget.clone());
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|error| {
            if error.is::<LengthLimitError>() {
                ObsError::Io("response body exceeds configured/requested limit".into())
            } else {
                ObsError::Transport(error.to_string())
            }
        })?;
        let Ok(chunk) = frame.into_data() else { continue; };
        if chunk.is_empty() { continue; }
        let chunk = budget.reserve(chunk.len() as u64)?.attach(chunk);
        if first.is_none() && combined.is_empty() {
            first = Some(chunk);
            continue;
        }
        let initial = first.take();
        let needed = combined.len()
            .checked_add(initial.as_ref().map_or(0, Bytes::len))
            .and_then(|n| n.checked_add(chunk.len()))
            .filter(|n| *n <= limit && *n <= isize::MAX as usize)
            .ok_or_else(|| ObsError::Io("response exceeds addressable buffer capacity".into()))?;
        let capacity = needed.max(combined.capacity().saturating_mul(2)).max(initial_reserve)
            .min(limit).min(isize::MAX as usize);
        if let Some(initial) = initial { combined.append(&initial, capacity)?; }
        combined.append(&chunk, capacity)?;
        // Drop each source frame now, rather than retaining every frame to EOF.
    }
    Ok(first.unwrap_or_else(|| combined.take_bytes()))
}

#[cfg(test)]
async fn collect_response_body<B>(body: B, limit: usize, known_length: Option<usize>) -> ObsResult<Bytes>
where B: hyper::body::Body<Data = Bytes> + Unpin, B::Error: std::error::Error + Send + Sync + 'static {
    collect_response_body_budgeted(body, limit, known_length, BufferBudget::new(0)).await
}

#[cfg(test)]
mod response_frame_tests {
    #[tokio::test]
    async fn cancelled_and_failed_body_collection_return_frame_leases() {
        use http_body_util::StreamBody;
        use hyper::body::Frame;
        use futures::StreamExt;
        let budget = BufferBudget::new(4);
        {
            let frames = futures::stream::iter([Ok::<_, std::io::Error>(
                Frame::data(Bytes::from_static(b"data"))
            )]).chain(futures::stream::pending());
            let future = collect_response_body_budgeted(StreamBody::new(frames), 4, Some(4), budget.clone());
            tokio::pin!(future);
            assert!(futures::poll!(&mut future).is_pending());
            assert!(budget.reserve(1).is_err());
        }
        assert!(budget.reserve(4).is_ok());
        let frames = futures::stream::iter([
            Ok(Frame::data(Bytes::from_static(b"data"))),
            Err(std::io::Error::other("late body failure")),
        ]);
        assert!(collect_response_body_budgeted(StreamBody::new(frames), 4, Some(4), budget.clone()).await.is_err());
        assert!(budget.reserve(4).is_ok());
    }
    #[tokio::test]
    async fn merge_budget_counts_frames_and_destination_and_releases_on_error() {
        use http_body_util::StreamBody;
        use hyper::body::Frame;
        for limit in [7, 8] {
            let budget = BufferBudget::new(limit);
            let frames = futures::stream::iter([
                Ok::<_, std::io::Error>(Frame::data(Bytes::from_static(b"ab"))),
                Ok(Frame::data(Bytes::from_static(b"cd"))),
            ]);
            let result = collect_response_body_budgeted(StreamBody::new(frames), 4, Some(4), budget.clone()).await;
            if limit == 7 {
                assert!(result.unwrap_err().to_string().contains("shared buffer budget"));
            } else {
                let bytes = result.unwrap();
                assert_eq!(&bytes[..], b"abcd");
                let slice = bytes.slice(..1);
                drop(bytes);
                assert!(budget.reserve(5).is_err());
                drop(slice);
            }
            assert!(budget.reserve(limit).is_ok());
        }
    }
    use super::*;
    use futures::StreamExt;
    use hyper::body::Frame;
    use http_body_util::StreamBody;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    #[ignore = "manual release benchmark; no network or RSS measurement"]
    async fn benchmark_http_frame_collection() {
        let mut samples = Vec::new();
        for (total, frame_size) in [(65536, 1), (8 * 1024 * 1024, 1024),
            (8 * 1024 * 1024, 65536), (8 * 1024 * 1024, 8 * 1024 * 1024)] {
            // Independent backing allocations, prepared outside timed work.
            // Both collectors see the same cached bytes and lazy clone overhead.
            let frames: Vec<Bytes> = (0..total / frame_size)
                .map(|_| Bytes::from(vec![42; frame_size])).collect();
            for round in 0..6 {
                let mut totals = [0u128; 3];
                for iteration in 0..16 {
                    for slot in 0..3 {
                        let strategy = (slot + round + iteration) % 3;
                        let body = StreamBody::new(futures::stream::iter(frames.iter().cloned()
                            .map(|bytes| Ok::<_, std::io::Error>(Frame::data(bytes)))));
                        let start = Instant::now();
                        let bytes = if strategy == 0 {
                            Limited::new(body, total).collect().await.unwrap().to_bytes()
                        } else {
                            collect_response_body(body, total, (strategy == 2).then_some(total)).await.unwrap()
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
                        "round": round, "iterations": 16,
                        "collect_all_ns": totals[0], "incremental_ns": totals[1], "known_length_ns": totals[2],
                    }));
                }
            }
        }
        println!("HTTP_FRAME_BENCH={}", serde_json::json!({
            "samples": samples, "warmup_rounds": 1,
            "scope": "release cached in-memory frames; excludes source allocation, result verification and drop; no network or RSS"
        }));
    }

    struct OwnedFrame {
        data: [u8; 1],
        active: Arc<AtomicUsize>,
    }
    impl AsRef<[u8]> for OwnedFrame {
        fn as_ref(&self) -> &[u8] { &self.data }
    }
    impl Drop for OwnedFrame {
        fn drop(&mut self) { self.active.fetch_sub(1, Ordering::SeqCst); }
    }

    #[tokio::test]
    async fn fragmented_response_releases_copied_frame_owners_incrementally() {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let frames = futures::stream::iter(0..64u8).map(|value| {
            let count = active.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(count, Ordering::SeqCst);
            Ok::<_, std::io::Error>(Frame::data(Bytes::from_owner(OwnedFrame {
                data: [value], active: active.clone(),
            })))
        });
        let result = collect_response_body(StreamBody::new(frames), 64, None).await.unwrap();
        assert_eq!(result.as_ref(), &(0..64u8).collect::<Vec<_>>());
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert!(peak.load(Ordering::SeqCst) <= 2, "retained copied frame owners until EOF");
    }

    #[tokio::test]
    async fn one_frame_keeps_allocation_and_ignores_empty_data_and_trailers() {
        let bytes = Bytes::from(vec![42; 8]);
        let pointer = bytes.as_ptr();
        let frames = vec![
            Ok::<_, std::io::Error>(Frame::data(Bytes::new())),
            Ok(Frame::data(bytes)),
            Ok(Frame::trailers(http::HeaderMap::new())),
        ];
        let result = collect_response_body(StreamBody::new(futures::stream::iter(frames)), 8, Some(usize::MAX))
            .await.unwrap();
        assert_eq!(result.as_ptr(), pointer);
        assert_eq!(&result[..], &[42; 8]);
    }

    #[tokio::test]
    async fn body_limit_and_late_transport_errors_remain_distinct() {
        for late_error in [false, true] {
            let mut frames = vec![Ok(Frame::data(Bytes::from_static(b"ab")))];
            frames.push(if late_error {
                Err(std::io::Error::other("late body failure"))
            } else {
                Ok(Frame::data(Bytes::from_static(b"c")))
            });
            let error = collect_response_body(StreamBody::new(futures::stream::iter(frames)), 2, Some(2))
                .await.unwrap_err();
            if late_error {
                assert!(matches!(error, ObsError::Transport(ref message) if message.contains("late body failure")));
            } else {
                assert!(matches!(error, ObsError::Io(_)));
            }
        }
    }

    #[tokio::test]
    async fn reservation_hint_never_pads_or_truncates_received_bytes() {
        for hint in [0, 1, 1024, usize::MAX] {
            let frames = vec![
                Ok::<_, std::io::Error>(Frame::data(Bytes::from_static(b"ab"))),
                Ok(Frame::data(Bytes::from_static(b"cd"))),
            ];
            let result = collect_response_body(
                StreamBody::new(futures::stream::iter(frames)), 8, Some(hint),
            ).await.unwrap();
            assert_eq!(&result[..], b"abcd");
        }
    }
}

#[cfg(test)]
mod debug_tests {
    use super::*;
    #[test]
    fn response_debug_redacts_headers_and_body() {
        let response = HttpResponse {
            status: StatusCode::OK,
            headers: BTreeMap::from([("Set-Cookie".into(), "private-cookie".into()),
                ("x-obs-meta-custom".into(), "private-metadata".into())]),
            body: Bytes::from_static(b"private-object-content"),
            request_id: "request-123".into(), duration_us: 42,
        };
        for rendered in [format!("{response:?}"), format!("{response:#?}")] {
            assert!(!rendered.contains("private-"), "Debug exposed protected response data");
            assert!(rendered.contains("200"));
            assert!(rendered.contains("request-123"));
        }
        assert_eq!(response.header("set-cookie"), Some("private-cookie"));
        assert_eq!(response.body.as_ref(), b"private-object-content");
    }
}
