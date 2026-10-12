pub mod pool;
pub mod request;
pub mod tls;

pub use pool::{HttpPool, HttpResponse};
pub use request::{HttpRequest, RequestBuilder};

/// Enclose the whole retry loop so expiration cannot itself trigger a retry.
pub(crate) async fn with_request_timeout<T>(
    timeout_ms: u64,
    request: impl std::future::Future<Output = crate::error::ObsResult<T>>,
) -> crate::error::ObsResult<T> {
    if timeout_ms == 0 {
        return request.await;
    }
    let deadline = tokio::time::Instant::now()
        .checked_add(std::time::Duration::from_millis(timeout_ms))
        .ok_or_else(|| {
            crate::error::ObsError::InvalidArgument("request timeout is too large".into())
        })?;
    tokio::time::timeout_at(deadline, request)
        .await
        .map_err(|_| crate::error::ObsError::Transport("HTTP request total timeout".into()))?
}

/// Retry throttling and transient failures; e.g. 501 is a capability result, not
/// a reason to repeat an unsupported request before falling back.
pub(crate) fn retryable_status(status: http::StatusCode) -> bool {
    matches!(status.as_u16(), 429 | 500 | 502 | 503 | 504)
}

/// Decode delay-seconds or the standard IMF-fixdate HTTP date. Invalid values
/// fall back to local backoff. Absolute dates use the local UTC clock.
pub(crate) fn retry_after(
    value: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<std::time::Duration> {
    let value = value.trim();
    if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) {
        return value
            .parse::<u64>()
            .ok()
            .map(std::time::Duration::from_secs);
    }
    let date = chrono::DateTime::parse_from_rfc2822(value).ok()?;
    Some(date.signed_duration_since(now).to_std().unwrap_or_default())
}

pub(crate) fn response_retry_after(
    result: &crate::error::ObsResult<HttpResponse>,
) -> Option<std::time::Duration> {
    result
        .as_ref()
        .ok()?
        .header("retry-after")
        .and_then(|v| retry_after(v, chrono::Utc::now()))
}

/// Shared policy; the limit counts retries after the initial request.
pub(crate) struct RetryPolicy {
    pub max_retries: u32,
    backoff_ms: u64,
}

impl RetryPolicy {
    pub fn new(config: &crate::config::ObsConfig, idempotent: bool) -> Self {
        Self {
            max_retries: if idempotent {
                config.retry_max_attempts
            } else {
                0
            },
            backoff_ms: config.retry_backoff_ms,
        }
    }

    pub async fn wait(
        &self,
        attempt: u32,
        server_delay: Option<std::time::Duration>,
    ) -> crate::error::ObsResult<()> {
        if attempt > 0 {
            let delay = std::time::Duration::from_millis(
                self.backoff_ms.saturating_mul(u64::from(attempt)),
            )
            .max(server_delay.unwrap_or_default());
            if delay.is_zero() {
                return Ok(());
            }
            let deadline = tokio::time::Instant::now()
                .checked_add(delay)
                .ok_or_else(|| {
                    crate::error::ObsError::InvalidArgument("retry backoff is too large".into())
                })?;
            tokio::time::sleep_until(deadline).await;
        }
        Ok(())
    }

    pub fn should_retry(
        &self,
        attempt: u32,
        result: &crate::error::ObsResult<HttpResponse>,
    ) -> bool {
        attempt < self.max_retries
            && match result {
                Ok(response) => retryable_status(response.status),
                Err(crate::error::ObsError::Transport(_)) => true,
                Err(_) => false,
            }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_after_parses_seconds_dates_and_rejects_invalid_values() {
        let now = chrono::DateTime::parse_from_rfc2822("Wed, 21 Oct 2015 07:28:00 GMT")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(retry_after(" 12 ", now).unwrap().as_secs(), 12);
        assert_eq!(
            retry_after("Wed, 21 Oct 2015 07:28:15 GMT", now)
                .unwrap()
                .as_secs(),
            15
        );
        assert_eq!(
            retry_after("Wed, 21 Oct 2015 07:27:00 GMT", now)
                .unwrap()
                .as_secs(),
            0
        );
        for value in [
            "",
            "-1",
            "+2",
            "0.5",
            "18446744073709551616",
            "invalid",
            "1, 2",
        ] {
            assert!(retry_after(value, now).is_none(), "accepted {value}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retry_after_never_shortens_local_backoff_and_is_cancellable() {
        use std::time::Duration;
        let policy = RetryPolicy::new(
            &crate::config::ObsConfig {
                retry_backoff_ms: 100,
                ..Default::default()
            },
            true,
        );
        let start = tokio::time::Instant::now();
        policy.wait(1, Some(Duration::from_secs(2))).await.unwrap();
        assert_eq!(start.elapsed(), Duration::from_secs(2));
        policy
            .wait(2, Some(Duration::from_millis(10)))
            .await
            .unwrap();
        assert_eq!(start.elapsed(), Duration::from_millis(2200));
        let result = with_request_timeout(20, policy.wait(1, Some(Duration::from_secs(60)))).await;
        assert!(
            matches!(result, Err(crate::error::ObsError::Transport(ref e)) if e == "HTTP request total timeout")
        );
        assert_eq!(start.elapsed(), Duration::from_millis(2220));
    }

    #[tokio::test(start_paused = true)]
    async fn request_deadline_cancels_work_and_releases_resources() {
        let gate = tokio::sync::Semaphore::new(1);
        let start = tokio::time::Instant::now();
        let result = with_request_timeout(20, async {
            let _permit = gate.acquire().await.unwrap();
            std::future::pending::<crate::error::ObsResult<()>>().await
        })
        .await;
        assert!(
            matches!(result, Err(crate::error::ObsError::Transport(ref e)) if e == "HTTP request total timeout")
        );
        assert_eq!(start.elapsed().as_millis(), 20);
        assert_eq!(gate.available_permits(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn disabled_request_deadline_preserves_long_request_result() {
        let start = tokio::time::Instant::now();
        let result = with_request_timeout(0, async {
            tokio::time::sleep(std::time::Duration::from_secs(120)).await;
            Ok(42)
        })
        .await
        .unwrap();
        assert_eq!(result, 42);
        assert_eq!(start.elapsed().as_secs(), 120);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_backoff_is_linear_and_initial_attempt_does_not_wait() {
        let config = crate::config::ObsConfig {
            retry_backoff_ms: 25,
            ..Default::default()
        };
        let policy = RetryPolicy::new(&config, true);
        let start = tokio::time::Instant::now();
        policy.wait(0, None).await.unwrap();
        assert_eq!(start.elapsed().as_millis(), 0);
        policy.wait(1, None).await.unwrap();
        assert_eq!(start.elapsed().as_millis(), 25);
        policy.wait(2, None).await.unwrap();
        assert_eq!(start.elapsed().as_millis(), 75);
        assert_eq!(RetryPolicy::new(&config, false).max_retries, 0);
    }
}
