use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use crate::error::{ObsError, ObsResult};
use crate::types::{AddressingStyle, BucketMode};

/// Defensive bound for legacy constructors that cannot return configuration errors.
/// This is a representation limit, not a practical concurrency or memory budget.
pub(crate) fn bounded_permits(count: usize) -> usize {
    count.min(Semaphore::MAX_PERMITS)
}

/// OBS client configuration (fs.obs.* prefix, Hadoop compatible).
#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ObsConfig {
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
    pub bucket: String,
    pub secure: bool,

    pub bucket_mode: BucketMode,
    pub addressing_style: AddressingStyle,
    pub path_style_access: bool,

    pub connection_maximum: usize,
    pub connection_establish_timeout_ms: u64,
    /// Per HTTP attempt: connection queue, send/headers and full response body.
    /// Retries and their backoff each occur outside this attempt budget.
    pub connection_timeout_ms: u64,
    /// Total HTTP request budget including all retries/backoff; zero disables it.
    pub request_timeout_ms: u64,
    /// Hard cap for each buffered HTTP response, including unbounded GETs.
    pub http_response_max_bytes: u64,

    /// Shared managed allocation capacity and opaque frame bytes; 0 disables.
    pub buffer_total_bytes: u64,
    /// Optional local known-MPU journal directory; recovery is explicit.
    pub upload_journal_dir: Option<String>,
    pub multipart_size: u64,
    pub multipart_threshold: u64,
    pub upload_active_blocks: usize,
    pub upload_total_active_blocks: usize,

    pub readahead_chunk_size: u64,
    pub readahead_max_chunks: usize,
    pub readahead_total_chunks: usize,
    /// Pin ordinary object reads to an ETag. PFS uses mutable file semantics.
    pub read_change_detection: bool,

    pub metadata_cache_ttl_ms: u64,
    pub negative_cache_ttl_ms: u64,
    pub metadata_cache_size: usize,
    pub list_cache_ttl_ms: u64,

    pub paging_maximum: usize,
    pub pfs_append_concurrency: usize,
    pub pfs_append_chunk_size: u64,
    pub pfs_append_total_bytes: u64,
    pub pfs_inode_hints: bool,

    pub trash_enabled: bool,
    pub trash_dir: String,
    pub content_summary_enable: bool,
    pub permissions_mode: String,

    /// Extra attempts after the first request; zero disables automatic retries.
    pub retry_max_attempts: u32,
    pub retry_backoff_ms: u64,

    pub access_log_enabled: bool,
    pub block_size: i64,

    /// Per-bucket overrides: fs.obs.bucket.BUCKET.key -> value
    pub bucket_overrides: HashMap<String, HashMap<String, String>>,
}

impl std::fmt::Debug for ObsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Endpoint URLs and bucket overrides may also carry secrets. Keep the
        // diagnostic representation limited to known non-secret tuning values.
        f.debug_struct("ObsConfig")
            .field("bucket_mode", &self.bucket_mode)
            .field("connection_maximum", &self.connection_maximum)
            .field("multipart_size", &self.multipart_size)
            .field("readahead_chunk_size", &self.readahead_chunk_size)
            .field("read_change_detection", &self.read_change_detection)
            .field("credentials", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl ObsConfig {
    pub(crate) fn upload_buffer_sizes(&self) -> ObsResult<(usize, usize)> {
        fn capacity(key: &str, value: u64) -> ObsResult<usize> {
            usize::try_from(value)
                .ok()
                .filter(|size| *size <= isize::MAX as usize)
                .ok_or_else(|| ObsError::InvalidArgument(format!(
                    "{key} exceeds addressable buffer capacity {}",
                    isize::MAX
                )))
        }
        Ok((
            capacity("fs.obs.multipart.size", self.multipart_size.max(5 * 1024 * 1024))?,
            capacity("fs.obs.multipart.threshold", self.multipart_threshold.max(1))?,
        ))
    }

    pub(crate) fn validate_semaphore_limits(&self) -> ObsResult<()> {
        for (key, count) in [
            ("fs.obs.connection.maximum", self.connection_maximum),
            ("fs.obs.upload.active.blocks", self.upload_active_blocks),
            ("fs.obs.upload.total.active.blocks", self.upload_total_active_blocks),
            ("fs.obs.readahead.total.chunks", self.readahead_total_chunks),
        ] {
            if count > Semaphore::MAX_PERMITS {
                return Err(ObsError::InvalidArgument(format!(
                    "{key} exceeds the supported permit limit {}",
                    Semaphore::MAX_PERMITS
                )));
            }
        }
        Ok(())
    }

    pub fn metadata_cache_ttl(&self) -> Duration {
        Duration::from_millis(self.metadata_cache_ttl_ms)
    }

    pub fn negative_cache_ttl(&self) -> Duration {
        Duration::from_millis(self.negative_cache_ttl_ms)
    }

    pub fn list_cache_ttl(&self) -> Duration {
        Duration::from_millis(self.list_cache_ttl_ms)
    }

    /// Apply per-bucket override for a config key.
    pub fn bucket_override(&self, key: &str) -> Option<&str> {
        self.bucket_overrides
            .get(&self.bucket)
            .and_then(|m| m.get(key))
            .map(|s| s.as_str())
    }
}

impl Default for ObsConfig {
    fn default() -> Self {
        let cores = num_cpus::get().max(1);
        Self {
            endpoint: String::new(),
            access_key: String::new(),
            secret_key: String::new(),
            session_token: None,
            bucket: String::new(),
            secure: true,
            bucket_mode: BucketMode::Auto,
            addressing_style: AddressingStyle::Path,
            path_style_access: true,
            connection_maximum: cores.max(16) * 2,
            connection_establish_timeout_ms: 5000,
            connection_timeout_ms: 60_000,
            request_timeout_ms: 0,
            http_response_max_bytes: 64 * 1024 * 1024,
            buffer_total_bytes: 0,
            upload_journal_dir: None,
            multipart_size: 16 * 1024 * 1024,
            multipart_threshold: 16 * 1024 * 1024,
            upload_active_blocks: 12,
            upload_total_active_blocks: cores.max(4) * 8,
            readahead_chunk_size: 8 * 1024 * 1024,
            readahead_max_chunks: 8,
            readahead_total_chunks: cores.max(2) * 16,
            read_change_detection: true,
            metadata_cache_ttl_ms: 1000,
            negative_cache_ttl_ms: 300,
            metadata_cache_size: 4096,
            list_cache_ttl_ms: 500,
            paging_maximum: 1000,
            pfs_append_chunk_size: 1024 * 1024,
            pfs_append_total_bytes: 64 * 1024 * 1024,
            pfs_append_concurrency: 4,
            pfs_inode_hints: true,
            trash_enabled: false,
            trash_dir: crate::types::DEFAULT_TRASH_DIR.into(),
            content_summary_enable: false,
            permissions_mode: "none".into(),
            retry_max_attempts: 2,
            retry_backoff_ms: 100,
            access_log_enabled: false,
            block_size: 128 * 1024 * 1024,
            bucket_overrides: HashMap::new(),
        }
    }
}

/// Build ObsConfig from Hadoop-style key-value map.
pub fn config_from_hadoop(props: &HashMap<String, String>, bucket: &str) -> ObsConfig {
    let mut effective = props.clone();
    let prefix = format!("fs.obs.bucket.{bucket}.");
    for (key, value) in props {
        if let Some(suffix) = key.strip_prefix(&prefix) {
            effective.insert(format!("fs.obs.{suffix}"), value.clone());
        }
    }
    let props = &effective;
    let mut cfg = ObsConfig {
        bucket: bucket.to_string(),
        ..Default::default()
    };

    macro_rules! set_str {
        ($field:ident, $key:expr) => {
            if let Some(v) = props.get($key) {
                cfg.$field = v.clone();
            }
        };
    }

    macro_rules! set_bool {
        ($field:ident, $key:expr) => {
            if let Some(v) = props.get($key) {
                cfg.$field = v.parse().unwrap_or(cfg.$field);
            }
        };
    }

    macro_rules! set_usize {
        ($field:ident, $key:expr) => {
            if let Some(v) = props.get($key) {
                cfg.$field = parse_size(v)
                    .and_then(|n| usize::try_from(n).ok())
                    .unwrap_or(cfg.$field);
            }
        };
    }

    macro_rules! set_u64 {
        ($field:ident, $key:expr) => {
            if let Some(v) = props.get($key) {
                cfg.$field = parse_size(v).unwrap_or(cfg.$field);
            }
        };
    }

    set_str!(endpoint, "fs.obs.endpoint");
    if let Some(value) = props.get("fs.obs.block.size") {
        cfg.block_size = parse_size(value)
            .and_then(|size| i64::try_from(size).ok())
            .filter(|size| *size > 0)
            .unwrap_or(cfg.block_size);
    }
    set_str!(access_key, "fs.obs.access.key");
    set_str!(secret_key, "fs.obs.secret.key");
    if let Some(v) = props.get("fs.obs.session.token") {
        cfg.session_token = Some(v.clone());
    }
    set_bool!(secure, "fs.obs.connection.ssl.enabled");
    set_usize!(connection_maximum, "fs.obs.connection.maximum");
    set_u64!(
        connection_establish_timeout_ms,
        "fs.obs.connection.establish.timeout"
    );
    set_u64!(connection_timeout_ms, "fs.obs.connection.timeout");
    if let Some(value) = props.get("fs.obs.request.timeout.ms") {
        cfg.request_timeout_ms = value.trim().parse().unwrap_or(cfg.request_timeout_ms);
    }
    if let Some(value) = props.get("fs.obs.retry.max.attempts") {
        cfg.retry_max_attempts = value.trim().parse().unwrap_or(cfg.retry_max_attempts);
    }
    if let Some(value) = props.get("fs.obs.retry.backoff.ms") {
        cfg.retry_backoff_ms = value.trim().parse().unwrap_or(cfg.retry_backoff_ms);
    }
    set_u64!(http_response_max_bytes, "fs.obs.http.response.max.bytes");
    cfg.upload_journal_dir = props.get("fs.obs.upload.journal.dir").filter(|s| !s.is_empty()).cloned();
    set_u64!(buffer_total_bytes, "fs.obs.buffer.total.bytes");
    set_u64!(multipart_size, "fs.obs.multipart.size");
    set_u64!(multipart_threshold, "fs.obs.multipart.threshold");
    set_usize!(upload_active_blocks, "fs.obs.upload.active.blocks");
    set_usize!(
        upload_total_active_blocks,
        "fs.obs.upload.total.active.blocks"
    );
    set_u64!(readahead_chunk_size, "fs.obs.readahead.chunk.size");
    set_usize!(readahead_max_chunks, "fs.obs.readahead.max.chunks");
    set_usize!(readahead_total_chunks, "fs.obs.readahead.total.chunks");
    set_bool!(
        read_change_detection,
        "fs.obs.read.change.detection.enabled"
    );
    set_u64!(metadata_cache_ttl_ms, "fs.obs.metadata.cache.ttl");
    set_u64!(negative_cache_ttl_ms, "fs.obs.negative.cache.ttl");
    set_usize!(metadata_cache_size, "fs.obs.metadata.cache.size");
    set_usize!(paging_maximum, "fs.obs.paging.maximum");
    set_usize!(pfs_append_concurrency, "fs.obs.pfs.append.concurrency");
    set_u64!(pfs_append_chunk_size, "fs.obs.pfs.append.chunk.size");
    set_u64!(pfs_append_total_bytes, "fs.obs.pfs.append.total.bytes");
    set_bool!(pfs_inode_hints, "fs.obs.pfs.inode.hints");
    set_bool!(trash_enabled, "fs.obs.trash.enabled");
    set_str!(trash_dir, "fs.obs.trash.dir");
    set_bool!(content_summary_enable, "fs.obs.content.summary.enable");
    set_str!(permissions_mode, "fs.obs.permissions.mode");
    set_bool!(access_log_enabled, "fs.obs.access.log.enabled");
    set_bool!(path_style_access, "fs.obs.path.style.access");

    if let Some(v) = props.get("fs.obs.bucket.mode") {
        cfg.bucket_mode = match v.to_lowercase().as_str() {
            "object" => BucketMode::Object,
            "pfs" | "posix" => BucketMode::Pfs,
            _ => BucketMode::Auto,
        };
    }

    if !cfg.path_style_access {
        cfg.addressing_style = AddressingStyle::Vhost;
    }

    // Per-bucket overrides: fs.obs.bucket.<bucket>.<key>
    let prefix = format!("fs.obs.bucket.{}.", bucket);
    for (k, v) in props {
        if let Some(suffix) = k.strip_prefix(&prefix) {
            cfg.bucket_overrides
                .entry(bucket.to_string())
                .or_default()
                .insert(suffix.to_string(), v.clone());
        }
    }

    cfg.connection_maximum = cfg.connection_maximum.max(1);
    cfg.upload_active_blocks = cfg.upload_active_blocks.max(1);
    cfg.upload_total_active_blocks = cfg.upload_total_active_blocks.max(1);
    cfg.readahead_chunk_size = cfg.readahead_chunk_size.max(1);
    cfg.pfs_append_concurrency = cfg.pfs_append_concurrency.max(1);
    cfg.paging_maximum = cfg.paging_maximum.clamp(1, 1000);
    cfg.multipart_size = cfg.multipart_size.max(5 * 1024 * 1024);
    cfg
}

fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim();
    if let Ok(n) = s.parse::<u64>() {
        return Some(n);
    }
    let s_lower = s.to_lowercase();
    if let Some(num) = s_lower.strip_suffix('k') {
        return num.parse::<u64>().ok().and_then(|n| n.checked_mul(1024));
    }
    if let Some(num) = s_lower.strip_suffix('m') {
        return num
            .parse::<u64>()
            .ok()
            .and_then(|n| n.checked_mul(1024 * 1024));
    }
    if let Some(num) = s_lower.strip_suffix('g') {
        return num
            .parse::<u64>()
            .ok()
            .and_then(|n| n.checked_mul(1024 * 1024 * 1024));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn journal_directory_and_buffer_capacity_support_bucket_overrides() {
        let props = HashMap::from([
            ("fs.obs.upload.journal.dir".into(), "common".into()),
            ("fs.obs.bucket.special.upload.journal.dir".into(), "isolated".into()),
            ("fs.obs.buffer.total.bytes".into(), "32M".into()),
            ("fs.obs.bucket.special.buffer.total.bytes".into(), "8M".into()),
        ]);
        let common = config_from_hadoop(&props, "other");
        let special = config_from_hadoop(&props, "special");
        assert_eq!(common.upload_journal_dir.as_deref(), Some("common"));
        assert_eq!(special.upload_journal_dir.as_deref(), Some("isolated"));
        assert_eq!(common.buffer_total_bytes, 32 * 1024 * 1024);
        assert_eq!(special.buffer_total_bytes, 8 * 1024 * 1024);
        assert!(ObsConfig::default().upload_journal_dir.is_none());
    }

    #[test]
    fn parse_human_size() {
        assert_eq!(parse_size("16M"), Some(16 * 1024 * 1024));
        assert_eq!(parse_size("8192"), Some(8192));
    }

    #[test]
    fn permissions_mode_defaults_none_and_supports_bucket_override() {
        assert_eq!(config_from_hadoop(&HashMap::new(), "bucket").permissions_mode, "none");
        let props = HashMap::from([
            ("fs.obs.permissions.mode".into(), "none".into()),
            ("fs.obs.bucket.special.permissions.mode".into(), "disguise".into()),
        ]);
        assert_eq!(config_from_hadoop(&props, "other").permissions_mode, "none");
        assert_eq!(config_from_hadoop(&props, "special").permissions_mode, "disguise");
    }

    #[test]
    fn content_summary_switch_defaults_off_and_accepts_bucket_override() {
        assert!(!config_from_hadoop(&HashMap::new(), "bucket").content_summary_enable);
        for enabled in [false, true] {
            let props = HashMap::from([
                ("fs.obs.content.summary.enable".into(), enabled.to_string()),
                ("fs.obs.bucket.special.content.summary.enable".into(), (!enabled).to_string()),
            ]);
            assert_eq!(config_from_hadoop(&props, "other").content_summary_enable, enabled);
            assert_eq!(config_from_hadoop(&props, "special").content_summary_enable, !enabled);
        }
    }

    #[test]
    fn block_size_is_positive_signed_and_bucket_overridable() {
        let props = HashMap::from([
            ("fs.obs.block.size".into(), "64M".into()),
            ("fs.obs.bucket.special.block.size".into(), "32M".into()),
        ]);
        assert_eq!(config_from_hadoop(&props, "other").block_size, 64 * 1024 * 1024);
        assert_eq!(config_from_hadoop(&props, "special").block_size, 32 * 1024 * 1024);
        for value in ["0", "-1", "9223372036854775808", "18446744073709551616", "garbage"] {
            let props = HashMap::from([("fs.obs.block.size".into(), value.into())]);
            assert_eq!(config_from_hadoop(&props, "bucket").block_size, ObsConfig::default().block_size);
        }
    }

    #[test]
    fn retry_settings_support_bucket_overrides_and_zero() {
        let props = HashMap::from([
            ("fs.obs.retry.max.attempts".into(), "3".into()),
            ("fs.obs.retry.backoff.ms".into(), "25".into()),
            ("fs.obs.request.timeout.ms".into(), "2000".into()),
            (
                "fs.obs.bucket.special.request.timeout.ms".into(),
                "0".into(),
            ),
            (
                "fs.obs.bucket.special.retry.max.attempts".into(),
                "0".into(),
            ),
            ("fs.obs.bucket.special.retry.backoff.ms".into(), "0".into()),
        ]);
        let common = config_from_hadoop(&props, "other");
        assert_eq!(common.request_timeout_ms, 2000);
        assert_eq!(
            (common.retry_max_attempts, common.retry_backoff_ms),
            (3, 25)
        );
        let special = config_from_hadoop(&props, "special");
        assert_eq!(special.request_timeout_ms, 0);
        assert_eq!(
            (special.retry_max_attempts, special.retry_backoff_ms),
            (0, 0)
        );
    }

    #[test]
    fn invalid_retry_settings_keep_defaults_without_wrapping() {
        for value in ["-1", "4294967296", "18446744073709551616", "1s", "garbage"] {
            let props = HashMap::from([("fs.obs.retry.max.attempts".into(), value.into())]);
            assert_eq!(config_from_hadoop(&props, "bucket").retry_max_attempts, 2);
        }
        for value in ["-1", "18446744073709551616", "1s"] {
            let props = HashMap::from([("fs.obs.retry.backoff.ms".into(), value.into())]);
            assert_eq!(config_from_hadoop(&props, "bucket").retry_backoff_ms, 100);
        }
    }
}

#[cfg(test)]
mod semaphore_tests {
    use super::*;

    #[test]
    fn permit_bounds_preserve_zero_and_accept_the_exact_limit() {
        for count in [0, 1, Semaphore::MAX_PERMITS] {
            let config = ObsConfig {
                connection_maximum: count,
                upload_active_blocks: count,
                upload_total_active_blocks: count,
                readahead_total_chunks: count,
                ..Default::default()
            };
            config.validate_semaphore_limits().unwrap();
            assert_eq!(bounded_permits(count), count);
        }
        assert_eq!(bounded_permits(usize::MAX), Semaphore::MAX_PERMITS);
    }

    #[test]
    fn effective_bucket_permit_limits_are_validated() {
        for key in [
            "connection.maximum",
            "upload.active.blocks",
            "upload.total.active.blocks",
            "readahead.total.chunks",
        ] {
            let props = HashMap::from([
                (format!("fs.obs.{key}"), "1".into()),
                (format!("fs.obs.bucket.special.{key}"), usize::MAX.to_string()),
            ]);
            config_from_hadoop(&props, "other").validate_semaphore_limits().unwrap();
            let error = config_from_hadoop(&props, "special")
                .validate_semaphore_limits().unwrap_err();
            assert!(matches!(error, ObsError::InvalidArgument(ref message) if message.contains(key)));
        }
    }
}

#[cfg(test)]
mod upload_capacity_tests {
    use super::*;

    #[test]
    fn upload_capacity_normalizes_minima_and_checks_platform_boundaries() {
        let mut config = ObsConfig {
            multipart_size: 0,
            multipart_threshold: 0,
            ..Default::default()
        };
        assert_eq!(config.upload_buffer_sizes().unwrap(), (5 * 1024 * 1024, 1));
        for value in [isize::MAX as u64, isize::MAX as u64 + 1, u64::MAX] {
            config.multipart_size = value;
            config.multipart_threshold = value;
            let result = config.upload_buffer_sizes();
            if value == isize::MAX as u64 {
                assert_eq!(result.unwrap(), (isize::MAX as usize, isize::MAX as usize));
            } else {
                assert!(matches!(result, Err(ObsError::InvalidArgument(_))));
            }
        }
    }
}
