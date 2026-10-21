use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;

use crate::error::ObsResult;
use crate::types::{CompletedPart, InodeHint, ListPage, ObjectMeta};

/// Async object storage interface — all OBS wire protocol operations.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Some means GET bodies already carry leases from this shared budget.
    /// Custom stores returning None are accounted by the input collector instead.
    fn buffer_budget(&self) -> Option<std::sync::Arc<crate::io::budget::BufferBudget>> { None }
    // ── Core ──
    async fn head(&self, key: &str) -> ObsResult<ObjectMeta>;
    async fn get(
        &self,
        key: &str,
        range: Option<(u64, u64)>,
        inode_hint: Option<&InodeHint>,
    ) -> ObsResult<GetResult>;
    /// Backends should enforce the condition server-side when supported. The
    /// default still rejects changed/missing response revisions before reading.
    async fn get_with_etag(
        &self,
        key: &str,
        range: Option<(u64, u64)>,
        inode_hint: Option<&InodeHint>,
        expected_etag: Option<&str>,
    ) -> ObsResult<GetResult> {
        let result = self.get(key, range, inode_hint).await?;
        result.validate_etag(key, expected_etag)?;
        Ok(result)
    }
    async fn put(&self, key: &str, body: Bytes, headers: PutHeaders) -> ObsResult<ObjectMeta>;
    async fn put_empty(&self, key: &str) -> ObsResult<ObjectMeta>;
    async fn delete(&self, key: &str, hdfs_header: bool) -> ObsResult<()>;
    async fn delete_batch(&self, keys: &[String]) -> ObsResult<()>;
    async fn list(
        &self,
        prefix: &str,
        delimiter: &str,
        marker: Option<&str>,
        max_keys: usize,
    ) -> ObsResult<ListPage>;
    async fn copy(&self, src_key: &str, dst_key: &str) -> ObsResult<ObjectMeta>;

    // ── MPU ──
    async fn initiate_multipart_upload(&self, key: &str) -> ObsResult<String>;
    async fn initiate_multipart_upload_with_headers(
        &self,
        key: &str,
        headers: PutHeaders,
    ) -> ObsResult<String> {
        if headers.content_type.is_some() || !headers.user_meta.is_empty() || headers.hdfs_client {
            return Err(crate::error::ObsError::Unsupported(
                "backend does not support multipart metadata".into(),
            ));
        }
        self.initiate_multipart_upload(key).await
    }
    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: Bytes,
    ) -> ObsResult<String>;
    async fn complete_multipart_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[CompletedPart],
    ) -> ObsResult<ObjectMeta>;
    async fn abort_multipart_upload(&self, key: &str, upload_id: &str) -> ObsResult<()>;

    // ── PFS ──
    async fn posix_rename(&self, src_key: &str, dst_key: &str) -> ObsResult<()>;
    async fn posix_truncate(&self, key: &str, length: u64) -> ObsResult<()>;
    async fn posix_append(&self, key: &str, position: u64, body: Bytes) -> ObsResult<()>;
    async fn create_folder(&self, key: &str) -> ObsResult<()>;
    async fn set_object_metadata(
        &self,
        key: &str,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> ObsResult<()>;

    // ── Meta ──
    async fn patch_user_metadata(
        &self,
        _key: &str,
        _updates: std::collections::HashMap<String, String>,
    ) -> ObsResult<()> {
        Err(crate::error::ObsError::Unsupported("user metadata updates".into()))
    }

    /// Replace the credentials used to sign subsequent requests.
    fn update_credentials(&self, _creds: crate::auth::Credentials) -> ObsResult<()> {
        Err(crate::error::ObsError::Unsupported("credential updates".into()))
    }

    // ── PFS directory access labels (`?x-obs-accesslabel`) ──
    async fn set_access_label(&self, _dir_key: &str, _labels: &[String]) -> ObsResult<()> {
        Err(crate::error::ObsError::Unsupported("access labels".into()))
    }
    async fn get_access_label(&self, _dir_key: &str) -> ObsResult<Vec<String>> {
        Err(crate::error::ObsError::Unsupported("access labels".into()))
    }
    async fn delete_access_label(&self, _dir_key: &str) -> ObsResult<()> {
        Err(crate::error::ObsError::Unsupported("access labels".into()))
    }

    async fn probe_posix_bucket(&self) -> ObsResult<bool>;
    fn is_pfs(&self) -> bool;
}

/// GET result with streaming body.
pub struct GetResult {
    pub meta: ObjectMeta,
    pub body: Pin<Box<dyn Stream<Item = ObsResult<Bytes>> + Send>>,
}

impl GetResult {
    pub fn validate_etag(&self, key: &str, expected: Option<&str>) -> ObsResult<()> {
        if let Some(expected) = expected {
            let actual = self
                .meta
                .etag
                .as_deref()
                .filter(|tag| !tag.trim_matches('"').is_empty())
                .ok_or_else(|| crate::error::ObsError::MissingVersion(key.into()))?;
            if actual.trim_matches('"') != expected.trim_matches('"') {
                return Err(crate::error::ObsError::RemoteFileChanged(key.into()));
            }
        }
        Ok(())
    }
}

/// Optional headers for PUT.
#[derive(Debug, Clone, Default)]
pub struct PutHeaders {
    pub content_type: Option<String>,
    pub user_meta: Vec<(String, String)>,
    pub hdfs_client: bool,
}
