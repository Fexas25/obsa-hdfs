use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use bytes::Bytes;
use chrono::Utc;
use futures::stream;
use http::Method;
use md5::{Digest, Md5};

use crate::auth::{CredentialChain, ObsSigner};
use crate::config::ObsConfig;
use crate::error::{conflict_codes, ObsError, ObsResult};
use crate::pfs::inode::InodeState;
use crate::store::traits::{GetResult, ObjectStore, PutHeaders};
use crate::transport::{HttpPool, RequestBuilder};
use crate::types::{BucketMode, CompletedPart, InodeHint, ListPage, ObjectMeta};
use crate::xml::{self, build_complete_mpu_xml, build_delete_xml};

/// OBS ObjectStore implementation with V2 signing and PFS support.
pub struct ObsStore {
    config: ObsConfig,
    pool: Arc<HttpPool>,
    creds: CredentialChain,
    is_pfs: bool,
    inode_state: InodeState,
}

impl ObsStore {
    pub fn new(config: ObsConfig, creds: CredentialChain) -> Self {
        let pool = Arc::new(HttpPool::new(config.clone()));
        Self {
            config,
            pool,
            creds,
            is_pfs: false,
            inode_state: InodeState::new(),
        }
    }

    pub async fn init_bucket_mode(&mut self) -> ObsResult<()> {
        match self.config.bucket_mode {
            BucketMode::Pfs => self.is_pfs = true,
            BucketMode::Object => self.is_pfs = false,
            BucketMode::Auto => {
                self.is_pfs = self.probe_posix_bucket().await?;
            }
        }
        Ok(())
    }

    async fn signed_execute(
        &self,
        builder: RequestBuilder,
        content_md5: &str,
        content_type: &str,
    ) -> ObsResult<crate::transport::pool::HttpResponse> {
        crate::transport::with_request_timeout(
            self.config.request_timeout_ms,
            self.signed_execute_retries(builder, content_md5, content_type),
        )
        .await
    }

    async fn signed_execute_retries(
        &self,
        builder: RequestBuilder,
        content_md5: &str,
        content_type: &str,
    ) -> ObsResult<crate::transport::pool::HttpResponse> {
        let uri = builder
            .build_uri()
            .map_err(|e| ObsError::InvalidArgument(e.to_string()))?;
        let (_, method, bucket, key, sub_resources, mut obs_headers, hws_headers, body, idempotent) =
            builder.into_parts();
        if !content_type.is_empty() {
            obs_headers.insert("Content-Type".into(), content_type.into());
        }
        let canonical_bucket =
            if self.config.addressing_style == crate::types::AddressingStyle::Cname {
                uri.host().unwrap_or(&bucket)
            } else {
                &bucket
            };
        let canonical = ObsSigner::canonical_resource(
            canonical_bucket,
            &key,
            &sub_resources
                .iter()
                .map(|(n, v)| (n.as_str(), v.as_deref()))
                .collect::<Vec<_>>(),
        );
        let policy = crate::transport::RetryPolicy::new(&self.config, idempotent);
        for attempt in 0..=policy.max_retries {
            // Each attempt obtains credentials and signs a fresh Date.
            let signed = ObsSigner::sign(
                method.as_str(),
                content_md5,
                content_type,
                &ObsSigner::rfc7231_date(Utc::now()),
                &obs_headers,
                &canonical,
                &self.creds.get(),
            );
            let mut headers = signed.headers;
            headers.extend(hws_headers.clone());
            let result = self
                .pool
                .execute_once(&method, uri.clone(), &headers, body.clone())
                .await;
            if !policy.should_retry(attempt, &result) {
                return result;
            }
            let server_delay = crate::transport::response_retry_after(&result);
            drop(result); // Do not retain response buffers during backoff.
            policy.wait(attempt + 1, server_delay).await?;
        }
        unreachable!("retry loop always returns on final attempt")
    }

    fn map_response(
        &self,
        resp: crate::transport::pool::HttpResponse,
    ) -> ObsResult<crate::transport::pool::HttpResponse> {
        if let Some(id) = resp.header("x-hws-fs-dht-id").and_then(|v| v.parse().ok()) {
            self.inode_state.update_dht(id);
        }
        let status = resp.status.as_u16();
        if resp.status.is_success() || status == 204 {
            return Ok(resp);
        }

        let (code, message) = if !resp.body.is_empty() {
            xml::parse_error_xml(std::str::from_utf8(&resp.body).unwrap_or(""))
        } else {
            (String::new(), format!("HTTP {status}"))
        };

        Err(ObsError::from_status(
            status,
            code,
            message,
            resp.request_id,
        ))
    }

    fn normalize_user_metadata(
        metadata: impl IntoIterator<Item = (String, String)>,
    ) -> ObsResult<std::collections::HashMap<String, String>> {
        let entries = metadata.into_iter();
        let mut normalized = std::collections::HashMap::with_capacity(entries.size_hint().0);
        for (name, value) in entries {
            // Validate before a metadata patch reads remote state. Keep errors
            // independent of user names/values, which may contain private data.
            http::HeaderName::from_bytes(format!("x-obs-meta-{name}").as_bytes())
                .map_err(|_| ObsError::InvalidArgument("invalid metadata name".into()))?;
            http::HeaderValue::from_str(&value)
                .map_err(|_| ObsError::InvalidArgument("invalid metadata value".into()))?;
            if normalized.insert(name.to_ascii_lowercase(), value).is_some() {
                return Err(ObsError::InvalidArgument("duplicate metadata key (case-insensitive)".into()));
            }
        }
        Ok(normalized)
    }

    fn meta_from_headers(key: &str, headers: &BTreeMap<String, String>) -> ObsResult<ObjectMeta> {
        let get = |name: &str| -> Option<String> {
            headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.clone())
        };

        let mut user_meta = std::collections::HashMap::new();
        for (k, v) in headers {
            let lower = k.to_ascii_lowercase();
            // Remove only the protocol prefix: the user key can start with it too.
            if let Some(name) = lower.strip_prefix("x-obs-meta-") {
                user_meta.insert(name.to_string(), v.clone());
            }
        }

        let etag = get("etag").map(|value| {
            crate::types::unquote_etag(&value).map(str::to_owned)
                .ok_or_else(|| ObsError::MissingVersion(format!("malformed ETag for {key}")))
        }).transpose()?;
        Ok(ObjectMeta {
            key: key.to_string(),
            content_length: get("content-length")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
            etag,
            last_modified: get("last-modified"),
            content_type: get("content-type"),
            mode: get("x-obs-meta-mode").and_then(|s| u32::from_str_radix(&s, 8).ok()),
            uid: get("x-obs-meta-uid").and_then(|s| s.parse().ok()),
            gid: get("x-obs-meta-gid").and_then(|s| s.parse().ok()),
            inode_no: get("x-hws-fs-inode-no").and_then(|s| s.parse().ok()),
            dentry_name: get("x-hws-fs-inode-dentryname"),
            dht_id: get("x-hws-fs-dht-id").and_then(|s| s.parse().ok()),
            crc32c: get("x-obs-checksum-crc32c").and_then(|value| Self::parse_crc32c(&value)),
            user_meta,
        })
    }

    /// The Java OBSA reference parses an unsigned decimal value; S3-style
    /// services send base64 of the 4 big-endian bytes. Anything else is
    /// treated as absent rather than guessed.
    fn parse_crc32c(value: &str) -> Option<u32> {
        let value = value.trim();
        if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) {
            return value.parse::<u64>().ok().and_then(|n| u32::try_from(n).ok());
        }
        let bytes = BASE64.decode(value).ok()?;
        Some(u32::from_be_bytes(bytes.try_into().ok()?))
    }

    fn apply_inode_hint(
        builder: RequestBuilder,
        hint: Option<&InodeHint>,
        state: &InodeState,
    ) -> RequestBuilder {
        let mut b = builder;
        if let Some(h) = hint {
            b = b
                .hws_fs_header("x-hws-fs-inode-no", h.inode_no.to_string())
                .hws_fs_header("x-hws-fs-inode-dentryname", h.dentry_name.clone());
        }
        if let Some(dht) = state.dht_id() {
            b = b.hws_fs_header("x-hws-fs-dht-id", dht.to_string());
        }
        b
    }

    fn apply_head_put_inode_headers(builder: RequestBuilder) -> RequestBuilder {
        builder.hws_fs_header("x-hws-fs-client", "true")
    }
}

#[async_trait]
impl ObjectStore for ObsStore {
    fn buffer_budget(&self) -> Option<Arc<crate::io::budget::BufferBudget>> {
        Some(self.pool.buffer_budget())
    }
    async fn head(&self, key: &str) -> ObsResult<ObjectMeta> {
        let builder = RequestBuilder::new(&self.config, &self.config.bucket, Method::HEAD).key(key);
        let builder = Self::apply_head_put_inode_headers(builder);

        let resp = self.signed_execute(builder, "", "").await?;
        let resp = self.map_response(resp)?;

        if resp.status != http::StatusCode::OK {
            return Err(ObsError::Io(format!(
                "unexpected status {} for object HEAD (expected 200)", resp.status.as_u16())));
        }
        // A successful HEAD has no body from which to infer the object length.
        // Missing metadata must not turn a nonempty object into an empty stream.
        if resp.content_length().is_none() {
            return Err(ObsError::Io("HEAD response missing or invalid Content-Length".into()));
        }
        let meta = Self::meta_from_headers(key, &resp.headers)?;
        if let Some(dht) = meta.dht_id {
            self.inode_state.update_dht(dht);
        }
        Ok(meta)
    }

    async fn get(
        &self,
        key: &str,
        range: Option<(u64, u64)>,
        inode_hint: Option<&InodeHint>,
    ) -> ObsResult<GetResult> {
        self.get_with_etag(key, range, inode_hint, None).await
    }

    async fn get_with_etag(
        &self,
        key: &str,
        range: Option<(u64, u64)>,
        inode_hint: Option<&InodeHint>,
        expected_etag: Option<&str>,
    ) -> ObsResult<GetResult> {
        let mut builder =
            RequestBuilder::new(&self.config, &self.config.bucket, Method::GET).key(key);

        if let Some(etag) = expected_etag {
            let tag = etag
                .strip_prefix('"')
                .and_then(|s| s.strip_suffix('"'))
                .unwrap_or(etag);
            if tag.is_empty() || tag.bytes().any(|b| b < 0x21 || b == b'"' || b == 0x7f) {
                return Err(ObsError::MissingVersion(
                    "invalid strong ETag for If-Match".into(),
                ));
            }
            // RFC 9110 entity-tags are quoted opaque values, not bare digests.
            builder = builder.header("If-Match", format!("\"{tag}\""));
        }

        if let Some((start, end)) = range {
            if end < start {
                return Err(ObsError::InvalidArgument("GET range end precedes start".into()));
            }
            builder = builder.header("Range", format!("bytes={start}-{end}"));
        }

        builder = Self::apply_inode_hint(builder, inode_hint, &self.inode_state);

        let resp = self.signed_execute(builder, "", "").await?;
        if expected_etag.is_some() && resp.status.as_u16() == 412 {
            return Err(ObsError::RemoteFileChanged(format!(
                "{key} (request_id={})",
                resp.request_id
            )));
        }
        let resp = self.map_response(resp)?;

        if range.is_none() && resp.status != http::StatusCode::OK {
            return Err(ObsError::Io(format!(
                "unexpected status {} for full object GET (expected 200)", resp.status.as_u16())));
        }
        let mut meta = Self::meta_from_headers(key, &resp.headers)?;
        // The transport has already decoded and collected the complete body.
        // Content-Length may be absent for chunked or close-delimited responses.
        meta.content_length = resp.body.len() as u64;
        let body_bytes = resp.body;
        let stream = stream::once(async move { Ok(body_bytes) });

        let result = GetResult {
            meta,
            body: Box::pin(stream),
        };
        result.validate_etag(key, expected_etag)?;
        Ok(result)
    }

    async fn put(&self, key: &str, body: Bytes, headers: PutHeaders) -> ObsResult<ObjectMeta> {
        let metadata = Self::normalize_user_metadata(headers.user_meta)?;
        let mut builder =
            RequestBuilder::new(&self.config, &self.config.bucket, Method::PUT).key(key);

        if headers.hdfs_client {
            builder = builder.obs_header("x-obs-hdfs", "true");
        }
        if let Some(ct) = &headers.content_type {
            builder = builder.header("Content-Type", ct.clone());
        }
        for (k, v) in &metadata {
            builder = builder.obs_header(format!("x-obs-meta-{k}"), v.clone());
        }

        builder = Self::apply_head_put_inode_headers(builder);
        let md5_hash = Md5::digest(&body);
        let content_md5 = BASE64.encode(md5_hash);
        builder = builder.header("Content-MD5", content_md5.clone());

        // PUT response Content-Length describes the response, not the object.
        let content_length = body.len() as u64;
        let ct = headers.content_type.as_deref().unwrap_or("");
        let resp = self
            .signed_execute(builder.body(body), &content_md5, ct)
            .await?;
        let resp = self.map_response(resp)?;
        let mut meta = Self::meta_from_headers(key, &resp.headers)?;
        meta.content_length = content_length;
        // The response describes its own entity, not the uploaded object.
        meta.content_type = headers.content_type;
        Ok(meta)
    }

    async fn put_empty(&self, key: &str) -> ObsResult<ObjectMeta> {
        self.put(key, Bytes::new(), PutHeaders::default()).await
    }

    async fn delete(&self, key: &str, hdfs_header: bool) -> ObsResult<()> {
        let mut builder =
            RequestBuilder::new(&self.config, &self.config.bucket, Method::DELETE).key(key);

        if hdfs_header {
            builder = builder.obs_header("x-obs-hdfs", "true");
        }

        let resp = self.signed_execute(builder, "", "").await?;
        match self.map_response(resp) {
            Ok(_) => Ok(()),
            // A missing bucket (or an unclassified 404) is not proof that
            // deletion of the requested object succeeded.
            Err(error) if !hdfs_header && error.is_not_found()
                && error.code() == Some("NoSuchKey") => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn delete_batch(&self, keys: &[String]) -> ObsResult<()> {
        if keys.is_empty() {
            return Ok(());
        }

        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let xml_body = build_delete_xml(&key_refs);
        let body = Bytes::from(xml_body);
        let md5_hash = Md5::digest(&body);
        let content_md5 = BASE64.encode(md5_hash);

        let builder = RequestBuilder::new(&self.config, &self.config.bucket, Method::POST)
            .key("")
            .sub_resource("delete", None)
            .header("Content-Type", "application/xml")
            .header("Content-MD5", content_md5.clone())
            .body(body);

        let resp = self
            .signed_execute(builder, &content_md5, "application/xml")
            .await?;
        if resp.status.as_u16() == 501 {
            // The fallback no longer needs this response. Return its shared
            // body budget before issuing any individual delete requests.
            drop(resp);
            for key in keys {
                match self.delete(key, self.is_pfs()).await {
                    Ok(()) => {}
                    Err(e) if e.is_not_found() && e.code() == Some("NoSuchKey") => {}
                    Err(e) => return Err(e),
                }
            }
            return Ok(());
        }
        let resp = self.map_response(resp)?;
        let (code, message) = xml::parse_error_xml(std::str::from_utf8(&resp.body).unwrap_or(""));
        if !code.is_empty() {
            return Err(ObsError::Http {
                status: 200,
                code,
                message,
                request_id: resp.request_id,
            });
        }
        if xml::delete_result_has_errors(
            std::str::from_utf8(&resp.body)
                .map_err(|_| ObsError::Io("invalid batch delete response encoding".into()))?,
        )? {
            return Err(ObsError::Io(
                "batch delete contains an error without an error code".into(),
            ));
        }
        Ok(())
    }

    async fn list(
        &self,
        prefix: &str,
        delimiter: &str,
        marker: Option<&str>,
        max_keys: usize,
    ) -> ObsResult<ListPage> {
        let mut builder =
            RequestBuilder::new(&self.config, &self.config.bucket, Method::GET).key("");

        builder = builder
            .sub_resource("prefix", Some(prefix.to_string()))
            .sub_resource("delimiter", Some(delimiter.to_string()))
            .sub_resource("max-keys", Some(max_keys.to_string()));

        if let Some(m) = marker {
            builder = builder.sub_resource("marker", Some(m.to_string()));
        }

        let resp = self.signed_execute(builder, "", "").await?;
        let resp = self.map_response(resp)?;

        let parsed = xml::parse_list_xml(std::str::from_utf8(&resp.body).unwrap_or(""))?;
        Ok(ListPage {
            contents: parsed.contents,
            common_prefixes: parsed.common_prefixes,
            is_truncated: parsed.is_truncated,
            next_marker: parsed.next_marker,
        })
    }

    async fn copy(&self, src_key: &str, dst_key: &str) -> ObsResult<ObjectMeta> {
        let builder = RequestBuilder::new(&self.config, &self.config.bucket, Method::PUT)
            .key(dst_key)
            .obs_header(
                "x-obs-copy-source",
                format!(
                    "/{}/{}",
                    self.config.bucket,
                    src_key
                        .split('/')
                        .map(|part| urlencoding::encode(part).into_owned())
                        .collect::<Vec<_>>()
                        .join("/")
                ),
            );

        let resp = self.signed_execute(builder, "", "").await?;
        let resp = self.map_response(resp)?;
        let text = std::str::from_utf8(&resp.body)
            .map_err(|_| ObsError::Io("invalid copy response encoding".into()))?;
        let (code, message) = xml::parse_error_xml(text);
        if !code.is_empty() {
            return Err(ObsError::Http {
                status: resp.status.as_u16(),
                code,
                message,
                request_id: resp.request_id,
            });
        }
        let etag = xml::parse_result_etag(text, "CopyObjectResult")?;
        let mut meta = Self::meta_from_headers(dst_key, &resp.headers)?;
        meta.content_length = 0; // Response XML length is not copied object length.
        meta.content_type = None; // XML response type is not the copied object type.
        meta.etag = Some(etag);
        Ok(meta)
    }

    async fn initiate_multipart_upload(&self, key: &str) -> ObsResult<String> {
        self.initiate_multipart_upload_with_headers(key, PutHeaders::default())
            .await
    }
    async fn initiate_multipart_upload_with_headers(
        &self,
        key: &str,
        headers: PutHeaders,
    ) -> ObsResult<String> {
        let metadata = Self::normalize_user_metadata(headers.user_meta)?;
        let mut builder = RequestBuilder::new(&self.config, &self.config.bucket, Method::POST)
            .key(key)
            .sub_resource("uploads", None)
            .idempotent(false);
        if headers.hdfs_client {
            builder = builder.obs_header("x-obs-hdfs", "true");
        }
        for (key, value) in &metadata {
            builder = builder.obs_header(format!("x-obs-meta-{key}"), value);
        }
        let resp = self
            .signed_execute(builder, "", headers.content_type.as_deref().unwrap_or(""))
            .await?;
        let resp = self.map_response(resp)?;
        xml::parse_initiate_mpu_xml(std::str::from_utf8(&resp.body).unwrap_or(""))
    }

    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: Bytes,
    ) -> ObsResult<String> {
        if !(1..=10_000).contains(&part_number) {
            return Err(ObsError::InvalidArgument("multipart part number must be 1..=10000".into()));
        }
        let md5_hash = Md5::digest(&body);
        let content_md5 = BASE64.encode(md5_hash);

        let builder = RequestBuilder::new(&self.config, &self.config.bucket, Method::PUT)
            .key(key)
            .sub_resource("partNumber", Some(part_number.to_string()))
            .sub_resource("uploadId", Some(upload_id.to_string()))
            .header("Content-MD5", content_md5.clone())
            .body(body);

        let resp = self
            .signed_execute(builder, &content_md5, "application/octet-stream")
            .await?;
        let resp = self.map_response(resp)?;
        // A successful status alone cannot supply a part for CompleteMultipartUpload.
        // Preserve opaque nonempty values; never manufacture a missing ETag.
        resp.etag()
            .filter(|etag| !etag.trim().is_empty())
            .ok_or_else(|| ObsError::Io("UploadPart response is missing a non-empty ETag".into()))
    }

    async fn complete_multipart_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[CompletedPart],
    ) -> ObsResult<ObjectMeta> {
        // Reject malformed manifests before allocating XML or sending a commit request.
        if parts.is_empty() {
            return Err(ObsError::InvalidArgument("multipart completion requires at least one part".into()));
        }
        let mut previous = 0;
        for part in parts {
            if !(1..=10_000).contains(&part.part_number) || part.part_number <= previous {
                return Err(ObsError::InvalidArgument(
                    "multipart completion part numbers must be strictly increasing in 1..=10000".into()));
            }
            if part.etag.trim().is_empty() {
                return Err(ObsError::InvalidArgument("multipart completion requires non-empty ETags".into()));
            }
            previous = part.part_number;
        }
        let part_tuples: Vec<(i32, &str)> = parts
            .iter()
            .map(|p| (p.part_number, p.etag.as_str()))
            .collect();
        let xml = build_complete_mpu_xml(&part_tuples);
        let body = Bytes::from(xml);

        let builder = RequestBuilder::new(&self.config, &self.config.bucket, Method::POST)
            .key(key)
            .sub_resource("uploadId", Some(upload_id.to_string()))
            .header("Content-Type", "application/xml")
            .body(body)
            .idempotent(false);

        let resp = self.signed_execute(builder, "", "application/xml").await?;
        let resp = self.map_response(resp)?;
        let text = std::str::from_utf8(&resp.body).unwrap_or("");
        let (code, message) = xml::parse_error_xml(text);
        if !code.is_empty() {
            return Err(ObsError::Http {
                status: 200,
                code,
                message,
                request_id: resp.request_id,
            });
        }
        let etag = xml::parse_result_etag(text, "CompleteMultipartUploadResult")?;
        let mut meta = Self::meta_from_headers(key, &resp.headers)?;
        // Part sizes are unavailable here; the output stream supplies the total.
        // As with copy, never expose the response XML length as object size.
        meta.content_length = 0;
        meta.content_type = None; // The result XML does not report object Content-Type.
        meta.etag = Some(etag);
        Ok(meta)
    }

    async fn abort_multipart_upload(&self, key: &str, upload_id: &str) -> ObsResult<()> {
        let builder = RequestBuilder::new(&self.config, &self.config.bucket, Method::DELETE)
            .key(key)
            .sub_resource("uploadId", Some(upload_id.to_string()));

        let resp = self.signed_execute(builder, "", "").await?;
        self.map_response(resp).map(|_| ())
    }

    async fn posix_rename(&self, src_key: &str, dst_key: &str) -> ObsResult<()> {
        let builder = RequestBuilder::new(&self.config, &self.config.bucket, Method::POST)
            .key(src_key)
            .sub_resource("rename", None)
            .sub_resource("name", Some(dst_key.to_string()))
            .obs_header("x-obs-hdfs", "true")
            .idempotent(false);

        let resp = self.signed_execute(builder, "", "").await?;
        self.map_response(resp).map(|_| ())
    }

    async fn posix_truncate(&self, key: &str, length: u64) -> ObsResult<()> {
        let builder = RequestBuilder::new(&self.config, &self.config.bucket, Method::PUT)
            .key(key)
            .sub_resource("truncate", None)
            .sub_resource("length", Some(length.to_string()))
            .obs_header("x-obs-hdfs", "true");

        let resp = self.signed_execute(builder, "", "").await?;
        self.map_response(resp).map(|_| ())
    }

    async fn posix_append(&self, key: &str, position: u64, body: Bytes) -> ObsResult<()> {
        let builder = RequestBuilder::new(&self.config, &self.config.bucket, Method::PUT)
            .key(key)
            .sub_resource("modify", None)
            .sub_resource("position", Some(position.to_string()))
            .obs_header("x-obs-hdfs", "true")
            .body(body);

        let resp = self
            .signed_execute(builder, "", "application/octet-stream")
            .await?;
        self.map_response(resp).map(|_| ())
    }

    async fn create_folder(&self, key: &str) -> ObsResult<()> {
        let folder_key = if key.ends_with('/') {
            key.to_string()
        } else {
            format!("{key}/")
        };

        let builder = RequestBuilder::new(&self.config, &self.config.bucket, Method::PUT)
            .key(&folder_key)
            .sub_resource("folder", None)
            .obs_header("x-obs-hdfs", "true");

        let resp = self.signed_execute(builder, "", "").await?;
        let status = resp.status.as_u16();

        if status == 200 {
            return Ok(());
        }

        if status == 409 {
            let (code, _) = xml::parse_error_xml(std::str::from_utf8(&resp.body).unwrap_or(""));
            return match code.as_str() {
                conflict_codes::DIRECTORY_ALREADY_EXISTS => Ok(()),
                conflict_codes::FILE_ALREADY_EXISTS => Err(ObsError::AlreadyExists(format!(
                    "FileAlreadyExists: {folder_key}"
                ))),
                conflict_codes::PARENT_IS_FILE => Err(ObsError::ParentNotDirectory(format!(
                    "parent is file: {folder_key}"
                ))),
                _ => Err(ObsError::from_status(
                    status,
                    code,
                    "mkdirs conflict".into(),
                    resp.request_id,
                )),
            };
        }

        self.map_response(resp).map(|_| ())
    }

    async fn set_object_metadata(
        &self,
        key: &str,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> ObsResult<()> {
        // REPLACE must carry forward unrelated user metadata. Read fresh values;
        // cached FileStatus is incomplete and may predate another writer.
        let current = self.head(key).await?;
        let mut metadata = current.user_meta;
        if let Some(mode) = mode {
            let mode = (current.mode.unwrap_or(0) & !0o7777) | (mode & 0o7777);
            metadata.insert("mode".into(), format!("{mode:o}"));
        }
        if let Some(uid) = uid { metadata.insert("uid".into(), uid.to_string()); }
        if let Some(gid) = gid { metadata.insert("gid".into(), gid.to_string()); }
        let mut builder = RequestBuilder::new(&self.config, &self.config.bucket, Method::PUT)
            .key(key)
            .sub_resource("metadata", None)
            .obs_header("x-obs-hdfs", "true")
            .obs_header("x-obs-metadata-directive", "REPLACE");

        for (name, value) in metadata {
            builder = builder.obs_header(format!("x-obs-meta-{name}"), value);
        }

        let resp = self.signed_execute(builder, "", "").await?;
        self.map_response(resp).map(|_| ())
    }

    async fn patch_user_metadata(
        &self,
        key: &str,
        updates: std::collections::HashMap<String, String>,
    ) -> ObsResult<()> {
        let normalized = Self::normalize_user_metadata(updates)?;
        let current = match self.head(key).await {
            Err(error) if error.is_not_found() && !key.ends_with('/') => self.head(&format!("{key}/")).await?,
            result => result?,
        };
        let mut metadata = current.user_meta;
        metadata.extend(normalized);
        let mut builder = RequestBuilder::new(&self.config, &self.config.bucket, Method::PUT)
            .key(&current.key)
            .sub_resource("metadata", None)
            .obs_header("x-obs-hdfs", "true")
            .obs_header("x-obs-metadata-directive", "REPLACE");
        for (name, value) in metadata {
            builder = builder.obs_header(format!("x-obs-meta-{name}"), value);
        }
        let response = self.signed_execute(builder, "", "").await?;
        self.map_response(response).map(|_| ())
    }

    fn update_credentials(&self, creds: crate::auth::Credentials) -> ObsResult<()> {
        self.creds.update(creds);
        Ok(())
    }

    // Wire format follows the OBS Java SDK (ObsFileService.*AccessLabelFsImpl):
    // the directory key without trailing slash, the `x-obs-accesslabel`
    // sub-resource (signed: x-obs- prefix) and a {"accesslabel":[...]} body.
    async fn set_access_label(&self, dir_key: &str, labels: &[String]) -> ObsResult<()> {
        let body = Bytes::from(
            serde_json::to_vec(&serde_json::json!({ "accesslabel": labels }))
                .map_err(|e| ObsError::Io(e.to_string()))?,
        );
        let builder = RequestBuilder::new(&self.config, &self.config.bucket, Method::PUT)
            .key(dir_key)
            .sub_resource("x-obs-accesslabel", None)
            .body(body);
        let resp = self.signed_execute(builder, "", "application/json").await?;
        self.map_response(resp).map(|_| ())
    }

    async fn get_access_label(&self, dir_key: &str) -> ObsResult<Vec<String>> {
        let builder = RequestBuilder::new(&self.config, &self.config.bucket, Method::GET)
            .key(dir_key)
            .sub_resource("x-obs-accesslabel", None);
        let resp = self.signed_execute(builder, "", "").await?;
        let resp = self.map_response(resp)?;
        let value: serde_json::Value = serde_json::from_slice(&resp.body)
            .map_err(|_| ObsError::Io("invalid access label response".into()))?;
        value
            .get("accesslabel")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| ObsError::Io("access label response lacks accesslabel".into()))?
            .iter()
            .map(|label| label.as_str().map(str::to_owned)
                .ok_or_else(|| ObsError::Io("non-string access label".into())))
            .collect()
    }

    async fn delete_access_label(&self, dir_key: &str) -> ObsResult<()> {
        let builder = RequestBuilder::new(&self.config, &self.config.bucket, Method::DELETE)
            .key(dir_key)
            .sub_resource("x-obs-accesslabel", None);
        let resp = self.signed_execute(builder, "", "").await?;
        self.map_response(resp).map(|_| ())
    }

    async fn probe_posix_bucket(&self) -> ObsResult<bool> {
        let builder = Self::apply_head_put_inode_headers(
            RequestBuilder::new(&self.config, &self.config.bucket, Method::HEAD).key(""),
        );
        let resp = self.signed_execute(builder, "", "").await?;
        let resp = self.map_response(resp)?;
        // The OBS SDK's getBucketFSStatus (used by the Java OBSA reference)
        // classifies PFS solely by `x-obs-fs-file-interface: Enabled` on HEAD
        // bucket; accept the bucket-type header too where a service sends it.
        let file_interface = resp
            .header("x-obs-fs-file-interface")
            .is_some_and(|v| v.trim().eq_ignore_ascii_case("Enabled"));
        let posix_type = resp
            .header("x-obs-bucket-type")
            .is_some_and(|t| t.eq_ignore_ascii_case("POSIX") || t.eq_ignore_ascii_case("PFS"));
        Ok(file_interface || posix_type)
    }

    fn is_pfs(&self) -> bool {
        self.is_pfs
    }
}
