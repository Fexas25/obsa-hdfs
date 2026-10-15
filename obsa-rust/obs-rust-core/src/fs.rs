use crate::{
    auth::{CredentialChain, Credentials},
    cache::status::StatusCache,
    config::{bounded_permits, ObsConfig},
    error::{ObsError, ObsResult},
    io::{
        append_coordination::AppendCoordinator,
        budget::BufferBudget,
        journal::{UploadJournal, RecoveryReport},
        input::ObsInputStream,
        output::ObsOutputStream,
        pfs_append::{AppendBudget, PfsAppendStream},
    },
    listing::{list_all, list_entry_to_status, ListPager},
    pfs::ops::PfsOps,
    store::{obs::ObsStore, ObjectStore, PutHeaders},
    types::*,
};
use std::{collections::BTreeSet, sync::Arc};
use tokio::sync::Semaphore;

// Mutations can partially succeed, or be cancelled after the server commits.
// Invalidate on every exit, including errors and dropped futures.
struct MutationInvalidation {
    cache: Arc<StatusCache>,
    keys: Vec<String>,
}
impl MutationInvalidation {
    fn disarm(&mut self) {
        self.keys.clear();
    }
}
impl Drop for MutationInvalidation {
    fn drop(&mut self) {
        for key in &self.keys {
            self.cache.invalidate_prefix(key);
            self.cache.invalidate_ancestors(key);
        }
    }
}

// File-content mutations do not change directory membership. Invalidate only
// the affected key, without allocating a key list or scanning its prefix.
struct StatusInvalidation<'a> {
    cache: &'a StatusCache,
    key: &'a str,
}
impl Drop for StatusInvalidation<'_> {
    fn drop(&mut self) {
        self.cache.invalidate(self.key);
    }
}

/// Hadoop-compatible filesystem orchestration shared by native and JNI callers.
pub struct ObsFileSystem {
    store: Arc<dyn ObjectStore>,
    cache: Arc<StatusCache>,
    config: ObsConfig,
    pfs: PfsOps,
    buffer_budget: Arc<BufferBudget>,
    upload_journal: Arc<tokio::sync::Mutex<Option<Arc<UploadJournal>>>>,
    upload_gate: Arc<Semaphore>,
    append_budget: Arc<AppendBudget>,
    append_coordinator: Arc<AppendCoordinator>,
    prefetch_gate: Arc<Semaphore>,
    working_dir: String,
}
impl ObsFileSystem {
    pub async fn from_config(config: ObsConfig) -> ObsResult<Self> {
        if !matches!(config.permissions_mode.as_str(), "none" | "disguise") {
            return Err(ObsError::InvalidArgument("permissions.mode must be none or disguise".into()));
        }
        config.validate_semaphore_limits()?;
        config.upload_buffer_sizes()?;
        if config.endpoint.is_empty() || config.bucket.is_empty() {
            return Err(ObsError::InvalidArgument(
                "endpoint and bucket are required".into(),
            ));
        }
        let credentials = Credentials {
            access_key: config.access_key.clone(),
            secret_key: config.secret_key.clone(),
            session_token: config.session_token.clone(),
        };
        let mut store = ObsStore::new(config.clone(), CredentialChain::new(credentials));
        store.init_bucket_mode().await?;
        Ok(Self::with_store(config, Arc::new(store)))
    }
    pub fn with_store(config: ObsConfig, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            upload_journal: Arc::new(tokio::sync::Mutex::new(None)),
            cache: Arc::new(StatusCache::new(
                config.metadata_cache_ttl(),
                config.negative_cache_ttl(),
                config.metadata_cache_size,
            )),
            pfs: PfsOps::new(store.clone()),
            buffer_budget: store.buffer_budget().unwrap_or_else(|| BufferBudget::new(config.buffer_total_bytes)),
            store,
            upload_gate: Arc::new(Semaphore::new(bounded_permits(config.upload_total_active_blocks.max(1)))),
            prefetch_gate: Arc::new(Semaphore::new(bounded_permits(config.readahead_total_chunks))),
            append_budget: AppendBudget::new(&config),
            append_coordinator: Arc::new(AppendCoordinator::default()),
            config,
            working_dir: "/user/unknown".into(),
        }
    }
    pub fn is_pfs(&self) -> bool {
        self.store.is_pfs()
    }
    fn disguise_permissions(&self) -> bool {
        self.is_pfs() && self.config.permissions_mode == "disguise"
    }
    /// Hadoop may select its inherited traversal when the optimization is disabled.
    pub fn content_summary_enabled(&self) -> bool {
        self.config.content_summary_enable
    }
    pub fn default_block_size(&self) -> i64 {
        if self.config.block_size > 0 { self.config.block_size } else { 128 * 1024 * 1024 }
    }
    pub fn path_to_key(&self, path: &str) -> String {
        let path = if let Some(rest) = path.strip_prefix("obs://") {
            rest.find('/').map(|i| &rest[i..]).unwrap_or("/")
        } else {
            path
        };
        let absolute = if path.starts_with('/') {
            path.to_string()
        } else {
            format!("{}/{path}", self.working_dir)
        };
        let mut parts = Vec::new();
        for part in absolute.split('/') {
            match part {
                "" | "." => {}
                ".." => {
                    parts.pop();
                }
                _ => parts.push(part),
            }
        }
        parts.join("/")
    }
    fn key(&self, path: &str) -> ObsResult<String> {
        if let Some(rest) = path.strip_prefix("obs://") {
            if rest.split('/').next() != Some(self.config.bucket.as_str()) {
                return Err(ObsError::InvalidArgument(
                    "path belongs to another bucket".into(),
                ));
            }
        } else if path.contains("://") {
            return Err(ObsError::InvalidArgument(
                "unsupported filesystem scheme".into(),
            ));
        }
        Ok(self.path_to_key(path))
    }
    pub fn key_to_path(&self, key: &str) -> String {
        format!("/{}", key.trim_start_matches('/'))
    }
    pub fn working_directory(&self) -> &str {
        &self.working_dir
    }
    pub fn set_working_directory(&mut self, path: &str) -> ObsResult<()> {
        self.working_dir = self.key_to_path(&self.key(path)?);
        Ok(())
    }
    fn not_found(key: &str) -> ObsError {
        ObsError::NotFound {
            code: "NoSuchKey".into(),
            message: key.into(),
            request_id: String::new(),
        }
    }
    fn remember_at(&self, key: &str, mut status: FileStatus, epoch: u64) -> FileStatus {
        status.path = self.key_to_path(key);
        status.block_size = self.default_block_size();
        self.cache.put_if_unchanged(key, Some(status.clone()), epoch);
        status
    }
    fn meta_status(&self, key: &str, meta: ObjectMeta) -> FileStatus {
        let dir = meta.mode.map(|m| m & S_IFMT == S_IFDIR).unwrap_or(false)
            || meta.key.ends_with('/')
            || meta
                .user_meta
                .get(META_DIR_MARKER)
                .is_some_and(|v| v == "true");
        let mut status = if dir {
            FileStatus::dir(key, 0o755)
        } else {
            FileStatus::file(key, meta.content_length, 0o644)
        };
        status.permission = meta.mode.map(|m| m & 0o7777).unwrap_or(status.permission);
        status.owner = meta.uid.map(|n| n.to_string()).unwrap_or(status.owner);
        status.group = meta.gid.map(|n| n.to_string()).unwrap_or(status.group);
        if self.disguise_permissions() {
            if let Some(owner) = meta.user_meta.get("user") { status.owner = owner.clone(); }
            if let Some(group) = meta.user_meta.get("group") { status.group = group.clone(); }
            if let Some(permission) = meta.user_meta.get("permission").and_then(|p| p.parse::<u32>().ok()).filter(|p| *p <= 0o7777) {
                status.permission = permission;
            }
        }
        status.etag = meta.etag;
        status.inode_no = meta.inode_no;
        status.dentry_name = meta.dentry_name;
        status.modification_time = meta
            .last_modified
            .as_deref()
            .and_then(|s| chrono::DateTime::parse_from_rfc2822(s).ok())
            .map_or(0, |t| t.timestamp_millis());
        status
    }
    pub async fn get_file_status(&self, path: &str) -> ObsResult<FileStatus> {
        let key = self.key(path)?;
        if key.is_empty() {
            let mut status = FileStatus::dir("/", 0o755);
            status.block_size = self.default_block_size();
            return Ok(status);
        }
        self.ensure_visible_path(&key).await?;
        if let Some(cached) = self.cache.get(&key) {
            return cached.ok_or_else(|| Self::not_found(&key));
        }
        let epoch = self.cache.snapshot_epoch();
        match self.store.head(&key).await {
            Ok(meta) => return Ok(self.remember_at(&key, self.meta_status(&key, meta), epoch)),
            Err(e) if e.is_path_not_found() => {}
            Err(e) => return Err(e),
        }
        if !self.is_pfs() || self.disguise_permissions() {
            match self.store.head(&format!("{key}/")).await {
                Ok(meta) => return Ok(self.remember_at(&key, self.meta_status(&key, meta), epoch)),
                Err(e) if e.is_path_not_found() => {}
                Err(e) => return Err(e),
            }
        }
        let page = self.store.list(&format!("{key}/"), "/", None, 1).await?;
        if !page.contents.is_empty() || !page.common_prefixes.is_empty() {
            return Ok(self.remember_at(&key, FileStatus::dir(&key, 0o755), epoch));
        }
        self.cache.put_if_unchanged(&key, None, epoch);
        Err(Self::not_found(&key))
    }
    pub async fn exists(&self, path: &str) -> ObsResult<bool> {
        match self.get_file_status(path).await {
            Ok(_) => Ok(true),
            Err(e) if e.is_path_not_found() => Ok(false),
            Err(e) => Err(e),
        }
    }
    pub async fn open(&self, path: &str, options: OpenOptions) -> ObsResult<ObsInputStream> {
        let key = self.key(path)?;
        let status = match options.status_hint {
            Some(s) if self.key(&s.path)? == key => {
                self.ensure_visible_path(&key).await?;
                s
            },
            _ => self.get_file_status(path).await?,
        };
        if status.is_dir {
            return Err(ObsError::IsDirectory(path.into()));
        }
        // Never invent the dentry value from an inode number or a path.
        let hint = if self.config.pfs_inode_hints {
            InodeHint::new(status.inode_no, status.dentry_name)
        } else {
            None
        };
        Ok(ObsInputStream::with_gate(
            self.store.clone(),
            &self.config,
            key,
            status.length,
            hint,
            self.prefetch_gate.clone(),
        )
        .with_buffer_budget(self.buffer_budget.clone())
        .with_etag(status.etag))
    }
    async fn upload_journal(&self) -> ObsResult<Option<Arc<UploadJournal>>> {
        let Some(directory) = &self.config.upload_journal_dir else { return Ok(None); };
        let mut journal = self.upload_journal.clone().lock_owned().await;
        if journal.is_some() { return Ok(journal.clone()); }
        let namespace = serde_json::to_vec(&(
            &self.config.endpoint, &self.config.bucket, self.config.secure,
            self.config.path_style_access,
        )).map_err(|e| ObsError::Io(e.to_string()))?;
        let directory = directory.clone();
        // The worker owns both initialization serialization and result publication.
        // A cancelled waiter cannot trigger a competing open or lose a successful lock.
        tokio::task::spawn_blocking(move || -> ObsResult<Option<Arc<UploadJournal>>> {
            let opened = UploadJournal::open(std::path::Path::new(&directory), &namespace)?;
            *journal = Some(opened.clone());
            Ok(Some(opened))
        }).await.map_err(|e| ObsError::Io(format!("upload journal worker: {e}")))?
    }
    /// Explicitly abort inactive known MPUs from the configured local journal.
    /// Does not list a bucket, resume writes, or delete completed objects.
    pub async fn recover_pending_uploads(&self, limit: usize) -> ObsResult<RecoveryReport> {
        if !(1..=10_000).contains(&limit) {
            return Err(ObsError::InvalidArgument("recovery limit must be 1..=10000".into()));
        }
        self.upload_journal().await?.ok_or_else(|| ObsError::InvalidArgument(
            "fs.obs.upload.journal.dir is not configured".into()
        ))?.recover(self.store.as_ref(), limit).await
    }
    pub async fn create(&self, path: &str, options: CreateOptions) -> ObsResult<ObsOutputStream> {
        let journal = self.upload_journal().await?;
        let key = self.key(path)?;
        self.ensure_visible_path(&key).await?;
        if key.is_empty() {
            return Err(ObsError::AlreadyExists("root directory".into()));
        }
        // Validate directory conflicts before returning a buffered output
        // stream, including PFS. Cached status keeps this check local on hits.
        match self.get_file_status(path).await {
            Ok(status) if status.is_dir || !options.overwrite => {
                return Err(ObsError::AlreadyExists(path.into()))
            }
            Ok(_) => {}
            Err(e) if e.is_path_not_found() => {}
            Err(e) => return Err(e),
        }
        if let Some((parent, _)) = key.rsplit_once('/') {
            let parent_path = self.key_to_path(parent);
            if options.create_parent {
                self.mkdirs(&parent_path, 0o755).await?;
            } else if !self.get_file_status(&parent_path).await?.is_dir {
                return Err(ObsError::ParentNotDirectory(parent_path));
            }
        }
        let cache = self.cache.clone();
        let epoch = cache.snapshot_epoch();
        let callback_key = key.clone();
        let permission = options.permission;
        let block_size = self.default_block_size();
        // The callback owns this guard even when it is never invoked: dropping
        // a failed/cancelled stream invalidates potentially committed writes.
        let mut invalidation = MutationInvalidation {
            cache: cache.clone(),
            keys: vec![callback_key.clone()],
        };
        Ok(ObsOutputStream::new(
            self.store.clone(),
            &self.config,
            key,
            self.upload_gate.clone(),
        )
        .with_buffer_budget(self.buffer_budget.clone())
        .with_journal(journal)
        .with_headers(PutHeaders {
            user_meta: vec![("mode".into(), format!("{:o}", options.permission | S_IFREG))],
            hdfs_client: self.is_pfs(),
            ..Default::default()
        })
        .on_commit(move |meta| {
            let mut status = FileStatus::file(format!("/{callback_key}"), meta.content_length, permission);
            status.block_size = block_size;
            cache.complete_mutation(&callback_key, status, epoch);
            invalidation.disarm();
        }))
    }
    pub async fn mkdirs(&self, path: &str, permission: u32) -> ObsResult<bool> {
        let key = self.key(path)?;
        self.ensure_visible_path(&key).await?;
        if key.is_empty() {
            return Ok(true);
        }
        let epoch = self.cache.snapshot_epoch();
        let mut invalidation;
        if self.is_pfs() {
            invalidation = MutationInvalidation {
                cache: self.cache.clone(), keys: vec![key.clone()],
            };
            if !self.pfs.mkdirs(&key).await? {
                return Ok(false);
            }
        } else {
            match self.get_file_status(path).await {
                Ok(s) if s.is_dir => return Ok(true),
                Ok(_) => return Err(ObsError::AlreadyExists(path.into())),
                Err(e) if e.is_path_not_found() => {}
                Err(e) => return Err(e),
            }
            for (i, _) in key.match_indices('/') {
                let parent = &key[..i];
                match self.get_file_status(&self.key_to_path(parent)).await {
                    Ok(s) if !s.is_dir => return Err(ObsError::ParentNotDirectory(parent.into())),
                    Err(e) if !e.is_path_not_found() => return Err(e),
                    _ => {}
                }
            }
            invalidation = MutationInvalidation {
                cache: self.cache.clone(), keys: vec![key.clone()],
            };
            self.store
                .put(
                    &format!("{key}/"),
                    bytes::Bytes::new(),
                    PutHeaders {
                        user_meta: vec![(META_DIR_MARKER.into(), "true".into())],
                        ..Default::default()
                    },
                )
                .await?;
        }
        let mut status = FileStatus::dir(self.key_to_path(&key), permission);
        status.block_size = self.default_block_size();
        self.cache.complete_mutation(&key, status, epoch);
        invalidation.disarm();
        Ok(true)
    }
    /// Stateless directory page for JNI iterators: abandoning iteration owns no
    /// native cursor or background task. Callers deduplicate across pages.
    async fn listing_status(&self, entry: &ListEntry, epoch: u64) -> ObsResult<FileStatus> {
        let key = entry.key.trim_end_matches('/');
        // A common prefix or slash marker does not prove that the unsuffixed
        // object is absent. Do not cache a directory over a colliding file.
        if !self.is_pfs() && entry.is_directory() {
            if let Some(status) = self.direct_file_status(key).await? {
                return Ok(self.remember_at(key, status, epoch));
            }
        }
        if self.disguise_permissions() {
            match self.store.head(&entry.key).await {
                Ok(meta) => return Ok(self.remember_at(key, self.meta_status(key, meta), epoch)),
                Err(error) if error.is_path_not_found() && entry.is_directory() => {},
                Err(error) => return Err(error),
            }
        }
        Ok(self.remember_at(key, list_entry_to_status(entry, ""), epoch))
    }

    pub async fn list_status_page(
        &self,
        path: &str,
        marker: Option<&str>,
    ) -> ObsResult<FileStatusPage> {
        self.list_page(path, marker, false, false).await
    }

    pub async fn list_files_page(&self, path: &str, marker: Option<&str>, recursive: bool) -> ObsResult<FileStatusPage> {
        self.list_page(path, marker, recursive, true).await
    }

    // Ordinary buckets may contain both `file` and `file/child`. Resolve the
    // requested object before listing its prefix, matching get_file_status.
    // Cached status avoids another HEAD; PFS has no file/prefix ambiguity.
    async fn ensure_visible_path(&self, key: &str) -> ObsResult<()> {
        if !self.is_pfs() {
            for (end, _) in key.match_indices('/') {
                if end > 0 && self.direct_file_status(&key[..end]).await?.is_some() {
                    return Err(Self::not_found(key));
                }
            }
        }
        Ok(())
    }

    async fn listed_file_status(&self, key: &str) -> ObsResult<Option<FileStatus>> {
        self.ensure_visible_path(key).await?;
        self.direct_file_status(key).await
    }

    async fn direct_file_status(&self, key: &str) -> ObsResult<Option<FileStatus>> {
        if key.is_empty() { return Ok(None); }
        let status = match self.cache.get_positive(key) {
            Some(status) => Some(status),
            None if !self.is_pfs() => match self.store.head(key).await {
                Ok(meta) => {
                    let mut status = self.meta_status(key, meta);
                    status.path = self.key_to_path(key);
                    status.block_size = self.default_block_size();
                    Some(status)
                },
                Err(error) if error.is_path_not_found() => None,
                Err(error) => return Err(error),
            },
            None => None,
        };
        Ok(status.filter(|status| !status.is_dir))
    }

    // Flat object listings may include descendants hidden by a file. Resolve
    // ancestors independently of page order, remembering each probe per scan.
    async fn visible_listing_entry(
        &self, entry: &ListEntry, root: &str,
        ancestors: &mut std::collections::HashMap<String, bool>,
        previous: Option<&std::collections::HashMap<String, bool>>,
    ) -> ObsResult<bool> {
        if self.is_pfs() { return Ok(true); }
        for (end, _) in entry.key.match_indices('/') {
            if end <= root.len() { continue; }
            let ancestor = &entry.key[..end];
            let file = match ancestors.get(ancestor).or_else(|| previous.and_then(|memo| memo.get(ancestor))) {
                Some(file) => *file,
                None => {
                    let file = self.direct_file_status(ancestor).await?.is_some();
                    ancestors.insert(ancestor.to_owned(), file);
                    file
                }
            };
            if file { return Ok(false); }
        }
        Ok(true)
    }

    async fn list_page(&self, path: &str, marker: Option<&str>, recursive: bool, files_only: bool) -> ObsResult<FileStatusPage> {
        self.list_page_inner(path, marker, recursive, files_only, None).await.map(|(page, _)| page)
    }

    pub(crate) async fn list_files_page_cached(
        &self, path: &str, marker: Option<&str>,
        ancestors: &std::collections::HashMap<String, bool>,
    ) -> ObsResult<(FileStatusPage, std::collections::HashMap<String, bool>)> {
        self.list_page_inner(path, marker, true, true, Some(ancestors)).await
    }

    async fn list_page_inner(
        &self, path: &str, marker: Option<&str>, recursive: bool, files_only: bool,
        previous: Option<&std::collections::HashMap<String, bool>>,
    ) -> ObsResult<(FileStatusPage, std::collections::HashMap<String, bool>)> {
        let key = self.key(path)?;
        self.ensure_visible_path(&key).await?;
        if marker.is_none() {
            if let Some(status) = self.direct_file_status(&key).await? {
                return Ok((FileStatusPage { entries: vec![status], next_marker: None }, Default::default()));
            }
        }
        let epoch = self.cache.snapshot_epoch();
        let prefix = if key.is_empty() {
            String::new()
        } else {
            format!("{key}/")
        };
        let mut pager = ListPager::new(
            self.store.as_ref(),
            &prefix,
            if recursive { "" } else { "/" },
            self.config.paging_maximum,
        )
        .resume(marker);
        let entries = pager.next_page().await?.unwrap_or_default();
        let next_marker = pager.continuation_marker().map(str::to_owned);
        if entries.is_empty() && marker.is_none() && next_marker.is_none() && !key.is_empty() {
            let status = self.get_file_status(path).await?;
            if !status.is_dir {
                return Ok((FileStatusPage {
                    entries: vec![status],
                    next_marker: None,
                }, Default::default()));
            }
        }
        let mut statuses = Vec::with_capacity(entries.len());
        let mut seen = BTreeSet::new();
        let mut ancestors = std::collections::HashMap::new();
        for entry in entries {
            if recursive && !self.visible_listing_entry(&entry, &key, &mut ancestors, previous).await? { continue; }
            let k = entry.key.trim_end_matches('/');
            if k != key && seen.insert(k.to_owned()) {
                let status = self.listing_status(&entry, epoch).await?;
                if !files_only || !status.is_dir { statuses.push(status); }
            }
        }
        if !self.disguise_permissions() { self.remember_at(&key, FileStatus::dir(&key, 0o755), epoch); }
        Ok((FileStatusPage {
            entries: statuses,
            next_marker,
        }, ancestors))
    }

    pub async fn list_status(&self, path: &str) -> ObsResult<Vec<FileStatus>> {
        let key = self.key(path)?;
        if let Some(status) = self.listed_file_status(&key).await? {
            return Ok(vec![status]);
        }
        let epoch = self.cache.snapshot_epoch();
        let prefix = if key.is_empty() {
            String::new()
        } else {
            format!("{key}/")
        };
        let entries = list_all(
            self.store.as_ref(),
            &prefix,
            "/",
            self.config.paging_maximum,
        )
        .await?;
        if entries.is_empty() && !key.is_empty() {
            let status = self.get_file_status(path).await?;
            if !status.is_dir {
                return Ok(vec![status]);
            }
        }
        let mut statuses = Vec::new();
        let mut seen = BTreeSet::new();
        for entry in entries {
            let k = entry.key.trim_end_matches('/');
            if k == key || !seen.insert(k.to_string()) {
                continue;
            }
            statuses.push(self.listing_status(&entry, epoch).await?);
        }
        if !self.disguise_permissions() { self.remember_at(&key, FileStatus::dir(&key, 0o755), epoch); }
        Ok(statuses)
    }
    pub async fn list_files_recursive(&self, path: &str) -> ObsResult<Vec<FileStatus>> {
        let key = self.key(path)?;
        if let Some(status) = self.listed_file_status(&key).await? {
            return Ok(vec![status]);
        }
        let epoch = self.cache.snapshot_epoch();
        let prefix = if key.is_empty() {
            String::new()
        } else {
            format!("{key}/")
        };
        let entries =
            list_all(self.store.as_ref(), &prefix, "", self.config.paging_maximum).await?;
        if entries.is_empty() && !key.is_empty() {
            let s = self.get_file_status(path).await?;
            return Ok(if s.is_dir { vec![] } else { vec![s] });
        }
        let mut statuses = Vec::new();
        let mut ancestors = std::collections::HashMap::new();
        for entry in entries.iter().filter(|entry| entry.is_file()) {
            if self.visible_listing_entry(entry, &key, &mut ancestors, None).await? {
                statuses.push(self.listing_status(entry, epoch).await?);
            }
        }
        Ok(statuses)
    }
    // Persist an implicit parent before removing its last possible child.
    // Do not overwrite an existing marker: it may carry user metadata.
    async fn preserve_object_parent(&self, key: &str) -> ObsResult<()> {
        if let Some((parent, _)) = key.rsplit_once('/') {
            let marker = format!("{parent}/");
            match self.store.head(&marker).await {
                Ok(_) => return Ok(()),
                Err(e) if e.is_path_not_found() => {}
                Err(e) => return Err(e),
            }
            self.store
                .put(
                    &marker,
                    bytes::Bytes::new(),
                    PutHeaders {
                        user_meta: vec![(META_DIR_MARKER.into(), "true".into())],
                        ..Default::default()
                    },
                )
                .await?;
        }
        Ok(())
    }
    pub async fn delete(&self, path: &str, recursive: bool) -> ObsResult<bool> {
        let key = self.key(path)?;
        if key.is_empty() {
            if !recursive {
                let page = self.store.list("", "/", None, 1).await?;
                if !page.contents.is_empty() || !page.common_prefixes.is_empty() {
                    return Err(ObsError::DirectoryNotEmpty(path.into()));
                }
            }
            return Ok(false);
        }
        let _invalidation = MutationInvalidation {
            cache: self.cache.clone(),
            keys: vec![key.clone()],
        };
        let deleted = if self.is_pfs() {
            let trash = self.config.trash_dir.trim_matches('/');
            if self.config.trash_enabled && !key.starts_with(&format!("{trash}/")) {
                if trash.is_empty() || key == trash || trash.starts_with(&format!("{key}/")) {
                    return Err(ObsError::InvalidArgument(
                        "trash must be outside the deleted directory".into(),
                    ));
                }
                let status = match self.get_file_status(path).await {
                    Ok(status) => status,
                    Err(e) if e.is_path_not_found() => return Ok(false),
                    Err(e) => return Err(e),
                };
                if status.is_dir && !recursive && !self.list_status(path).await?.is_empty() {
                    return Err(ObsError::DirectoryNotEmpty(path.into()));
                }
                static TRASH_ID: std::sync::atomic::AtomicU64 =
                    std::sync::atomic::AtomicU64::new(0);
                let id = TRASH_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let target = format!(
                    "{trash}/{}-{id}/{key}",
                    chrono::Utc::now().timestamp_micros()
                );
                let _trash_invalidation = MutationInvalidation {
                    cache: self.cache.clone(),
                    keys: vec![trash.to_string()],
                };
                if let Some((parent, _)) = target.rsplit_once('/') {
                    self.pfs.mkdirs(parent).await?;
                }
                self.rename_pfs(&key, &target, status.is_dir).await?;
                self.cache.invalidate_prefix(&key);
                self.cache.invalidate_ancestors(&key);
                self.cache.invalidate_prefix(trash);
                return Ok(true);
            }
            self.pfs.dual_key_delete(&key, recursive).await?
        } else {
            let status = match self.get_file_status(path).await {
                Ok(s) => s,
                Err(e) if e.is_path_not_found() => return Ok(false),
                Err(e) => return Err(e),
            };
            if status.is_dir {
                let prefix = format!("{key}/");
                let entries =
                    crate::listing::iterator::list_all_for_mutation(
                        self.store.as_ref(), &prefix, "", self.config.paging_maximum,
                    ).await?;
                let keys: Vec<_> = entries
                    .into_iter()
                    .filter(|e| !e.is_common_prefix)
                    .map(|e| e.key)
                    .filter(|k| k != &prefix)
                    .collect();
                if !recursive && !keys.is_empty() {
                    return Err(ObsError::DirectoryNotEmpty(path.into()));
                }
                self.preserve_object_parent(&key).await?;
                for batch in keys.chunks(1000) {
                    self.store.delete_batch(batch).await?;
                }
                self.store.delete(&prefix, false).await?;
            } else {
                self.preserve_object_parent(&key).await?;
                self.store.delete(&key, false).await?;
            }
            true
        };
        Ok(deleted)
    }
    pub async fn truncate(&self, path: &str, length: u64) -> ObsResult<()> {
        if !self.is_pfs() {
            return Err(ObsError::Unsupported("truncate requires PFS".into()));
        }
        let key = self.key(path)?;
        let _invalidation = StatusInvalidation { cache: &self.cache, key: &key };
        self.pfs.truncate(&key, length).await
    }
    pub async fn append_stream(&self, path: &str, position: u64) -> ObsResult<PfsAppendStream> {
        if !self.is_pfs() {
            return Err(ObsError::Unsupported("append requires PFS".into()));
        }
        let key = self.key(path)?;
        self.cache.invalidate(&key);
        let cache = self.cache.clone();
        let callback_key = key.clone();
        let target = self.append_coordinator.register(key.clone()).await;
        let callback_target = target.clone();
        Ok(PfsAppendStream::with_budget(
            self.store.clone(),
            &self.config,
            key,
            position,
            self.append_budget.clone(),
        )
        .with_buffer_budget(self.buffer_budget.clone())
        .with_coordination(self.append_coordinator.clone(), target)
        .on_invalidate(move || {
            cache.invalidate(&callback_key);
            if let Ok(key) = callback_target.key() {
                // Stable streams need only one epoch write and cache eviction.
                // After a rename, invalidate both the original and current path.
                if key != callback_key { cache.invalidate(&key); }
            }
        }))
    }
    async fn rename_pfs(&self, source: &str, target: &str, directory: bool) -> ObsResult<()> {
        let guard = self.append_coordinator.rename(source, target, directory).await;
        let result = self.pfs.rename(source, target, directory).await;
        guard.finish(&result);
        result
    }
    pub async fn rename(&self, src: &str, dst: &str, overwrite: bool) -> ObsResult<bool> {
        let source = self.key(src)?;
        let mut target = self.key(dst)?;
        if source.is_empty() {
            return Ok(false);
        }
        let status = match self.get_file_status(src).await {
            Ok(s) => s,
            Err(e) if e.is_path_not_found() => return Ok(false),
            Err(e) => return Err(e),
        };
        if source == target {
            return Ok(!status.is_dir);
        }
        match self.get_file_status(dst).await {
            Ok(s) if s.is_dir => {
                target = format!("{target}/{}", source.rsplit('/').next().unwrap())
                    .trim_start_matches('/')
                    .to_string()
            }
            Ok(_) if !overwrite => return Ok(false),
            Err(e) if !e.is_path_not_found() => return Err(e),
            _ => {}
        }
        if source == target {
            return Ok(!status.is_dir);
        }
        if target.starts_with(&format!("{source}/")) {
            return Ok(false);
        }
        // Validate the resolved child when dst denotes a directory too.
        // Overwrite only replaces a file with a file; never merge directory
        // trees or create file/descendant collisions in an object namespace.
        match self.get_file_status(&self.key_to_path(&target)).await {
            Ok(existing) if !overwrite || existing.is_dir || status.is_dir => return Ok(false),
            Ok(_) => {}
            Err(e) if e.is_path_not_found() => {}
            Err(e) => return Err(e),
        }
        if let Some((parent, _)) = target.rsplit_once('/') {
            match self.get_file_status(&self.key_to_path(parent)).await {
                Ok(s) if s.is_dir => {}
                Ok(_) => return Ok(false),
                Err(e) if e.is_path_not_found() => return Ok(false),
                Err(e) => return Err(e),
            }
        }
        let _invalidation = MutationInvalidation {
            cache: self.cache.clone(),
            keys: vec![source.clone(), target.clone()],
        };
        if self.is_pfs() {
            match self.rename_pfs(&source, &target, status.is_dir).await {
                Ok(()) => {}
                Err(e) if e.is_path_not_found() || e.is_conflict_with_code("FileAlreadyExists") => {
                    return Ok(false)
                }
                Err(e) => return Err(e),
            }
        } else {
            let keys = if status.is_dir {
                crate::listing::iterator::list_all_for_mutation(
                    self.store.as_ref(),
                    &format!("{source}/"),
                    "",
                    self.config.paging_maximum,
                )
                .await?
                .into_iter()
                .filter(|e| !e.is_common_prefix)
                .map(|e| e.key)
                .collect::<Vec<_>>()
            } else {
                vec![source.clone()]
            };
            for key in &keys {
                self.store
                    .copy(key, &format!("{target}{}", &key[source.len()..]))
                    .await?;
            }
            self.preserve_object_parent(&source).await?;
            for batch in keys.chunks(1000) {
                self.store.delete_batch(batch).await?;
            }
        }
        Ok(true)
    }
    pub async fn get_content_summary(&self, path: &str) -> ObsResult<ContentSummary> {
        let key = self.key(path)?;
        if let Some(status) = self.listed_file_status(&key).await? {
            return Ok(ContentSummary { file_count: 1, directory_count: 0,
                length: status.length, space_consumed: status.length });
        }
        let prefix = if key.is_empty() {
            String::new()
        } else {
            format!("{key}/")
        };
        let mut pager =
            ListPager::new(self.store.as_ref(), &prefix, "", self.config.paging_maximum);
        let mut saw_entries = false;
        let mut summary = ContentSummary::default();
        let mut directories = BTreeSet::from([key.clone()]);
        let mut objects = std::collections::HashSet::new();
        let mut ancestors = std::collections::HashMap::new();
        while let Some(page) = pager.next_page().await? {
            saw_entries |= !page.is_empty();
            for e in page {
                if self.visible_listing_entry(&e, &key, &mut ancestors, None).await? {
                    if e.is_directory() {
                        directories.insert(e.key.trim_end_matches('/').to_string());
                    } else {
                        summary.file_count = summary.file_count.checked_add(1).ok_or_else(|| {
                            ObsError::Io("content summary file count overflow".into())
                        })?;
                        summary.length = summary
                            .length
                            .checked_add(e.size)
                            .ok_or_else(|| ObsError::Io("content summary length overflow".into()))?;
                    }
                    let mut parent = e.key.trim_end_matches('/');
                    while let Some((p, _)) = parent.rsplit_once('/') {
                        if p.len() < key.len() {
                            break;
                        }
                        directories.insert(p.to_string());
                        parent = p;
                    }
                }
                // Keep only owned keys, not LIST metadata, to reject overlapping
                // pages rather than returning a double-counted summary. Common
                // prefixes are directory hints and may legitimately repeat.
                if !e.is_common_prefix && !objects.insert(e.key) {
                    return Err(ObsError::Io(
                        "LIST returned duplicate object keys across pages".into(),
                    ));
                }
            }
        }
        if !saw_entries && !key.is_empty() {
            let s = self.get_file_status(path).await?;
            return Ok(ContentSummary {
                file_count: if s.is_dir { 0 } else { 1 },
                directory_count: if s.is_dir { 1 } else { 0 },
                length: s.length,
                space_consumed: s.length,
            });
        }
        summary.directory_count = directories.len() as u64;
        summary.space_consumed = summary.length;
        Ok(summary)
    }
    pub async fn set_permission(&self, path: &str, mode: u32) -> ObsResult<()> {
        let key = self.key(path)?;
        self.ensure_visible_path(&key).await?;
        if !self.is_pfs() {
            return Ok(());
        }
        let _invalidation = MutationInvalidation { cache: self.cache.clone(), keys: vec![key.clone()] };
        self.store
            .set_object_metadata(&key, Some(mode), None, None)
            .await?;
        Ok(())
    }
    /// Hadoop names are descriptive metadata, not numeric POSIX identities or ACLs.
    pub async fn set_hadoop_owner(&self, path: &str, owner: Option<&str>, group: Option<&str>) -> ObsResult<()> {
        let key = self.key(path)?;
        self.ensure_visible_path(&key).await?;
        if owner.is_none() && group.is_none() {
            return Err(ObsError::InvalidArgument("owner and group cannot both be null".into()));
        }
        if !self.disguise_permissions() { return Ok(()); }
        let mut updates = std::collections::HashMap::new();
        for (field, value) in [("user", owner), ("group", group)] {
            if let Some(value) = value {
                // HTTP metadata cannot safely represent control characters or arbitrary Unicode.
                if value.is_empty() || !value.bytes().all(|b| (0x20..=0x7e).contains(&b)) {
                    return Err(ObsError::InvalidArgument("owner/group must be nonempty printable ASCII".into()));
                }
                updates.insert(field.into(), value.into());
            }
        }
        let _invalidation = MutationInvalidation { cache: self.cache.clone(), keys: vec![key.clone()] };
        self.store.patch_user_metadata(&key, updates).await
    }

    pub async fn set_hadoop_permission(&self, path: &str, mode: u32) -> ObsResult<()> {
        let key = self.key(path)?;
        self.ensure_visible_path(&key).await?;
        if !self.disguise_permissions() { return Ok(()); }
        let _invalidation = MutationInvalidation { cache: self.cache.clone(), keys: vec![key.clone()] };
        self.store.patch_user_metadata(&key, std::collections::HashMap::from([("permission".into(), (mode & 0o7777).to_string())])).await
    }
    pub async fn set_owner(&self, path: &str, uid: u32, gid: u32) -> ObsResult<()> {
        let key = self.key(path)?;
        self.ensure_visible_path(&key).await?;
        if !self.is_pfs() {
            return Ok(());
        }
        let _invalidation = MutationInvalidation { cache: self.cache.clone(), keys: vec![key.clone()] };
        self.store
            .set_object_metadata(&key, None, Some(uid), Some(gid))
            .await?;
        Ok(())
    }
}
