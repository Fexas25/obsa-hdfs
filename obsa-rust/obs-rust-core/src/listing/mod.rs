use crate::error::ObsResult;
use crate::store::ObjectStore;
use crate::types::{FileStatus, ListEntry};

/// Owned listing state shared by native consumers. No background work or IO in Drop.
pub struct StatusCursor {
    path: String,
    marker: Option<String>,
    markers: std::collections::HashSet<String>,
    paths: std::collections::HashSet<String>,
    ancestors: std::collections::HashMap<String, bool>,
    done: bool,
    failure: Option<crate::error::ObsError>,
    files: Option<bool>,
}

impl StatusCursor {
    pub fn new(path: String) -> Self {
        Self {
            path,
            marker: None,
            markers: Default::default(),
            paths: Default::default(),
            ancestors: Default::default(),
            done: false,
            failure: None,
            files: None,
        }
    }

    pub fn files(path: String, recursive: bool) -> Self {
        Self { files: Some(recursive), ..Self::new(path) }
    }

    pub async fn next_page(
        &mut self,
        fs: &crate::ObsFileSystem,
    ) -> ObsResult<crate::types::FileStatusPage> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if self.done {
            return Ok(crate::types::FileStatusPage {
                entries: vec![],
                next_marker: None,
            });
        }
        let result = match self.files {
            Some(true) => fs.list_files_page_cached(&self.path, self.marker.as_deref(), &self.ancestors).await,
            Some(false) => fs.list_files_page(&self.path, self.marker.as_deref(), false).await
                .map(|page| (page, Default::default())),
            None => fs.list_status_page(&self.path, self.marker.as_deref()).await
                .map(|page| (page, Default::default())),
        };
        let (page, probes) = match result {
            Ok(result) => result,
            Err(error) => return self.fail(error),
        };
        let page = self.accept_page(page)?;
        // Probe only into a page-local delta until the page is accepted. A
        // cancelled or rejected fetch cannot change the cursor's decisions.
        if !self.done { self.ancestors.extend(probes); }
        Ok(page)
    }

    // Commit a fetched page without an await, so cancellation cannot consume
    // continuation or deduplication state before returning it to the caller.
    fn accept_page(
        &mut self,
        mut page: crate::types::FileStatusPage,
    ) -> ObsResult<crate::types::FileStatusPage> {
        if let Some(marker) = &page.next_marker {
            if !self.markers.insert(marker.clone()) {
                return self.fail(crate::error::ObsError::Io(
                    "LIST did not advance marker".into(),
                ));
            }
        }
        page.entries
            .retain(|entry| self.paths.insert(entry.path.clone()));
        self.marker = page.next_marker.clone();
        self.done = self.marker.is_none();
        if self.done {
            self.release_history();
        }
        Ok(page)
    }

    fn fail(&mut self, error: crate::error::ObsError) -> ObsResult<crate::types::FileStatusPage> {
        self.release_history();
        self.failure = Some(error.clone());
        Err(error)
    }

    fn release_history(&mut self) {
        // Terminal cursors may remain held by native callers. clear() keeps
        // bucket allocations proportional to the entire completed listing.
        self.markers = Default::default();
        self.paths = Default::default();
        self.ancestors = Default::default();
    }
}

/// Paginated async listing with LIST back-fill support.
pub mod iterator {
    use super::*;
    use crate::{error::ObsError, types::ListPage};
    use std::collections::HashSet;

    /// Pull-based pages: no request is made until next_page is awaited.
    /// Cancellation leaves the cursor unchanged; a later call retries that page.
    /// Marker history grows with page count to detect non-adjacent cycles.
    pub struct ListPager<'a> {
        store: &'a dyn ObjectStore,
        prefix: &'a str,
        delimiter: &'a str,
        page_size: usize,
        marker: Option<String>,
        seen_markers: HashSet<String>,
        done: bool,
        failure: Option<ObsError>,
    }

    impl<'a> ListPager<'a> {
        pub fn new(
            store: &'a dyn ObjectStore,
            prefix: &'a str,
            delimiter: &'a str,
            page_size: usize,
        ) -> Self {
            Self {
                store,
                prefix,
                delimiter,
                page_size: page_size.clamp(1, 1000),
                marker: None,
                seen_markers: HashSet::new(),
                done: false,
                failure: None,
            }
        }

        /// Resume a stateless page request. The caller must track earlier markers
        /// to detect cycles spanning multiple requests.
        pub fn resume(mut self, marker: Option<&str>) -> Self {
            self.marker = marker.map(str::to_owned);
            self.seen_markers.extend(self.marker.iter().cloned());
            self
        }

        pub fn continuation_marker(&self) -> Option<&str> {
            self.marker.as_deref()
        }

        pub async fn next_page(&mut self) -> ObsResult<Option<Vec<ListEntry>>> {
            if let Some(error) = &self.failure {
                return Err(error.clone());
            }
            if self.done {
                return Ok(None);
            }
            let page = self
                .store
                .list(
                    self.prefix,
                    self.delimiter,
                    self.marker.as_deref(),
                    self.page_size,
                )
                .await?;
            self.accept_page(page)
        }

        fn accept_page(&mut self, page: ListPage) -> ObsResult<Option<Vec<ListEntry>>> {
            let truncated = page.is_truncated;
            let (entries, next) = match self.validate(page) {
                Ok(value) => value,
                Err(error) => {
                    // Validation failures are terminal; fetch errors above
                    // retain history so retries still detect marker cycles.
                    self.seen_markers = HashSet::new();
                    self.failure = Some(error.clone());
                    return Err(error);
                }
            };
            // Commit only after the complete page passes validation. No await
            // separates this commit from returning the page to its caller.
            if let Some(next) = &next {
                self.seen_markers.insert(next.clone());
            }
            self.marker = next;
            self.done = !truncated;
            if self.done {
                // Release both strings and bucket storage while the caller
                // may still hold this completed pager.
                self.seen_markers = HashSet::new();
            }
            Ok(Some(entries))
        }

        fn validate(&self, page: ListPage) -> ObsResult<(Vec<ListEntry>, Option<String>)> {
            if page
                .contents
                .iter()
                .map(|e| e.key.as_str())
                .chain(page.common_prefixes.iter().map(String::as_str))
                .any(|key| key.is_empty() || !key.starts_with(self.prefix))
            {
                return Err(ObsError::Io(
                    "LIST returned a key outside the requested prefix".into(),
                ));
            }
            let next = if page.is_truncated {
                let next = page
                    .next_marker
                    .filter(|s| !s.is_empty())
                    .or_else(|| page.contents.last().map(|e| e.key.clone()));
                match next {
                    Some(next) if !self.seen_markers.contains(&next) => Some(next),
                    _ => return Err(ObsError::Io("truncated LIST did not advance marker".into())),
                }
            } else {
                None
            };
            let mut known_keys = HashSet::with_capacity(page.contents.len());
            for entry in &page.contents {
                if !known_keys.insert(entry.key.as_str()) {
                    return Err(ObsError::Io(
                        "LIST returned duplicate object keys in one page".into(),
                    ));
                }
            }
            let mut entries = Vec::new();
            for prefix in &page.common_prefixes {
                if known_keys.insert(prefix.as_str()) {
                    entries.push(ListEntry {
                        key: prefix.clone(),
                        is_common_prefix: true,
                        ..Default::default()
                    });
                }
            }
            entries.extend(page.contents);
            Ok((entries, next))
        }
    }

    #[cfg(test)]
    mod lifecycle_tests {
        use super::*;
        use crate::{auth::{CredentialChain, Credentials}, config::ObsConfig, store::ObsStore};

        fn store() -> ObsStore {
            ObsStore::new(ObsConfig {
                endpoint: "invalid host with spaces".into(), bucket: "bucket".into(),
                ..Default::default()
            }, CredentialChain::new(Credentials {
                access_key: "test".into(), secret_key: "test".into(), session_token: None,
            }))
        }

        fn populate(pager: &mut ListPager<'_>) {
            for index in 0..256 {
                pager.accept_page(ListPage {
                    is_truncated: true, next_marker: Some(format!("marker-{index}")),
                    ..Default::default()
                }).unwrap();
            }
            assert_eq!(pager.seen_markers.len(), 256);
            assert!(pager.seen_markers.capacity() >= 256);
        }

        #[test]
        #[ignore = "release key-validation benchmark; scripts/build-windows.ps1 -BenchmarkListValidation"]
        fn benchmark_mutation_key_validation() {
            // Retain the previous growth strategy as the benchmark baseline.
            fn growing(entries: &[ListEntry]) -> ObsResult<()> {
                let mut keys = HashSet::new();
                for entry in entries.iter().filter(|entry| !entry.is_common_prefix) {
                    if !keys.insert(entry.key.as_str()) {
                        return Err(ObsError::Io("duplicate object keys".into()));
                    }
                }
                Ok(())
            }
            for count in [10_000, 100_000] {
                for prefix_len in [16, 240] {
                    let entries: Vec<_> = (0..count).map(|index| ListEntry {
                        key: format!("{}/{index:08}", "x".repeat(prefix_len)),
                        ..Default::default()
                    }).collect();
                    // Fixtures and correctness checks stay outside measurement.
                    let duplicate = vec![entries[0].clone(), entries[0].clone()];
                    assert!(validate_mutation_entries(&duplicate).is_err());
                    assert!(growing(&duplicate).is_err());
                    for round in 0..=5 {
                        for preallocate in if round % 2 == 0 { [false, true] } else { [true, false] } {
                            let mut elapsed = std::time::Duration::ZERO;
                            for _ in 0..3 {
                                let start = std::time::Instant::now();
                                let result = if preallocate {
                                    validate_mutation_entries(std::hint::black_box(&entries))
                                } else {
                                    growing(std::hint::black_box(&entries))
                                };
                                elapsed += start.elapsed();
                                result.unwrap();
                            }
                            if round > 0 {
                                println!("BENCH_JSON {}", serde_json::json!({
                                    "variant": if preallocate { "reserved" } else { "growing" },
                                    "entries": count, "key_bytes": prefix_len + 9,
                                    "round": round, "iterations": 3,
                                    "mean_ms": elapsed.as_secs_f64() * 1000.0 / 3.0
                                }));
                            }
                        }
                    }
                }
            }
        }

        #[tokio::test]
        async fn completed_pager_releases_marker_history_and_stays_at_eof() {
            let store = store();
            let mut pager = ListPager::new(&store, "dir/", "", 1000);
            populate(&mut pager);
            let entries = pager.accept_page(ListPage {
                contents: vec![ListEntry { key: "dir/last".into(), ..Default::default() }],
                ..Default::default()
            }).unwrap().unwrap();
            assert_eq!(entries[0].key, "dir/last");
            assert_eq!(pager.seen_markers.capacity(), 0);
            assert!(pager.continuation_marker().is_none());
            // The invalid endpoint would fail if EOF issued another request.
            assert!(pager.next_page().await.unwrap().is_none());
        }

        #[tokio::test]
        async fn invalid_page_releases_history_and_preserves_sticky_error() {
            for cycle in [false, true] {
                let store = store();
                let mut pager = ListPager::new(&store, "dir/", "", 1000);
                populate(&mut pager);
                let page = if cycle {
                    ListPage { is_truncated: true, next_marker: Some("marker-0".into()), ..Default::default() }
                } else {
                    ListPage { contents: vec![ListEntry { key: "outside".into(), ..Default::default() }], ..Default::default() }
                };
                let error = pager.accept_page(page).unwrap_err().to_string();
                assert_eq!(pager.seen_markers.capacity(), 0);
                // Preserve the last committed marker for callers inspecting failure.
                assert_eq!(pager.continuation_marker(), Some("marker-255"));
                assert_eq!(pager.next_page().await.unwrap_err().to_string(), error);
            }
        }

        #[tokio::test]
        async fn fetch_error_retains_history_for_retry_and_cycle_detection() {
            let store = store();
            let mut pager = ListPager::new(&store, "dir/", "", 1000);
            populate(&mut pager);
            let capacity = pager.seen_markers.capacity();
            assert!(pager.next_page().await.is_err());
            assert!(pager.failure.is_none());
            assert_eq!(pager.continuation_marker(), Some("marker-255"));
            assert_eq!(pager.seen_markers.len(), 256);
            assert_eq!(pager.seen_markers.capacity(), capacity);
            // A later successful fetch must still reject a non-adjacent cycle.
            assert!(pager.accept_page(ListPage {
                is_truncated: true, next_marker: Some("marker-0".into()), ..Default::default()
            }).unwrap_err().to_string().contains("marker"));
        }
    }

    /// Collect all validated pages before callers begin destructive operations.
    pub async fn list_all(
        store: &dyn ObjectStore,
        prefix: &str,
        delimiter: &str,
        page_size: usize,
    ) -> ObsResult<Vec<ListEntry>> {
        let mut pager = ListPager::new(store, prefix, delimiter, page_size);
        let mut all = Vec::new();
        while let Some(page) = pager.next_page().await? {
            all.extend(page);
        }
        Ok(all)
    }

    /// Mutation callers must not copy/delete an object twice when pages overlap.
    /// Borrow keys from the collected entries to avoid cloning every key.
    pub(crate) async fn list_all_for_mutation(
        store: &dyn ObjectStore,
        prefix: &str,
        delimiter: &str,
        page_size: usize,
    ) -> ObsResult<Vec<ListEntry>> {
        let entries = list_all(store, prefix, delimiter, page_size).await?;
        validate_mutation_entries(&entries)?;
        Ok(entries)
    }

    fn validate_mutation_entries(entries: &[ListEntry]) -> ObsResult<()> {
        // The full list already exists; reserve once to avoid rehashing keys
        // during geometric growth. Keys themselves remain borrowed.
        let mut keys = HashSet::with_capacity(entries.len());
        for entry in entries.iter().filter(|entry| !entry.is_common_prefix) {
            if !keys.insert(entry.key.as_str()) {
                return Err(ObsError::Io("LIST returned duplicate object keys across pages".into()));
            }
        }
        Ok(())
    }
}

/// Convert ListEntry to FileStatus (0 HEAD per entry).
pub fn list_entry_to_status(entry: &ListEntry, base_path: &str) -> FileStatus {
    let name = entry
        .key
        .strip_prefix(base_path)
        .unwrap_or(&entry.key)
        .trim_end_matches('/')
        .to_string();

    let full_path = if base_path.is_empty() {
        name.clone()
    } else {
        format!("{}/{}", base_path.trim_end_matches('/'), name)
    };

    let permission = entry
        .mode
        .map(|m| m & 0o7777)
        .unwrap_or(if entry.is_directory() { 0o755 } else { 0o644 });
    let owner = entry
        .uid
        .map(|u| u.to_string())
        .unwrap_or_else(|| "unknown".into());
    let group = entry
        .gid
        .map(|g| g.to_string())
        .unwrap_or_else(|| "unknown".into());

    if entry.is_directory() {
        FileStatus {
            path: full_path,
            is_dir: true,
            length: 0,
            modification_time: entry
                .last_modified
                .as_deref()
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map_or(0, |t| t.timestamp_millis()),
            access_time: 0,
            block_size: 128 * 1024 * 1024,
            replication: 1,
            owner,
            group,
            permission,
            etag: entry.etag.clone(),
            inode_no: entry.inode_no,
            dentry_name: None,
            fs_dentry_no: entry.fs_dentry_no,
            fs_link_count: entry.fs_link_count,
        }
    } else {
        FileStatus {
            path: full_path,
            is_dir: false,
            length: entry.size,
            modification_time: entry
                .last_modified
                .as_deref()
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map_or(0, |t| t.timestamp_millis()),
            access_time: 0,
            block_size: 128 * 1024 * 1024,
            replication: 1,
            owner,
            group,
            permission,
            etag: entry.etag.clone(),
            inode_no: entry.inode_no,
            dentry_name: None,
            fs_dentry_no: entry.fs_dentry_no,
            fs_link_count: entry.fs_link_count,
        }
    }
}

pub use iterator::{list_all, ListPager};

#[cfg(test)]
mod cursor_tests {
    use super::*;
    use crate::{error::ObsError, types::FileStatusPage};

    fn page(start: usize, end: usize, marker: Option<&str>) -> FileStatusPage {
        FileStatusPage {
            entries: (start..end).map(|index| list_entry_to_status(&ListEntry {
                key: format!("dir/file-{index}"), ..Default::default()
            }, "")).collect(),
            next_marker: marker.map(str::to_owned),
        }
    }

    #[test]
    fn completed_cursor_releases_history_after_final_page_deduplication() {
        let mut cursor = StatusCursor::new("/dir".into());
        assert_eq!(cursor.accept_page(page(0, 4096, Some("next"))).unwrap().entries.len(), 4096);
        assert!(cursor.paths.capacity() >= 4096);
        assert!(cursor.markers.capacity() > 0);
        cursor.ancestors.extend((0..4096).map(|index| (format!("parent-{index}"), false)));
        assert!(cursor.ancestors.capacity() >= 4096);
        let last = cursor.accept_page(page(4095, 4097, None)).unwrap();
        assert_eq!(last.entries.len(), 1);
        assert_eq!(last.entries[0].path, "dir/file-4096");
        assert!(last.next_marker.is_none());
        assert!(cursor.done);
        assert_eq!(cursor.paths.capacity(), 0);
        assert_eq!(cursor.markers.capacity(), 0);
        assert_eq!(cursor.ancestors.capacity(), 0);
    }

    #[test]
    fn failed_cursor_releases_history_for_fetch_errors_and_marker_cycles() {
        for cycle in [false, true] {
            let mut cursor = StatusCursor::new("/dir".into());
            cursor.accept_page(page(0, 4096, Some("next"))).unwrap();
            assert!(cursor.paths.capacity() >= 4096);
            assert!(cursor.markers.capacity() > 0);
            cursor.ancestors.extend((0..4096).map(|index| (format!("parent-{index}"), false)));
            assert!(cursor.ancestors.capacity() >= 4096);
            let error = if cycle {
                cursor.accept_page(page(4096, 4097, Some("next"))).unwrap_err()
            } else {
                cursor.fail(ObsError::Transport("injected fetch failure".into())).unwrap_err()
            };
            assert_eq!(cursor.failure.as_ref().unwrap().to_string(), error.to_string());
            assert_eq!(cursor.paths.capacity(), 0);
            assert_eq!(cursor.markers.capacity(), 0);
            assert_eq!(cursor.ancestors.capacity(), 0);
        }
    }
}
