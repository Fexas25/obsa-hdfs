use std::sync::Arc;

use crate::error::{conflict_codes, ObsError, ObsResult};
use crate::store::ObjectStore;
use crate::types::InodeHint;

/// PFS-specific operations implementing HEAD elimination strategies.
pub struct PfsOps {
    store: Arc<dyn ObjectStore>,
}

impl PfsOps {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    /// Dual-key delete (0 HEAD): DELETE dir_key/ then DELETE file_key.
    pub async fn dual_key_delete(&self, path: &str, recursive: bool) -> ObsResult<bool> {
        let dir_key = format!("{}/", path.trim_end_matches('/'));
        let file_key = path.trim_end_matches('/').to_string();

        // Directory key first (spec rule #5)
        match self.store.delete(&dir_key, true).await {
            Ok(()) => return Ok(true),
            Err(ObsError::Conflict { code, .. }) if code == conflict_codes::DIRECTORY_NOT_EMPTY => {
                if recursive {
                    return self.recursive_delete_dir(&dir_key).await;
                }
                return Err(ObsError::DirectoryNotEmpty(format!(
                    "Directory not empty: {dir_key}"
                )));
            }
            Err(error) if error.is_path_not_found() => {}
            Err(e) => return Err(e),
        }

        match self.store.delete(&file_key, true).await {
            Ok(()) => Ok(true),
            Err(error) if error.is_path_not_found() => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn recursive_delete_dir(&self, dir_key: &str) -> ObsResult<bool> {
        let entries =
            crate::listing::iterator::list_all_for_mutation(self.store.as_ref(), dir_key, "", 1000).await?;

        let mut keys: Vec<String> = entries
            .into_iter()
            .filter(|e| e.key != dir_key)
            .map(|e| e.key)
            .collect();

        // Send separate depth groups: a batch response has no ordering guarantee.
        // Compute depth once per key instead of repeatedly scanning long paths
        // during comparisons. Moving keys above also releases LIST metadata.
        keys.sort_by_cached_key(|k| {
            std::cmp::Reverse(k.trim_end_matches('/').matches('/').count())
        });
        let mut start = 0;
        while start < keys.len() {
            let depth = keys[start].trim_end_matches('/').matches('/').count();
            let mut end = start + 1;
            while end < keys.len() && keys[end].trim_end_matches('/').matches('/').count() == depth
            {
                end += 1;
            }
            for chunk in keys[start..end].chunks(1000) {
                self.store.delete_batch(chunk).await?;
            }
            start = end;
        }
        self.store.delete(dir_key, true).await?;
        Ok(true)
    }

    /// Truncate with x-obs-hdfs header (0 HEAD).
    pub async fn truncate(&self, key: &str, new_length: u64) -> ObsResult<()> {
        self.store.posix_truncate(key, new_length).await
    }

    /// Atomic PFS rename (0-1 HEAD).
    pub async fn rename(&self, src_key: &str, dst_key: &str, src_is_dir: bool) -> ObsResult<()> {
        let src = if src_is_dir && !src_key.ends_with('/') {
            format!("{src_key}/")
        } else {
            src_key.to_string()
        };
        self.store.posix_rename(&src, dst_key).await
    }

    /// Direct newFolder mkdirs (0 HEAD).
    pub async fn mkdirs(&self, key: &str) -> ObsResult<bool> {
        match self.store.create_folder(key).await {
            Ok(()) => Ok(true),
            Err(error) if error.is_path_not_found() => {
                // Parent missing — recursive mkdirs on parent
                let parent = parent_path(key);
                if parent.is_empty() {
                    return Ok(false);
                }
                Box::pin(self.mkdirs(&parent)).await?;
                self.store.create_folder(key).await.map(|_| true)
            }
            Err(e) => Err(e),
        }
    }

    pub fn inode_hint_from_meta(
        inode_no: Option<u64>,
        dentry_name: Option<String>,
    ) -> Option<InodeHint> {
        InodeHint::new(inode_no, dentry_name)
    }
}

fn parent_path(key: &str) -> String {
    let trimmed = key.trim_end_matches('/');
    if let Some(idx) = trimmed.rfind('/') {
        trimmed[..idx].to_string()
    } else {
        String::new()
    }
}
