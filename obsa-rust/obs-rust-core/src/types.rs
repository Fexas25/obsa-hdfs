use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub struct FileStatusPage {
    pub entries: Vec<FileStatus>,
    pub next_marker: Option<String>,
}

/// Bucket mode: auto-detect via HEAD probe, or force object/pfs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum BucketMode {
    #[default]
    Auto,
    Object,
    Pfs,
}

/// Addressing style for OBS requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum AddressingStyle {
    #[default]
    Path,
    Vhost,
    Cname,
}

/// Parsed file/directory metadata from LIST or HEAD.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileStatus {
    pub path: String,
    pub is_dir: bool,
    pub length: u64,
    pub modification_time: i64,
    pub access_time: i64,
    pub block_size: i64,
    pub replication: i16,
    pub owner: String,
    pub group: String,
    pub permission: u32,
    pub etag: Option<String>,
    pub inode_no: Option<u64>,
    #[serde(default)]
    pub dentry_name: Option<String>,
    pub fs_dentry_no: Option<u64>,
    pub fs_link_count: Option<u64>,
}

impl FileStatus {
    pub fn dir(path: impl Into<String>, permission: u32) -> Self {
        Self {
            path: path.into(),
            is_dir: true,
            length: 0,
            modification_time: 0,
            access_time: 0,
            block_size: 128 * 1024 * 1024,
            replication: 1,
            owner: "unknown".into(),
            group: "unknown".into(),
            permission,
            etag: None,
            inode_no: None,
            dentry_name: None,
            fs_dentry_no: None,
            fs_link_count: None,
        }
    }

    pub fn file(path: impl Into<String>, length: u64, permission: u32) -> Self {
        Self {
            path: path.into(),
            is_dir: false,
            length,
            modification_time: 0,
            access_time: 0,
            block_size: 128 * 1024 * 1024,
            replication: 1,
            owner: "unknown".into(),
            group: "unknown".into(),
            permission,
            etag: None,
            inode_no: None,
            dentry_name: None,
            fs_dentry_no: None,
            fs_link_count: None,
        }
    }
}

/// Inode hint for PFS fast-path GET routing.
#[derive(Debug, Clone)]
pub struct InodeHint {
    pub inode_no: u64,
    pub dentry_name: String,
}

impl InodeHint {
    /// Both fields must be present; partial hints are discarded per spec.
    pub fn new(inode_no: Option<u64>, dentry_name: Option<String>) -> Option<Self> {
        match (inode_no, dentry_name) {
            (Some(no), Some(name)) if !name.is_empty() => Some(Self {
                inode_no: no,
                dentry_name: name,
            }),
            _ => None,
        }
    }
}

/// Single entry from a LIST response.
#[derive(Debug, Clone, Default)]
pub struct ListEntry {
    pub key: String,
    pub size: u64,
    pub last_modified: Option<String>,
    pub etag: Option<String>,
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub inode_no: Option<u64>,
    pub fs_dentry_no: Option<u64>,
    pub fs_link_count: Option<u64>,
    pub is_common_prefix: bool,
}

impl ListEntry {
    pub fn is_directory(&self) -> bool {
        if self.is_common_prefix {
            return true;
        }
        if let Some(mode) = self.mode {
            return (mode & 0o170000) == 0o040000;
        }
        self.key.ends_with('/')
    }

    pub fn is_file(&self) -> bool {
        if self.is_common_prefix {
            return false;
        }
        if let Some(mode) = self.mode {
            return (mode & 0o170000) == 0o100000;
        }
        !self.key.ends_with('/')
    }

    /// Parse Mode field as octal string per spec rule #8.
    pub fn parse_mode_octal(s: &str) -> Option<u32> {
        u32::from_str_radix(s.trim(), 8).ok()
    }
}

/// Paginated LIST result page.
#[derive(Debug, Clone, Default)]
pub struct ListPage {
    pub contents: Vec<ListEntry>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_marker: Option<String>,
}

/// HEAD/GET object metadata.
#[derive(Debug, Clone, Default)]
pub struct ObjectMeta {
    pub key: String,
    pub content_length: u64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub content_type: Option<String>,
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub inode_no: Option<u64>,
    pub dentry_name: Option<String>,
    pub dht_id: Option<u64>,
    pub user_meta: HashMap<String, String>,
}

/// Completed MPU part descriptor.
#[derive(Debug, Clone)]
pub struct CompletedPart {
    pub part_number: i32,
    pub etag: String,
}

/// Content summary for getContentSummary.
#[derive(Debug, Clone, Default)]
pub struct ContentSummary {
    pub file_count: u64,
    pub directory_count: u64,
    pub length: u64,
    pub space_consumed: u64,
}

/// Block location for listLocatedStatus.
#[derive(Debug, Clone)]
pub struct BlockLocation {
    pub offset: i64,
    pub length: i64,
    pub hosts: Vec<String>,
    pub names: Vec<String>,
}

/// Open file options (Hadoop 3.x openFile API).
#[derive(Debug, Clone, Default)]
pub struct OpenOptions {
    pub buffer_size: usize,
    pub status_hint: Option<FileStatus>,
    pub must_use_cache: bool,
}

/// Create file options.
#[derive(Debug, Clone)]
pub struct CreateOptions {
    pub overwrite: bool,
    pub buffer_size: usize,
    pub replication: i16,
    pub block_size: i64,
    /// Permission/special bits (0o7777); file type bits are supplied by create.
    pub permission: u32,
    pub create_parent: bool,
}

impl Default for CreateOptions {
    fn default() -> Self {
        Self {
            overwrite: false,
            buffer_size: 256 * 1024,
            replication: 1,
            block_size: 128 * 1024 * 1024,
            permission: 0o644,
            create_parent: true,
        }
    }
}

/// POSIX S_IFDIR / S_IFREG constants.
pub const S_IFDIR: u32 = 0o040000;
pub const S_IFREG: u32 = 0o100000;
pub const S_IFMT: u32 = 0o170000;

/// Default cache TTLs.
pub const DEFAULT_POSITIVE_CACHE_TTL: Duration = Duration::from_millis(1000);
pub const DEFAULT_NEGATIVE_CACHE_TTL: Duration = Duration::from_millis(300);
pub const DEFAULT_LIST_CACHE_TTL: Duration = Duration::from_millis(500);

/// Trash directory prefix.
pub const DEFAULT_TRASH_DIR: &str = ".obs_trash";

/// Directory marker metadata key.
pub const META_DIR_MARKER: &str = "dir";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_mode_octal() {
        assert_eq!(ListEntry::parse_mode_octal("40755"), Some(0o40755));
        assert_eq!(ListEntry::parse_mode_octal("100644"), Some(0o100644));
    }

    #[test]
    fn inode_hint_requires_both_fields() {
        assert!(InodeHint::new(Some(1), Some("abc".into())).is_some());
        assert!(InodeHint::new(Some(1), None).is_none());
        assert!(InodeHint::new(None, Some("abc".into())).is_none());
        assert!(InodeHint::new(Some(1), Some("".into())).is_none());
    }

    #[test]
    fn list_entry_directory_detection() {
        let dir = ListEntry {
            key: "a/b/".into(),
            mode: Some(0o040755),
            ..Default::default()
        };
        assert!(dir.is_directory());

        let file = ListEntry {
            key: "a/b".into(),
            mode: Some(0o100644),
            ..Default::default()
        };
        assert!(file.is_file());
    }
}
