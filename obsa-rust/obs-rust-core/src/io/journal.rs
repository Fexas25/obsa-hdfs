//! Opt-in, local known-MPU cleanup journal. No credentials or object bodies.
//! A held OS file lock prevents two processes from owning the same namespace.
use crate::{error::{ObsError, ObsResult}, store::ObjectStore};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::{collections::HashSet, fs::{self, File, OpenOptions}, io::{Read, Write},
    path::{Path, PathBuf}, sync::{Arc, Mutex}};

const MAX_RECORD: usize = 64 * 1024;
fn error(e: impl std::fmt::Display) -> ObsError { ObsError::Io(format!("upload journal: {e}")) }
fn hash(bytes: &[u8]) -> String { format!("{:x}", Sha1::digest(bytes)) }

#[derive(Default, Debug)]
pub struct RecoveryReport {
    pub aborted: usize,
    pub active: usize,
    pub errors: Vec<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record { version: u32, scope: String, key: String, upload_id: String }
fn record_name(key: &str, id: &str) -> String {
    format!("{}.json", hash(&serde_json::to_vec(&(key, id)).unwrap()))
}

enum RecoveryItem {
    Active,
    Loaded(JournalEntry, ObsResult<Record>),
}

pub(crate) struct UploadJournal {
    directory: PathBuf,
    scope: String,
    active: Mutex<HashSet<String>>,
    scan: Mutex<Option<fs::ReadDir>>,
    _lock: File,
}
impl UploadJournal {
    pub(crate) fn open(directory: &Path, namespace: &[u8]) -> ObsResult<Arc<Self>> {
        let scope = hash(namespace);
        let directory = directory.join(&scope);
        fs::create_dir_all(&directory).map_err(error)?;
        let lock = OpenOptions::new().create(true).truncate(false).read(true).write(true)
            .open(directory.join("owner.lock")).map_err(error)?;
        Self::acquire(&lock)?;
        Ok(Arc::new(Self { directory, scope, active: Mutex::new(HashSet::new()), scan: Mutex::new(None), _lock: lock }))
    }
    /// Exclusive, non-blocking namespace lock. On Unix a concurrently forked
    /// child (any thread spawning a process, as JVMs routinely do) briefly
    /// shares the flock until it execs and its close-on-exec descriptors go,
    /// so a just-released lock can look held for an instant. Retry a contended
    /// lock for up to ~200 ms; a live owner still reports the conflict.
    /// Runs on a blocking worker, so sleeping here does not stall the runtime.
    fn acquire(lock: &File) -> ObsResult<()> {
        const ATTEMPTS: u32 = 20;
        for attempt in 1..=ATTEMPTS {
            match lock.try_lock() {
                Ok(()) => return Ok(()),
                Err(std::fs::TryLockError::WouldBlock) if attempt < ATTEMPTS => {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(e) => return Err(error(e)),
            }
        }
        unreachable!("the final attempt always returns")
    }
    pub(crate) fn record(self: &Arc<Self>, key: &str, id: &str) -> ObsResult<JournalEntry> {
        let name = record_name(key, id);
        if !self.active.lock().unwrap().insert(name.clone()) {
            return Err(error("upload ID already active"));
        }
        let entry = JournalEntry { journal: self.clone(), active_name: name.clone(), name };
        let record = serde_json::to_vec(&Record {
            version: 1, scope: self.scope.clone(), key: key.into(), upload_id: id.into(),
        }).map_err(error)?;
        if record.len() > MAX_RECORD { return Err(error("record exceeds 64 KiB")); }
        let path = self.directory.join(&entry.name);
        if path.exists() { return Err(error("record already exists; recover it first")); }
        let temporary = path.with_extension("pending");
        let mut file = OpenOptions::new().write(true).create_new(true).open(&temporary).map_err(error)?;
        file.write_all(&record).map_err(error)?;
        file.sync_all().map_err(error)?;
        drop(file);
        fs::rename(&temporary, &path).map_err(error)?;
        #[cfg(unix)]
        File::open(&self.directory).and_then(|f| f.sync_all()).map_err(error)?;
        Ok(entry)
    }
    // The returned guard owns the claim even if its async receiver is cancelled.
    // Discarding a worker result drops the guard, leaving the record for a later pass.
    async fn next_record_async(self: &Arc<Self>) -> ObsResult<Option<RecoveryItem>> {
        let journal = self.clone();
        tokio::task::spawn_blocking(move || journal.next_record()).await.map_err(error)?
    }
    fn next_record(self: &Arc<Self>) -> ObsResult<Option<RecoveryItem>> {
        loop {
            let item = {
                let mut scan = self.scan.lock().unwrap();
                if scan.is_none() { *scan = Some(fs::read_dir(&self.directory).map_err(error)?); }
                let item = scan.as_mut().unwrap().next();
                if item.is_none() { *scan = None; }
                item
            };
            let Some(item) = item else { return Ok(None); };
            let item = item.map_err(error)?;
            if !matches!(item.path().extension().and_then(|x| x.to_str()), Some("json" | "pending")) { continue; }
            let name = item.file_name().to_string_lossy().into_owned();
            let active_name = item.path().with_extension("json").file_name().unwrap().to_string_lossy().into_owned();
            if !self.active.lock().unwrap().insert(active_name.clone()) {
                return Ok(Some(RecoveryItem::Active));
            }
            let entry = JournalEntry { journal: self.clone(), name, active_name };
            let loaded = (|| -> ObsResult<Record> {
                if !item.file_type().map_err(error)?.is_file() { return Err(error("not a regular record file")); }
                let mut bytes = Vec::new();
                File::open(item.path()).map_err(error)?.take((MAX_RECORD + 1) as u64)
                    .read_to_end(&mut bytes).map_err(error)?;
                if bytes.len() > MAX_RECORD { return Err(error("oversized record")); }
                let record: Record = serde_json::from_slice(&bytes).map_err(error)?;
                if record.version != 1 || record.scope != self.scope || record.upload_id.is_empty()
                    || record_name(&record.key, &record.upload_id) != entry.active_name {
                    return Err(error("invalid record identity"));
                }
                Ok(record)
            })();
            return Ok(Some(RecoveryItem::Loaded(entry, loaded)));
        }
    }
    pub(crate) async fn recover(self: &Arc<Self>, store: &dyn ObjectStore, limit: usize) -> ObsResult<RecoveryReport> {
        if limit == 0 || limit > 10_000 { return Err(error("recovery limit must be 1..=10000")); }
        let mut report = RecoveryReport::default();
        let mut inspected = 0;
        let mut restarted = false;
        while inspected < limit {
            let Some(item) = self.next_record_async().await? else {
                if inspected == 0 && !restarted { restarted = true; continue; }
                break;
            };
            inspected += 1;
            let (entry, loaded) = match item {
                RecoveryItem::Active => { report.active += 1; continue; },
                RecoveryItem::Loaded(entry, loaded) => (entry, loaded),
            };
            let name = entry.name.clone();
            let result = match loaded {
                Ok(record) => match super::abort_known_upload(store, &record.key, &record.upload_id).await {
                    Ok(()) => entry.remove().await,
                    Err(e) => Err(e),
                },
                Err(e) => Err(e),
            };
            match result {
                Ok(()) => report.aborted += 1,
                Err(e) => report.errors.push(format!("{name}: {e}")),
            }
        }
        Ok(report)
    }
}

pub(crate) struct JournalEntry { journal: Arc<UploadJournal>, name: String, active_name: String }
impl JournalEntry {
    pub(crate) async fn remove(self) -> ObsResult<()> {
        // Transfer the entire guard: cancellation must not release the claim or
        // namespace lock while a queued deletion can still touch the record.
        tokio::task::spawn_blocking(move || {
            match fs::remove_file(self.journal.directory.join(&self.name)) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(error(e)),
            }
        }).await.map_err(error)?
    }
}
impl Drop for JournalEntry {
    fn drop(&mut self) { self.journal.active.lock().unwrap().remove(&self.active_name); }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancelled_queued_delete_retains_claim_and_lock_until_worker_finishes() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all().max_blocking_threads(1).build().unwrap();
        let directory = std::env::current_dir().unwrap().join("target").join(format!(
            "journal-delete-worker-{}-{}", std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        let journal = UploadJournal::open(&directory, b"delete-worker-test").unwrap();
        let entry = journal.record("key", "id").unwrap();
        let record = journal.directory.join(record_name("key", "id"));
        let weak = Arc::downgrade(&journal);
        let (release, wait) = std::sync::mpsc::channel::<()>();
        let (started, ready) = std::sync::mpsc::channel();
        let worker = runtime.spawn_blocking(move || {
            started.send(()).unwrap();
            let _ = wait.recv();
        });
        ready.recv().unwrap();
        runtime.block_on(async {
            assert!(tokio::time::timeout(std::time::Duration::from_millis(20), entry.remove())
                .await.is_err());
            assert_eq!(journal.active.lock().unwrap().len(), 1);
            drop(journal);
            assert!(weak.upgrade().is_some());
            assert!(record.exists());
            assert!(UploadJournal::open(&directory, b"delete-worker-test").is_err());
            drop(release);
            worker.await.unwrap();
        });
        drop(runtime); // waits for the detached queued delete
        assert!(weak.upgrade().is_none());
        assert!(!record.exists());
        drop(UploadJournal::open(&directory, b"delete-worker-test").unwrap());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn cancelled_queued_record_read_does_not_block_runtime_or_leak_claim() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all().max_blocking_threads(1).build().unwrap();
        let directory = std::env::current_dir().unwrap().join("target").join(format!(
            "journal-worker-{}-{}", std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        let journal = UploadJournal::open(&directory, b"worker-test").unwrap();
        drop(journal.record("key", "id").unwrap());
        let (release, wait) = std::sync::mpsc::channel::<()>();
        let (started, ready) = std::sync::mpsc::channel();
        let worker = runtime.spawn_blocking(move || {
            started.send(()).unwrap();
            let _ = wait.recv();
        });
        ready.recv().unwrap();
        runtime.block_on(async {
            // The sole blocking worker is occupied. A timer must still run on
            // the current-thread executor while the record read stays queued.
            assert!(tokio::time::timeout(std::time::Duration::from_millis(20),
                journal.next_record_async()).await.is_err());
            assert!(journal.active.lock().unwrap().is_empty());
            drop(release);
            worker.await.unwrap();
        });
        // Runtime shutdown waits for the detached blocking read to finish.
        drop(runtime);
        assert!(journal.active.lock().unwrap().is_empty());
        assert!(journal.directory.join(record_name("key", "id")).exists());
        drop(journal);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
