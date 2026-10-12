use async_trait::async_trait;
use bytes::Bytes;
use md5::{Digest, Md5};
use obs_rust_core::{
    error::ObsResult,
    io::{input::ObsInputStream, output::ObsOutputStream, pfs_append::PfsAppendStream},
    store::{GetResult, ObjectStore, PutHeaders},
    types::*,
    ObsConfig, ObsError, ObsFileSystem,
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{Notify, Semaphore};

struct JournalTestDir(std::path::PathBuf);
impl JournalTestDir {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let name = format!("journal-test-{}-{}-{}", std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed));
        let path = std::env::current_dir().unwrap().join("target").join(name);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn config(&self) -> ObsConfig {
        ObsConfig {
            endpoint: "journal-mock".into(), bucket: "bucket".into(),
            access_key: "never-store-this-access-key".into(), secret_key: "never-store-this-secret".into(),
            upload_journal_dir: Some(self.0.to_str().unwrap().into()), multipart_threshold: 1,
            ..Default::default()
        }
    }
    fn records(&self) -> Vec<std::path::PathBuf> {
        std::fs::read_dir(&self.0).unwrap().flat_map(|dir| {
            std::fs::read_dir(dir.unwrap().path()).unwrap().map(|file| file.unwrap().path())
        }).filter(|p| matches!(p.extension().and_then(|x| x.to_str()), Some("json" | "pending"))).collect()
    }
}
impl Drop for JournalTestDir {
    fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); }
}

#[test]
fn journal_crash_child() {
    let Ok(path) = std::env::var("OBSA_TEST_CRASH_JOURNAL") else { return; };
    let directory = JournalTestDir(std::path::PathBuf::from(path));
    assert!(directory.0.starts_with(std::env::current_dir().unwrap().join("target")));
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let fs = ObsFileSystem::with_store(directory.config(), Arc::new(MemoryStore::default()));
        let mut a = fs.create("/orphan-a", CreateOptions::default()).await.unwrap();
        let mut b = fs.create("/orphan-b", CreateOptions::default()).await.unwrap();
        a.write(b"pending").await.unwrap();
        b.write(b"pending").await.unwrap();
        assert_eq!(directory.records().len(), 2);
        std::process::exit(0); // bypass all Rust destructors, including lock/stream cleanup
    });
}

#[tokio::test]
async fn journal_recovers_after_process_exit_preserves_failures_and_limits_scope() {
    let directory = JournalTestDir::new();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "journal_crash_child", "--nocapture"])
        .env("OBSA_TEST_CRASH_JOURNAL", &directory.0).output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(directory.records().len(), 2);
    // Simulate exit after file sync but before publication rename.
    let record = directory.records().remove(0);
    std::fs::rename(&record, record.with_extension("pending")).unwrap();
    for record in directory.records() {
        let text = std::fs::read_to_string(record).unwrap();
        assert!(!text.contains("never-store-this"));
    }
    let store = Arc::new(MemoryStore { abort_failures: AtomicUsize::new(1), ..Default::default() });
    let mut other = directory.config(); other.bucket = "other-bucket".into();
    let other_fs = ObsFileSystem::with_store(other, store.clone());
    assert_eq!(other_fs.recover_pending_uploads(10).await.unwrap().aborted, 0);
    assert_eq!(store.count("abort"), 0);
    let fs = ObsFileSystem::with_store(directory.config(), store.clone());
    let report = fs.recover_pending_uploads(1).await.unwrap();
    assert_eq!(report.errors.len(), 1);
    assert_eq!(directory.records().len(), 2); // failed remote cleanup is retained
    assert_eq!(fs.recover_pending_uploads(1).await.unwrap().aborted, 1);
    assert_eq!(directory.records().len(), 1);
    assert_eq!(fs.recover_pending_uploads(1).await.unwrap().aborted, 1);
    assert_eq!(fs.recover_pending_uploads(1).await.unwrap().aborted, 0);
    assert_eq!(store.count("abort"), 3);
    assert_eq!(store.count("complete"), 0);
    drop(fs); drop(other_fs);
}

fn create_crash_records(directory: &JournalTestDir) {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "journal_crash_child", "--nocapture"])
        .env("OBSA_TEST_CRASH_JOURNAL", &directory.0).output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
}

#[tokio::test]
async fn journal_small_batches_advance_past_retained_corruption() {
    let directory = JournalTestDir::new();
    create_crash_records(&directory);
    // Corrupt the first entry in the actual filesystem enumeration order.
    let first = directory.records().remove(0);
    std::fs::write(&first, b"broken").unwrap();
    let store = Arc::new(MemoryStore::default());
    let fs = ObsFileSystem::with_store(directory.config(), store.clone());
    let mut aborted = 0;
    for _ in 0..4 {
        aborted += fs.recover_pending_uploads(1).await.unwrap().aborted;
    }
    assert_eq!(aborted, 1);
    assert_eq!(store.count("abort"), 1);
    assert_eq!(directory.records(), vec![first]);
    drop(fs);
}

#[tokio::test]
async fn journal_cancelled_recovery_releases_claim_and_concurrent_recovery_skips_it() {
    let directory = JournalTestDir::new();
    create_crash_records(&directory);
    std::fs::remove_file(directory.records().remove(0)).unwrap();
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore { abort_control: Some(control.clone()), ..Default::default() });
    let fs = ObsFileSystem::with_store(directory.config(), store.clone());
    let mut recovery = Box::pin(fs.recover_pending_uploads(1));
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::select! {
            result = &mut recovery => panic!("unexpected completion: {result:?}"),
            _ = control.started.notified() => {},
        }
    }).await.unwrap();
    let report = fs.recover_pending_uploads(10).await.unwrap();
    assert_eq!(report.active, 1);
    assert_eq!(store.count("abort"), 1);
    drop(recovery);
    assert_eq!(directory.records().len(), 1);
    control.gate.add_permits(1);
    let report = tokio::time::timeout(Duration::from_secs(2), fs.recover_pending_uploads(10))
        .await.unwrap().unwrap();
    assert_eq!(report.aborted, 1);
    assert!(report.errors.is_empty());
    assert!(directory.records().is_empty());
    assert_eq!(store.count("abort"), 2);
    drop(fs);
}

#[tokio::test]
async fn journal_already_absent_uploads_remove_records_idempotently() {
    let directory = JournalTestDir::new();
    create_crash_records(&directory);
    let store = Arc::new(MemoryStore { abort_missing: true, ..Default::default() });
    let fs = ObsFileSystem::with_store(directory.config(), store.clone());
    let report = fs.recover_pending_uploads(10).await.unwrap();
    assert_eq!(report.aborted, 2);
    assert!(report.errors.is_empty());
    assert!(directory.records().is_empty());
    assert_eq!(fs.recover_pending_uploads(10).await.unwrap().aborted, 0);
    assert_eq!(store.count("abort"), 2);
    drop(fs);
}

#[tokio::test]
async fn journal_other_not_found_and_wrong_status_do_not_discard_records() {
    for error in [
        ObsError::NotFound { code: "NoSuchBucket".into(), message: "missing bucket".into(), request_id: "mock".into() },
        ObsError::NotFound { code: "NoSuchKey".into(), message: "missing key".into(), request_id: "mock".into() },
        ObsError::NotFound { code: "".into(), message: "unclassified 404".into(), request_id: "mock".into() },
        ObsError::Http { status: 403, code: "NoSuchUpload".into(), message: "inconsistent response".into(), request_id: "mock".into() },
    ] {
        let directory = JournalTestDir::new();
        create_crash_records(&directory);
        let store = Arc::new(MemoryStore { abort_error: Some(error.clone()), ..Default::default() });
        let fs = ObsFileSystem::with_store(directory.config(), store.clone());
        let report = fs.recover_pending_uploads(10).await.unwrap();
        assert_eq!(report.aborted, 0, "{error}");
        assert_eq!(report.errors.len(), 2, "{error}");
        assert_eq!(directory.records().len(), 2);
        assert_eq!(store.count("abort"), 2);
        drop(fs);
    }
}

#[tokio::test]
async fn explicit_abort_accepts_absent_mpu_without_fallback_or_stale_journal() {
    let directory = JournalTestDir::new();
    let store = Arc::new(MemoryStore { abort_missing: true, ..Default::default() });
    let fs = ObsFileSystem::with_store(directory.config(), store.clone());
    let mut output = fs.create("/discard", CreateOptions::default()).await.unwrap()
        .on_close(|_| panic!("abort must not call success callback"));
    output.write(b"data").await.unwrap();
    output.abort().await.unwrap();
    tokio::task::yield_now().await;
    assert!(directory.records().is_empty());
    assert_eq!(store.count("abort"), 1);
    assert_eq!(store.count("complete"), 0);
    drop(fs);
}

#[tokio::test]
async fn failed_close_preserves_original_error_when_cleanup_reports_absent_mpu() {
    let directory = JournalTestDir::new();
    let store = Arc::new(MemoryStore { abort_missing: true, fail_part: true, ..Default::default() });
    let fs = ObsFileSystem::with_store(directory.config(), store.clone());
    let mut output = fs.create("/failed", CreateOptions::default()).await.unwrap()
        .on_close(|_| panic!("failed close must not call success callback"));
    output.write(b"data").await.unwrap();
    assert!(output.close().await.unwrap_err().to_string().contains("injected part failure"));
    tokio::task::yield_now().await;
    assert!(directory.records().is_empty());
    assert_eq!(store.count("abort"), 1);
    assert_eq!(store.count("complete"), 0);
    drop(fs);
}

#[tokio::test]
async fn dropped_upload_removes_journal_when_cleanup_reports_absent_mpu() {
    let directory = JournalTestDir::new();
    let store = Arc::new(MemoryStore { abort_missing: true, ..Default::default() });
    let fs = ObsFileSystem::with_store(directory.config(), store.clone());
    let mut output = fs.create("/dropped", CreateOptions::default()).await.unwrap();
    output.write(b"data").await.unwrap();
    drop(output);
    tokio::time::timeout(Duration::from_secs(2), async {
        while !directory.records().is_empty() { tokio::task::yield_now().await; }
    }).await.unwrap();
    assert_eq!(store.count("abort"), 1);
    assert_eq!(store.count("complete"), 0);
    drop(fs);
}

#[tokio::test]
async fn upload_cleanup_retains_journal_for_missing_bucket_in_all_paths() {
    for mode in ["abort", "close", "drop"] {
        let directory = JournalTestDir::new();
        let store = Arc::new(MemoryStore {
            abort_error: Some(ObsError::NotFound {
                code: "NoSuchBucket".into(), message: "missing bucket".into(), request_id: "mock".into(),
            }), fail_part: mode == "close", ..Default::default()
        });
        let fs = ObsFileSystem::with_store(directory.config(), store.clone());
        let mut output = fs.create("/retained", CreateOptions::default()).await.unwrap();
        output.write(b"data").await.unwrap();
        match mode {
            "abort" => assert_eq!(output.abort().await.unwrap_err().code(), Some("NoSuchBucket")),
            "close" => assert!(output.close().await.unwrap_err().to_string().contains("injected part failure")),
            _ => drop(output),
        }
        let expected = if mode == "drop" { 1 } else { 2 };
        tokio::time::timeout(Duration::from_secs(2), async {
            while store.count("abort") < expected { tokio::task::yield_now().await; }
        }).await.unwrap();
        assert_eq!(directory.records().len(), 1);
        assert_eq!(store.count("complete"), 0);
        drop(fs);
    }
}

fn cancel_queued_journal_initialization(fs: &ObsFileSystem) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all().max_blocking_threads(1).build().unwrap();
    let (release, wait) = std::sync::mpsc::channel::<()>();
    let (started, ready) = std::sync::mpsc::channel();
    let worker = runtime.spawn_blocking(move || {
        started.send(()).unwrap();
        let _ = wait.recv();
    });
    ready.recv().unwrap();
    runtime.block_on(async {
        assert!(tokio::time::timeout(Duration::from_millis(20), fs.recover_pending_uploads(1))
            .await.is_err());
        drop(release);
        worker.await.unwrap();
    });
    drop(runtime); // all detached initialization work must finish before returning
}

#[test]
fn cancelled_initialization_keeps_successful_namespace_owned_by_filesystem() {
    let directory = JournalTestDir::new();
    let store = Arc::new(MemoryStore::default());
    let fs = ObsFileSystem::with_store(directory.config(), store.clone());
    cancel_queued_journal_initialization(&fs);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let other = ObsFileSystem::with_store(directory.config(), store.clone());
        assert!(other.recover_pending_uploads(1).await.is_err());
        assert_eq!(fs.recover_pending_uploads(1).await.unwrap().aborted, 0);
        assert_eq!(store.count("abort"), 0);
        drop(fs);
        assert_eq!(other.recover_pending_uploads(1).await.unwrap().aborted, 0);
        drop(other);
    });
}

#[test]
fn cancelled_failed_initialization_is_not_cached_and_can_retry() {
    let directory = JournalTestDir::new();
    let store = Arc::new(MemoryStore::default());
    let owner = ObsFileSystem::with_store(directory.config(), store.clone());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(owner.recover_pending_uploads(1)).unwrap();
    let fs = ObsFileSystem::with_store(directory.config(), store.clone());
    cancel_queued_journal_initialization(&fs); // worker encounters the owner's OS lock
    drop(owner);
    assert_eq!(runtime.block_on(fs.recover_pending_uploads(1)).unwrap().aborted, 0);
    assert_eq!(store.count("abort"), 0);
    drop(fs);
}

#[tokio::test]
async fn concurrent_first_creates_share_one_journal_initialization() {
    let directory = JournalTestDir::new();
    let store = Arc::new(MemoryStore::default());
    let fs = ObsFileSystem::with_store(directory.config(), store.clone());
    let (a, b) = tokio::join!(fs.create("/a", CreateOptions::default()), fs.create("/b", CreateOptions::default()));
    let (mut a, mut b) = (a.unwrap(), b.unwrap());
    let (a_write, b_write) = tokio::join!(a.write(b"a"), b.write(b"b"));
    a_write.unwrap(); b_write.unwrap();
    let report = fs.recover_pending_uploads(10).await.unwrap();
    assert_eq!(report.active, 2);
    assert_eq!(store.count("abort"), 0);
    let (a_abort, b_abort) = tokio::join!(a.abort(), b.abort());
    a_abort.unwrap(); b_abort.unwrap();
    assert!(directory.records().is_empty());
    drop(fs);
}

#[tokio::test]
async fn journal_invalid_limit_does_not_create_namespace_or_take_lock() {
    let directory = JournalTestDir::new();
    let store = Arc::new(MemoryStore::default());
    let fs = ObsFileSystem::with_store(directory.config(), store.clone());
    for limit in [0, 10001, usize::MAX] {
        assert!(matches!(fs.recover_pending_uploads(limit).await,
            Err(ObsError::InvalidArgument(_))));
    }
    assert_eq!(std::fs::read_dir(&directory.0).unwrap().count(), 0);
    let other = ObsFileSystem::with_store(directory.config(), store.clone());
    assert_eq!(other.recover_pending_uploads(1).await.unwrap().aborted, 0);
    // Invalid calls must retain argument error precedence even while another
    // instance owns the namespace, rather than attempting to acquire its lock.
    assert!(matches!(fs.recover_pending_uploads(0).await,
        Err(ObsError::InvalidArgument(_))));
    assert_eq!(store.count("abort"), 0);
    drop(other); drop(fs);
}

// Windows share modes provide a real deletion failure without modifying ACLs
// or replacing the journal contents. Reads remain allowed for recovery.
#[cfg(windows)]
fn hold_journal_against_deletion(path: &std::path::Path) -> std::fs::File {
    use std::os::windows::fs::OpenOptionsExt;
    std::fs::OpenOptions::new().read(true).share_mode(1 | 2).open(path).unwrap()
}

#[cfg(windows)]
#[tokio::test]
async fn journal_delete_failure_does_not_turn_committed_upload_into_failure() {
    let directory = JournalTestDir::new();
    let store = Arc::new(MemoryStore { abort_missing: true, ..Default::default() });
    let fs = ObsFileSystem::with_store(directory.config(), store.clone());
    let commits = Arc::new(AtomicUsize::new(0));
    let called = commits.clone();
    let mut output = fs.create("/committed", CreateOptions::default()).await.unwrap()
        .on_close(move |_| { called.fetch_add(1, Ordering::SeqCst); });
    output.write(b"saved").await.unwrap();
    let path = directory.records().remove(0);
    let held = hold_journal_against_deletion(&path);
    output.close().await.unwrap();
    assert_eq!(commits.load(Ordering::SeqCst), 1);
    assert_eq!(store.count("complete"), 1);
    assert_eq!(store.count("abort"), 0);
    assert_eq!(store.objects.lock().unwrap().get("committed").unwrap().as_ref(), b"saved");
    assert!(path.exists());
    drop(held);
    let report = fs.recover_pending_uploads(10).await.unwrap();
    assert_eq!(report.aborted, 1);
    assert_eq!(report.active, 0);
    assert!(report.errors.is_empty());
    assert!(!path.exists());
    assert_eq!(store.objects.lock().unwrap().get("committed").unwrap().as_ref(), b"saved");
    assert_eq!(store.count("complete"), 1);
    drop(fs);
}

#[cfg(windows)]
#[tokio::test]
async fn journal_failed_local_delete_is_reported_and_retry_releases_claim() {
    let directory = JournalTestDir::new();
    create_crash_records(&directory);
    std::fs::remove_file(directory.records().remove(0)).unwrap();
    let path = directory.records().remove(0);
    let held = hold_journal_against_deletion(&path);
    let store = Arc::new(MemoryStore { abort_missing: true, ..Default::default() });
    let fs = ObsFileSystem::with_store(directory.config(), store.clone());
    let report = fs.recover_pending_uploads(10).await.unwrap();
    assert_eq!(report.aborted, 0); // remote absence is insufficient if local deletion fails
    assert_eq!(report.active, 0);
    assert_eq!(report.errors.len(), 1);
    assert!(report.errors[0].contains("upload journal"));
    assert!(path.exists());
    drop(held);
    let report = fs.recover_pending_uploads(10).await.unwrap();
    assert_eq!(report.aborted, 1);
    assert_eq!(report.active, 0);
    assert!(report.errors.is_empty());
    assert!(!path.exists());
    assert_eq!(store.count("abort"), 2);
    drop(fs);
}

#[cfg(windows)]
#[tokio::test]
async fn journal_explicit_abort_retains_undeletable_record_for_later_cleanup() {
    let directory = JournalTestDir::new();
    let store = Arc::new(MemoryStore::default());
    let fs = ObsFileSystem::with_store(directory.config(), store.clone());
    let mut output = fs.create("/discard", CreateOptions::default()).await.unwrap()
        .on_close(|_| panic!("abort must not publish success"));
    output.write(b"discarded").await.unwrap();
    let path = directory.records().remove(0);
    let held = hold_journal_against_deletion(&path);
    output.abort().await.unwrap();
    assert!(path.exists());
    assert_eq!(store.count("abort"), 1);
    assert_eq!(store.count("complete"), 0);
    assert!(!store.objects.lock().unwrap().contains_key("discard"));
    drop(held);
    let report = fs.recover_pending_uploads(10).await.unwrap();
    assert_eq!(report.aborted, 1);
    assert!(report.errors.is_empty());
    assert!(!path.exists());
    drop(fs);
}

#[tokio::test]
async fn journal_persistence_failure_stops_parts_and_keeps_known_id_for_abort() {
    let directory = JournalTestDir::new();
    let store = Arc::new(MemoryStore::default());
    let fs = ObsFileSystem::with_store(directory.config(), store.clone());
    let mut output = fs.create("/file", CreateOptions::default()).await.unwrap();
    output.write(b"a").await.unwrap();
    let path = directory.records().remove(0);
    output.abort().await.unwrap();
    // MemoryStore repeats its upload ID; occupy that record's staging path to
    // inject a real filesystem publication failure without changing permissions.
    std::fs::create_dir(path.with_extension("pending")).unwrap();
    let mut output = fs.create("/file", CreateOptions::default()).await.unwrap();
    let error = output.write(&vec![1; 16 * 1024 * 1024]).await.unwrap_err();
    assert!(error.to_string().contains("upload journal"));
    assert_eq!(store.count("part:"), 0);
    assert!(output.write(b"retry").await.is_err());
    assert!(output.close().await.is_err());
    assert_eq!(store.count("abort"), 2);
    assert_eq!(store.count("complete"), 0);
    assert!(!store.objects.lock().unwrap().contains_key("file"));
    drop(fs);
}

#[tokio::test]
async fn journal_rejects_corrupt_and_oversized_records_without_remote_mutation() {
    let directory = JournalTestDir::new();
    let store = Arc::new(MemoryStore::default());
    let fs = ObsFileSystem::with_store(directory.config(), store.clone());
    assert_eq!(fs.recover_pending_uploads(10).await.unwrap().aborted, 0);
    let namespace = std::fs::read_dir(&directory.0).unwrap().next().unwrap().unwrap().path();
    std::fs::write(namespace.join("broken.pending"), b"{\"version\":").unwrap();
    std::fs::write(namespace.join("oversized.json"), vec![b' '; 65537]).unwrap();
    std::fs::write(namespace.join("wrong-scope.json"), br#"{"version":1,"scope":"foreign","key":"victim","upload_id":"id"}"#).unwrap();
    let report = fs.recover_pending_uploads(10).await.unwrap();
    assert_eq!(report.errors.len(), 3);
    assert_eq!(report.aborted, 0);
    assert_eq!(store.count("abort"), 0);
    assert_eq!(directory.records().len(), 3);
    assert!(fs.recover_pending_uploads(0).await.is_err());
    assert!(fs.recover_pending_uploads(10001).await.is_err());
    drop(fs);
}

#[tokio::test]
async fn journal_skips_live_uploads_locks_namespace_and_removes_completed_records() {
    let directory = JournalTestDir::new();
    let store = Arc::new(MemoryStore::default());
    let fs = ObsFileSystem::with_store(directory.config(), store.clone());
    let mut output = fs.create("/live", CreateOptions::default()).await.unwrap();
    output.write(b"pending").await.unwrap();
    let report = fs.recover_pending_uploads(10).await.unwrap();
    assert_eq!(report.active, 1);
    assert_eq!(report.aborted, 0);
    let other = ObsFileSystem::with_store(directory.config(), store.clone());
    assert!(other.recover_pending_uploads(10).await.is_err());
    assert_eq!(store.count("abort"), 0);
    output.close().await.unwrap();
    assert!(directory.records().is_empty());
    let mut output = fs.create("/discard", CreateOptions::default()).await.unwrap();
    output.write(b"pending").await.unwrap();
    output.abort().await.unwrap();
    assert!(directory.records().is_empty());
    drop(fs);
    // Failure to acquire the lock must not poison subsequent attempts.
    assert_eq!(other.recover_pending_uploads(10).await.unwrap().aborted, 0);
    drop(other);
}

#[tokio::test]
async fn explicit_abort_discards_buffered_put_and_releases_shared_budget() {
    let store = Arc::new(MemoryStore { single_get_chunk: true, ..Default::default() });
    store.put("source", Bytes::from_static(b"data"), PutHeaders::default()).await.unwrap();
    let fs = ObsFileSystem::with_store(ObsConfig {
        buffer_total_bytes: 4, readahead_chunk_size: 4, ..Default::default()
    }, store.clone());
    let mut output = fs.create("/discarded", CreateOptions::default()).await.unwrap()
        .on_close(|_| panic!("abort must not call success callback"));
    output.write(b"data").await.unwrap();
    output.abort().await.unwrap();
    assert_eq!(store.count("abort"), 0); // no MPU was initiated
    assert_eq!(store.count("put:discarded"), 0);
    let mut input = fs.open("/source", OpenOptions::default()).await.unwrap();
    input.read(&mut [0]).await.unwrap();
}

#[tokio::test]
async fn explicit_abort_drains_active_parts_and_reports_cleanup_failure() {
    for fail_abort in [false, true] {
        let control = AppendControl::blocked();
        let store = Arc::new(MemoryStore {
            part_control: Some((1, control.clone())),
            abort_failures: AtomicUsize::new(usize::from(fail_abort)),
            ..Default::default()
        });
        let config = ObsConfig {
            buffer_total_bytes: 5 * 1024 * 1024,
            multipart_size: 5 * 1024 * 1024, multipart_threshold: 1,
            ..Default::default()
        };
        let gate = Arc::new(Semaphore::new(1));
        let mut output = ObsOutputStream::new(store.clone(), &config, "discarded".into(), gate.clone())
            .on_close(|_| panic!("abort must not call success callback"));
        output.write(&vec![9; 5 * 1024 * 1024]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), control.started.notified()).await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(2), output.abort()).await.unwrap();
        if fail_abort {
            assert!(result.unwrap_err().to_string().contains("injected abort failure"));
            tokio::time::timeout(Duration::from_secs(2), async {
                while store.count("abort") < 2 { tokio::task::yield_now().await; }
            }).await.unwrap();
        } else {
            result.unwrap();
            assert_eq!(store.count("abort"), 1);
        }
        assert_eq!(gate.available_permits(), 1);
        assert_eq!(store.count("part:"), 0); // active part was cancelled before commit
        assert_eq!(store.count("complete"), 0);
        assert!(!store.objects.lock().unwrap().contains_key("discarded"));
    }
}

#[tokio::test]
async fn shared_payload_budget_crosses_read_write_append_and_recovers_after_pressure() {
    let store = Arc::new(MemoryStore { single_get_chunk: true, pfs: true, ..Default::default() });
    store.put("source", Bytes::from_static(b"abcdefgh"), PutHeaders::default()).await.unwrap();
    store.put_empty("append").await.unwrap();
    let fs = ObsFileSystem::with_store(ObsConfig {
        buffer_total_bytes: 4, readahead_chunk_size: 4, readahead_max_chunks: 0,
        ..Default::default()
    }, store.clone());
    let mut output = fs.create("/buffered", CreateOptions::default()).await.unwrap();
    output.write(b"held").await.unwrap();
    let mut input = fs.open("/source", OpenOptions::default()).await.unwrap();
    let error = input.read(&mut [0]).await.unwrap_err();
    assert!(error.to_string().contains("shared buffer budget exhausted"));
    assert_eq!(store.count("get:"), 0);
    assert_eq!(input.position(), 0);
    drop(output);
    let mut byte = [0];
    input.read(&mut byte).await.unwrap();
    assert_eq!(byte, [b'a']);
    let mut append = fs.append_stream("/append", 0).await.unwrap();
    assert!(append.write(b"x").await.unwrap_err().to_string().contains("shared buffer budget"));
    assert!(append.flush().await.is_err());
    assert_eq!(store.count("append:"), 0);
    drop(input);
    assert!(append.write(b"x").await.is_err()); // failed writes cannot be resumed
    let mut append = fs.append_stream("/append", 0).await.unwrap();
    append.write(b"good").await.unwrap();
    append.close().await.unwrap();
    let mut output = fs.create("/next", CreateOptions::default()).await.unwrap();
    output.write(b"full").await.unwrap(); // append returned every byte of its budget
    assert!(output.write(b"!").await.is_err());
    assert!(output.close().await.is_err());
    assert!(!store.objects.lock().unwrap().contains_key("next"));
    let mut input = fs.open("/source", OpenOptions::default()).await.unwrap();
    let mut all = [0; 8];
    assert_eq!(input.read(&mut all).await.unwrap(), 8); // old range released before next
    assert_eq!(&all, b"abcdefgh");
}

#[tokio::test]
async fn failed_multipart_returns_payload_budget_before_blocked_abort_and_cancelled_cleanup() {
    let control = AppendControl::blocked();
    let size = 5 * 1024 * 1024;
    let store = Arc::new(MemoryStore {
        single_get_chunk: true,        fail_part: true, abort_control: Some(control.clone()), ..Default::default()
    });
    store.put("source", Bytes::from(vec![7; size]), PutHeaders::default()).await.unwrap();
    let fs = ObsFileSystem::with_store(ObsConfig {
        buffer_total_bytes: size as u64, multipart_threshold: 1, multipart_size: size as u64,
        readahead_chunk_size: size as u64, readahead_max_chunks: 0,
        ..Default::default()
    }, store.clone());
    let mut output = fs.create("/failed", CreateOptions::default()).await.unwrap();
    output.write(&vec![9; size]).await.unwrap();
    {
        let close = output.close();
        tokio::pin!(close);
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::select! {
                _ = &mut close => panic!("abort must block"),
                _ = control.started.notified() => {}
            }
        }).await.unwrap();
        let mut input = fs.open("/source", OpenOptions::default()).await.unwrap();
        let mut byte = [0];
        input.read(&mut byte).await.unwrap(); // needs the entire budget while abort is blocked
        assert_eq!(byte, [7]);
        assert_eq!(store.count("complete"), 0);
        // Cancel close while aborting; Drop must retry cleanup without retaining payloads.
    }
    tokio::time::timeout(Duration::from_secs(2), control.started.notified()).await.unwrap();
    assert_eq!(store.count("abort"), 2);
    control.gate.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), control.stopped.notified()).await.unwrap();
    assert!(!store.objects.lock().unwrap().contains_key("failed"));
    let mut input = fs.open("/source", OpenOptions::default()).await.unwrap();
    input.read(&mut [0]).await.unwrap();
}

#[tokio::test]
async fn prefetch_skips_insufficient_budget_without_failing_sequential_reads() {
    let store = Arc::new(MemoryStore { single_get_chunk: true, ..Default::default() });
    let bytes: Vec<u8> = (0..64).collect();
    store.put("source", Bytes::from(bytes.clone()), PutHeaders::default()).await.unwrap();
    let fs = ObsFileSystem::with_store(ObsConfig {
        buffer_total_bytes: 4, readahead_chunk_size: 4, readahead_max_chunks: 8,
        ..Default::default()
    }, store.clone());
    let mut input = fs.open("/source", OpenOptions::default()).await.unwrap();
    let mut actual = [0; 64];
    assert_eq!(input.read(&mut actual).await.unwrap(), 64);
    assert_eq!(&actual[..], &bytes);
    assert_eq!(store.count("get:"), 16);
}

#[tokio::test]
async fn unaddressable_read_range_is_rejected_before_get_but_short_tail_is_allowed() {
    let length = isize::MAX as u64 + 1;
    let config = ObsConfig {
        readahead_chunk_size: length, readahead_max_chunks: 0,
        read_change_detection: false, ..Default::default()
    };
    let store = Arc::new(MemoryStore {
        fail_get_at: Some(0), get_failures: AtomicUsize::new(1), ..Default::default()
    });
    let mut input = ObsInputStream::new(store.clone(), &config, "file".into(), length, None);
    let mut byte = [0];
    let error = input.read(&mut byte).await.unwrap_err();
    assert!(matches!(error, ObsError::InvalidArgument(_)), "{error}");
    assert_eq!(store.count("get:"), 0);
    assert_eq!(input.position(), 0);

    let store = Arc::new(MemoryStore::default());
    store.put("file", Bytes::from_static(b"data"), PutHeaders::default()).await.unwrap();
    let mut input = ObsInputStream::new(store.clone(), &config, "file".into(), 4, None);
    let mut bytes = [0; 4];
    assert_eq!(input.read(&mut bytes).await.unwrap(), 4);
    assert_eq!(&bytes, b"data");
    assert_eq!(store.count("get:"), 1);
}

#[tokio::test]
async fn empty_append_write_reports_completed_failure_and_stays_sticky() {
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        pfs: true, append_commit_control: Some(control.clone()),
        append_commit_failure: true, ..Default::default()
    });
    store.put_empty("file").await.unwrap();
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    let mut stream = fs.append_stream("/file", 0).await.unwrap();
    assert_eq!(stream.write(b"").await.unwrap(), 0);
    stream.write(b"data").await.unwrap();
    control.started.notified().await;
    let mut empty = Box::pin(stream.write(b""));
    assert!(matches!(futures::poll!(empty.as_mut()), std::task::Poll::Ready(Ok(0))));
    drop(empty);
    control.gate.add_permits(1);
    control.stopped.notified().await;
    let error = stream.write(b"").await.unwrap_err();
    assert!(matches!(error, ObsError::Transport(_)));
    let expected = error.to_string();
    for result in [stream.write(b"").await, stream.write(b"more").await] {
        assert_eq!(result.unwrap_err().to_string(), expected);
    }
    assert_eq!(stream.flush().await.unwrap_err().to_string(), expected);
    assert_eq!(stream.close().await.unwrap_err().to_string(), expected);
    assert_eq!(store.count("append:"), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_releases_shared_byte_budget_before_user_callback() {
    let store = Arc::new(MemoryStore { pfs: true, ..Default::default() });
    store.put_empty("first").await.unwrap();
    store.put_empty("second").await.unwrap();
    let config = ObsConfig { pfs_append_total_bytes: 4, pfs_append_chunk_size: 4, ..Default::default() };
    let fs = ObsFileSystem::with_store(config, store);
    let started = Arc::new(Notify::new());
    let entered = started.clone();
    let calls = AtomicUsize::new(0);
    let (release, wait) = std::sync::mpsc::channel();
    let wait = Mutex::new(wait);
    let mut first = fs.append_stream("/first", 0).await.unwrap().on_flush(move || {
        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
            entered.notify_one();
            // Bound the deliberately blocked synchronous callback even if the test fails.
            let _ = wait.lock().unwrap().recv_timeout(Duration::from_secs(10));
        }
    });
    first.write(b"data").await.unwrap();
    started.notified().await;
    let mut second = fs.append_stream("/second", 0).await.unwrap();
    let progress = tokio::time::timeout(Duration::from_secs(3), async {
        second.write(b"more").await?;
        second.flush().await
    }).await;
    let _ = release.send(());
    first.flush().await.unwrap();
    progress.expect("completed append retained byte budget during user callback").unwrap();
    assert_eq!(fs.get_file_status("/second").await.unwrap().length, 4);
}

#[tokio::test]
async fn user_close_callback_cannot_replace_filesystem_cache_cleanup() {
    for fail in [false, true] {
        let store = Arc::new(MemoryStore { put_failures: AtomicUsize::new(usize::from(fail)), ..Default::default() });
        let fs = ObsFileSystem::with_store(ObsConfig::default(), store);
        let called = Arc::new(AtomicUsize::new(0));
        let counter = called.clone();
        let mut stream = fs.create("/file", CreateOptions::default()).await.unwrap()
            .on_close(|_| panic!("replaced user callback must not run"))
            .on_close(move |_| { counter.fetch_add(1, Ordering::SeqCst); });
        // Populate after installing callbacks, to detect premature guard drops.
        assert!(fs.get_file_status("/file").await.unwrap_err().is_not_found());
        stream.write(b"data").await.unwrap();
        assert_eq!(stream.close().await.is_err(), fail);
        assert_eq!(called.load(Ordering::SeqCst), usize::from(!fail));
        assert_eq!(fs.get_file_status("/file").await.unwrap().length, 4);
    }
}

#[tokio::test]
async fn user_append_callback_cannot_replace_task_cache_cleanup() {
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        pfs: true, append_commit_control: Some(control.clone()), ..Default::default()
    });
    store.put("file", Bytes::from_static(b"old"), PutHeaders::default()).await.unwrap();
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store);
    let called = Arc::new(AtomicUsize::new(0));
    let counter = called.clone();
    let mut stream = fs.append_stream("/file", 3).await.unwrap()
        .on_flush(|| panic!("replaced user callback must not run"))
        .on_flush(move || { counter.fetch_add(1, Ordering::SeqCst); });
    assert_eq!(fs.get_file_status("/file").await.unwrap().length, 3);
    stream.write(b"more").await.unwrap();
    control.started.notified().await;
    control.gate.add_permits(1);
    control.stopped.notified().await;
    assert_eq!(fs.get_file_status("/file").await.unwrap().length, 7);
    stream.flush().await.unwrap();
    assert!(called.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn append_task_exit_invalidates_status_before_flush_or_close() {
    for fail in [false, true] {
        let control = AppendControl::blocked();
        let store = Arc::new(MemoryStore {
            pfs: true, append_commit_control: Some(control.clone()),
            append_commit_failure: fail, ..Default::default()
        });
        store.put("file", Bytes::from_static(b"old"), PutHeaders::default()).await.unwrap();
        let fs = ObsFileSystem::with_store(ObsConfig::default(), store);
        let mut stream = fs.append_stream("/file", 3).await.unwrap();
        assert_eq!(fs.get_file_status("/file").await.unwrap().length, 3);
        stream.write(b"more").await.unwrap();
        control.started.notified().await;
        control.gate.add_permits(1);
        // On this current-thread runtime the append task drops its invalidation
        // guard in the same poll as the completion notification, before resuming us.
        control.stopped.notified().await;
        assert_eq!(fs.get_file_status("/file").await.unwrap().length, 7);
        let result = stream.flush().await;
        if fail { assert!(matches!(result, Err(ObsError::Transport(_)))); }
        else { result.unwrap(); }
    }
}

#[tokio::test]
async fn cancelled_append_flush_then_stream_drop_invalidates_committed_data() {
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        pfs: true, append_commit_control: Some(control.clone()), ..Default::default()
    });
    store.put("file", Bytes::from_static(b"old"), PutHeaders::default()).await.unwrap();
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store);
    let mut stream = fs.append_stream("/file", 3).await.unwrap();
    assert_eq!(fs.get_file_status("/file").await.unwrap().length, 3);
    stream.write(b"more").await.unwrap();
    control.started.notified().await;
    let mut flush = Box::pin(stream.flush());
    assert!(futures::poll!(flush.as_mut()).is_pending());
    drop(flush);
    drop(stream);
    control.stopped.notified().await;
    assert_eq!(fs.get_file_status("/file").await.unwrap().length, 7);
}

#[tokio::test]
async fn truncate_completion_invalidates_status_on_success_error_and_cancellation() {
    for outcome in 0..3 {
        for length in [0, 3] {
            let control = AppendControl::blocked();
            let store = Arc::new(MemoryStore {
                pfs: true, truncate_control: Some(control.clone()),
                truncate_failure: outcome == 1, ..Default::default()
            });
            store.put("file", Bytes::from_static(b"abcdef"), PutHeaders::default()).await.unwrap();
            let fs = Arc::new(ObsFileSystem::with_store(ObsConfig::default(), store.clone()));
            assert_eq!(fs.get_file_status("/file").await.unwrap().length, 6);
            let heads = store.count("head:");
            let writer = fs.clone();
            let pending = tokio::spawn(async move { writer.truncate("/file", length).await });
            control.started.notified().await;
            assert_eq!(store.count("head:"), heads, "truncate must not add a HEAD");
            if outcome == 2 {
                pending.abort();
                assert!(pending.await.unwrap_err().is_cancelled());
            } else {
                control.gate.add_permits(1);
                let result = pending.await.unwrap();
                if outcome == 1 { assert!(matches!(result, Err(ObsError::Transport(_)))); }
                else { result.unwrap(); }
            }
            assert_eq!(fs.get_file_status("/file").await.unwrap().length, length, "outcome={outcome}");
        }
    }
}

#[tokio::test]
async fn failed_truncate_fences_an_older_status_query() {
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        pfs: true, truncate_failure: true,
        head_control: Some(control.clone()), head_pauses: AtomicUsize::new(1),
        ..Default::default()
    });
    store.put("file", Bytes::from_static(b"abcdef"), PutHeaders::default()).await.unwrap();
    let fs = Arc::new(ObsFileSystem::with_store(ObsConfig::default(), store));
    let reader = fs.clone();
    let pending = tokio::spawn(async move { reader.get_file_status("/file").await });
    control.started.notified().await;
    assert!(matches!(fs.truncate("/file", 2).await, Err(ObsError::Transport(_))));
    control.gate.add_permits(1);
    assert_eq!(pending.await.unwrap().unwrap().length, 6);
    assert_eq!(fs.get_file_status("/file").await.unwrap().length, 2);
}

#[tokio::test]
async fn uncertain_create_completion_invalidates_cached_status() {
    for pfs in [false, true] {
        for cancel in [false, true] {
            for overwrite in [false, true] {
                for multipart in [false, true] {
                    let control = AppendControl::blocked();
                    let store = Arc::new(MemoryStore {
                        pfs, put_control: Some(control.clone()), put_pauses: AtomicUsize::new(1),
                        put_failures: AtomicUsize::new(1), ..Default::default()
                    });
                    if overwrite {
                        store.objects.lock().unwrap().insert("file".into(), Bytes::from_static(b"old"));
                    }
                    let mut config = ObsConfig::default();
                    if multipart { config.multipart_threshold = 1; }
                    let fs = ObsFileSystem::with_store(config, store.clone());
                    let old = fs.get_file_status("/file").await;
                    assert_eq!(old.is_ok(), overwrite);
                    let mut stream = fs.create("/file", CreateOptions { overwrite: true, ..Default::default() }).await.unwrap();
                    stream.write(b"newer").await.unwrap();
                    let pending = tokio::spawn(stream.close());
                    control.started.notified().await;
                    assert_eq!(store.count("complete"), usize::from(multipart));
                    if cancel {
                        pending.abort();
                        assert!(pending.await.unwrap_err().is_cancelled());
                    } else {
                        control.gate.add_permits(1);
                        assert!(matches!(pending.await.unwrap(), Err(ObsError::Transport(_))));
                    }
                    assert_eq!(fs.get_file_status("/file").await.unwrap().length, 5,
                        "pfs={pfs}, cancel={cancel}, overwrite={overwrite}, multipart={multipart}");
                }
            }
        }
    }
}

#[tokio::test]
async fn uncertain_mkdir_completion_invalidates_negative_status() {
    for pfs in [false, true] {
        for cancel in [false, true] {
            let control = AppendControl::blocked();
            let store = Arc::new(MemoryStore {
                pfs, put_control: Some(control.clone()), put_pauses: AtomicUsize::new(1),
                put_failures: AtomicUsize::new(1), ..Default::default()
            });
            let fs = Arc::new(ObsFileSystem::with_store(ObsConfig::default(), store));
            assert!(fs.get_file_status("/parent").await.unwrap_err().is_not_found());
            assert!(fs.get_file_status("/parent/dir").await.unwrap_err().is_not_found());
            let writer = fs.clone();
            let pending = tokio::spawn(async move { writer.mkdirs("/parent/dir", 0o700).await });
            control.started.notified().await;
            if cancel {
                pending.abort();
                assert!(pending.await.unwrap_err().is_cancelled());
            } else {
                control.gate.add_permits(1);
                assert!(matches!(pending.await.unwrap(), Err(ObsError::Transport(_))));
            }
            assert!(fs.get_file_status("/parent/dir").await.unwrap().is_dir);
            assert!(fs.get_file_status("/parent").await.unwrap().is_dir);
        }
    }
}

#[tokio::test]
async fn uncontended_mutations_keep_status_cache_and_permissions() {
    for pfs in [false, true] {
        let store = Arc::new(MemoryStore { pfs, ..Default::default() });
        let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
        fs.mkdirs("/dir", 0o700).await.unwrap();
        let heads = store.count("head:");
        assert_eq!(fs.get_file_status("/dir").await.unwrap().permission, 0o700);
        assert_eq!(store.count("head:"), heads);
        let mut stream = fs.create("/dir/file", CreateOptions { permission: 0o600, ..Default::default() }).await.unwrap();
        stream.write(b"data").await.unwrap();
        stream.close().await.unwrap();
        let heads = store.count("head:");
        let status = fs.get_file_status("/dir/file").await.unwrap();
        assert_eq!(status.length, 4);
        assert_eq!(status.permission, 0o600);
        assert_eq!(store.count("head:"), heads);
    }
}

#[tokio::test]
async fn delayed_create_completion_cannot_cache_deleted_or_overwritten_file() {
    for pfs in [false, true] {
        for overwrite in [false, true] {
            let control = AppendControl::blocked();
            let store = Arc::new(MemoryStore {
                pfs, put_control: Some(control.clone()), put_pauses: AtomicUsize::new(1),
                ..Default::default()
            });
            let fs = Arc::new(ObsFileSystem::with_store(ObsConfig::default(), store));
            let mut stream = fs.create("/file", CreateOptions::default()).await.unwrap();
            stream.write(b"old").await.unwrap();
            let pending = tokio::spawn(stream.close());
            control.started.notified().await;
            if overwrite {
                let mut newer = fs.create("/file", CreateOptions { overwrite: true, ..Default::default() }).await.unwrap();
                newer.write(b"newer").await.unwrap();
                newer.close().await.unwrap();
            } else {
                // Refresh the pre-create negative cache before deleting through the FS.
                fs.list_status("/").await.unwrap();
                assert!(fs.delete("/file", false).await.unwrap());
            }
            control.gate.add_permits(1);
            assert_eq!(pending.await.unwrap().unwrap().content_length, 3);
            if overwrite {
                assert_eq!(fs.get_file_status("/file").await.unwrap().length, 5);
            } else {
                assert!(fs.get_file_status("/file").await.unwrap_err().is_not_found());
            }
        }
    }
}

#[tokio::test]
async fn delayed_mkdir_completion_cannot_cache_deleted_directory() {
    for pfs in [false, true] {
        let control = AppendControl::blocked();
        let store = Arc::new(MemoryStore {
            pfs, put_control: Some(control.clone()), put_pauses: AtomicUsize::new(1),
            ..Default::default()
        });
        let fs = Arc::new(ObsFileSystem::with_store(ObsConfig::default(), store));
        let writer = fs.clone();
        let pending = tokio::spawn(async move { writer.mkdirs("/dir", 0o700).await });
        control.started.notified().await;
        fs.list_status("/").await.unwrap();
        assert!(fs.delete("/dir", true).await.unwrap());
        control.gate.add_permits(1);
        assert!(pending.await.unwrap().unwrap());
        assert!(fs.get_file_status("/dir").await.unwrap_err().is_not_found());
    }
}

#[tokio::test]
async fn delayed_list_cannot_repopulate_cache_after_delete() {
    for mode in 0..4 {
        let control = AppendControl::blocked();
        let store = Arc::new(MemoryStore {
            list_control: Some(control.clone()),
            ..Default::default()
        });
        store.put("file", Bytes::from_static(b"old"), PutHeaders::default()).await.unwrap();
        store.list_pages.lock().unwrap().push_back(ListPage {
            contents: vec![ListEntry { key: "file".into(), size: 3, ..Default::default() }],
            ..Default::default()
        });
        let fs = Arc::new(ObsFileSystem::with_store(ObsConfig::default(), store));
        let reader = fs.clone();
        let pending = tokio::spawn(async move {
            match mode {
                0 => reader.list_status("/").await.unwrap(),
                1 => reader.list_status_page("/", None).await.unwrap().entries,
                2 => reader.list_files_page("/", None, true).await.unwrap().entries,
                _ => reader.list_files_recursive("/").await.unwrap(),
            }
        });
        control.started.notified().await;
        assert!(fs.delete("/file", false).await.unwrap());
        control.gate.add_permits(8);
        assert_eq!(pending.await.unwrap()[0].length, 3);
        assert!(fs.get_file_status("/file").await.unwrap_err().is_not_found(), "mode {mode}");
    }
}

#[tokio::test]
async fn delayed_listing_metadata_cannot_repopulate_cache_after_delete() {
    for mode in 0..4 {
        let control = AppendControl::blocked();
        let store = Arc::new(MemoryStore {
            pfs: true,
            head_control: Some(control.clone()),
            head_pauses: AtomicUsize::new(1),
            ..Default::default()
        });
        store.put("file", Bytes::from_static(b"old"), PutHeaders::default()).await.unwrap();
        let config = ObsConfig { permissions_mode: "disguise".into(), ..Default::default() };
        let fs = Arc::new(ObsFileSystem::with_store(config, store));
        let reader = fs.clone();
        let pending = tokio::spawn(async move {
            match mode {
                0 => reader.list_status("/").await.unwrap(),
                1 => reader.list_status_page("/", None).await.unwrap().entries,
                2 => reader.list_files_page("/", None, false).await.unwrap().entries,
                _ => reader.list_files_recursive("/").await.unwrap(),
            }
        });
        control.started.notified().await;
        assert!(fs.delete("/file", false).await.unwrap());
        control.gate.add_permits(1);
        assert_eq!(pending.await.unwrap()[0].length, 3);
        assert!(fs.get_file_status("/file").await.unwrap_err().is_not_found(), "mode {mode}");
    }
}

#[tokio::test]
async fn delayed_status_query_cannot_repopulate_cache_after_delete() {
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        head_control: Some(control.clone()),
        head_pauses: AtomicUsize::new(1),
        ..Default::default()
    });
    store.put("file", Bytes::from_static(b"old"), PutHeaders::default()).await.unwrap();
    let fs = Arc::new(ObsFileSystem::with_store(ObsConfig::default(), store));
    let reader = fs.clone();
    let pending = tokio::spawn(async move { reader.get_file_status("/file").await });
    control.started.notified().await;
    assert!(fs.delete("/file", false).await.unwrap());
    control.gate.add_permits(1);
    // The already-started query may return its snapshot, but must not cache it.
    assert_eq!(pending.await.unwrap().unwrap().length, 3);
    assert!(fs.get_file_status("/file").await.unwrap_err().is_not_found());
}

#[tokio::test]
async fn unaddressable_upload_buffers_fail_without_store_calls() {
    for threshold in [false, true] {
        let mut config = ObsConfig::default();
        let key = if threshold {
            config.multipart_threshold = isize::MAX as u64 + 1;
            "multipart.threshold"
        } else {
            config.multipart_size = isize::MAX as u64 + 1;
            "multipart.size"
        };
        let store = Arc::new(MemoryStore::default());
        let make_stream = || ObsOutputStream::new(
            store.clone(), &config, "key".into(), Arc::new(Semaphore::new(1)),
        );
        let mut stream = make_stream();
        for error in [
            stream.write(b"x").await.unwrap_err(),
            stream.write(b"").await.unwrap_err(),
            stream.flush().await.unwrap_err(),
            stream.close().await.unwrap_err(),
            make_stream().close().await.unwrap_err(),
        ] {
            assert!(matches!(error, ObsError::InvalidArgument(ref message) if message.contains(key)));
        }
        assert!(store.calls.lock().unwrap().is_empty());
        match ObsFileSystem::from_config(config).await {
            Err(ObsError::InvalidArgument(message)) => assert!(message.contains(key)),
            _ => panic!("invalid buffer size must fail before initialization"),
        }
    }
}

#[tokio::test]
async fn oversized_semaphore_configuration_is_rejected_before_initialization() {
    for key in [
        "connection.maximum",
        "upload.active.blocks",
        "upload.total.active.blocks",
        "readahead.total.chunks",
    ] {
        let props = std::collections::HashMap::from([
            ("fs.obs.endpoint".into(), "http://127.0.0.1:1".into()),
            ("fs.obs.bucket.mode".into(), "object".into()),
            (format!("fs.obs.{key}"), (Semaphore::MAX_PERMITS + 1).to_string()),
        ]);
        let config = obs_rust_core::config::config_from_hadoop(&props, "bucket");
        match ObsFileSystem::from_config(config).await {
            Err(ObsError::InvalidArgument(message)) => assert!(message.contains(key)),
            _ => panic!("unsupported {key} must be rejected as a configuration error"),
        }
    }
}

#[tokio::test]
async fn infallible_constructors_bound_semaphore_counts_without_panicking() {
    let config = ObsConfig {
        connection_maximum: usize::MAX,
        upload_active_blocks: usize::MAX,
        upload_total_active_blocks: usize::MAX,
        readahead_total_chunks: usize::MAX,
        ..Default::default()
    };
    let store = Arc::new(MemoryStore::default());
    let _pool = obs_rust_core::transport::pool::HttpPool::new(config.clone());
    let _fs = ObsFileSystem::with_store(config.clone(), store.clone());
    let _input = ObsInputStream::new(store.clone(), &config, "key".into(), 0, None);
    let _output = ObsOutputStream::new(store, &config, "key".into(), Arc::new(Semaphore::new(1)));
}

struct AppendControl {
    started: Notify,
    stopped: Notify,
    gate: Semaphore,
}
impl AppendControl {
    fn blocked() -> Arc<Self> {
        Arc::new(Self {
            started: Notify::new(),
            stopped: Notify::new(),
            gate: Semaphore::new(0),
        })
    }
}
struct NotifyOnDrop<'a>(&'a Notify);
impl Drop for NotifyOnDrop<'_> {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}
struct ActiveGet<'a>(&'a AtomicUsize);
impl Drop for ActiveGet<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct MemoryStore {
    append_profile: Option<Arc<append_latency::AppendProfile>>,
    append_inplace: bool,
    single_get_chunk: bool,
    append_commit_control: Option<Arc<AppendControl>>,
    append_commit_failure: bool,
    truncate_control: Option<Arc<AppendControl>>,
    truncate_failure: bool,
    put_control: Option<Arc<AppendControl>>,
    put_pauses: AtomicUsize,
    put_failures: AtomicUsize,
    synthetic_listing: Option<usize>,
    metadata_failure: bool,
    head_control: Option<Arc<AppendControl>>,
    head_pauses: AtomicUsize,
    objects: Mutex<BTreeMap<String, Bytes>>,
    calls: Mutex<Vec<String>>,
    parts: Mutex<BTreeMap<i32, Bytes>>,
    pfs: bool,
    fail_part: bool,
    part_control: Option<(i32, Arc<AppendControl>)>,
    abort_control: Option<Arc<AppendControl>>,
    abort_failures: AtomicUsize,
    abort_missing: bool,
    abort_error: Option<ObsError>,
    directory_conflict: bool,
    fail_delete: Option<String>,
    rename_commit_control: Option<Arc<AppendControl>>,
    append_control: Option<Arc<AppendControl>>,
    append_control_position: Option<u64>,
    get_control: Option<(u64, Arc<AppendControl>)>,
    fail_get_at: Option<u64>,
    get_failures: AtomicUsize,
    append_bytes: AtomicUsize,
    append_failures: AtomicUsize,
    append_peak_bytes: AtomicUsize,
    get_delay_ms: u64,
    get_base: u64,
    list_control: Option<Arc<AppendControl>>,
    get_active: AtomicUsize,
    get_peak: AtomicUsize,
    list_pages: Mutex<std::collections::VecDeque<ListPage>>,
    list_markers: Mutex<Vec<Option<String>>>,
    omit_etag: bool,
}
impl MemoryStore {
    fn record(&self, call: String) {
        self.calls.lock().unwrap().push(call);
    }
    fn missing() -> ObsError {
        ObsError::NotFound {
            code: "NoSuchKey".into(),
            message: "missing".into(),
            request_id: String::new(),
        }
    }
    fn count(&self, prefix: &str) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c.starts_with(prefix))
            .count()
    }
}
#[async_trait]
impl ObjectStore for MemoryStore {
    async fn head(&self, key: &str) -> ObsResult<ObjectMeta> {
        self.record(format!("head:{key}"));
        let bytes = self.objects.lock().unwrap().get(key).cloned().ok_or_else(Self::missing)?;
        if let Some(control) = &self.head_control {
            if self.head_pauses.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)).is_ok() {
                control.started.notify_one();
                control.gate.acquire().await.unwrap().forget();
            }
        }
        Ok(ObjectMeta {
            key: key.into(),
            content_length: bytes.len() as u64,
            etag: (!self.omit_etag).then(|| format!("{:x}", Md5::digest(&bytes))),
            ..Default::default()
        })
    }
    async fn get(
        &self,
        key: &str,
        range: Option<(u64, u64)>,
        _: Option<&InodeHint>,
    ) -> ObsResult<GetResult> {
        self.record(format!("get:{key}"));
        let start = range.map(|r| r.0);
        self.record(format!("range:{key}:{start:?}"));
        let _get_completion = self
            .get_control
            .as_ref()
            .filter(|(offset, _)| start == Some(*offset))
            .map(|(_, control)| NotifyOnDrop(&control.stopped));
        if start == self.fail_get_at
            && self
                .get_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
        {
            return Err(ObsError::Transport("injected range failure".into()));
        }
        if let Some((offset, control)) = &self.get_control {
            if start == Some(*offset) {
                control.started.notify_one();
                control.gate.acquire().await.unwrap().forget();
            }
        }
        let active = self.get_active.fetch_add(1, Ordering::SeqCst) + 1;
        let _active = ActiveGet(&self.get_active);
        self.get_peak.fetch_max(active, Ordering::SeqCst);
        if self.get_delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(self.get_delay_ms)).await;
        }
        let bytes = self
            .objects
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(Self::missing)?;
        let etag = (!self.omit_etag).then(|| format!("{:x}", Md5::digest(&bytes)));
        let bytes = match range {
            Some((s, e)) => {
                bytes.slice((s - self.get_base) as usize..=(e - self.get_base) as usize)
            }
            None => bytes,
        };
        let split = bytes.len() / 2;
        let chunks = if self.single_get_chunk { vec![Ok(bytes)] } else {
            vec![Ok(bytes.slice(..split)), Ok(bytes.slice(split..))]
        };
        Ok(GetResult {
            meta: ObjectMeta {
                etag,
                ..Default::default()
            },
            body: Box::pin(futures::stream::iter(chunks)),
        })
    }
    async fn put(&self, key: &str, body: Bytes, _: PutHeaders) -> ObsResult<ObjectMeta> {
        self.record(format!("put:{key}"));
        let length = body.len() as u64;
        self.objects.lock().unwrap().insert(key.into(), body);
        if let Some(control) = &self.put_control {
            if self.put_pauses.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)).is_ok() {
                control.started.notify_one();
                control.gate.acquire().await.unwrap().forget();
            }
        }
        if self.put_failures.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)).is_ok() {
            return Err(ObsError::Transport("injected failure after PUT commit".into()));
        }
        Ok(ObjectMeta {
            key: key.into(),
            content_length: length,
            ..Default::default()
        })
    }
    async fn put_empty(&self, key: &str) -> ObsResult<ObjectMeta> {
        self.put(key, Bytes::new(), PutHeaders::default()).await
    }
    async fn delete(&self, key: &str, hdfs: bool) -> ObsResult<()> {
        self.record(format!("delete:{key}"));
        if self.fail_delete.as_deref() == Some(key) {
            return Err(ObsError::Io("injected delete failure".into()));
        }
        if self.directory_conflict
            && key.ends_with('/')
            && self
                .objects
                .lock()
                .unwrap()
                .keys()
                .any(|k| k.starts_with(key) && k != key)
        {
            return Err(ObsError::Conflict {
                code: "DirectoryNotEmpty".into(),
                message: key.into(),
                request_id: String::new(),
            });
        }
        let found = self.objects.lock().unwrap().remove(key).is_some();
        if hdfs && !found {
            Err(Self::missing())
        } else {
            Ok(())
        }
    }
    async fn delete_batch(&self, keys: &[String]) -> ObsResult<()> {
        for k in keys {
            self.delete(k, false).await?;
        }
        Ok(())
    }
    async fn list(
        &self,
        prefix: &str,
        delimiter: &str,
        marker: Option<&str>,
        maximum: usize,
    ) -> ObsResult<ListPage> {
        self.record(format!("list:{prefix}"));
        self.list_markers
            .lock()
            .unwrap()
            .push(marker.map(str::to_owned));
        if let Some(control) = &self.list_control {
            control.started.notify_one();
            control.gate.acquire().await.unwrap().forget();
        }
        if let Some(page) = self.list_pages.lock().unwrap().pop_front() {
            return Ok(page);
        }
        if let Some(total) = self.synthetic_listing {
            assert_eq!(delimiter, "");
            tokio::time::sleep(Duration::from_millis(1)).await;
            let start = marker.map(|value| value.rsplit("file").next().unwrap().parse::<usize>().unwrap() + 1).unwrap_or(0);
            let end = (start + maximum).min(total);
            return Ok(ListPage {
                contents: (start..end).map(|index| ListEntry {
                    key: format!("{prefix}file{index:09}"), size: 1, ..Default::default()
                }).collect(),
                is_truncated: end < total,
                next_marker: if end < total { Some(format!("{prefix}file{:09}", end - 1)) } else { None },
                ..Default::default()
            });
        }
        let mut page = ListPage::default();
        let waiting = self.append_profile.as_ref().map(|_| std::time::Instant::now());
        let objects = self.objects.lock().unwrap();
        let acquired = waiting.map(|_| std::time::Instant::now());
        let scanned = objects.len();
        for (key, value) in objects.iter().filter(|(k, _)| k.starts_with(prefix))
        {
            if delimiter == "/" {
                if let Some(i) = key[prefix.len()..].find('/') {
                    let dir = key[..prefix.len() + i + 1].to_string();
                    if !page.common_prefixes.contains(&dir) {
                        page.common_prefixes.push(dir);
                    }
                    continue;
                }
            }
            page.contents.push(ListEntry {
                key: key.clone(),
                size: value.len() as u64,
                ..Default::default()
            });
        }
        drop(objects);
        if let (Some(profile), Some(waiting), Some(acquired)) = (&self.append_profile, waiting, acquired) {
            let ended = std::time::Instant::now();
            profile.lists.lock().unwrap().push(append_latency::ListTiming { waiting, acquired, ended, scanned });
        }
        Ok(page)
    }
    async fn copy(&self, src: &str, dst: &str) -> ObsResult<ObjectMeta> {
        let bytes = self
            .objects
            .lock()
            .unwrap()
            .get(src)
            .cloned()
            .ok_or_else(Self::missing)?;
        self.put(dst, bytes, PutHeaders::default()).await
    }
    async fn initiate_multipart_upload(&self, _: &str) -> ObsResult<String> {
        self.record("init".into());
        Ok("upload".into())
    }
    async fn initiate_multipart_upload_with_headers(&self, key: &str, _: PutHeaders) -> ObsResult<String> {
        // This in-memory fixture models data commits; wire tests cover metadata.
        self.initiate_multipart_upload(key).await
    }
    async fn upload_part(&self, _: &str, _: &str, number: i32, body: Bytes) -> ObsResult<String> {
        let _notify = self
            .part_control
            .as_ref()
            .filter(|(part, _)| *part == number)
            .map(|(_, control)| NotifyOnDrop(&control.stopped));
        if let Some((part, control)) = &self.part_control {
            if *part == number {
                control.started.notify_one();
                control.gate.acquire().await.unwrap().forget();
            }
        }
        tokio::time::sleep(Duration::from_millis(if number == 1 { 25 } else { 1 })).await;
        self.record(format!("part:{number}"));
        if self.fail_part && number == 1 {
            return Err(ObsError::Io("injected part failure".into()));
        }
        self.parts.lock().unwrap().insert(number, body);
        Ok(number.to_string())
    }
    async fn complete_multipart_upload(
        &self,
        key: &str,
        _: &str,
        parts: &[CompletedPart],
    ) -> ObsResult<ObjectMeta> {
        self.record("complete".into());
        let bytes = {
            let uploaded = self.parts.lock().unwrap();
            assert_eq!(uploaded.len(), parts.len());
            let mut bytes = Vec::new();
            for p in parts {
                bytes.extend_from_slice(uploaded.get(&p.part_number).unwrap());
            }
            Bytes::from(bytes)
        };
        self.put(key, bytes, PutHeaders::default()).await
    }
    async fn abort_multipart_upload(&self, _: &str, _: &str) -> ObsResult<()> {
        self.record("abort".into());
        if let Some(error) = &self.abort_error { return Err(error.clone()); }
        if self.abort_missing {
            return Err(ObsError::NotFound {
                code: "NoSuchUpload".into(), message: "already absent".into(), request_id: "mock".into(),
            });
        }
        if let Some(control) = &self.abort_control {
            control.started.notify_one();
            control.gate.acquire().await.unwrap().forget();
            control.stopped.notify_one();
        }
        if self
            .abort_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(ObsError::Transport("injected abort failure".into()));
        }
        Ok(())
    }
    async fn posix_rename(&self, src: &str, dst: &str) -> ObsResult<()> {
        self.record(format!("rename:{src}:{dst}"));
        self.copy(src, dst).await?;
        self.delete(src, false).await?;
        if let Some(control) = &self.rename_commit_control {
            control.started.notify_one();
            control.gate.acquire().await.unwrap().forget();
        }
        Ok(())
    }
    async fn posix_truncate(&self, key: &str, length: u64) -> ObsResult<()> {
        self.record(format!("truncate:{key}:{length}"));
        {
            let mut objects = self.objects.lock().unwrap();
            let bytes = objects.get_mut(key).ok_or_else(Self::missing)?;
            if length > bytes.len() as u64 {
                return Err(ObsError::InvalidArgument("mock truncate cannot extend".into()));
            }
            bytes.truncate(length as usize);
        }
        if let Some(control) = &self.truncate_control {
            control.started.notify_one();
            control.gate.acquire().await.unwrap().forget();
        }
        if self.truncate_failure {
            return Err(ObsError::Transport("injected failure after truncate commit".into()));
        }
        Ok(())
    }
    async fn posix_append(&self, key: &str, position: u64, body: Bytes) -> ObsResult<()> {
        let profile_started = self.append_profile.as_ref().map(|_| std::time::Instant::now());
        self.record(format!("append:{key}:{position}"));
        struct ReleaseBytes<'a>(&'a AtomicUsize, usize);
        impl Drop for ReleaseBytes<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(self.1, Ordering::SeqCst);
            }
        }
        let active = self.append_bytes.fetch_add(body.len(), Ordering::SeqCst) + body.len();
        let _bytes = ReleaseBytes(&self.append_bytes, body.len());
        self.append_peak_bytes.fetch_max(active, Ordering::SeqCst);
        let control = self.append_control.as_ref().filter(|_| {
            self.append_control_position.map_or(true, |offset| offset == position)
        });
        let _stopped = control.map(|c| NotifyOnDrop(&c.stopped));
        if let Some(control) = control {
            control.started.notify_one();
            control.gate.acquire().await.unwrap().forget();
        }
        if self
            .append_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(ObsError::Io("injected append failure".into()));
        }
        {
            let waiting = self.append_profile.as_ref().map(|_| std::time::Instant::now());
            let mut objects = self.objects.lock().unwrap();
            let acquired = waiting.map(|_| std::time::Instant::now());
            let start = usize::try_from(position).unwrap();
            let previous_len = objects.get(key).ok_or_else(Self::missing)?.len();
            let (capacity_before, capacity_after, copy_fallback);
            if self.append_inplace {
                // Diagnostic alternative only: preserve unique allocation capacity.
                // The default benchmark still copies the full prefix each append.
                let object = objects.get_mut(key).unwrap();
                let old = std::mem::take(object);
                let (mut bytes, shared) = match old.try_into_mut() {
                    Ok(bytes) => (bytes, false),
                    Err(old) => (bytes::BytesMut::from(old.as_ref()), true),
                };
                copy_fallback = shared;
                capacity_before = bytes.capacity();
                bytes.resize(bytes.len().max(start + body.len()), 0);
                bytes[start..start + body.len()].copy_from_slice(&body);
                capacity_after = bytes.capacity();
                *object = bytes.freeze();
            } else {
                let mut bytes = objects.get(key).unwrap().to_vec();
                copy_fallback = false;
                capacity_before = bytes.capacity();
                bytes.resize(bytes.len().max(start + body.len()), 0);
                bytes[start..start + body.len()].copy_from_slice(&body);
                capacity_after = bytes.capacity();
                objects.insert(key.into(), bytes.into());
            }
            drop(objects);
            if let (Some(profile), Some(waiting), Some(acquired)) = (&self.append_profile, waiting, acquired) {
                let ended = std::time::Instant::now();
                if let Some(worker) = key.strip_prefix("file-").and_then(|n| n.parse::<usize>().ok()).filter(|n| *n < 4) {
                    profile.operations[worker].lock().unwrap().push(append_latency::StoreTiming {
                        capacity_before, capacity_after, copy_fallback,
                        position, started: profile_started.unwrap(), waiting, acquired, ended,
                    });
                }
                profile.wait_ns.fetch_add(acquired.duration_since(waiting).as_nanos() as u64, Ordering::Relaxed);
                profile.hold_ns.fetch_add(ended.duration_since(acquired).as_nanos() as u64, Ordering::Relaxed);
                if !self.append_inplace { profile.copied_prefix_bytes.fetch_add(previous_len as u64, Ordering::Relaxed); }
            }
        }
        let _commit_stopped = self.append_commit_control.as_ref().map(|c| NotifyOnDrop(&c.stopped));
        if let Some(control) = &self.append_commit_control {
            control.started.notify_one();
            control.gate.acquire().await.unwrap().forget();
        }
        if self.append_commit_failure {
            return Err(ObsError::Transport("injected failure after append commit".into()));
        }
        if let (Some(profile), Some(started)) = (&self.append_profile, profile_started) {
            profile.total_ns.fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
            profile.calls.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
    async fn create_folder(&self, key: &str) -> ObsResult<()> {
        self.record(format!("folder:{key}"));
        self.put_empty(&format!("{}/", key.trim_end_matches('/')))
            .await?;
        Ok(())
    }
    async fn set_object_metadata(
        &self,
        _: &str,
        _: Option<u32>,
        _: Option<u32>,
        _: Option<u32>,
    ) -> ObsResult<()> {
        if self.metadata_failure { return Err(ObsError::Io("metadata response lost".into())); }
        Ok(())
    }
    async fn probe_posix_bucket(&self) -> ObsResult<bool> {
        Ok(self.pfs)
    }
    fn is_pfs(&self) -> bool {
        self.pfs
    }
}

#[tokio::test]
async fn metadata_mutation_error_invalidates_cached_status() {
    let store = Arc::new(MemoryStore { pfs: true, metadata_failure: true, ..Default::default() });
    store.put_empty("file").await.unwrap();
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    for owner in [false, true] {
        fs.get_file_status("/file").await.unwrap();
        let heads = store.count("head:");
        let result = if owner { fs.set_owner("/file", 1, 2).await } else { fs.set_permission("/file", 0o600).await };
        assert!(result.is_err());
        fs.get_file_status("/file").await.unwrap();
        assert_eq!(store.count("head:"), heads + 1);
    }
}
#[tokio::test]
async fn small_file_larger_than_part_is_not_discarded() {
    let store = Arc::new(MemoryStore::default());
    let config = ObsConfig {
        multipart_size: 5 * 1024 * 1024,
        multipart_threshold: 12 * 1024 * 1024,
        ..Default::default()
    };
    let mut stream = ObsOutputStream::new(
        store.clone(),
        &config,
        "file".into(),
        Arc::new(Semaphore::new(2)),
    );
    let data = vec![42; 7 * 1024 * 1024];
    stream.write(&data).await.unwrap();
    let meta = stream.close().await.unwrap();
    assert_eq!(meta.content_length, data.len() as u64);
    assert_eq!(&store.objects.lock().unwrap()["file"][..], data);
    assert_eq!(store.count("init"), 0);
}
#[tokio::test]
async fn multipart_close_waits_and_uploads_short_tail() {
    let store = Arc::new(MemoryStore::default());
    let config = ObsConfig {
        multipart_size: 5 * 1024 * 1024,
        multipart_threshold: 5 * 1024 * 1024,
        ..Default::default()
    };
    let mut stream = ObsOutputStream::new(
        store.clone(),
        &config,
        "file".into(),
        Arc::new(Semaphore::new(1)),
    );
    let data = vec![23; 11 * 1024 * 1024 + 7];
    stream.write(&data).await.unwrap();
    stream.close().await.unwrap();
    assert_eq!(&store.objects.lock().unwrap()["file"][..], data);
    assert_eq!(store.count("part:"), 3);
    assert_eq!(store.count("complete"), 1);
}
#[tokio::test]
async fn failed_upload_aborts_without_completing() {
    let store = Arc::new(MemoryStore {
        fail_part: true,
        ..Default::default()
    });
    let config = ObsConfig {
        multipart_threshold: 1,
        ..Default::default()
    };
    let mut stream = ObsOutputStream::new(
        store.clone(),
        &config,
        "file".into(),
        Arc::new(Semaphore::new(2)),
    );
    stream.write(b"broken").await.unwrap();
    assert!(stream.close().await.is_err());
    assert_eq!(store.count("complete"), 0);
    assert!(store.count("abort") >= 1);
}
#[tokio::test]
async fn reads_all_response_chunks_and_seek_resets() {
    let store = Arc::new(MemoryStore::default());
    store
        .put(
            "file",
            Bytes::from_static(b"abcdefghijklmnop"),
            PutHeaders::default(),
        )
        .await
        .unwrap();
    let cfg = ObsConfig {
        readahead_chunk_size: 4,
        ..Default::default()
    };
    let mut stream = ObsInputStream::new(store, &cfg, "file".into(), 16, None);
    let mut out = [0; 16];
    assert_eq!(stream.read(&mut out).await.unwrap(), 16);
    assert_eq!(&out, b"abcdefghijklmnop");
    stream.seek(3).unwrap();
    let mut part = [0; 5];
    stream.read(&mut part).await.unwrap();
    assert_eq!(&part, b"defgh");
    stream.seek(20).unwrap();
    assert_eq!(stream.read(&mut part).await.unwrap(), 0);
}
#[tokio::test]
async fn list_backfill_opens_without_head_and_keeps_zero_byte_file() {
    let store = Arc::new(MemoryStore::default());
    store.put_empty("dir/empty").await.unwrap();
    store.put_empty("dir/sub/file").await.unwrap();
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    let statuses = fs.list_status("/dir").await.unwrap();
    assert_eq!(statuses.len(), 2);
    assert!(
        !statuses
            .iter()
            .find(|s| s.path == "/dir/empty")
            .unwrap()
            .is_dir
    );
    fs.open("/dir/empty", OpenOptions::default()).await.unwrap();
    assert_eq!(store.count("head:"), 0);
}
#[tokio::test]
async fn pfs_delete_probes_directory_before_file_without_head() {
    let store = Arc::new(MemoryStore {
        pfs: true,
        ..Default::default()
    });
    store.put_empty("file").await.unwrap();
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    assert!(fs.delete("/file", false).await.unwrap());
    assert!(!fs.delete("/file", false).await.unwrap());
    let calls = store.calls.lock().unwrap();
    let deletes: Vec<_> = calls
        .iter()
        .filter(|c| c.starts_with("delete:"))
        .map(|s| s.as_str())
        .collect();
    assert_eq!(
        deletes,
        vec!["delete:file/", "delete:file", "delete:file/", "delete:file"]
    );
    assert!(!calls.iter().any(|c| c.starts_with("head:")));
}
#[tokio::test]
async fn create_invalidates_negative_status_on_close() {
    let store = Arc::new(MemoryStore::default());
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store);
    assert!(!fs.exists("/file").await.unwrap());
    let mut stream = fs.create("/file", CreateOptions::default()).await.unwrap();
    stream.write(b"abc").await.unwrap();
    stream.close().await.unwrap();
    assert_eq!(fs.get_file_status("/file").await.unwrap().length, 3);
}
#[tokio::test]
async fn pfs_mkdir_and_truncate_use_no_head() {
    let store = Arc::new(MemoryStore {
        pfs: true,
        ..Default::default()
    });
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    fs.mkdirs("/dir", 0o755).await.unwrap();
    store.put("file", Bytes::from_static(b"data"), PutHeaders::default()).await.unwrap();
    fs.truncate("/file", 0).await.unwrap();
    assert_eq!(store.count("head:"), 0);
}

#[tokio::test]
async fn recursive_pfs_delete_orders_by_depth_not_string_length() {
    let store = Arc::new(MemoryStore {
        pfs: true,
        directory_conflict: true,
        ..Default::default()
    });
    for key in ["root/", "root/a/", "root/a/b", "root/a_very_long_file_name"] {
        store.put_empty(key).await.unwrap();
    }
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    assert!(fs.delete("/root", true).await.unwrap());
    let calls = store.calls.lock().unwrap();
    let child = calls.iter().position(|c| c == "delete:root/a/b").unwrap();
    let parent = calls.iter().position(|c| c == "delete:root/a/").unwrap();
    let shallow = calls
        .iter()
        .position(|c| c == "delete:root/a_very_long_file_name")
        .unwrap();
    assert!(child < parent && child < shallow);
    assert_eq!(calls.last().unwrap(), "delete:root/");
    assert!(store.objects.lock().unwrap().is_empty());
}
#[tokio::test]
async fn trash_moves_file_instead_of_deleting_it() {
    let store = Arc::new(MemoryStore {
        pfs: true,
        ..Default::default()
    });
    store
        .put("file", Bytes::from_static(b"kept"), PutHeaders::default())
        .await
        .unwrap();
    let fs = ObsFileSystem::with_store(
        ObsConfig {
            trash_enabled: true,
            ..Default::default()
        },
        store.clone(),
    );
    assert!(fs.delete("/file", false).await.unwrap());
    let objects = store.objects.lock().unwrap();
    assert!(!objects.contains_key("file"));
    assert!(objects
        .iter()
        .any(|(key, bytes)| key.starts_with(".obs_trash/")
            && key.ends_with("/file")
            && bytes.as_ref() == b"kept"));
}
#[tokio::test]
async fn rename_file_to_itself_and_missing_parent() {
    let store = Arc::new(MemoryStore::default());
    store.put_empty("file").await.unwrap();
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    assert!(fs.rename("/file", "/file", false).await.unwrap());
    assert!(!fs.rename("/file", "/missing/file", false).await.unwrap());
    assert!(store.objects.lock().unwrap().contains_key("file"));
}

#[tokio::test]
async fn nearby_seeks_reuse_cached_range() {
    let store = Arc::new(MemoryStore::default());
    store
        .put(
            "file",
            Bytes::from_static(b"abcdefghijklmnop"),
            PutHeaders::default(),
        )
        .await
        .unwrap();
    let config = ObsConfig {
        readahead_chunk_size: 8,
        readahead_max_chunks: 0,
        ..Default::default()
    };
    let mut stream = ObsInputStream::new(store.clone(), &config, "file".into(), 16, None);
    let mut out = [0; 2];
    stream.read(&mut out).await.unwrap();
    stream.seek(5).unwrap();
    stream.read(&mut out).await.unwrap();
    assert_eq!(&out, b"fg");
    stream.seek(0).unwrap();
    stream.read(&mut out).await.unwrap();
    assert_eq!(&out, b"ab");
    assert_eq!(store.count("get:"), 1);
    stream.seek(8).unwrap();
    stream.read(&mut out).await.unwrap();
    assert_eq!(&out, b"ij");
    assert_eq!(store.count("get:"), 2);
}

async fn stream_with_blocked_seek_target() -> (
    Arc<MemoryStore>,
    ObsInputStream,
    Arc<AppendControl>,
    Arc<Semaphore>,
) {
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        get_control: Some((16, control.clone())),
        ..Default::default()
    });
    store
        .put(
            "file",
            Bytes::from_static(b"abcdefghijklmnopqrstuvwx"),
            PutHeaders::default(),
        )
        .await
        .unwrap();
    let config = ObsConfig {
        readahead_chunk_size: 4,
        readahead_max_chunks: 2,
        ..Default::default()
    };
    let gate = Arc::new(Semaphore::new(2));
    let mut stream = ObsInputStream::with_gate(
        store.clone(),
        &config,
        "file".into(),
        24,
        None,
        gate.clone(),
    );
    let mut prefix = [0; 12];
    assert_eq!(stream.read(&mut prefix).await.unwrap(), 12);
    assert_eq!(&prefix, b"abcdefghijkl");
    tokio::time::timeout(Duration::from_secs(2), control.started.notified())
        .await
        .unwrap();
    (store, stream, control, gate)
}

#[tokio::test]
async fn seek_reuses_ready_and_inflight_target_prefetch() {
    for ready in [false, true] {
        let (store, mut stream, control, gate) = stream_with_blocked_seek_target().await;
        if ready {
            control.gate.add_permits(1);
            tokio::time::timeout(Duration::from_secs(2), control.stopped.notified())
                .await
                .unwrap();
        }
        stream.seek(17).unwrap();
        assert_eq!(stream.position(), 17);
        if !ready {
            control.gate.add_permits(1);
        }
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), stream.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            1
        );
        assert_eq!(&byte, b"r");
        assert_eq!(stream.position(), 18);
        assert_eq!(store.count("range:file:Some(16)"), 1);
        let permits = tokio::time::timeout(Duration::from_secs(2), gate.acquire_many(2))
            .await
            .unwrap()
            .unwrap();
        drop(permits);
    }
}

#[tokio::test]
async fn seek_preserves_prefetch_version_failure() {
    let (store, mut stream, control, gate) = stream_with_blocked_seek_target().await;
    store
        .put(
            "file",
            Bytes::from_static(b"ABCDEFGHIJKLMNOPQRSTUVWX"),
            PutHeaders::default(),
        )
        .await
        .unwrap();
    stream.seek(17).unwrap();
    control.gate.add_permits(1);
    let mut byte = [0];
    let result = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut byte))
        .await
        .unwrap();
    assert!(matches!(result, Err(ObsError::RemoteFileChanged(_))));
    assert_eq!(stream.position(), 17);
    assert!(matches!(
        stream.read(&mut byte).await,
        Err(ObsError::RemoteFileChanged(_))
    ));
    assert_eq!(store.count("range:file:Some(16)"), 1);
    let permits = tokio::time::timeout(Duration::from_secs(2), gate.acquire_many(2))
        .await
        .unwrap()
        .unwrap();
    drop(permits);
}

#[tokio::test]
async fn seek_to_eof_cancels_prefetch_and_releases_budget() {
    let (_, mut stream, control, gate) = stream_with_blocked_seek_target().await;
    stream.seek(24).unwrap();
    tokio::time::timeout(Duration::from_secs(2), control.stopped.notified())
        .await
        .unwrap();
    let permits = tokio::time::timeout(Duration::from_secs(2), gate.acquire_many(2))
        .await
        .unwrap()
        .unwrap();
    drop(permits);
    assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
}

#[tokio::test]
async fn upload_failure_is_reported_before_close_and_stays_sticky() {
    let store = Arc::new(MemoryStore {
        fail_part: true,
        ..Default::default()
    });
    let config = ObsConfig {
        multipart_threshold: 1,
        multipart_size: 5 * 1024 * 1024,
        ..Default::default()
    };
    let gate = Arc::new(Semaphore::new(1));
    let mut stream = ObsOutputStream::new(store.clone(), &config, "file".into(), gate.clone());
    stream.write(&vec![7; 5 * 1024 * 1024]).await.unwrap();
    // Acquiring the only permit waits for the upload to finish, without sleeps.
    let permit = gate.acquire().await.unwrap();
    drop(permit);
    tokio::task::yield_now().await;
    assert!(stream.flush().await.is_err());
    assert!(stream.write(b"more").await.is_err());
    assert!(stream.close().await.is_err());
    assert_eq!(store.count("part:"), 1);
    assert_eq!(store.count("complete"), 0);
    assert_eq!(store.count("abort"), 1);
}

#[tokio::test]
async fn partial_delete_invalidates_cached_children() {
    let store = Arc::new(MemoryStore {
        fail_delete: Some("dir/b".into()),
        ..Default::default()
    });
    store.put_empty("dir/a").await.unwrap();
    store.put_empty("dir/b").await.unwrap();
    let fs = ObsFileSystem::with_store(
        ObsConfig {
            metadata_cache_ttl_ms: 60_000,
            ..Default::default()
        },
        store,
    );
    fs.list_status("/dir").await.unwrap();
    assert!(fs.delete("/dir", true).await.is_err());
    assert!(!fs.exists("/dir/a").await.unwrap());
    assert!(fs.exists("/dir/b").await.unwrap());
}

#[tokio::test]
async fn partial_rename_invalidates_both_source_and_destination() {
    let store = Arc::new(MemoryStore {
        fail_delete: Some("src/b".into()),
        ..Default::default()
    });
    store.put_empty("src/a").await.unwrap();
    store.put_empty("src/b").await.unwrap();
    let fs = ObsFileSystem::with_store(
        ObsConfig {
            metadata_cache_ttl_ms: 60_000,
            negative_cache_ttl_ms: 60_000,
            ..Default::default()
        },
        store,
    );
    fs.list_status("/src").await.unwrap();
    assert!(!fs.exists("/dst/a").await.unwrap());
    assert!(fs.rename("/src", "/dst", false).await.is_err());
    assert!(!fs.exists("/src/a").await.unwrap());
    assert!(fs.exists("/src/b").await.unwrap());
    assert!(fs.exists("/dst/a").await.unwrap());
    assert!(fs.exists("/dst/b").await.unwrap());
}

#[tokio::test]
async fn cancelled_upload_write_cannot_commit_incomplete_file() {
    let store = Arc::new(MemoryStore::default());
    let config = ObsConfig {
        multipart_threshold: 1,
        multipart_size: 5 * 1024 * 1024,
        ..Default::default()
    };
    let gate = Arc::new(Semaphore::new(0));
    let mut stream = ObsOutputStream::new(store.clone(), &config, "file".into(), gate.clone());
    let data = vec![17; 5 * 1024 * 1024];
    let mut write = Box::pin(stream.write(&data));
    assert!(futures::poll!(write.as_mut()).is_pending());
    drop(write);
    gate.add_permits(1);
    assert!(stream.flush().await.is_err());
    assert!(stream.write(b"more").await.is_err());
    assert!(stream.close().await.is_err());
    assert_eq!(store.count("complete"), 0);
    assert_eq!(store.count("abort"), 1);
    assert!(!store.objects.lock().unwrap().contains_key("file"));
}

#[tokio::test]
async fn cancelled_append_flush_retains_task_for_drop() {
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        append_control: Some(control.clone()),
        ..Default::default()
    });
    store.put_empty("file").await.unwrap();
    let mut stream = PfsAppendStream::new(store.clone(), &ObsConfig::default(), "file".into(), 0);
    stream.write(b"abc").await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), control.started.notified())
        .await
        .unwrap();
    let mut flush = Box::pin(stream.flush());
    assert!(futures::poll!(flush.as_mut()).is_pending());
    drop(flush);
    drop(stream);
    tokio::time::timeout(Duration::from_secs(2), control.stopped.notified())
        .await
        .unwrap();
    assert!(store.objects.lock().unwrap()["file"].is_empty());
}

#[tokio::test]
async fn cancelled_append_flush_can_be_resumed() {
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        append_control: Some(control.clone()),
        ..Default::default()
    });
    store.put_empty("file").await.unwrap();
    let mut stream = PfsAppendStream::new(store.clone(), &ObsConfig::default(), "file".into(), 0);
    stream.write(b"abc").await.unwrap();
    let mut flush = Box::pin(stream.flush());
    assert!(futures::poll!(flush.as_mut()).is_pending());
    drop(flush);
    control.gate.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), stream.flush())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&store.objects.lock().unwrap()["file"][..], b"abc");
    assert_eq!(store.count("append:"), 1);
}

#[tokio::test]
async fn append_completion_invalidates_status_before_flush() {
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        append_control: Some(control.clone()),
        pfs: true,
        ..Default::default()
    });
    store.put_empty("file").await.unwrap();
    let fs = ObsFileSystem::with_store(
        ObsConfig {
            metadata_cache_ttl_ms: 60_000,
            ..Default::default()
        },
        store,
    );
    let mut stream = fs.append_stream("/file", 0).await.unwrap();
    assert_eq!(fs.get_file_status("/file").await.unwrap().length, 0);
    stream.write(b"abc").await.unwrap();
    control.gate.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), control.stopped.notified())
        .await
        .unwrap();
    assert_eq!(fs.get_file_status("/file").await.unwrap().length, 3);
    stream.close().await.unwrap();
}

#[tokio::test]
async fn append_empty_and_overflow_do_not_issue_requests() {
    let store = Arc::new(MemoryStore::default());
    let mut stream = PfsAppendStream::new(
        store.clone(),
        &ObsConfig::default(),
        "file".into(),
        u64::MAX,
    );
    assert_eq!(stream.write(b"").await.unwrap(), 0);
    assert!(stream.write(b"x").await.is_err());
    stream.close().await.unwrap();
    assert_eq!(store.count("append:"), 0);
}

#[tokio::test]
#[ignore = "local latency benchmark; run explicitly with --ignored --nocapture"]
async fn benchmark_large_sequential_read() {
    for trial in 1..=3 {
        let store = Arc::new(MemoryStore {
            get_delay_ms: 5,
            omit_etag: true,
            ..Default::default()
        });
        let size = 64 * 64 * 1024;
        let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        store
            .put("file", Bytes::from(data.clone()), PutHeaders::default())
            .await
            .unwrap();
        let config = ObsConfig {
            readahead_chunk_size: 64 * 1024,
            readahead_max_chunks: 8,
            readahead_total_chunks: 8,
            read_change_detection: false,
            ..Default::default()
        };
        let mut stream =
            ObsInputStream::new(store.clone(), &config, "file".into(), size as u64, None);
        let mut out = vec![0; size];
        let start = std::time::Instant::now();
        assert_eq!(stream.read(&mut out).await.unwrap(), size);
        let elapsed = start.elapsed();
        assert_eq!(out, data);
        println!(
            "trial={trial} bytes={size} elapsed_ms={:.3} requests={} peak_gets={}",
            elapsed.as_secs_f64() * 1000.0,
            store.count("get:"),
            store.get_peak.load(Ordering::SeqCst)
        );
    }
}

#[tokio::test]
#[ignore = "release listing benchmark; scripts/build-windows.ps1 -BenchmarkListing"]
async fn benchmark_paginated_listing() {
    for (entries, cache) in [(10_000, 0), (100_000, 0), (10_000, 4096)] {
        for trial in 0..=5 {
            // Alternate order to avoid always favoring the second variant.
            for paged in if trial % 2 == 0 { [false, true] } else { [true, false] } {
                let store = Arc::new(MemoryStore { synthetic_listing: Some(entries), ..Default::default() });
                let fs = ObsFileSystem::with_store(ObsConfig {
                    paging_maximum: 1000, metadata_cache_size: cache,
                    metadata_cache_ttl_ms: 60_000, ..Default::default()
                }, store.clone());
                let start = std::time::Instant::now();
                let first_ms;
                let first_requests;
                let count;
                let total_ms;
                if paged {
                    let mut cursor = obs_rust_core::listing::StatusCursor::files("/bench".into(), true);
                    let first = cursor.next_page(&fs).await.unwrap();
                    first_ms = start.elapsed().as_secs_f64() * 1000.0;
                    first_requests = store.count("list:");
                    let mut seen = first.entries.len();
                    let mut more = first.next_marker.is_some();
                    drop(first);
                    while more {
                        let page = cursor.next_page(&fs).await.unwrap();
                        seen += page.entries.len();
                        more = page.next_marker.is_some();
                    }
                    total_ms = start.elapsed().as_secs_f64() * 1000.0;
                    count = seen;
                } else {
                    let statuses = fs.list_files_recursive("/bench").await.unwrap();
                    total_ms = start.elapsed().as_secs_f64() * 1000.0;
                    first_ms = total_ms;
                    first_requests = store.count("list:");
                    count = statuses.len();
                }
                assert_eq!(count, entries);
                assert_eq!(store.count("list:"), entries / 1000);
                if trial != 0 {
                    println!("BENCH_JSON {}", serde_json::json!({
                        "variant": if paged { "paged" } else { "eager" },
                        "entries": entries, "cache_entries": cache, "trial": trial,
                        "page_size": 1000, "simulated_delay_ms": 1,
                        "first_ms": first_ms, "total_ms": total_ms,
                        "first_requests": first_requests, "requests": store.count("list:")
                    }));
                }
            }
        }
    }
}

#[tokio::test]
async fn large_read_pipelines_without_exceeding_prefetch_budget() {
    let store = Arc::new(MemoryStore {
        get_delay_ms: 1,
        ..Default::default()
    });
    let data: Vec<u8> = (0..32 * 1024).map(|i| (i % 251) as u8).collect();
    store
        .put("file", Bytes::from(data.clone()), PutHeaders::default())
        .await
        .unwrap();
    let config = ObsConfig {
        readahead_chunk_size: 1024,
        readahead_max_chunks: 8,
        readahead_total_chunks: 2,
        ..Default::default()
    };
    let gate = Arc::new(Semaphore::new(2));
    let mut stream = ObsInputStream::with_gate(
        store.clone(),
        &config,
        "file".into(),
        data.len() as u64,
        None,
        gate.clone(),
    );
    let mut out = vec![0; data.len()];
    assert_eq!(stream.read(&mut out).await.unwrap(), data.len());
    assert_eq!(out, data);
    assert_eq!(store.count("get:"), 32);
    let peak = store.get_peak.load(Ordering::SeqCst);
    assert!(
        peak > 1 && peak <= 3,
        "prefetch budget plus one foreground request: {peak}"
    );
    assert_eq!(gate.available_permits(), 2);
}

#[tokio::test]
async fn disabled_prefetch_stays_serial_for_large_read() {
    let store = Arc::new(MemoryStore {
        get_delay_ms: 1,
        ..Default::default()
    });
    let data = vec![9; 8 * 1024];
    store
        .put("file", Bytes::from(data.clone()), PutHeaders::default())
        .await
        .unwrap();
    let config = ObsConfig {
        readahead_chunk_size: 1024,
        readahead_max_chunks: 0,
        ..Default::default()
    };
    let mut stream = ObsInputStream::new(
        store.clone(),
        &config,
        "file".into(),
        data.len() as u64,
        None,
    );
    let mut out = vec![0; data.len()];
    stream.read(&mut out).await.unwrap();
    assert_eq!(out, data);
    assert_eq!(store.get_peak.load(Ordering::SeqCst), 1);
    assert_eq!(store.count("get:"), 8);
}

#[tokio::test]
async fn cancelled_prefetch_wait_reuses_request_on_resume() {
    let store = Arc::new(MemoryStore {
        get_delay_ms: 1,
        ..Default::default()
    });
    let data: Vec<u8> = (0..8 * 1024).map(|i| (i % 251) as u8).collect();
    store
        .put("file", Bytes::from(data.clone()), PutHeaders::default())
        .await
        .unwrap();
    let config = ObsConfig {
        readahead_chunk_size: 1024,
        readahead_max_chunks: 2,
        ..Default::default()
    };
    let mut stream = ObsInputStream::new(
        store.clone(),
        &config,
        "file".into(),
        data.len() as u64,
        None,
    );
    let mut prefix = vec![0; 3 * 1024];
    stream.read(&mut prefix).await.unwrap();
    let mut next = vec![0; 1024];
    let mut waiting = Box::pin(stream.read(&mut next));
    assert!(futures::poll!(waiting.as_mut()).is_pending());
    drop(waiting);
    assert_eq!(stream.position(), 3 * 1024);
    let mut suffix = vec![0; 5 * 1024];
    stream.read(&mut suffix).await.unwrap();
    prefix.extend_from_slice(&suffix);
    assert_eq!(prefix, data);
    assert_eq!(store.count("get:"), 8);
}

#[tokio::test]
async fn listing_rejects_multi_page_marker_cycle() {
    let store = MemoryStore::default();
    for marker in ["a", "b", "a"] {
        store.list_pages.lock().unwrap().push_back(ListPage {
            is_truncated: true,
            next_marker: Some(marker.into()),
            ..Default::default()
        });
    }
    let result = obs_rust_core::listing::list_all(&store, "", "", 1000).await;
    assert!(result.is_err());
    assert_eq!(store.count("list:"), 3);
}

#[tokio::test]
async fn listing_rejects_truncated_page_without_continuation() {
    let store = MemoryStore::default();
    store.list_pages.lock().unwrap().push_back(ListPage {
        is_truncated: true,
        ..Default::default()
    });
    assert!(obs_rust_core::listing::list_all(&store, "", "", 1000)
        .await
        .is_err());
    assert_eq!(store.count("list:"), 1);
}

#[tokio::test]
async fn listing_deduplicates_prefixes_and_retains_directory_metadata() {
    let store = MemoryStore::default();
    store.list_pages.lock().unwrap().push_back(ListPage {
        contents: vec![ListEntry {
            key: "dir/".into(),
            mode: Some(0o40700),
            ..Default::default()
        }],
        common_prefixes: vec!["dir/".into(), "sub/".into(), "sub/".into()],
        ..Default::default()
    });
    let entries = obs_rust_core::listing::list_all(&store, "", "/", 1000)
        .await
        .unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(
        entries.iter().find(|e| e.key == "dir/").unwrap().mode,
        Some(0o40700)
    );
    assert!(
        entries
            .iter()
            .find(|e| e.key == "sub/")
            .unwrap()
            .is_common_prefix
    );
}

#[tokio::test]
async fn listing_uses_last_object_when_next_marker_is_absent() {
    let store = MemoryStore::default();
    store.list_pages.lock().unwrap().extend([
        ListPage {
            contents: vec![ListEntry {
                key: "a".into(),
                ..Default::default()
            }],
            is_truncated: true,
            ..Default::default()
        },
        ListPage {
            contents: vec![ListEntry {
                key: "b".into(),
                ..Default::default()
            }],
            ..Default::default()
        },
    ]);
    let entries = obs_rust_core::listing::list_all(&store, "", "", 1000)
        .await
        .unwrap();
    assert_eq!(
        entries.iter().map(|e| e.key.as_str()).collect::<Vec<_>>(),
        vec!["a", "b"]
    );
    assert_eq!(store.count("list:"), 2);
    assert_eq!(
        *store.list_markers.lock().unwrap(),
        vec![None, Some("a".into())]
    );
}

#[test]
fn list_default_permissions_match_head_defaults() {
    use obs_rust_core::listing::list_entry_to_status;
    for (key, expected) in [("file", 0o644), ("dir/", 0o755)] {
        let entry = ListEntry {
            key: key.into(),
            ..Default::default()
        };
        assert_eq!(list_entry_to_status(&entry, "").permission, expected);
    }
    let entry = ListEntry {
        key: "file".into(),
        mode: Some(0o100600),
        ..Default::default()
    };
    assert_eq!(list_entry_to_status(&entry, "").permission, 0o600);
}

#[tokio::test]
async fn overwrite_rename_rejects_type_conflicts_and_directory_merges() {
    for pfs in [false, true] {
        for (source, destination, keys) in [
            ("/src", "/dst", vec!["src/child", "dst"]),
            ("/src", "/dst", vec!["src", "dst/src/child"]),
            ("/src", "/dst", vec!["src/child", "dst/src/existing"]),
        ] {
            let store = Arc::new(MemoryStore {
                pfs,
                ..Default::default()
            });
            for key in keys {
                store
                    .put(key, Bytes::from_static(b"preserve"), PutHeaders::default())
                    .await
                    .unwrap();
            }
            let before = store.objects.lock().unwrap().clone();
            let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
            assert!(!fs.rename(source, destination, true).await.unwrap());
            assert_eq!(*store.objects.lock().unwrap(), before);
            assert_eq!(store.count("rename:"), 0);
            assert_eq!(store.count("delete:"), 0);
        }
    }
}

#[tokio::test]
async fn object_file_overwrite_requires_flag_and_updates_content() {
    let store = Arc::new(MemoryStore::default());
    store
        .put("src", Bytes::from_static(b"new"), PutHeaders::default())
        .await
        .unwrap();
    store
        .put("dst", Bytes::from_static(b"old"), PutHeaders::default())
        .await
        .unwrap();
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    assert!(!fs.rename("/src", "/dst", false).await.unwrap());
    assert_eq!(&store.objects.lock().unwrap()["dst"][..], b"old");
    assert!(fs.rename("/src", "/dst", true).await.unwrap());
    assert!(!fs.exists("/src").await.unwrap());
    assert_eq!(&store.objects.lock().unwrap()["dst"][..], b"new");
}

#[tokio::test]
async fn object_rename_into_directory_resolves_child_before_overwrite() {
    let store = Arc::new(MemoryStore::default());
    store
        .put("src", Bytes::from_static(b"new"), PutHeaders::default())
        .await
        .unwrap();
    store
        .put("dst/src", Bytes::from_static(b"old"), PutHeaders::default())
        .await
        .unwrap();
    store.put_empty("dst/other").await.unwrap();
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    assert!(!fs.rename("/src", "/dst", false).await.unwrap());
    assert!(fs.rename("/src", "/dst", true).await.unwrap());
    assert_eq!(&store.objects.lock().unwrap()["dst/src"][..], b"new");
    assert!(store.objects.lock().unwrap().contains_key("dst/other"));
    assert!(!store.objects.lock().unwrap().contains_key("src"));
}

#[tokio::test]
async fn listing_rejects_out_of_scope_objects_and_prefixes() {
    for (contents, common_prefixes) in [
        (
            vec![ListEntry {
                key: "dir-neighbor/file".into(),
                ..Default::default()
            }],
            vec![],
        ),
        (vec![], vec!["other/".into()]),
        (
            vec![ListEntry {
                key: "".into(),
                ..Default::default()
            }],
            vec![],
        ),
    ] {
        let store = MemoryStore::default();
        store.list_pages.lock().unwrap().push_back(ListPage {
            contents,
            common_prefixes,
            ..Default::default()
        });
        assert!(obs_rust_core::listing::list_all(&store, "dir/", "/", 1000)
            .await
            .is_err());
    }
}

#[tokio::test]
async fn recursive_delete_cannot_delete_out_of_scope_list_entries() {
    for pfs in [false, true] {
        let store = Arc::new(MemoryStore {
            pfs,
            directory_conflict: pfs,
            ..Default::default()
        });
        for key in ["dir/", "dir/file", "outside"] {
            store.put_empty(key).await.unwrap();
        }
        let before = store.objects.lock().unwrap().clone();
        let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
        fs.get_file_status("/dir").await.unwrap();
        store.list_pages.lock().unwrap().push_back(ListPage {
            contents: vec![ListEntry {
                key: "outside".into(),
                ..Default::default()
            }],
            ..Default::default()
        });
        assert!(fs.delete("/dir", true).await.is_err());
        assert_eq!(*store.objects.lock().unwrap(), before);
        assert_eq!(store.count("delete:outside"), 0);
    }
}

#[tokio::test]
async fn rename_rejects_malformed_listing_before_copying_or_deleting() {
    let store = Arc::new(MemoryStore::default());
    for key in ["source/", "source/file", "x"] {
        store.put_empty(key).await.unwrap();
    }
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    fs.get_file_status("/source").await.unwrap();
    assert!(!fs.exists("/target").await.unwrap());
    let before = store.objects.lock().unwrap().clone();
    store.list_pages.lock().unwrap().extend([
        ListPage {
            contents: vec![ListEntry {
                key: "source/file".into(),
                ..Default::default()
            }],
            is_truncated: true,
            next_marker: Some("source/file".into()),
            ..Default::default()
        },
        ListPage {
            contents: vec![ListEntry {
                key: "x".into(),
                ..Default::default()
            }],
            ..Default::default()
        },
    ]);
    assert!(fs.rename("/source", "/target", false).await.is_err());
    assert_eq!(*store.objects.lock().unwrap(), before);
    assert_eq!(store.count("delete:"), 0);
}

#[tokio::test]
async fn reader_rejects_same_length_overwrite_between_ranges() {
    let store = Arc::new(MemoryStore::default());
    store
        .put(
            "file",
            Bytes::from_static(b"abcdefgh"),
            PutHeaders::default(),
        )
        .await
        .unwrap();
    let config = ObsConfig {
        readahead_chunk_size: 4,
        readahead_max_chunks: 0,
        ..Default::default()
    };
    let mut stream = ObsInputStream::new(store.clone(), &config, "file".into(), 8, None);
    let mut first = [0; 4];
    stream.read(&mut first).await.unwrap();
    assert_eq!(&first, b"abcd");
    store
        .put(
            "file",
            Bytes::from_static(b"12345678"),
            PutHeaders::default(),
        )
        .await
        .unwrap();
    let mut next = [99; 4];
    assert!(matches!(
        stream.read(&mut next).await,
        Err(ObsError::RemoteFileChanged(_))
    ));
    assert_eq!(next, [99; 4]);
    stream.seek(0).unwrap();
    assert!(matches!(
        stream.read(&mut next).await,
        Err(ObsError::RemoteFileChanged(_))
    ));
}

#[tokio::test]
async fn open_pins_cached_status_etag_without_extra_head() {
    let store = Arc::new(MemoryStore::default());
    store
        .put("file", Bytes::from_static(b"old"), PutHeaders::default())
        .await
        .unwrap();
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    fs.get_file_status("/file").await.unwrap();
    let mut stream = fs.open("/file", OpenOptions::default()).await.unwrap();
    store
        .put("file", Bytes::from_static(b"new"), PutHeaders::default())
        .await
        .unwrap();
    assert!(matches!(
        stream.read(&mut [0; 3]).await,
        Err(ObsError::RemoteFileChanged(_))
    ));
    assert_eq!(store.count("head:"), 1);
}

#[tokio::test]
async fn missing_etag_fails_closed_unless_detection_is_disabled() {
    let store = Arc::new(MemoryStore {
        omit_etag: true,
        ..Default::default()
    });
    store
        .put("file", Bytes::from_static(b"abc"), PutHeaders::default())
        .await
        .unwrap();
    let mut strict =
        ObsInputStream::new(store.clone(), &ObsConfig::default(), "file".into(), 3, None);
    assert!(matches!(
        strict.read(&mut [0; 3]).await,
        Err(ObsError::MissingVersion(_))
    ));
    let config = ObsConfig {
        read_change_detection: false,
        ..Default::default()
    };
    let mut compatible = ObsInputStream::new(store, &config, "file".into(), 3, None);
    let mut bytes = [0; 3];
    compatible.read(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"abc");
}

#[tokio::test]
async fn prefetch_uses_the_same_pinned_revision() {
    let (store, mut stream, control, _) = stream_with_blocked_seek_target().await;
    store
        .put(
            "file",
            Bytes::from_static(b"ABCDEFGHIJKLMNOPQRSTUVWX"),
            PutHeaders::default(),
        )
        .await
        .unwrap();
    control.gate.add_permits(1);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), stream.read(&mut [0; 8]))
            .await
            .unwrap(),
        Err(ObsError::RemoteFileChanged(_))
    ));
}

#[tokio::test]
async fn pfs_read_does_not_require_immutable_object_etags() {
    let store = Arc::new(MemoryStore {
        pfs: true,
        omit_etag: true,
        ..Default::default()
    });
    store
        .put("file", Bytes::from_static(b"abc"), PutHeaders::default())
        .await
        .unwrap();
    let mut stream = ObsInputStream::new(store, &ObsConfig::default(), "file".into(), 3, None);
    let mut bytes = [0; 3];
    stream.read(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"abc");
}

#[test]
fn read_change_detection_configuration_defaults_on() {
    let mut props = std::collections::HashMap::new();
    assert!(obs_rust_core::config::config_from_hadoop(&props, "bucket").read_change_detection);
    props.insert(
        "fs.obs.read.change.detection.enabled".into(),
        "false".into(),
    );
    assert!(!obs_rust_core::config::config_from_hadoop(&props, "bucket").read_change_detection);
}

#[test]
fn debug_output_redacts_credentials_and_overrides() {
    let credentials = obs_rust_core::auth::Credentials::new("private-ak", "private-sk")
        .with_session_token("private-token");
    let mut config = ObsConfig {
        access_key: "private-ak".into(),
        secret_key: "private-sk".into(),
        session_token: Some("private-token".into()),
        endpoint: "private-endpoint".into(),
        ..Default::default()
    };
    config.bucket_overrides.insert(
        "bucket".into(),
        std::collections::HashMap::from([("secret.key".into(), "private-override".into())]),
    );
    let text = format!("{credentials:?} {config:?}");
    for secret in [
        "private-ak",
        "private-sk",
        "private-token",
        "private-endpoint",
        "private-override",
    ] {
        assert!(!text.contains(secret));
    }
    assert!(text.contains("<redacted>"));
}

#[tokio::test]
async fn failed_large_read_does_not_advance_logical_position() {
    let store = Arc::new(MemoryStore {
        fail_get_at: Some(4),
        get_failures: AtomicUsize::new(1),
        ..Default::default()
    });
    store
        .put(
            "file",
            Bytes::from_static(b"abcdefgh"),
            PutHeaders::default(),
        )
        .await
        .unwrap();
    let config = ObsConfig {
        readahead_chunk_size: 4,
        readahead_max_chunks: 0,
        ..Default::default()
    };
    let mut stream = ObsInputStream::new(store, &config, "file".into(), 8, None);
    let mut bytes = [0; 8];
    assert!(matches!(
        stream.read(&mut bytes).await,
        Err(ObsError::Transport(_))
    ));
    assert_eq!(&bytes[..4], b"abcd");
    assert_eq!(stream.position(), 0);
    assert_eq!(stream.read(&mut bytes).await.unwrap(), 8);
    assert_eq!(&bytes, b"abcdefgh");
    assert_eq!(stream.position(), 8);
}
#[tokio::test]
async fn cancelled_large_read_restores_cursor_before_resume() {
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        get_control: Some((4, control.clone())),
        ..Default::default()
    });
    store
        .put(
            "file",
            Bytes::from_static(b"abcdefgh"),
            PutHeaders::default(),
        )
        .await
        .unwrap();
    let config = ObsConfig {
        readahead_chunk_size: 4,
        readahead_max_chunks: 0,
        ..Default::default()
    };
    let mut stream = ObsInputStream::new(store, &config, "file".into(), 8, None);
    let mut bytes = [0; 8];
    {
        let read = stream.read(&mut bytes);
        tokio::pin!(read);
        tokio::select! { _ = &mut read => panic!("read must block"), _ = control.started.notified() => {} }
    }
    assert_eq!(&bytes[..4], b"abcd");
    assert_eq!(stream.position(), 0);
    control.gate.add_permits(1);
    assert_eq!(stream.read(&mut bytes).await.unwrap(), 8);
    assert_eq!(&bytes, b"abcdefgh");
}
#[tokio::test]
async fn append_chunks_large_writes_and_shares_byte_budget_between_streams() {
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        pfs: true,
        append_control: Some(control.clone()),
        ..Default::default()
    });
    store.put_empty("a").await.unwrap();
    store.put_empty("b").await.unwrap();
    let config = ObsConfig {
        pfs_append_chunk_size: 4,
        pfs_append_total_bytes: 8,
        pfs_append_concurrency: 8,
        ..Default::default()
    };
    let fs = ObsFileSystem::with_store(config, store.clone());
    let mut a = fs.append_stream("/a", 0).await.unwrap();
    let mut b = fs.append_stream("/b", 0).await.unwrap();
    a.write(b"abcdefgh").await.unwrap();
    {
        let write = b.write(b"ijklmnop");
        tokio::pin!(write);
        assert!(futures::poll!(write.as_mut()).is_pending());
        tokio::time::timeout(Duration::from_secs(2), control.started.notified())
            .await
            .unwrap();
        assert!(store.count("append:a") > 0);
        assert_eq!(store.count("append:b"), 0);
        control.gate.add_permits(4);
        tokio::time::timeout(Duration::from_secs(2), &mut write)
            .await
            .unwrap()
            .unwrap();
    }
    a.close().await.unwrap();
    b.close().await.unwrap();
    assert_eq!(
        store.objects.lock().unwrap().get("a").unwrap().as_ref(),
        b"abcdefgh"
    );
    assert_eq!(
        store.objects.lock().unwrap().get("b").unwrap().as_ref(),
        b"ijklmnop"
    );
    assert!(store.append_peak_bytes.load(Ordering::SeqCst) <= 8);
    assert_eq!(store.count("append:"), 4);
    assert_eq!(store.append_bytes.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn append_small_request_cannot_take_large_waiters_reserved_bytes() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let control = AppendControl::blocked();
        let store = Arc::new(MemoryStore { pfs: true, append_control: Some(control.clone()), ..Default::default() });
        for key in ["holder", "large", "small"] { store.put_empty(key).await.unwrap(); }
        let fs = ObsFileSystem::with_store(ObsConfig {
            pfs_append_chunk_size: 8, pfs_append_total_bytes: 8, ..Default::default()
        }, store.clone());
        let mut holder = fs.append_stream("/holder", 0).await.unwrap();
        let mut large = fs.append_stream("/large", 0).await.unwrap();
        let mut small = fs.append_stream("/small", 0).await.unwrap();
        holder.write(b"1234").await.unwrap();
        let mut large_write = Box::pin(large.write(b"12345678"));
        assert!(futures::poll!(large_write.as_mut()).is_pending());
        // The earlier eight-byte request has reserved the four free bytes.
        let mut small_write = Box::pin(small.write(b"x"));
        assert!(futures::poll!(small_write.as_mut()).is_pending());
        control.gate.add_permits(1);
        holder.flush().await.unwrap();
        // Even before the large waiter is polled again, released bytes belong
        // to it and must not be stolen by a newly polled small request.
        assert!(futures::poll!(small_write.as_mut()).is_pending());
        large_write.as_mut().await.unwrap();
        drop(large_write);
        assert!(futures::poll!(small_write.as_mut()).is_pending());
        control.gate.add_permits(1);
        large.flush().await.unwrap();
        small_write.as_mut().await.unwrap();
        drop(small_write);
        control.gate.add_permits(1);
        small.flush().await.unwrap();
        assert_eq!(store.objects.lock().unwrap()["small"].as_ref(), b"x");
        assert_eq!(store.objects.lock().unwrap()["large"].as_ref(), b"12345678");
        assert!(store.append_peak_bytes.load(Ordering::SeqCst) <= 8);
        assert_eq!(store.append_bytes.load(Ordering::SeqCst), 0);
    }).await.expect("budget waiter failed to make progress");
}
#[tokio::test]
async fn cancelled_partial_append_is_sticky_and_drop_releases_shared_budget() {
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        pfs: true,
        append_control: Some(control.clone()),
        ..Default::default()
    });
    store.put_empty("file").await.unwrap();
    let config = ObsConfig {
        pfs_append_chunk_size: 4,
        pfs_append_total_bytes: 4,
        ..Default::default()
    };
    let fs = ObsFileSystem::with_store(config, store.clone());
    let mut stream = fs.append_stream("/file", 0).await.unwrap();
    {
        let write = stream.write(b"abcdefgh");
        tokio::pin!(write);
        tokio::select! { _ = &mut write => panic!("write must block"), _ = control.started.notified() => {} }
    }
    assert!(stream
        .write(b"replacement")
        .await
        .unwrap_err()
        .to_string()
        .contains("cancelled"));
    drop(stream);
    tokio::time::timeout(Duration::from_secs(2), control.stopped.notified())
        .await
        .unwrap();
    let mut next = fs.append_stream("/file", 0).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), next.write(b"test"))
        .await
        .unwrap()
        .unwrap();
    control.gate.add_permits(1);
    next.close().await.unwrap();
    assert_eq!(
        store.objects.lock().unwrap().get("file").unwrap().as_ref(),
        b"test"
    );
}

#[tokio::test]
async fn append_failure_releases_budget_and_blocks_further_chunks() {
    let store = Arc::new(MemoryStore {
        pfs: true,
        append_failures: AtomicUsize::new(1),
        ..Default::default()
    });
    store.put_empty("file").await.unwrap();
    let config = ObsConfig {
        pfs_append_chunk_size: 4,
        pfs_append_total_bytes: 4,
        ..Default::default()
    };
    let fs = ObsFileSystem::with_store(config, store.clone());
    let mut stream = fs.append_stream("/file", 0).await.unwrap();
    assert!(stream.write(b"abcdefgh").await.is_err());
    assert!(stream.flush().await.is_err());
    assert_eq!(store.count("append:"), 1);
    let mut next = fs.append_stream("/file", 0).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), next.write(b"pass"))
        .await
        .unwrap()
        .unwrap();
    next.close().await.unwrap();
    assert_eq!(
        store.objects.lock().unwrap().get("file").unwrap().as_ref(),
        b"pass"
    );
}
#[test]
fn hadoop_append_byte_budget_supports_size_suffix_and_bucket_override() {
    let props = std::collections::HashMap::from([
        ("fs.obs.pfs.append.chunk.size".into(), "2m".into()),
        ("fs.obs.pfs.append.total.bytes".into(), "16m".into()),
        (
            "fs.obs.bucket.bucket.pfs.append.total.bytes".into(),
            "8m".into(),
        ),
    ]);
    let config = obs_rust_core::config::config_from_hadoop(&props, "bucket");
    assert_eq!(config.pfs_append_chunk_size, 2 * 1024 * 1024);
    assert_eq!(config.pfs_append_total_bytes, 8 * 1024 * 1024);
}

#[tokio::test(start_paused = true)]
async fn failed_part_cancels_stalled_sibling_and_releases_permits() {
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        fail_part: true,
        part_control: Some((2, control.clone())),
        ..Default::default()
    });
    let config = ObsConfig {
        multipart_size: 5 * 1024 * 1024,
        multipart_threshold: 1,
        upload_active_blocks: 2,
        ..Default::default()
    };
    let gate = Arc::new(Semaphore::new(2));
    let mut stream = ObsOutputStream::new(store.clone(), &config, "file".into(), gate.clone());
    stream.write(&vec![42; 10 * 1024 * 1024]).await.unwrap();
    control.started.notified().await;
    let error = tokio::time::timeout(Duration::from_secs(2), stream.close())
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("injected part failure"));
    assert_eq!(gate.available_permits(), 2);
    assert_eq!(store.count("complete"), 0);
    assert_eq!(store.count("abort"), 1);
    assert!(!store.objects.lock().unwrap().contains_key("file"));
}
#[tokio::test]
async fn cancelling_close_during_abort_keeps_cleanup_id() {
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        fail_part: true,
        abort_control: Some(control.clone()),
        ..Default::default()
    });
    let config = ObsConfig {
        multipart_threshold: 1,
        ..Default::default()
    };
    let mut stream = ObsOutputStream::new(
        store.clone(),
        &config,
        "file".into(),
        Arc::new(Semaphore::new(1)),
    );
    stream.write(b"broken").await.unwrap();
    {
        let close = stream.close();
        tokio::pin!(close);
        tokio::select! { _ = &mut close => panic!("abort must block"), _ = control.started.notified() => {} }
    }
    tokio::time::timeout(Duration::from_secs(2), control.started.notified())
        .await
        .unwrap();
    assert_eq!(store.count("abort"), 2);
    control.gate.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), control.stopped.notified())
        .await
        .unwrap();
    assert_eq!(store.count("complete"), 0);
}
#[tokio::test]
async fn failed_abort_gets_one_drop_cleanup_attempt() {
    let store = Arc::new(MemoryStore {
        fail_part: true,
        abort_failures: AtomicUsize::new(1),
        ..Default::default()
    });
    let config = ObsConfig {
        multipart_threshold: 1,
        ..Default::default()
    };
    let mut stream = ObsOutputStream::new(
        store.clone(),
        &config,
        "file".into(),
        Arc::new(Semaphore::new(1)),
    );
    stream.write(b"broken").await.unwrap();
    assert!(stream
        .close()
        .await
        .unwrap_err()
        .to_string()
        .contains("injected part failure"));
    tokio::time::timeout(Duration::from_secs(2), async {
        while store.count("abort") < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(store.count("abort"), 2);
    assert_eq!(store.count("complete"), 0);
}
#[test]
fn multipart_xml_escapes_opaque_etag_text() {
    let body = obs_rust_core::xml::build_complete_mpu_xml(&[(1, "tag&<opaque>")]);
    assert!(body.contains("tag&amp;&lt;opaque&gt;"));
    obs_rust_core::xml::validate_document(&body, "CompleteMultipartUpload").unwrap();
}

#[tokio::test]
async fn tiny_reads_do_not_prefetch_until_three_chunks_are_consumed() {
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        get_control: Some((24, control.clone())),
        ..Default::default()
    });
    let data: Vec<u8> = (0..64).collect();
    store
        .put("file", Bytes::from(data.clone()), PutHeaders::default())
        .await
        .unwrap();
    let config = ObsConfig {
        readahead_chunk_size: 8,
        readahead_max_chunks: 2,
        ..Default::default()
    };
    let gate = Arc::new(Semaphore::new(2));
    let mut stream = ObsInputStream::with_gate(
        store.clone(),
        &config,
        "file".into(),
        64,
        None,
        gate.clone(),
    );
    for expected in 0..23 {
        let mut out = [0];
        assert_eq!(stream.read(&mut out).await.unwrap(), 1);
        assert_eq!(out[0], expected);
        assert_eq!(
            gate.available_permits(),
            2,
            "early prefetch after byte {expected}"
        );
        assert_eq!(store.count("get:"), expected as usize / 8 + 1);
    }
    let mut last = [0];
    stream.read(&mut last).await.unwrap();
    assert_eq!(last, [23]);
    tokio::time::timeout(Duration::from_secs(2), control.started.notified())
        .await
        .unwrap();
    control.gate.add_permits(1);
    let mut suffix = [0; 40];
    assert_eq!(stream.read(&mut suffix).await.unwrap(), 40);
    assert_eq!(suffix.as_slice(), &data[24..]);
    assert_eq!(store.count("get:"), 8);
    assert_eq!(gate.available_permits(), 2);
}

#[tokio::test]
async fn seek_resets_chunk_progress_before_prefetch() {
    let store = Arc::new(MemoryStore::default());
    store
        .put("file", Bytes::from(vec![7; 64]), PutHeaders::default())
        .await
        .unwrap();
    let config = ObsConfig {
        readahead_chunk_size: 8,
        readahead_max_chunks: 2,
        ..Default::default()
    };
    let gate = Arc::new(Semaphore::new(2));
    let mut stream = ObsInputStream::with_gate(
        store.clone(),
        &config,
        "file".into(),
        64,
        None,
        gate.clone(),
    );
    stream.read(&mut [0; 16]).await.unwrap();
    stream.seek(1).unwrap();
    // Two completed ranges after seek are insufficient to resume prefetch.
    let mut bytes = [0; 15];
    stream.read(&mut bytes).await.unwrap();
    assert_eq!(bytes, [7; 15]);
    assert_eq!(gate.available_permits(), 2);
    assert_eq!(store.count("range:file:Some(16)"), 0);
    stream.read(&mut [0; 8]).await.unwrap();
    assert!(gate.available_permits() < 2);
    drop(stream);
    let permits = tokio::time::timeout(Duration::from_secs(2), gate.acquire_many(2))
        .await
        .unwrap()
        .unwrap();
    drop(permits);
}

#[tokio::test]
async fn failed_read_restores_prefetch_progress_with_cursor() {
    for warm in [false, true] {
        let store = Arc::new(MemoryStore {
            fail_get_at: Some(8),
            get_failures: AtomicUsize::new(1),
            ..Default::default()
        });
        let data: Vec<u8> = (0..32).collect();
        store
            .put("file", Bytes::from(data.clone()), PutHeaders::default())
            .await
            .unwrap();
        let config = ObsConfig {
            readahead_chunk_size: 4,
            readahead_max_chunks: 2,
            ..Default::default()
        };
        let gate = Arc::new(Semaphore::new(2));
        let mut stream =
            ObsInputStream::with_gate(store, &config, "file".into(), 32, None, gate.clone());
        let start = if warm {
            stream.read(&mut [0; 4]).await.unwrap();
            4
        } else {
            0
        };
        assert!(stream.read(&mut [0; 12]).await.is_err());
        assert_eq!(stream.position(), start);
        let mut retry = [0; 4];
        assert_eq!(stream.read(&mut retry).await.unwrap(), 4);
        assert_eq!(retry.as_slice(), &data[start as usize..start as usize + 4]);
        assert_eq!(gate.available_permits(), 2, "failed chunks counted again");
        let mut suffix = vec![0; 32 - start as usize - 4];
        stream.read(&mut suffix).await.unwrap();
        assert_eq!(suffix, data[start as usize + 4..]);
        assert_eq!(gate.available_permits(), 2);
    }
}

#[tokio::test(start_paused = true)]
async fn transient_read_failure_releases_prefetch_and_still_allows_retry() {
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        fail_get_at: Some(12),
        get_failures: AtomicUsize::new(1),
        get_control: Some((16, control.clone())),
        ..Default::default()
    });
    let data: Vec<u8> = (0..32).collect();
    store.put("file", Bytes::from(data.clone()), PutHeaders::default()).await.unwrap();
    let config = ObsConfig {
        readahead_chunk_size: 4,
        readahead_max_chunks: 2,
        ..Default::default()
    };
    let gate = Arc::new(Semaphore::new(2));
    let mut stream = ObsInputStream::with_gate(store.clone(), &config, "file".into(), 32, None, gate.clone());
    let mut prefix = [0; 12];
    stream.read(&mut prefix).await.unwrap();
    assert_eq!(&prefix[..], &data[..12]);
    control.started.notified().await;
    assert!(stream.read(&mut [0; 4]).await.unwrap_err().to_string().contains("injected range failure"));
    assert_eq!(stream.position(), 12);
    tokio::time::timeout(Duration::from_secs(2), control.stopped.notified())
        .await.expect("failed read retained an unrelated stalled prefetch");
    let permits = tokio::time::timeout(Duration::from_secs(2), gate.acquire_many(2))
        .await.unwrap().unwrap();
    drop(permits);
    // Retry on the same stream, after allowing the cancelled range to be fetched anew.
    control.gate.add_permits(1);
    let mut suffix = [0; 20];
    stream.read(&mut suffix).await.unwrap();
    assert_eq!(&suffix[..], &data[12..]);
    assert_eq!(stream.position(), 32);
    assert_eq!(gate.available_permits(), 2);
}

#[tokio::test]
async fn cancelled_read_restores_prefetch_progress_with_cursor() {
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        get_control: Some((8, control.clone())),
        ..Default::default()
    });
    let data: Vec<u8> = (0..32).collect();
    store
        .put("file", Bytes::from(data.clone()), PutHeaders::default())
        .await
        .unwrap();
    let config = ObsConfig {
        readahead_chunk_size: 4,
        readahead_max_chunks: 2,
        ..Default::default()
    };
    let gate = Arc::new(Semaphore::new(2));
    let mut stream =
        ObsInputStream::with_gate(store, &config, "file".into(), 32, None, gate.clone());
    let mut partial = [0; 12];
    {
        let read = stream.read(&mut partial);
        tokio::pin!(read);
        tokio::select! {
            _ = &mut read => panic!("read should block"),
            _ = control.started.notified() => {},
            _ = tokio::time::sleep(Duration::from_secs(2)) => panic!("GET did not start"),
        }
    }
    assert_eq!(stream.position(), 0);
    assert_eq!(&partial[..8], &data[..8]);
    let mut retry = [0; 4];
    stream.read(&mut retry).await.unwrap();
    assert_eq!(&retry, &data[..4]);
    assert_eq!(
        gate.available_permits(),
        2,
        "cancelled chunks counted again"
    );
    control.gate.add_permits(1);
    let mut suffix = [0; 28];
    tokio::time::timeout(Duration::from_secs(2), stream.read(&mut suffix))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&suffix, &data[4..]);
    assert_eq!(gate.available_permits(), 2);
}

#[tokio::test]
async fn large_logical_file_remainder_does_not_stall_small_reads() {
    for length in [1u64 << 32, (1u64 << 32) + 7, u64::MAX] {
        let store = Arc::new(MemoryStore::default());
        store
            .put(
                "file",
                Bytes::from_static(b"abcdefgh"),
                PutHeaders::default(),
            )
            .await
            .unwrap();
        let config = ObsConfig {
            readahead_chunk_size: 4,
            readahead_max_chunks: 0,
            ..Default::default()
        };
        let mut stream = ObsInputStream::new(store.clone(), &config, "file".into(), length, None);
        let mut bytes = [0; 8];
        assert_eq!(stream.read(&mut bytes).await.unwrap(), 8);
        assert_eq!(&bytes, b"abcdefgh");
        assert_eq!(stream.position(), 8);
        assert_eq!(store.count("get:"), 2);
    }
}

#[tokio::test]
async fn reads_near_u64_boundary_preserve_ranges_data_and_eof() {
    for base in [1u64 << 32, u64::MAX - 31] {
        let store = Arc::new(MemoryStore {
            get_base: base,
            ..Default::default()
        });
        let data: Vec<u8> = (0..31).collect();
        store
            .put("file", Bytes::from(data.clone()), PutHeaders::default())
            .await
            .unwrap();
        let config = ObsConfig {
            readahead_chunk_size: 4,
            readahead_max_chunks: 2,
            ..Default::default()
        };
        let mut stream =
            ObsInputStream::new(store.clone(), &config, "file".into(), base + 31, None);
        stream.seek(base).unwrap();
        let mut bytes = [0; 40];
        assert_eq!(stream.read(&mut bytes).await.unwrap(), 31);
        assert_eq!(&bytes[..31], data.as_slice());
        assert_eq!(stream.position(), base + 31);
        assert_eq!(store.count("get:"), 8);
        assert_eq!(stream.read(&mut bytes).await.unwrap(), 0);
        stream.seek(u64::MAX).unwrap();
        assert_eq!(stream.read(&mut bytes).await.unwrap(), 0);
    }
}

fn duplicate_object_page(key: &str, conflict: bool) -> ListPage {
    ListPage {
        contents: vec![
            ListEntry {
                key: key.into(),
                size: 1,
                etag: Some("first".into()),
                ..Default::default()
            },
            ListEntry {
                key: key.into(),
                size: if conflict { 2 } else { 1 },
                etag: Some(if conflict { "second" } else { "first" }.into()),
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

#[tokio::test]
async fn listing_rejects_duplicate_objects_even_with_matching_metadata() {
    for conflict in [false, true] {
        let store = MemoryStore::default();
        store
            .list_pages
            .lock()
            .unwrap()
            .push_back(duplicate_object_page("dir/file", conflict));
        let result = obs_rust_core::listing::list_all(&store, "dir/", "", 1000).await;
        assert!(matches!(result, Err(ObsError::Io(ref e)) if e.contains("duplicate object keys")));
        assert_eq!(store.count("list:"), 1);
    }
}

#[tokio::test]
async fn recursive_delete_rejects_duplicate_objects_without_mutation() {
    for pfs in [false, true] {
        let store = Arc::new(MemoryStore {
            pfs,
            directory_conflict: pfs,
            ..Default::default()
        });
        for key in ["dir/", "dir/file", "outside"] {
            store.put_empty(key).await.unwrap();
        }
        let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
        fs.get_file_status("/dir").await.unwrap();
        let before = store.objects.lock().unwrap().clone();
        store
            .list_pages
            .lock()
            .unwrap()
            .push_back(duplicate_object_page("dir/file", true));
        assert!(fs.delete("/dir", true).await.is_err());
        assert_eq!(*store.objects.lock().unwrap(), before);
        assert_eq!(store.count("delete:dir/file"), 0);
    }
}

#[tokio::test]
async fn directory_rename_rejects_later_page_duplicates_before_mutation() {
    let store = Arc::new(MemoryStore::default());
    for key in ["source/", "source/first", "source/file"] {
        store.put_empty(key).await.unwrap();
    }
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    fs.get_file_status("/source").await.unwrap();
    assert!(!fs.exists("/target").await.unwrap());
    let before = store.objects.lock().unwrap().clone();
    store.list_pages.lock().unwrap().extend([
        ListPage {
            contents: vec![ListEntry {
                key: "source/first".into(),
                ..Default::default()
            }],
            is_truncated: true,
            next_marker: Some("source/first".into()),
            ..Default::default()
        },
        duplicate_object_page("source/file", true),
    ]);
    assert!(fs.rename("/source", "/target", false).await.is_err());
    assert_eq!(*store.objects.lock().unwrap(), before);
    assert_eq!(store.count("copy:"), 0);
    assert_eq!(store.count("delete:"), 0);
}

fn overlapping_mutation_pages(prefix: &str, conflict: bool) -> Vec<ListPage> {
    [("file", 1), ("other", 2), ("file", if conflict { 99 } else { 1 })]
        .into_iter().enumerate().map(|(index, (name, size))| ListPage {
            contents: vec![ListEntry {
                key: format!("{prefix}/{name}"), size, ..Default::default()
            }],
            is_truncated: index < 2,
            next_marker: (index < 2).then(|| format!("page-{index}")),
            ..Default::default()
        }).collect()
}

#[tokio::test]
async fn recursive_delete_rejects_cross_page_duplicates_before_child_mutation() {
    for pfs in [false, true] {
        for conflict in [false, true] {
            let store = Arc::new(MemoryStore {
                pfs, directory_conflict: pfs, ..Default::default()
            });
            for key in ["dir/", "dir/file", "dir/other", "outside"] {
                store.put_empty(key).await.unwrap();
            }
            let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
            fs.get_file_status("/dir").await.unwrap();
            let before = store.objects.lock().unwrap().clone();
            store.list_pages.lock().unwrap().extend(overlapping_mutation_pages("dir", conflict));
            assert!(matches!(fs.delete("/dir", true).await,
                Err(ObsError::Io(ref message)) if message.contains("duplicate object keys")));
            assert_eq!(*store.objects.lock().unwrap(), before);
            assert_eq!(store.count("delete:dir/file"), 0);
            assert_eq!(store.count("delete:dir/other"), 0);
        }
    }
}

#[tokio::test]
async fn directory_rename_rejects_cross_page_duplicates_before_copy() {
    for conflict in [false, true] {
        let store = Arc::new(MemoryStore::default());
        for key in ["source/", "source/file", "source/other"] {
            store.put_empty(key).await.unwrap();
        }
        let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
        fs.get_file_status("/source").await.unwrap();
        assert!(!fs.exists("/target").await.unwrap());
        let before = store.objects.lock().unwrap().clone();
        store.list_pages.lock().unwrap().extend(overlapping_mutation_pages("source", conflict));
        assert!(matches!(fs.rename("/source", "/target", false).await,
            Err(ObsError::Io(ref message)) if message.contains("duplicate object keys")));
        assert_eq!(*store.objects.lock().unwrap(), before);
        assert_eq!(store.count("copy:"), 0);
        assert_eq!(store.count("delete:"), 0);
    }
}

#[tokio::test]
async fn pager_fetches_on_demand_and_distinguishes_empty_page_from_eof() {
    use obs_rust_core::listing::ListPager;
    let store = MemoryStore::default();
    store.list_pages.lock().unwrap().extend([
        ListPage {
            is_truncated: true,
            next_marker: Some("a".into()),
            ..Default::default()
        },
        ListPage {
            contents: vec![ListEntry {
                key: "b".into(),
                ..Default::default()
            }],
            ..Default::default()
        },
    ]);
    let mut pager = ListPager::new(&store, "", "", 3);
    assert_eq!(store.count("list:"), 0);
    assert!(pager.next_page().await.unwrap().unwrap().is_empty());
    assert_eq!(store.count("list:"), 1);
    assert_eq!(pager.next_page().await.unwrap().unwrap()[0].key, "b");
    assert!(pager.next_page().await.unwrap().is_none());
    assert!(pager.next_page().await.unwrap().is_none());
    assert_eq!(store.count("list:"), 2);
}

#[tokio::test]
async fn pager_validation_failure_is_sticky_without_more_requests() {
    use obs_rust_core::listing::ListPager;
    let store = MemoryStore::default();
    store
        .list_pages
        .lock()
        .unwrap()
        .push_back(duplicate_object_page("dir/file", true));
    let mut pager = ListPager::new(&store, "dir/", "", 3);
    assert!(pager.next_page().await.is_err());
    assert!(pager.next_page().await.is_err());
    assert_eq!(store.count("list:"), 1);
}

#[tokio::test]
async fn status_cursor_deduplicates_across_pages_and_stops_at_eof() {
    let store = Arc::new(MemoryStore::default());
    for (keys, marker) in [(vec!["dir/a"], Some("a")), (vec!["dir/a", "dir/b"], None)] {
        store.list_pages.lock().unwrap().push_back(ListPage {
            contents: keys
                .into_iter()
                .map(|key| ListEntry {
                    key: key.into(),
                    ..Default::default()
                })
                .collect(),
            is_truncated: marker.is_some(),
            next_marker: marker.map(str::to_owned),
            ..Default::default()
        });
    }
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    let mut cursor = obs_rust_core::listing::StatusCursor::new("/dir".into());
    assert_eq!(
        cursor.next_page(&fs).await.unwrap().entries[0].path,
        "/dir/a"
    );
    let last = cursor.next_page(&fs).await.unwrap();
    assert_eq!(last.entries.len(), 1);
    assert_eq!(last.entries[0].path, "/dir/b");
    assert!(cursor.next_page(&fs).await.unwrap().entries.is_empty());
    assert_eq!(store.count("list:"), 2);
}

#[tokio::test]
async fn file_cursor_keeps_continuation_after_directory_only_page() {
    let store = Arc::new(MemoryStore::default());
    store.list_pages.lock().unwrap().extend([
        ListPage {
            contents: vec![ListEntry { key: "tree/empty/".into(), ..Default::default() }],
            is_truncated: true, next_marker: Some("tree/empty/".into()), ..Default::default()
        },
        ListPage {
            contents: vec![ListEntry { key: "tree/sub/file".into(), size: 7, ..Default::default() }],
            ..Default::default()
        },
    ]);
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    let mut cursor = obs_rust_core::listing::StatusCursor::files("/tree".into(), true);
    let first = cursor.next_page(&fs).await.unwrap();
    assert!(first.entries.is_empty());
    assert_eq!(first.next_marker.as_deref(), Some("tree/empty/"));
    let second = cursor.next_page(&fs).await.unwrap();
    assert_eq!(second.entries[0].path, "/tree/sub/file");
    assert_eq!(second.entries[0].length, 7);
    assert!(cursor.next_page(&fs).await.unwrap().entries.is_empty());
    assert_eq!(store.count("list:"), 2);
}

#[tokio::test]
async fn status_cursor_rejects_non_adjacent_cycles_and_preserves_error() {
    let store = Arc::new(MemoryStore::default());
    for marker in ["a", "b", "a"] {
        store.list_pages.lock().unwrap().push_back(ListPage {
            is_truncated: true,
            next_marker: Some(marker.into()),
            ..Default::default()
        });
    }
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    let mut cursor = obs_rust_core::listing::StatusCursor::new("/dir".into());
    cursor.next_page(&fs).await.unwrap();
    cursor.next_page(&fs).await.unwrap();
    let first = cursor.next_page(&fs).await.unwrap_err().to_string();
    assert!(first.contains("marker"));
    assert_eq!(first, cursor.next_page(&fs).await.unwrap_err().to_string());
    assert_eq!(store.count("list:"), 3);
}

#[tokio::test]
async fn stateless_status_pages_resume_and_reject_stalled_markers() {
    let store = Arc::new(MemoryStore::default());
    for (name, marker) in [("dir/a", "first"), ("dir/b", "first")] {
        store.list_pages.lock().unwrap().push_back(ListPage {
            contents: vec![ListEntry {
                key: name.into(),
                size: 7,
                ..Default::default()
            }],
            is_truncated: true,
            next_marker: Some(marker.into()),
            ..Default::default()
        });
    }
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    let first = fs.list_status_page("/dir", None).await.unwrap();
    assert_eq!(first.entries.len(), 1);
    assert_eq!(first.entries[0].path, "/dir/a");
    assert_eq!(first.next_marker.as_deref(), Some("first"));
    assert_eq!(store.count("list:"), 1);
    assert!(fs
        .list_status_page("/dir", first.next_marker.as_deref())
        .await
        .is_err());
    assert_eq!(
        *store.list_markers.lock().unwrap(),
        vec![None, Some("first".into())]
    );
    assert_eq!(store.count("head:"), 0);
}

#[tokio::test]
async fn cancelled_page_fetch_does_not_advance_cursor() {
    use obs_rust_core::listing::ListPager;
    let control = AppendControl::blocked();
    let store = MemoryStore {
        list_control: Some(control.clone()),
        ..Default::default()
    };
    store.list_pages.lock().unwrap().extend([
        ListPage {
            is_truncated: true,
            next_marker: Some("a".into()),
            ..Default::default()
        },
        ListPage {
            contents: vec![ListEntry {
                key: "b".into(),
                ..Default::default()
            }],
            ..Default::default()
        },
    ]);
    let mut pager = ListPager::new(&store, "", "", 3);
    control.gate.add_permits(1);
    pager.next_page().await.unwrap();
    control.started.notified().await; // consume notification from first page
    {
        let fetch = pager.next_page();
        tokio::pin!(fetch);
        tokio::select! { _ = &mut fetch => panic!("must block"), _ = control.started.notified() => {} }
    }
    control.gate.add_permits(1);
    assert_eq!(pager.next_page().await.unwrap().unwrap()[0].key, "b");
    assert_eq!(
        *store.list_markers.lock().unwrap(),
        vec![None, Some("a".into()), Some("a".into())]
    );
}

#[tokio::test]
async fn content_summary_consumes_many_pages_and_counts_implicit_directories() {
    let store = Arc::new(MemoryStore::default());
    for i in 0..100 {
        store.list_pages.lock().unwrap().push_back(ListPage {
            contents: vec![ListEntry {
                key: format!("dir/sub/file{i:03}"),
                size: 7,
                ..Default::default()
            }],
            is_truncated: i < 99,
            next_marker: Some(format!("marker{i}")),
            ..Default::default()
        });
    }
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    let summary = fs.get_content_summary("/dir").await.unwrap();
    assert_eq!(summary.file_count, 100);
    assert_eq!(summary.directory_count, 2);
    assert_eq!(summary.length, 700);
    assert_eq!(summary.space_consumed, 700);
    assert_eq!(store.count("list:"), 100);
}

#[tokio::test]
async fn content_summary_rejects_cross_page_object_duplicates() {
    for pfs in [false, true] {
        for case in ["same", "size", "kind"] {
            let store = Arc::new(MemoryStore { pfs, ..Default::default() });
            let mut pages = overlapping_mutation_pages("dir", case == "size");
            if case == "kind" { pages[0].contents[0].mode = Some(0o040755); }
            // The error must stop enumeration before this fourth page.
            pages[2].is_truncated = true;
            pages[2].next_marker = Some("page-2".into());
            pages.push(ListPage::default());
            store.list_pages.lock().unwrap().extend(pages);
            let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
            assert!(matches!(fs.get_content_summary("/dir").await,
                Err(ObsError::Io(ref message)) if message.contains("duplicate object keys")),
                "pfs={pfs}, case={case}");
            assert_eq!(store.count("list:"), 3);
            assert_eq!(store.count("head:"), 0);
        }
    }
}

#[tokio::test]
async fn content_summary_allows_repeated_common_prefixes() {
    let store = Arc::new(MemoryStore::default());
    for (index, name, size) in [(0, "a", 3), (1, "b", 5)] {
        store.list_pages.lock().unwrap().push_back(ListPage {
            contents: vec![ListEntry {
                key: format!("dir/sub/{name}"), size, ..Default::default()
            }],
            common_prefixes: vec!["dir/sub/".into()],
            is_truncated: index == 0,
            next_marker: (index == 0).then(|| "next".into()),
            ..Default::default()
        });
    }
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    let summary = fs.get_content_summary("/dir").await.unwrap();
    assert_eq!(summary.file_count, 2);
    assert_eq!(summary.directory_count, 2);
    assert_eq!(summary.length, 8);
    assert_eq!(summary.space_consumed, 8);
    assert_eq!(store.count("list:"), 2);
    assert_eq!(store.count("head:"), 0);
}

#[tokio::test]
async fn content_summary_rejects_size_overflow_across_pages() {
    let store = Arc::new(MemoryStore::default());
    for (key, size, more) in [("dir/a", u64::MAX, true), ("dir/b", 1, false)] {
        store.list_pages.lock().unwrap().push_back(ListPage {
            contents: vec![ListEntry {
                key: key.into(),
                size,
                ..Default::default()
            }],
            is_truncated: more,
            next_marker: Some(key.into()),
            ..Default::default()
        });
    }
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store);
    assert!(
        matches!(fs.get_content_summary("/dir").await, Err(ObsError::Io(ref e)) if e.contains("overflow"))
    );
}

async fn failed_part_while_global_gate_busy(closing: bool) {
    const PART: usize = 5 * 1024 * 1024;
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        fail_part: true,
        part_control: Some((1, control.clone())),
        ..Default::default()
    });
    let config = ObsConfig {
        multipart_threshold: 1,
        multipart_size: PART as u64,
        upload_active_blocks: 2,
        ..Default::default()
    };
    let gate = Arc::new(Semaphore::new(2));
    let mut stream = ObsOutputStream::new(store.clone(), &config, "file".into(), gate.clone());
    stream.write(&vec![42; PART + 1]).await.unwrap();
    control.started.notified().await;
    let held = gate.clone().acquire_owned().await.unwrap();
    let competing = gate.clone().acquire_owned();
    tokio::pin!(competing);
    assert!(futures::poll!(competing.as_mut()).is_pending());
    let operation = async move {
        if closing {
            stream.close().await.map(|_| ())
        } else {
            let result = stream.write(&vec![43; PART]).await;
            let cleanup = stream.close().await.unwrap_err();
            assert!(cleanup.to_string().contains("injected part failure"));
            result.map(|_| ())
        }
    };
    tokio::pin!(operation);
    assert!(futures::poll!(operation.as_mut()).is_pending());
    control.gate.add_permits(1);
    // This waiter was queued first and takes the failed part's released slot.
    let competing = competing.await.unwrap();
    assert_eq!(gate.available_permits(), 0);
    let error = tokio::time::timeout(Duration::from_secs(2), operation.as_mut())
        .await
        .expect("failed upload remained queued behind unrelated transfers")
        .unwrap_err();
    assert!(error.to_string().contains("injected part failure"));
    assert_eq!(store.count("part:"), 1);
    assert_eq!(store.count("complete"), 0);
    assert_eq!(store.count("abort"), 1);
    assert!(!store.objects.lock().unwrap().contains_key("file"));
    drop(held);
    drop(competing);
    assert_eq!(gate.available_permits(), 2);
}

#[tokio::test(start_paused = true)]
async fn failed_part_interrupts_write_waiting_for_global_permit() {
    failed_part_while_global_gate_busy(false).await;
}

#[tokio::test(start_paused = true)]
async fn failed_part_interrupts_close_waiting_for_global_permit() {
    failed_part_while_global_gate_busy(true).await;
}

#[tokio::test(start_paused = true)]
async fn cancelling_permit_wait_keeps_part_owned_for_drop_cleanup() {
    const PART: usize = 5 * 1024 * 1024;
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        part_control: Some((1, control.clone())),
        ..Default::default()
    });
    let config = ObsConfig {
        multipart_threshold: 1,
        multipart_size: PART as u64,
        upload_active_blocks: 2,
        ..Default::default()
    };
    let gate = Arc::new(Semaphore::new(2));
    let mut stream = ObsOutputStream::new(store.clone(), &config, "file".into(), gate.clone());
    stream.write(&vec![42; PART]).await.unwrap();
    control.started.notified().await;
    let held = gate.clone().acquire_owned().await.unwrap();
    let data = vec![43; PART];
    let mut write = Box::pin(stream.write(&data));
    assert!(futures::poll!(write.as_mut()).is_pending());
    drop(write);
    assert!(stream.flush().await.unwrap_err().to_string().contains("cancelled"));
    tokio::time::timeout(Duration::from_secs(2), control.stopped.notified())
        .await.expect("cancelled stream retained a running part until drop");
    assert_eq!(gate.available_permits(), 1);
    drop(stream);
    tokio::time::timeout(Duration::from_secs(2), async {
        while store.count("abort") == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }).await.unwrap();
    assert_eq!(gate.available_permits(), 1);
    drop(held);
    assert_eq!(gate.available_permits(), 2);
    assert_eq!(store.count("part:"), 0);
    assert_eq!(store.count("complete"), 0);
    assert_eq!(store.count("abort"), 1);
}

#[tokio::test(start_paused = true)]
async fn retained_failed_upload_cancels_siblings_before_close() {
    const PART: usize = 5 * 1024 * 1024;
    for observe_with_write in [false, true] {
        let control = AppendControl::blocked();
        let store = Arc::new(MemoryStore {
            fail_part: true,
            part_control: Some((2, control.clone())),
            ..Default::default()
        });
        let config = ObsConfig {
            multipart_threshold: 1,
            multipart_size: PART as u64,
            upload_active_blocks: 2,
            ..Default::default()
        };
        let gate = Arc::new(Semaphore::new(2));
        let mut stream = ObsOutputStream::new(store.clone(), &config, "file".into(), gate.clone());
        stream.write(&vec![42; 2 * PART + 17]).await.unwrap();
        control.started.notified().await;
        tokio::time::advance(Duration::from_millis(30)).await;
        tokio::task::yield_now().await;
        let error = if observe_with_write {
            stream.write(b"next").await.unwrap_err()
        } else {
            stream.flush().await.unwrap_err()
        };
        assert!(error.to_string().contains("injected part failure"));
        tokio::time::timeout(Duration::from_secs(2), control.stopped.notified())
            .await.expect("failed stream retained a running sibling until close");
        assert_eq!(gate.available_permits(), 2);
        assert!(stream.flush().await.unwrap_err().to_string().contains("injected part failure"));
        assert!(stream.close().await.unwrap_err().to_string().contains("injected part failure"));
        assert_eq!(store.count("complete"), 0);
        assert_eq!(store.count("abort"), 1);
    }
}

async fn append_later_failure_interrupts_stalled_first(write_more: bool) {
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        pfs: true,
        append_control: Some(control.clone()),
        append_control_position: Some(0),
        append_failures: AtomicUsize::new(1),
        ..Default::default()
    });
    store.put_empty("file").await.unwrap();
    let config = ObsConfig {
        pfs_append_chunk_size: 4,
        pfs_append_total_bytes: 12,
        pfs_append_concurrency: 2,
        ..Default::default()
    };
    let mut stream = PfsAppendStream::new(store.clone(), &config, "file".into(), 0);
    stream.write(b"abcdefgh").await.unwrap();
    {
        let operation = async {
            if write_more { stream.write(b"ijkl").await.map(|_| ()) }
            else { stream.flush().await }
        };
        tokio::pin!(operation);
        assert!(futures::poll!(operation.as_mut()).is_pending());
        control.started.notified().await;
        let error = tokio::time::timeout(Duration::from_secs(2), &mut operation)
            .await.expect("later append failure hidden by stalled first request").unwrap_err();
        assert!(error.to_string().contains("injected append failure"));
    }
    let error = tokio::time::timeout(Duration::from_secs(2), stream.flush())
        .await.unwrap().unwrap_err();
    assert!(error.to_string().contains("injected append failure"));
    assert_eq!(store.append_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(store.count("append:file:"), 2);
    assert!(store.objects.lock().unwrap()["file"].is_empty());
    assert!(stream.write(b"more").await.is_err());
}

#[tokio::test(start_paused = true)]
async fn append_flush_observes_later_failure_and_cancels_stalled_first() {
    append_later_failure_interrupts_stalled_first(false).await;
}

#[tokio::test(start_paused = true)]
async fn cancelled_append_write_releases_budget_when_next_write_rejects() {
    for concurrency in [1, 2] {
        let control = AppendControl::blocked();
        let store = Arc::new(MemoryStore {
            pfs: true,
            append_control: Some(control.clone()),
            append_control_position: Some(0),
            ..Default::default()
        });
        store.put_empty("a").await.unwrap();
        store.put("b", Bytes::from_static(b"seed"), PutHeaders::default()).await.unwrap();
        let config = ObsConfig {
            pfs_append_chunk_size: 4,
            pfs_append_total_bytes: 4,
            pfs_append_concurrency: concurrency,
            ..Default::default()
        };
        let fs = ObsFileSystem::with_store(config, store.clone());
        let mut a = fs.append_stream("/a", 0).await.unwrap();
        let mut write = Box::pin(a.write(b"abcdefgh"));
        assert!(futures::poll!(write.as_mut()).is_pending());
        control.started.notified().await;
        drop(write);
        let error = a.write(b"").await.unwrap_err();
        assert!(error.to_string().contains("cancelled"));
        tokio::time::timeout(Duration::from_secs(2), control.stopped.notified())
            .await.expect("rejected write retained the previous append's byte lease");
        assert_eq!(store.append_bytes.load(Ordering::SeqCst), 0);
        // Keep a alive: another stream must be able to use its former budget.
        let mut b = fs.append_stream("/b", 4).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), b.write(b"next"))
            .await.unwrap().unwrap();
        b.close().await.unwrap();
        assert_eq!(&store.objects.lock().unwrap()["b"][..], b"seednext");
        assert!(a.flush().await.unwrap_err().to_string().contains("cancelled"));
        assert!(a.close().await.unwrap_err().to_string().contains("cancelled"));
        assert_eq!(store.count("append:a:"), 1);
        assert!(store.objects.lock().unwrap()["a"].is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn append_concurrency_wait_observes_later_failure() {
    append_later_failure_interrupts_stalled_first(true).await;
}

#[tokio::test(start_paused = true)]
async fn append_failure_interrupts_shared_byte_budget_wait() {
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        pfs: true,
        append_control: Some(control.clone()),
        append_control_position: Some(0),
        append_failures: AtomicUsize::new(1),
        ..Default::default()
    });
    for key in ["a", "b", "c"] { store.put_empty(key).await.unwrap(); }
    let config = ObsConfig {
        pfs_append_chunk_size: 4,
        pfs_append_total_bytes: 8,
        pfs_append_concurrency: 4,
        ..Default::default()
    };
    let fs = ObsFileSystem::with_store(config, store.clone());
    let mut a = fs.append_stream("/a", 0).await.unwrap();
    let mut b = fs.append_stream("/b", 0).await.unwrap();
    a.write(b"abcdefgh").await.unwrap();
    {
        let competing = b.write(b"1234");
        tokio::pin!(competing);
        assert!(futures::poll!(competing.as_mut()).is_pending());
        let write = a.write(b"ijkl");
        tokio::pin!(write);
        assert!(futures::poll!(write.as_mut()).is_pending());
        control.started.notified().await;
        // The other stream queues first for the failing task's four bytes.
        competing.await.unwrap();
        let error = tokio::time::timeout(Duration::from_secs(2), &mut write)
            .await.expect("append failure hidden by another stream's budget lease").unwrap_err();
        assert!(error.to_string().contains("injected append failure"));
    }
    drop(a);
    drop(b);
    tokio::time::timeout(Duration::from_secs(2), async {
        while store.append_bytes.load(Ordering::SeqCst) != 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }).await.unwrap();
    assert_eq!(store.count("append:a:8"), 0);
    // Both chunks must acquire the shared eight-byte budget before the blocked
    // first request is released, proving that the cancelled leases were freed.
    let mut c = fs.append_stream("/c", 0).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), c.write(b"verified"))
        .await.unwrap().unwrap();
    control.gate.add_permits(1);
    c.close().await.unwrap();
    assert_eq!(&store.objects.lock().unwrap()["c"][..], b"verified");
}

#[tokio::test(start_paused = true)]
async fn append_flush_cancel_after_out_of_order_success_can_resume() {
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        pfs: true,
        append_control: Some(control.clone()),
        append_control_position: Some(0),
        ..Default::default()
    });
    store.put_empty("file").await.unwrap();
    let config = ObsConfig {
        pfs_append_chunk_size: 4,
        pfs_append_total_bytes: 8,
        pfs_append_concurrency: 2,
        ..Default::default()
    };
    let mut stream = PfsAppendStream::new(store.clone(), &config, "file".into(), 0);
    stream.write(b"abcdefgh").await.unwrap();
    let mut flush = Box::pin(stream.flush());
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::select! {
            _ = &mut flush => panic!("first append must remain blocked"),
            _ = async {
                loop {
                    let finished = store.objects.lock().unwrap()["file"].len() == 8;
                    if finished { break; }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            } => {}
        }
    }).await.unwrap();
    // Consume the later completion, then cancel while the first is pending.
    assert!(futures::poll!(flush.as_mut()).is_pending());
    drop(flush);
    assert_eq!(&store.objects.lock().unwrap()["file"][4..], b"efgh");
    control.gate.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), stream.flush()).await.unwrap().unwrap();
    stream.write(b"ijkl").await.unwrap();
    stream.close().await.unwrap();
    assert_eq!(&store.objects.lock().unwrap()["file"][..], b"abcdefghijkl");
    assert_eq!(store.append_bytes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn multipart_lookahead_preserves_buffered_prefix_and_short_tail() {
    const MIB: usize = 1024 * 1024;
    for (prefix, suffix) in [(0, 12 * MIB), (7 * MIB, 5 * MIB), (7 * MIB, 5 * MIB + 11)] {
        let store = Arc::new(MemoryStore::default());
        let config = ObsConfig {
            multipart_size: (5 * MIB) as u64,
            multipart_threshold: (12 * MIB) as u64,
            ..Default::default()
        };
        let mut stream = ObsOutputStream::new(
            store.clone(), &config, "file".into(), Arc::new(Semaphore::new(1)),
        );
        let expected: Vec<u8> = (0..prefix + suffix).map(|i| (i % 251) as u8).collect();
        assert_eq!(stream.write(&expected[..prefix]).await.unwrap(), prefix);
        assert_eq!(store.count("init"), 0);
        assert_eq!(stream.write(&expected[prefix..]).await.unwrap(), suffix);
        let meta = stream.close().await.unwrap();
        assert_eq!(meta.content_length, expected.len() as u64);
        assert_eq!(&store.objects.lock().unwrap()["file"][..], expected);
        assert_eq!(store.count("init"), 1);
        assert_eq!(store.count("complete"), 1);
        assert_eq!(store.count("part:"), 3);
        let parts = store.parts.lock().unwrap();
        assert_eq!(parts[&1].len(), 5 * MIB);
        assert_eq!(parts[&2].len(), 5 * MIB);
        assert_eq!(parts[&3].len(), expected.len() - 10 * MIB);
    }
}


#[tokio::test]
async fn pfs_rename_waits_for_queued_append_and_retargets_future_writes() {
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        append_control: Some(control.clone()),
        append_control_position: Some(0),
        pfs: true,
        ..Default::default()
    });
    store.put_empty("file").await.unwrap();
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    let mut stream = fs.append_stream("/file", 0).await.unwrap();
    stream.write(b"abc").await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), control.started.notified()).await.unwrap();
    let rename = fs.rename("/file", "/moved", false);
    tokio::pin!(rename);
    assert!(futures::poll!(&mut rename).is_pending());
    assert_eq!(store.count("rename:"), 0);
    control.gate.add_permits(1);
    assert!(tokio::time::timeout(Duration::from_secs(2), &mut rename).await.unwrap().unwrap());
    stream.flush().await.unwrap();
    assert_eq!(fs.get_file_status("/moved").await.unwrap().length, 3);
    stream.write(b"def").await.unwrap();
    stream.close().await.unwrap();
    assert_eq!(fs.get_file_status("/moved").await.unwrap().length, 6);
    assert_eq!(&store.objects.lock().unwrap()["moved"][..], b"abcdef");
    assert!(!store.objects.lock().unwrap().contains_key("file"));
}

#[tokio::test]
async fn append_completion_invalidates_original_and_retargeted_cache_entries() {
    let store = Arc::new(MemoryStore { pfs: true, ..Default::default() });
    store.put_empty("file").await.unwrap();
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    let mut stream = fs.append_stream("/file", 0).await.unwrap();
    assert_eq!(fs.get_file_status("/file").await.unwrap().length, 0);
    stream.write(b"a").await.unwrap();
    stream.flush().await.unwrap();
    assert_eq!(fs.get_file_status("/file").await.unwrap().length, 1);
    assert!(fs.rename("/file", "/moved", false).await.unwrap());
    assert!(fs.get_file_status("/file").await.unwrap_err().is_not_found());
    assert_eq!(fs.get_file_status("/moved").await.unwrap().length, 1);
    // Simulate an external recreation while the original negative entry is cached.
    store.put_empty("file").await.unwrap();
    stream.write(b"b").await.unwrap();
    stream.flush().await.unwrap();
    assert_eq!(fs.get_file_status("/file").await.unwrap().length, 0);
    assert_eq!(fs.get_file_status("/moved").await.unwrap().length, 2);
    stream.close().await.unwrap();
}

#[tokio::test]
async fn pfs_rename_replacement_invalidates_old_destination_stream() {
    let store = Arc::new(MemoryStore { pfs: true, ..Default::default() });
    store.put_empty("source").await.unwrap();
    store.put_empty("destination").await.unwrap();
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    let mut source = fs.append_stream("/source", 0).await.unwrap();
    let mut destination = fs.append_stream("/destination", 0).await.unwrap();
    assert!(fs.rename("/source", "/destination", true).await.unwrap());
    assert!(destination.write(b"wrong").await.is_err());
    assert!(destination.close().await.is_err());
    source.write(b"right").await.unwrap();
    source.close().await.unwrap();
    assert_eq!(&store.objects.lock().unwrap()["destination"][..], b"right");
    assert_eq!(store.count("append:"), 1);
}

#[tokio::test]
async fn cancelled_committed_pfs_rename_blocks_stale_append_stream() {
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        rename_commit_control: Some(control.clone()), pfs: true, ..Default::default()
    });
    store.put_empty("source").await.unwrap();
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    let mut stream = fs.append_stream("/source", 0).await.unwrap();
    {
        let rename = fs.rename("/source", "/destination", false);
        tokio::pin!(rename);
        assert!(futures::poll!(&mut rename).is_pending());
        assert!(store.objects.lock().unwrap().contains_key("destination"));
    }
    // The server committed, but the caller never received a result.
    assert!(stream.write(b"unsafe").await.is_err());
    assert!(stream.close().await.is_err());
    assert_eq!(store.count("append:"), 0);
    let mut reopened = fs.append_stream("/destination", 0).await.unwrap();
    reopened.write(b"safe").await.unwrap();
    reopened.close().await.unwrap();
    assert_eq!(&store.objects.lock().unwrap()["destination"][..], b"safe");
}


#[tokio::test]
async fn unrelated_pfs_rename_does_not_wait_for_blocked_append() {
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        append_control: Some(control.clone()), pfs: true, ..Default::default()
    });
    store.put_empty("busy").await.unwrap();
    store.put_empty("other").await.unwrap();
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    let mut stream = fs.append_stream("/busy", 0).await.unwrap();
    stream.write(b"data").await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), control.started.notified()).await.unwrap();
    let rename = fs.rename("/other", "/moved", false);
    tokio::pin!(rename);
    assert!(matches!(futures::poll!(&mut rename), std::task::Poll::Ready(Ok(true))));
    assert_eq!(store.count("rename:"), 1);
    control.gate.add_permits(1);
    stream.close().await.unwrap();
    assert_eq!(&store.objects.lock().unwrap()["busy"][..], b"data");
}

#[tokio::test]
async fn slow_pfs_rename_does_not_block_unrelated_append_or_rename() {
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        rename_commit_control: Some(control.clone()), pfs: true, ..Default::default()
    });
    for key in ["a", "ab", "x"] { store.put_empty(key).await.unwrap(); }
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    let mut affected = fs.append_stream("/a", 0).await.unwrap();
    let mut other = fs.append_stream("/ab", 0).await.unwrap();
    let mut first = Box::pin(fs.rename("/a", "/b", false));
    assert!(futures::poll!(first.as_mut()).is_pending());
    assert_eq!(store.count("rename:"), 1);
    let mut affected_write = Box::pin(affected.write(b"after"));
    assert!(futures::poll!(affected_write.as_mut()).is_pending());
    // Both new registrations and writes to existing unrelated streams proceed.
    let mut opened = tokio::time::timeout(Duration::from_secs(2), fs.append_stream("/x", 0)).await.unwrap().unwrap();
    tokio::time::timeout(Duration::from_secs(2), other.write(b"other")).await.unwrap().unwrap();
    tokio::time::timeout(Duration::from_secs(2), other.close()).await.unwrap().unwrap();
    let mut second = Box::pin(fs.rename("/x", "/y", false));
    assert!(futures::poll!(second.as_mut()).is_pending());
    assert_eq!(store.count("rename:"), 2);
    control.gate.add_permits(2);
    assert!(first.await.unwrap());
    assert!(second.await.unwrap());
    affected_write.await.unwrap();
    affected.close().await.unwrap();
    opened.write(b"new").await.unwrap();
    opened.close().await.unwrap();
    let objects = store.objects.lock().unwrap();
    assert_eq!(&objects["ab"][..], b"other");
    assert_eq!(&objects["b"][..], b"after");
    assert_eq!(&objects["y"][..], b"new");
}

#[path = "support/append_latency.rs"]
mod append_latency;

#[tokio::test]
async fn append_cancelled_flush_retains_later_task_after_first_completed() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let control = AppendControl::blocked();
        let store = Arc::new(MemoryStore { pfs: true, append_control: Some(control.clone()),
            append_control_position: Some(4), ..Default::default() });
        store.put_empty("file").await.unwrap();
        let config = ObsConfig { pfs_append_chunk_size: 4, pfs_append_total_bytes: 12,
            pfs_append_concurrency: 3, ..Default::default() };
        let mut stream = PfsAppendStream::new(store.clone(), &config, "file".into(), 0);
        stream.write(b"abcdefgh").await.unwrap();
        control.started.notified().await;
        while store.objects.lock().unwrap()["file"].len() != 4 { tokio::task::yield_now().await; }
        let mut flush = Box::pin(stream.flush());
        assert!(futures::poll!(flush.as_mut()).is_pending());
        drop(flush); // The first task has completed, but the later task is retained.
        stream.write(b"ijkl").await.unwrap();
        while store.objects.lock().unwrap()["file"].len() != 12 { tokio::task::yield_now().await; }
        let mut flush = Box::pin(stream.flush());
        assert!(futures::poll!(flush.as_mut()).is_pending());
        drop(flush); // Consume another completion without losing the blocked task.
        control.gate.add_permits(1);
        stream.close().await.unwrap();
        assert_eq!(store.objects.lock().unwrap()["file"].as_ref(), b"abcdefghijkl");
        assert_eq!(store.append_bytes.load(Ordering::SeqCst), 0);
    }).await.expect("cancelled completion wait lost a task or its wake-up");
}


#[tokio::test]
async fn pfs_append_waiter_runs_before_later_rename_request() {
    let control = AppendControl::blocked();
    let store = Arc::new(MemoryStore {
        rename_commit_control: Some(control.clone()), pfs: true, ..Default::default()
    });
    store.put_empty("a").await.unwrap();
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    let mut stream = fs.append_stream("/a", 0).await.unwrap();
    let mut first = Box::pin(fs.rename("/a", "/b", false));
    assert!(futures::poll!(first.as_mut()).is_pending());
    let mut write = Box::pin(stream.write(b"data"));
    assert!(futures::poll!(write.as_mut()).is_pending());
    control.gate.add_permits(1);
    assert!(first.await.unwrap());
    let mut second = Box::pin(fs.rename("/b", "/c", false));
    assert!(futures::poll!(second.as_mut()).is_pending());
    assert_eq!(store.count("rename:"), 1);
    assert_eq!(write.await.unwrap(), 4);
    stream.flush().await.unwrap();
    assert_eq!(&store.objects.lock().unwrap()["b"][..], b"data");
    control.gate.add_permits(1);
    assert!(second.await.unwrap());
    stream.write(b"more").await.unwrap();
    stream.close().await.unwrap();
    assert_eq!(&store.objects.lock().unwrap()["c"][..], b"datamore");
}

#[tokio::test]
async fn append_diagnostic_store_preserves_snapshots_offsets_and_gaps() {
    for inplace in [false, true] {
        let store = MemoryStore { pfs: true, append_inplace: inplace, ..Default::default() };
        store.put_empty("file").await.unwrap();
        store.posix_append("file", 0, Bytes::from_static(b"abc")).await.unwrap();
        let snapshot = store.objects.lock().unwrap()["file"].clone();
        store.posix_append("file", 5, Bytes::from_static(b"z")).await.unwrap();
        store.posix_append("file", 1, Bytes::from_static(b"Y")).await.unwrap();
        assert_eq!(snapshot.as_ref(), b"abc");
        assert_eq!(store.objects.lock().unwrap()["file"].as_ref(), b"aYc\0\0z");
    }
}

#[cfg(feature = "append-diagnostics")]
#[tokio::test]
async fn append_task_diagnostics_record_success_and_started_task_cancellation() {
    use obs_rust_core::io::append_diagnostics::AppendTaskDiagnostics;
    for (cancel, fail) in [(false, false), (true, false), (false, true)] {
        let control = AppendControl::blocked();
        let store = Arc::new(MemoryStore { pfs: true, append_control: Some(control.clone()), append_failures: AtomicUsize::new(usize::from(fail)), ..Default::default() });
        store.put_empty("file").await.unwrap();
        let fs = ObsFileSystem::with_store(ObsConfig::default(), store);
        let diagnostics = Arc::new(AppendTaskDiagnostics::new(1));
        let callbacks = Arc::new(AtomicUsize::new(0));
        let observed_callbacks = callbacks.clone();
        let records_during_cleanup = Arc::new(AtomicUsize::new(usize::MAX));
        let observed_records = records_during_cleanup.clone();
        let callback_diagnostics = diagnostics.clone();
        let mut stream = fs.append_stream("/file", 0).await.unwrap()
            .with_task_diagnostics(diagnostics.clone())
            .on_flush(move || {
                let call = observed_callbacks.fetch_add(1, Ordering::SeqCst) + 1;
                if call == if cancel { 2 } else { 1 } {
                    observed_records.store(callback_diagnostics.snapshot().len(), Ordering::SeqCst);
                }
            });
        stream.write(b"abc").await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), control.started.notified()).await.unwrap();
        let before = diagnostics.completion_scans();
        assert_eq!(before[0], 2); // Two empty reaps before the first spawn.
        assert_eq!(before[1], 0);
        assert_eq!(before[3], 0);
        let mut flush = Box::pin(stream.flush());
        assert!(futures::poll!(flush.as_mut()).is_pending());
        drop(flush); // Cancel only the waiter; the task remains owned by the stream.
        let after = diagnostics.completion_scans();
        assert_eq!(after[3], before[3] + 1);
        assert_eq!(after[4], before[4] + 1);
        assert_eq!(&after[..3], &before[..3]);
        if !cancel {
            control.gate.add_permits(1);
            assert_eq!(stream.flush().await.is_err(), fail);
        }
        drop(stream);
        let rows = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let rows = diagnostics.snapshot();
                if !rows.is_empty() { break rows; }
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(diagnostics.dropped(), 0);
        let row = &rows[0];
        assert_eq!(row.position, 0);
        assert!(row.queued <= row.started.unwrap() && row.started.unwrap() <= row.completed);
        assert_eq!(row.store_finished.is_none(), cancel);
        assert!(row.polls >= if cancel { 1 } else { 2 });
        if let Some(finished) = row.store_finished {
            assert!(row.started.unwrap() <= finished && finished <= row.completed);
        }
        // Both stream drop and task cleanup have invoked the callback before
        // the diagnostic record is published, including task cancellation.
        assert!(callbacks.load(Ordering::SeqCst) >= 2);
        assert_eq!(records_during_cleanup.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn append_reserved_capacity_and_growth_diagnostics_preserve_data() {
    let profile = Arc::new(append_latency::AppendProfile::default());
    let store = MemoryStore { pfs: true, append_inplace: true, append_profile: Some(profile.clone()), ..Default::default() };
    let reserved = bytes::BytesMut::with_capacity(32).freeze();
    store.objects.lock().unwrap().insert("file-0".into(), reserved);
    store.posix_append("file-0", 0, Bytes::from_static(b"abc")).await.unwrap();
    let snapshot = store.objects.lock().unwrap()["file-0"].clone();
    store.posix_append("file-0", 5, Bytes::from_static(b"z")).await.unwrap();
    store.posix_append("file-0", 1, Bytes::from_static(b"Y")).await.unwrap();
    assert_eq!(snapshot.as_ref(), b"abc");
    assert_eq!(store.objects.lock().unwrap()["file-0"].as_ref(), b"aYc\0\0z");
    let timings = profile.operations[0].lock().unwrap();
    assert_eq!(timings.len(), 3);
    assert!(timings[0].capacity_before >= 32);
    assert_eq!(timings[0].capacity_before, timings[0].capacity_after);
    assert!(!timings[0].copy_fallback);
    assert!(timings[1].copy_fallback);
    assert!(timings[1].capacity_after > timings[1].capacity_before);
    assert!(!timings[2].copy_fallback);
    drop(timings);
    let page = store.list("absent/", "/", None, 1).await.unwrap();
    assert!(page.contents.is_empty());
    let lists = profile.lists.lock().unwrap();
    assert_eq!(lists.len(), 1);
    assert_eq!(lists[0].scanned, 1);
    assert!(lists[0].waiting <= lists[0].acquired && lists[0].acquired <= lists[0].ended);
}

#[tokio::test]
async fn append_batch_reaps_completed_tail_without_losing_cancelled_prefix_wait() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let control = AppendControl::blocked();
        let store = Arc::new(MemoryStore { pfs: true, append_control: Some(control.clone()),
            append_control_position: Some(0), ..Default::default() });
        store.put_empty("file").await.unwrap();
        let completed = Arc::new(AtomicUsize::new(0));
        let observed = completed.clone();
        let config = ObsConfig { pfs_append_chunk_size: 1, pfs_append_total_bytes: 64,
            pfs_append_concurrency: 64, ..Default::default() };
        let mut stream = PfsAppendStream::new(store.clone(), &config, "file".into(), 0)
            .on_flush(move || { observed.fetch_add(1, Ordering::SeqCst); });
        let payload: Vec<u8> = (0..32).collect();
        stream.write(&payload).await.unwrap();
        control.started.notified().await;
        while completed.load(Ordering::SeqCst) != 31 { tokio::task::yield_now().await; }
        // Tail tasks are complete, but the first request remains blocked. An
        // empty write must reap those completions without waiting for the prefix.
        assert_eq!(stream.write(&[]).await.unwrap(), 0);
        let mut flush = Box::pin(stream.flush());
        assert!(futures::poll!(flush.as_mut()).is_pending());
        drop(flush);
        stream.write(&[32]).await.unwrap();
        while completed.load(Ordering::SeqCst) != 32 { tokio::task::yield_now().await; }
        let mut flush = Box::pin(stream.flush());
        assert!(futures::poll!(flush.as_mut()).is_pending());
        drop(flush);
        control.gate.add_permits(1);
        stream.close().await.unwrap();
        assert_eq!(store.objects.lock().unwrap()["file"].as_ref(), (0..33).collect::<Vec<u8>>());
        assert_eq!(store.append_bytes.load(Ordering::SeqCst), 0);
    }).await.expect("completed tail blocked reaping or cancellation lost the prefix");
}

#[tokio::test]
async fn append_batch_budget_poll_preserves_large_waiter_reservation() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let control = AppendControl::blocked();
        let store = Arc::new(MemoryStore { pfs: true, append_control: Some(control.clone()), ..Default::default() });
        for key in ["holder", "large", "small", "follower"] { store.put_empty(key).await.unwrap(); }
        let fs = ObsFileSystem::with_store(ObsConfig {
            pfs_append_chunk_size: 8, pfs_append_total_bytes: 8, pfs_append_concurrency: 4,
            ..Default::default()
        }, store.clone());
        let mut holder = fs.append_stream("/holder", 0).await.unwrap();
        let mut large = fs.append_stream("/large", 0).await.unwrap();
        let mut small = fs.append_stream("/small", 0).await.unwrap();
        let mut follower = fs.append_stream("/follower", 0).await.unwrap();
        small.write(b"a").await.unwrap();
        small.write(b"b").await.unwrap();
        holder.write(b"12").await.unwrap();
        let mut large_write = Box::pin(large.write(b"12345678"));
        assert!(futures::poll!(large_write.as_mut()).is_pending());
        // This stream has two unfinished tasks, so its next acquisition enters
        // the batched path. The older large waiter owns the four free bytes.
        let mut small_write = Box::pin(small.write(b"x"));
        assert!(futures::poll!(small_write.as_mut()).is_pending());
        let mut follower_write = Box::pin(follower.write(b"12345678"));
        assert!(futures::poll!(follower_write.as_mut()).is_pending());
        control.gate.add_permits(3);
        holder.flush().await.unwrap();
        large_write.as_mut().await.unwrap();
        drop(large_write);
        // Reaping both completed tasks must not cancel/requeue the acquisition
        // or steal the large request's newly granted eight-byte reservation.
        assert!(futures::poll!(small_write.as_mut()).is_pending());
        control.gate.add_permits(1);
        large.flush().await.unwrap();
        // Small must keep its queue position ahead of this later large waiter,
        // even after processing the two completions while waiting for budget.
        assert!(futures::poll!(follower_write.as_mut()).is_pending());
        small_write.as_mut().await.unwrap();
        drop(small_write);
        control.gate.add_permits(1);
        assert!(futures::poll!(follower_write.as_mut()).is_pending());
        small.close().await.unwrap();
        follower_write.as_mut().await.unwrap();
        drop(follower_write);
        control.gate.add_permits(1);
        follower.close().await.unwrap();
        assert_eq!(store.objects.lock().unwrap()["follower"].as_ref(), b"12345678");
        assert_eq!(store.objects.lock().unwrap()["small"].as_ref(), b"abx");
        assert_eq!(store.objects.lock().unwrap()["large"].as_ref(), b"12345678");
        assert!(store.append_peak_bytes.load(Ordering::SeqCst) <= 8);
        assert_eq!(store.append_bytes.load(Ordering::SeqCst), 0);
    }).await.expect("batched acquisition bypassed a waiter or lost progress");
}

