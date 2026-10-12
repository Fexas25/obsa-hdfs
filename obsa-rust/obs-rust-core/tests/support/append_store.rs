//! Benchmark-only storage. Modes differ only in scan and serialization policy.
//! This is not a cloud emulator: unsupported APIs fail instead of faking success.
use super::*;
use std::sync::RwLock;

pub(super) struct BenchmarkStore {
    objects: RwLock<BTreeMap<String, Arc<Mutex<Bytes>>>>,
    gate: Mutex<()>,
    global: bool,
    scan_all: bool,
    profile: Option<Arc<AppendProfile>>,
    append_delay: Duration,
    metadata_only_head: bool,
    delay_active: AtomicUsize,
    delay_peak: AtomicUsize,
    delay_calls: AtomicU64,
    delay_elapsed_ns: AtomicU64,
}
impl BenchmarkStore {
    pub fn new(global: bool, scan_all: bool, profile: Option<Arc<AppendProfile>>) -> Self {
        Self { objects: RwLock::new(BTreeMap::new()), gate: Mutex::new(()), global, scan_all, profile,
            append_delay: Duration::ZERO, metadata_only_head: false, delay_active: AtomicUsize::new(0), delay_peak: AtomicUsize::new(0),
            delay_calls: AtomicU64::new(0), delay_elapsed_ns: AtomicU64::new(0) }
    }
    pub fn with_append_delay(mut self, delay: Duration) -> Self { self.append_delay = delay; self }
    // Mixed rename stress must not hash the entire growing object on every HEAD.
    // Omit the optional ETag rather than pretending a length is a content hash.
    pub fn with_metadata_only_head(mut self) -> Self { self.metadata_only_head = true; self }
    pub fn delay_stats(&self) -> serde_json::Value {
        serde_json::json!({"calls":self.delay_calls.load(Ordering::Relaxed),
            "active":self.delay_active.load(Ordering::Relaxed),"peak":self.delay_peak.load(Ordering::Relaxed),
            "elapsed_ns":self.delay_elapsed_ns.load(Ordering::Relaxed)})
    }
    fn object(&self, key: &str) -> ObsResult<Arc<Mutex<Bytes>>> {
        self.objects.read().unwrap().get(key).cloned().ok_or_else(MemoryStore::missing)
    }
    fn unsupported() -> ObsError { ObsError::Unsupported("append benchmark store operation".into()) }
    pub fn snapshot(&self, key: &str) -> ObsResult<Bytes> {
        Ok(self.object(key)?.lock().unwrap().clone())
    }
}

// The delay models asynchronous pre-commit service time. Never hold a map,
// object or legacy serialization lock across the wait. Cancellation drops the
// observation and must not commit the request body.
struct DelayObservation<'a> { store: &'a BenchmarkStore, started: Instant }
impl Drop for DelayObservation<'_> {
    fn drop(&mut self) {
        self.store.delay_elapsed_ns.fetch_add(self.started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.store.delay_active.fetch_sub(1, Ordering::Relaxed);
    }
}

#[async_trait]
impl ObjectStore for BenchmarkStore {
    async fn head(&self, key: &str) -> ObsResult<ObjectMeta> {
        if self.metadata_only_head {
            let content_length = self.object(key)?.lock().unwrap().len() as u64;
            return Ok(ObjectMeta { key: key.into(), content_length, ..Default::default() });
        }
        let data = self.snapshot(key)?;
        Ok(ObjectMeta { key: key.into(), content_length: data.len() as u64,
            etag: Some(format!("{:x}", Md5::digest(&data))), ..Default::default() })
    }
    async fn get(&self, key: &str, range: Option<(u64,u64)>, _: Option<&InodeHint>) -> ObsResult<GetResult> {
        let data = self.snapshot(key)?;
        let meta = ObjectMeta { key: key.into(), content_length: data.len() as u64,
            etag: Some(format!("{:x}", Md5::digest(&data))), ..Default::default() };
        let data = match range {
            Some((start,end)) => {
                let start = usize::try_from(start).map_err(|_| Self::unsupported())?;
                let end = usize::try_from(end).map_err(|_| Self::unsupported())?;
                if start > end || end >= data.len() { return Err(Self::unsupported()); }
                data.slice(start..=end)
            }
            None => data,
        };
        Ok(GetResult { meta, body: Box::pin(futures::stream::iter([Ok(data)])) })
    }
    async fn put(&self, key: &str, body: Bytes, _: PutHeaders) -> ObsResult<ObjectMeta> {
        let length = body.len() as u64;
        self.objects.write().unwrap().insert(key.into(), Arc::new(Mutex::new(body)));
        Ok(ObjectMeta { key:key.into(), content_length:length, ..Default::default() })
    }
    async fn put_empty(&self, key: &str) -> ObsResult<ObjectMeta> {
        self.put(key, Bytes::new(), PutHeaders::default()).await
    }
    async fn delete(&self, key: &str, hdfs: bool) -> ObsResult<()> {
        let found = self.objects.write().unwrap().remove(key).is_some();
        if hdfs && !found { Err(MemoryStore::missing()) } else { Ok(()) }
    }
    async fn delete_batch(&self, keys: &[String]) -> ObsResult<()> {
        for key in keys { self.delete(key, false).await?; }
        Ok(())
    }
    async fn list(&self, prefix: &str, delimiter: &str, marker: Option<&str>, maximum: usize) -> ObsResult<ListPage> {
        // The measured workload probes absent prefixes only. Reject pagination
        // instead of silently giving an incomplete implementation its semantics.
        if marker.is_some() || maximum == 0 || !matches!(delimiter, "" | "/") { return Err(Self::unsupported()); }
        let waiting = self.profile.as_ref().map(|_| Instant::now());
        let gate = self.global.then(|| self.gate.lock().unwrap());
        let objects = self.objects.read().unwrap();
        let acquired = waiting.map(|_| Instant::now());
        let mut scanned = 0;
        let mut page = ListPage::default();
        let mut append = |key: &String, object: &Arc<Mutex<Bytes>>| {
            if delimiter == "/" {
                if let Some(i) = key[prefix.len()..].find('/') {
                    let dir = key[..prefix.len()+i+1].to_string();
                    if page.common_prefixes.last() != Some(&dir) { page.common_prefixes.push(dir); }
                    return;
                }
            }
            page.contents.push(ListEntry { key:key.clone(), size:object.lock().unwrap().len() as u64, ..Default::default() });
        };
        if self.scan_all {
            for (key, object) in objects.iter() {
                scanned += 1;
                if key.starts_with(prefix) { append(key, object); }
            }
        } else {
            // Lower-bound search plus a contiguous prefix range, including the
            // terminating key in the inspected-key counter when one exists.
            for (key, object) in objects.range::<str, _>((std::ops::Bound::Included(prefix), std::ops::Bound::Unbounded)) {
                scanned += 1;
                if !key.starts_with(prefix) { break; }
                append(key, object);
            }
        }
        drop(objects);
        drop(gate);
        if let (Some(profile), Some(waiting), Some(acquired)) = (&self.profile, waiting, acquired) {
            let ended = Instant::now();
            profile.lists.lock().unwrap().push(ListTiming { waiting, acquired, ended, scanned });
        }
        if page.contents.len() + page.common_prefixes.len() > maximum { return Err(Self::unsupported()); }
        Ok(page)
    }
    async fn copy(&self, src: &str, dst: &str) -> ObsResult<ObjectMeta> {
        self.put(dst, self.snapshot(src)?, PutHeaders::default()).await
    }
    async fn posix_rename(&self, src: &str, dst: &str) -> ObsResult<()> {
        let mut objects = self.objects.write().unwrap();
        let object = objects.remove(src).ok_or_else(MemoryStore::missing)?;
        objects.insert(dst.into(), object);
        Ok(())
    }
    async fn posix_append(&self, key: &str, position: u64, body: Bytes) -> ObsResult<()> {
        let started = self.profile.as_ref().map(|_| Instant::now());
        if !self.append_delay.is_zero() {
            let _observation = DelayObservation { store: self, started: Instant::now() };
            self.delay_calls.fetch_add(1, Ordering::Relaxed);
            let active = self.delay_active.fetch_add(1, Ordering::Relaxed) + 1;
            self.delay_peak.fetch_max(active, Ordering::Relaxed);
            tokio::time::sleep(self.append_delay).await;
        }
        let object = self.object(key)?;
        let start = usize::try_from(position).map_err(|_| Self::unsupported())?;
        let end = start.checked_add(body.len()).ok_or_else(Self::unsupported)?;
        let waiting = started.map(|_| Instant::now());
        let gate = self.global.then(|| self.gate.lock().unwrap());
        let mut object = object.lock().unwrap();
        let acquired = started.map(|_| Instant::now());
        let (mut bytes, copy_fallback) = match std::mem::take(&mut *object).try_into_mut() {
            Ok(bytes) => (bytes, false), Err(bytes) => (bytes::BytesMut::from(bytes.as_ref()), true),
        };
        let capacity_before = bytes.capacity();
        bytes.resize(bytes.len().max(end), 0);
        bytes[start..end].copy_from_slice(&body);
        let capacity_after = bytes.capacity();
        *object = bytes.freeze();
        drop(object);
        drop(gate);
        if let (Some(profile), Some(started), Some(waiting), Some(acquired)) = (&self.profile, started, waiting, acquired) {
            let ended = Instant::now();
            if let Some(worker) = key.strip_prefix("file-").and_then(|v| v.parse::<usize>().ok()).filter(|v| *v < 4) {
                profile.operations[worker].lock().unwrap().push(StoreTiming {
                    started, waiting, acquired, ended, position, capacity_before, capacity_after, copy_fallback });
            }
            profile.calls.fetch_add(1, Ordering::Relaxed);
            profile.total_ns.fetch_add(ended.duration_since(started).as_nanos() as u64, Ordering::Relaxed);
            profile.wait_ns.fetch_add(acquired.duration_since(waiting).as_nanos() as u64, Ordering::Relaxed);
            profile.hold_ns.fetch_add(ended.duration_since(acquired).as_nanos() as u64, Ordering::Relaxed);
        }
        Ok(())
    }
    async fn posix_truncate(&self, _: &str, _: u64) -> ObsResult<()> { Err(Self::unsupported()) }
    async fn create_folder(&self, _: &str) -> ObsResult<()> { Err(Self::unsupported()) }
    async fn set_object_metadata(&self, _: &str, _: Option<u32>, _: Option<u32>, _: Option<u32>) -> ObsResult<()> { Err(Self::unsupported()) }
    async fn initiate_multipart_upload(&self, _: &str) -> ObsResult<String> { Err(Self::unsupported()) }
    async fn upload_part(&self, _: &str, _: &str, _: i32, _: Bytes) -> ObsResult<String> { Err(Self::unsupported()) }
    async fn complete_multipart_upload(&self, _: &str, _: &str, _: &[CompletedPart]) -> ObsResult<ObjectMeta> { Err(Self::unsupported()) }
    async fn abort_multipart_upload(&self, _: &str, _: &str) -> ObsResult<()> { Err(Self::unsupported()) }
    async fn probe_posix_bucket(&self) -> ObsResult<bool> { Ok(true) }
    fn is_pfs(&self) -> bool { true }
}

#[tokio::test]
async fn metadata_only_head_follows_append_and_rename_without_etag() {
    let store = BenchmarkStore::new(false, false, None).with_metadata_only_head();
    store.put_empty("a").await.unwrap();
    assert_eq!(store.head("a").await.unwrap().content_length, 0);
    store.posix_append("a", 0, Bytes::from_static(b"abc")).await.unwrap();
    store.posix_rename("a", "b").await.unwrap();
    assert!(store.head("a").await.is_err());
    let meta = store.head("b").await.unwrap();
    assert_eq!(meta.content_length, 3);
    assert!(meta.etag.is_none());
    assert_eq!(store.snapshot("b").unwrap().as_ref(), b"abc");
}

#[tokio::test]
async fn store_modes_preserve_snapshots_offsets_rename_and_prefix_boundaries() {
    for (global, scan) in [(true,true), (true,false), (false,false)] {
        let profile = Arc::new(AppendProfile::default());
        let store = BenchmarkStore::new(global, scan, Some(profile.clone()));
        store.put("file-0", bytes::BytesMut::with_capacity(32).freeze(), PutHeaders::default()).await.unwrap();
        store.posix_append("file-0",0,Bytes::from_static(b"abc")).await.unwrap();
        let snapshot = store.snapshot("file-0").unwrap();
        store.posix_append("file-0",5,Bytes::from_static(b"z")).await.unwrap();
        store.posix_append("file-0",1,Bytes::from_static(b"Y")).await.unwrap();
        assert_eq!(snapshot.as_ref(), b"abc");
        assert_eq!(store.snapshot("file-0").unwrap().as_ref(), b"aYc\0\0z");
        store.copy("file-0", "copy").await.unwrap();
        store.posix_rename("file-0", "moved").await.unwrap();
        assert!(store.head("file-0").await.is_err());
        store.posix_append("moved",6,Bytes::from_static(b"!")).await.unwrap();
        assert_eq!(store.snapshot("copy").unwrap().as_ref(), b"aYc\0\0z");
        assert_eq!(store.snapshot("moved").unwrap().as_ref(), b"aYc\0\0z!");
        for key in ["a/one", "a/sub/x", "a/sub/y", "ab/out", "中/文", "中外/out"] { store.put_empty(key).await.unwrap(); }
        let page = store.list("a/", "/", None, 10).await.unwrap();
        assert_eq!(page.contents.iter().map(|v| v.key.as_str()).collect::<Vec<_>>(), ["a/one"]);
        assert_eq!(page.common_prefixes, ["a/sub/"]);
        let page = store.list("中/", "", None, 10).await.unwrap();
        assert_eq!(page.contents.len(), 1);
        assert_eq!(page.contents[0].key, "中/文");
        let page = store.list("missing/", "/", None, 1).await.unwrap();
        assert!(page.contents.is_empty() && page.common_prefixes.is_empty());
        let last_scanned = profile.lists.lock().unwrap().last().unwrap().scanned;
        assert!(if scan { last_scanned == 8 } else { last_scanned <= 1 });
        assert!(store.list("a/", "", None, 1).await.is_err());
        assert!(store.list("", "", Some("x"), 10).await.is_err());
        let timing = profile.operations[0].lock().unwrap();
        assert_eq!(timing[0].capacity_before, timing[0].capacity_after);
        assert!(timing[0].capacity_before >= 32);
        assert!(timing[1].copy_fallback);
    }
}

#[test]
fn unrelated_append_and_prefix_probe_do_not_wait_for_an_object_lock() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let store = Arc::new(BenchmarkStore::new(false, false, None));
    runtime.block_on(async {
        store.put_empty("file-0").await.unwrap();
        store.put_empty("file-1").await.unwrap();
    });
    let object = store.object("file-0").unwrap();
    let held = object.lock().unwrap();
    let other = store.clone();
    let (send, receive) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        runtime.block_on(async {
            other.posix_append("file-1",0,Bytes::from_static(b"x")).await.unwrap();
            let page = other.list("absent/", "/", None, 1).await.unwrap();
            assert!(page.contents.is_empty());
        });
        send.send(()).unwrap();
    });
    let result = receive.recv_timeout(Duration::from_secs(5));
    drop(held); // Release even on failure so the helper thread can be joined.
    worker.join().unwrap();
    result.expect("unrelated operations were blocked by another object's lock");
    assert_eq!(store.snapshot("file-1").unwrap().as_ref(), b"x");
}

#[tokio::test]
async fn filesystem_stream_follows_rename_and_replaced_stream_is_rejected() {
    let store = Arc::new(BenchmarkStore::new(false, false, None));
    store.put_empty("source").await.unwrap();
    store.put("destination", Bytes::from_static(b"old"), PutHeaders::default()).await.unwrap();
    let fs = ObsFileSystem::with_store(ObsConfig::default(), store.clone());
    let mut source = fs.append_stream("/source", 0).await.unwrap();
    let mut replaced = fs.append_stream("/destination", 3).await.unwrap();
    source.write(b"a").await.unwrap();
    source.flush().await.unwrap();
    assert!(fs.rename("/source", "/destination", true).await.unwrap());
    assert!(replaced.write(b"bad").await.is_err());
    source.write(b"b").await.unwrap();
    source.close().await.unwrap();
    assert_eq!(store.snapshot("destination").unwrap().as_ref(), b"ab");
    assert!(store.snapshot("source").is_err());
}

#[tokio::test(start_paused = true)]
async fn delayed_append_waits_without_locks_and_cancellation_does_not_commit() {
    for global in [false, true] {
        let store = BenchmarkStore::new(global, false, None).with_append_delay(Duration::from_millis(10));
        store.put_empty("a").await.unwrap();
        store.put_empty("b").await.unwrap();
        let mut a = Box::pin(store.posix_append("a", 0, Bytes::from_static(b"cancelled")));
        let mut b = Box::pin(store.posix_append("b", 0, Bytes::from_static(b"kept")));
        assert!(futures::poll!(a.as_mut()).is_pending());
        assert!(futures::poll!(b.as_mut()).is_pending());
        assert_eq!(store.delay_active.load(Ordering::Relaxed), 2);
        assert_eq!(store.delay_peak.load(Ordering::Relaxed), 2);
        assert!(store.snapshot("a").unwrap().is_empty());
        store.put_empty("unrelated").await.unwrap();
        assert!(store.list("missing/", "/", None, 1).await.unwrap().contents.is_empty());
        tokio::time::advance(Duration::from_millis(5)).await;
        assert!(futures::poll!(a.as_mut()).is_pending());
        assert!(futures::poll!(b.as_mut()).is_pending());
        drop(a);
        assert_eq!(store.delay_active.load(Ordering::Relaxed), 1);
        tokio::time::advance(Duration::from_millis(10)).await;
        b.await.unwrap();
        assert!(store.snapshot("a").unwrap().is_empty());
        assert_eq!(store.snapshot("b").unwrap().as_ref(), b"kept");
        assert_eq!(store.delay_active.load(Ordering::Relaxed), 0);
        assert_eq!(store.delay_calls.load(Ordering::Relaxed), 2);
    }
}
