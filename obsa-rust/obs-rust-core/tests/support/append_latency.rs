//! Full Rust filesystem/append path with selectable local stores; no HTTP/JNI/cloud.
use super::*;
#[path = "append_store.rs"]
mod append_store;
use append_store::BenchmarkStore;
#[path = "append_sustained.rs"]
mod append_sustained;
#[path = "append_mixed.rs"]
mod append_mixed;
use std::time::Instant;
use tokio::sync::Barrier;
use std::sync::atomic::AtomicU64;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "isolated completion batching benchmark; release only"]
async fn benchmark_append_completion_batches() {
    let requests: usize = std::env::var("OBS_APPEND_BENCH_REQUESTS")
        .map(|value| value.parse().expect("OBS_APPEND_BENCH_REQUESTS must be an integer"))
        .unwrap_or(1024);
    assert!((64..=16384).contains(&requests) && requests % 64 == 0,
        "requests per worker must be a multiple of 64 in 64..=16384");
    let budget: u64 = std::env::var("OBS_APPEND_BENCH_TOTAL_BYTES")
        .map(|value| value.parse().expect("OBS_APPEND_BENCH_TOTAL_BYTES must be an integer"))
        .unwrap_or(ObsConfig::default().pfs_append_total_bytes);
    assert!(budget >= 4096, "batch benchmark requires at least one full request of budget");
    let concurrency: usize = std::env::var("OBS_APPEND_BENCH_CONCURRENCY")
        .map(|value| value.parse().expect("OBS_APPEND_BENCH_CONCURRENCY must be an integer"))
        .unwrap_or(64);
    assert!((1..=256).contains(&concurrency), "benchmark concurrency must be in 1..=256");
    let delay_ms: u64 = std::env::var("OBS_APPEND_BENCH_DELAY_MS")
        .map(|value| value.parse().expect("OBS_APPEND_BENCH_DELAY_MS must be an integer"))
        .unwrap_or(0);
    assert!(delay_ms <= 1000, "benchmark delay must be in 0..=1000 ms");
    let profile_enabled = cfg!(feature = "append-diagnostics") && std::env::var_os("OBS_APPEND_PROFILE").is_some();
    for trial in 0..=3 {
        for offset in 0..3 {
            let batch = [1, 16, 64][(trial + offset) % 3];
            let store = Arc::new(BenchmarkStore::new(false, false, None).with_append_delay(Duration::from_millis(delay_ms)));
            let fs = Arc::new(ObsFileSystem::with_store(ObsConfig {
                pfs_append_concurrency: concurrency, pfs_append_total_bytes: budget, ..Default::default()
            }, store.clone()));
            let barrier = Arc::new(Barrier::new(5));
            let mut tasks = Vec::new();
            #[cfg(feature = "append-diagnostics")]
            let mut diagnostics = Vec::new();
            for worker in 0..4 {
                let key = format!("file-{worker}");
                store.put(&key, bytes::BytesMut::with_capacity(requests * 4096).freeze(), PutHeaders::default()).await.unwrap();
                let mut stream = fs.append_stream(&format!("/{key}"), 0).await.unwrap();
                #[cfg(feature = "append-diagnostics")]
                if profile_enabled {
                    let sink = Arc::new(obs_rust_core::io::append_diagnostics::AppendTaskDiagnostics::new(requests));
                    stream = stream.with_task_diagnostics(sink.clone());
                    diagnostics.push(sink);
                }
                let barrier = barrier.clone();
                tasks.push(tokio::spawn(async move {
                    let payload = vec![b'x'; 4096];
                    let mut times = Vec::with_capacity(requests / batch);
                    let mut phases = if profile_enabled { Vec::with_capacity(requests / batch) } else { Vec::new() };
                    barrier.wait().await;
                    for _ in 0..requests / batch {
                        let start = Instant::now();
                        for _ in 0..batch { stream.write(&payload).await.unwrap(); }
                        let written = profile_enabled.then(Instant::now);
                        stream.flush().await.unwrap();
                        let ended = Instant::now();
                        times.push(ended.duration_since(start).as_nanos() as u64);
                        if let Some(written) = written { phases.push((start, written, ended)); }
                    }
                    stream.close().await.unwrap();
                    (times, phases)
                }));
            }
            let start = Instant::now();
            barrier.wait().await;
            let mut times = Vec::new();
            let mut batch_phases = Vec::new();
            for task in tasks {
                let (samples, phases) = task.await.unwrap();
                times.extend(samples);
                batch_phases.push(phases);
            }
            let elapsed = start.elapsed().as_secs_f64();
            for worker in 0..4 {
                let data = store.snapshot(&format!("file-{worker}")).unwrap();
                assert_eq!(data.len(), requests * 4096);
                assert!(data.iter().all(|b| *b == b'x'));
            }
            #[allow(unused_mut)]
            let mut completion_scans: Option<Vec<[u64; 6]>> = None;
            #[allow(unused_mut)]
            let mut phase_summary = None::<serde_json::Value>;
            #[cfg(feature = "append-diagnostics")]
            if !diagnostics.is_empty() {
                let (mut dispatch, mut service, mut cleanup) = (Vec::new(), Vec::new(), Vec::new());
                let (mut write, mut overlap, mut tail, mut flush) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
                let ns = |a: Instant, b: Instant| b.duration_since(a).as_nanos() as u64;
                for (worker, sink) in diagnostics.iter().enumerate() {
                    let mut records = sink.snapshot();
                    assert_eq!(records.len(), requests);
                    assert_eq!(sink.dropped(), 0);
                    records.sort_unstable_by_key(|record| record.position);
                    assert_eq!(batch_phases[worker].len(), requests / batch);
                    for (index, record) in records.iter().enumerate() {
                        assert_eq!(record.position, (index * 4096) as u64);
                        let entered = record.started.unwrap();
                        let finished = record.store_finished.unwrap();
                        assert!(record.queued <= entered && entered <= finished && finished <= record.completed);
                        dispatch.push(ns(record.queued, entered));
                        service.push(ns(entered, finished));
                        cleanup.push(ns(finished, record.completed));
                    }
                    for ((start, written, ended), records) in batch_phases[worker].iter().copied().zip(records.chunks_exact(batch)) {
                        assert!(start <= written && written <= ended);
                        assert!(records.iter().all(|record| start <= record.queued && record.completed <= ended));
                        let last = records.iter().map(|record| record.completed).max().unwrap().max(written);
                        write.push(ns(start, written));
                        overlap.push(ns(written, last));
                        tail.push(ns(last, ended));
                        flush.push(ns(written, ended));
                    }
                }
                assert_eq!(overlap.iter().sum::<u64>() + tail.iter().sum::<u64>(), flush.iter().sum::<u64>());
                phase_summary = Some(serde_json::json!({
                    "dispatch":latency(&mut dispatch),"service":latency(&mut service),"cleanup":latency(&mut cleanup),
                    "write":latency(&mut write),"flush":latency(&mut flush),"flush_overlap":latency(&mut overlap),"flush_tail":latency(&mut tail),
                    "write_sum_ns":write.iter().sum::<u64>(),"flush_sum_ns":flush.iter().sum::<u64>(),
                    "flush_overlap_sum_ns":overlap.iter().sum::<u64>(),"flush_tail_sum_ns":tail.iter().sum::<u64>()}));
                completion_scans = Some(diagnostics.iter().map(|sink| sink.completion_scans()).collect());
            }
            let delay_stats = store.delay_stats();
            assert_eq!(delay_stats["active"].as_u64().unwrap(), 0);
            assert_eq!(delay_stats["calls"].as_u64().unwrap(), if delay_ms == 0 { 0 } else { (requests * 4) as u64 });
            if trial > 0 {
                println!("APPEND_COMPLETION {}", serde_json::json!({"trial":trial,"batch":batch,
                    "delay_ms":delay_ms,"delay_stats":delay_stats,"concurrency":concurrency,"budget_bytes":budget,"phases":phase_summary,"requests":requests * 4,"payload_mib_s":(requests * 4 * 4096) as f64 / 1048576.0 / elapsed,"elapsed_s":elapsed,
                    "completion_scans":completion_scans,"batch_latency":latency(&mut times)}));
            }
        }
    }
}

#[derive(Default)]
pub(super) struct AppendProfile {
    pub operations: [Mutex<Vec<StoreTiming>>; 4],
    pub lists: Mutex<Vec<ListTiming>>,
    pub calls: AtomicU64,
    pub total_ns: AtomicU64,
    pub wait_ns: AtomicU64,
    pub hold_ns: AtomicU64,
    pub copied_prefix_bytes: AtomicU64,
}
// Recorded after releasing the simulated object-map lock, independently per writer.
// Instants share a monotonic clock with the outer operation; no wall-clock subtraction.
pub(super) struct ListTiming {
    pub waiting: Instant, pub acquired: Instant, pub ended: Instant, pub scanned: usize,
}
pub(super) struct StoreTiming {
    pub capacity_before: usize,
    pub capacity_after: usize,
    pub copy_fallback: bool,
    pub position: u64,
    pub started: Instant,
    pub waiting: Instant,
    pub acquired: Instant,
    pub ended: Instant,
}
struct OperationTiming { started: Instant, written: Instant, ended: Instant, wake_to_poll_ns: u64, wakes: u64 }

// Optional observer for the writer future only. It measures ready-to-poll delay,
// not service time or time spent waiting for the storage operation to finish.
struct WakeProbe { parent: std::task::Waker, signaled: Arc<Mutex<Option<Instant>>> }
impl futures::task::ArcWake for WakeProbe {
    fn wake_by_ref(probe: &Arc<Self>) {
        probe.signaled.lock().unwrap().get_or_insert_with(Instant::now);
        probe.parent.wake_by_ref();
    }
}
async fn observe_wakes<F: std::future::Future>(future: F, enabled: bool) -> (F::Output, u64, u64) {
    if !enabled { return (future.await, 0, 0); }
    let signaled = Arc::new(Mutex::new(None::<Instant>));
    let (mut delay, mut wakes) = (0, 0);
    tokio::pin!(future);
    let output = futures::future::poll_fn(|cx| {
        if let Some(woken) = signaled.lock().unwrap().take() {
            delay += woken.elapsed().as_nanos() as u64;
            wakes += 1;
        }
        let probe = Arc::new(WakeProbe { parent: cx.waker().clone(), signaled: signaled.clone() });
        let waker = futures::task::waker(probe);
        future.as_mut().poll(&mut std::task::Context::from_waker(&waker))
    }).await;
    (output, delay, wakes)
}

#[tokio::test]
async fn operation_wake_observer_handles_ready_and_pending() {
    let (value, delay, wakes) = observe_wakes(async { 7 }, true).await;
    assert_eq!((value, delay, wakes), (7, 0, 0));
    let (value, _, wakes) = observe_wakes(async { tokio::task::yield_now().await; 9 }, true).await;
    assert_eq!(value, 9);
    assert_eq!(wakes, 1);
    let (value, delay, wakes) = observe_wakes(async { tokio::task::yield_now().await; 11 }, false).await;
    assert_eq!((value, delay, wakes), (11, 0, 0));
}

impl AppendProfile {
    fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "calls": self.calls.load(Ordering::Relaxed),
            "total_ns": self.total_ns.load(Ordering::Relaxed),
            "wait_ns": self.wait_ns.load(Ordering::Relaxed),
            "hold_ns": self.hold_ns.load(Ordering::Relaxed),
            "copied_prefix_bytes": self.copied_prefix_bytes.load(Ordering::Relaxed),
        })
    }
}

fn latency(samples: &mut [u64]) -> serde_json::Value {
    if samples.is_empty() { return serde_json::json!({"count":0}); }
    samples.sort_unstable();
    let rank = |p: usize| samples[(samples.len() * p).div_ceil(100) - 1] as f64 / 1000.0;
    serde_json::json!({"count": samples.len(), "p50_us":rank(50), "p95_us":rank(95),
        "p99_us":rank(99), "max_us":*samples.last().unwrap() as f64 / 1000.0})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "release full append/rename local benchmark; run explicitly in isolation"]
async fn benchmark_append_filesystem() {
    let rename_count = if std::env::var_os("OBS_APPEND_BENCH_NO_RENAME").is_some() { 0 } else { 200 };
    let profile_enabled = std::env::var_os("OBS_APPEND_PROFILE").is_some();
    let prewarm = std::env::var_os("OBS_APPEND_BENCH_PREWARM").is_some();
    let inplace = std::env::var_os("OBS_APPEND_BENCH_INPLACE").is_some();
    let reserve = std::env::var_os("OBS_APPEND_BENCH_RESERVE").is_some();
    assert!(!reserve || inplace, "reserve requires inplace diagnostic mode");
    let backend = std::env::var("OBS_APPEND_BENCH_STORE").unwrap_or_else(|_| "legacy".into());
    assert!(["legacy", "scan-global", "range-global", "range-object"].contains(&backend.as_str()));
    assert!(backend == "legacy" || inplace, "isolated stores require inplace mode");
    for trial in 0..=3 {
        for offset in 0..2 {
            let streams = [32, 8192][(trial + offset) % 2];
            let profile = profile_enabled.then(|| Arc::new(AppendProfile::default()));
            let store: Arc<dyn ObjectStore> = if backend == "legacy" {
                Arc::new(MemoryStore { pfs: true, append_profile: profile.clone(),
                    append_inplace: inplace, ..Default::default() })
            } else {
                Arc::new(BenchmarkStore::new(backend != "range-object", backend == "scan-global", profile.clone()))
            };
            let fs = Arc::new(ObsFileSystem::with_store(ObsConfig::default(), store.clone()));
            let mut held = Vec::with_capacity(streams);
            for index in 0..streams {
                let key = format!("file-{index}");
                store.put_empty(&key).await.unwrap();
                held.push(fs.append_stream(&format!("/{key}"), 0).await.unwrap());
            }
            if reserve {
                for worker in 0..4 {
                    let bytes = bytes::BytesMut::with_capacity(250 * 4096).freeze();
                    let bytes = bytes.try_into_mut().expect("unique reserved buffer");
                    assert!(bytes.is_empty() && bytes.capacity() >= 250 * 4096);
                    store.put(&format!("file-{worker}"), bytes.freeze(), PutHeaders::default()).await.unwrap();
                }
            }
            store.put_empty("rename-a").await.unwrap();
            let warm_started = Instant::now();
            if prewarm {
                assert!(fs.rename("/rename-a", "/rename-b", false).await.unwrap());
                assert!(fs.rename("/rename-b", "/rename-a", false).await.unwrap());
            }
            let prewarm_ns = warm_started.elapsed().as_nanos() as u64;
            if let Some(profile) = &profile { profile.lists.lock().unwrap().clear(); }
            let barrier = Arc::new(Barrier::new(6));
            #[cfg(feature = "append-diagnostics")]
            let mut task_diagnostics = Vec::new();
            let mut tasks = Vec::new();
            for mut stream in held.drain(..4) {
                #[cfg(feature = "append-diagnostics")]
                if profile_enabled {
                    let sink = Arc::new(obs_rust_core::io::append_diagnostics::AppendTaskDiagnostics::new(250));
                    stream = stream.with_task_diagnostics(sink.clone());
                    task_diagnostics.push(sink);
                }
                let barrier = barrier.clone();
                tasks.push(tokio::spawn(async move {
                    let payload = vec![b'x'; 4096];
                    let mut samples = Vec::with_capacity(250);
                    barrier.wait().await;
                    for _ in 0..250 {
                        let started = Instant::now();
                        let (written, wake_to_poll_ns, wakes) = observe_wakes(async {
                            assert_eq!(stream.write(&payload).await.unwrap(), payload.len());
                            let written = if profile_enabled { Instant::now() } else { started };
                            stream.flush().await.unwrap();
                            written
                        }, profile_enabled).await;
                        samples.push(OperationTiming { started, written, ended: Instant::now(), wake_to_poll_ns, wakes });
                    }
                    stream.close().await.unwrap();
                    samples
                }));
            }
            let (rename_fs, rename_barrier) = (fs.clone(), barrier.clone());
            let rename = tokio::spawn(async move {
                let mut samples = Vec::with_capacity(rename_count);
                rename_barrier.wait().await;
                for index in 0..rename_count {
                    let (a, b) = if index % 2 == 0 { ("/rename-a", "/rename-b") } else { ("/rename-b", "/rename-a") };
                    let started = Instant::now();
                    assert!(rename_fs.rename(a, b, false).await.unwrap());
                    samples.push(started.elapsed().as_nanos() as u64);
                    tokio::task::yield_now().await;
                }
                samples
            });
            let started = Instant::now();
            barrier.wait().await;
            let mut operations = Vec::with_capacity(1000);
            for task in tasks { operations.extend(task.await.unwrap()); }
            let mut appends: Vec<_> = operations.iter().map(|op| op.ended.duration_since(op.started).as_nanos() as u64).collect();
            let mut renames = rename.await.unwrap();
            let elapsed = started.elapsed().as_secs_f64();
            let append_elapsed = operations.iter().map(|op| op.ended).max().unwrap().duration_since(started).as_secs_f64();
            for index in 0..4 {
                use futures::TryStreamExt;
                let bytes = store.get(&format!("file-{index}"), None, None).await.unwrap().body
                    .try_fold(Vec::new(), |mut result, chunk| async move { result.extend_from_slice(&chunk); Ok(result) }).await.unwrap();
                assert_eq!(bytes.len(), 250 * 4096);
                assert!(bytes.iter().all(|byte| *byte == b'x'));
            }
            assert!(store.head("rename-a").await.is_ok());
            let details = profile.as_ref().map(|profile| {
                let mut rows = Vec::with_capacity(1000);
                for (worker, timings) in profile.operations.iter().enumerate() {
                    let mut timings = timings.lock().unwrap();
                    timings.sort_unstable_by_key(|op| op.position);
                    assert_eq!(timings.len(), 250);
                    for (index, store) in timings.iter().enumerate() {
                        assert_eq!(store.position, (index * 4096) as u64);
                        let op = &operations[worker * 250 + index];
                        assert!(op.started <= store.started && store.started <= store.waiting);
                        assert!(store.waiting <= store.acquired && store.acquired <= store.ended && store.ended <= op.ended);
                        let ns = |a: Instant, b: Instant| b.duration_since(a).as_nanos() as u64;
                        rows.push(serde_json::json!({"worker":worker,"operation":index,
                            "capacity_before":store.capacity_before, "capacity_after":store.capacity_after,
                            "copy_fallback":store.copy_fallback, "start_ns":ns(started,op.started), "total_ns":ns(op.started,op.ended),
                            "write_ns":ns(op.started,op.written),
                            "wake_to_poll_ns":op.wake_to_poll_ns, "wakes":op.wakes,
                            "pre_store_ns":ns(op.started,store.started),
                            "store_setup_ns":ns(store.started,store.waiting),
                            "store_wait_ns":ns(store.waiting,store.acquired),
                            "store_hold_ns":ns(store.acquired,store.ended),
                            "post_store_ns":ns(store.ended,op.ended)}));
                    }
                }
                rows
            });
            #[allow(unused_mut)]
            let mut task_details: Option<Vec<serde_json::Value>> = None;
            #[allow(unused_mut)]
            let mut completion_scans: Option<Vec<[u64; 6]>> = None;
            #[cfg(feature = "append-diagnostics")]
            if profile_enabled {
                let mut rows = Vec::with_capacity(1000);
                for (worker, sink) in task_diagnostics.iter().enumerate() {
                    assert_eq!(sink.dropped(), 0);
                    let mut timings = sink.snapshot();
                    timings.sort_unstable_by_key(|op| op.position);
                    assert_eq!(timings.len(), 250);
                    for (index, timing) in timings.iter().enumerate() {
                        assert_eq!(timing.position, (index * 4096) as u64);
                        let op = &operations[worker * 250 + index];
                        let (entered, finished) = (timing.started.unwrap(), timing.store_finished.unwrap());
                        assert!(op.started <= timing.queued && timing.queued <= entered && entered <= finished);
                        assert!(finished <= timing.completed && timing.completed <= op.ended);
                        let ns = |a: Instant, b: Instant| b.duration_since(a).as_nanos() as u64;
                        rows.push(serde_json::json!({"worker":worker,"operation":index,
                            "prepare_ns":ns(op.started,timing.queued), "dispatch_ns":ns(timing.queued,entered),
                            "service_ns":ns(entered,finished), "store_polls":timing.polls, "store_wake_to_poll_ns":timing.wake_to_poll_ns, "cleanup_ns":ns(finished,timing.completed),
                            "join_ns":ns(timing.completed,op.ended), "total_ns":ns(op.started,op.ended)}));
                    }
                }
                completion_scans = Some(task_diagnostics.iter().map(|sink| sink.completion_scans()).collect());
                task_details = Some(rows);
            }
            if trial > 0 {
                println!("APPEND_BENCH {}", serde_json::json!({"kind":"filesystem", "trial":trial, "backend":backend,
                    "streams":streams, "files":streams + 1, "workers":4, "payload_bytes":4096,
                    "elapsed_s":elapsed, "append_ops_s":1000.0/elapsed, "rename_ops_s":rename_count as f64/elapsed,
                    "payload_mib_s":(1000.0*4096.0/1048576.0)/elapsed,
                    "append_elapsed_s":append_elapsed,
                    "append_active_mib_s":(1000.0*4096.0/1048576.0)/append_elapsed,
                    "list_operations":profile.as_ref().map(|p| p.lists.lock().unwrap().iter().map(|l| serde_json::json!({
                        "start_ns":l.waiting.duration_since(started).as_nanos() as u64,
                        "wait_ns":l.acquired.duration_since(l.waiting).as_nanos() as u64,
                        "hold_ns":l.ended.duration_since(l.acquired).as_nanos() as u64,"scanned":l.scanned
                    })).collect::<Vec<_>>()),
                    "completion_scans":completion_scans, "concurrent_renames":rename_count, "task_operations":task_details, "prewarm":prewarm, "prewarm_ns":prewarm_ns, "operations":details,
                    "reserved_capacity":reserve, "inplace_diagnostic":inplace, "profile":profile.as_ref().map(|p|p.snapshot()),
                    "append_sum_ns":appends.iter().sum::<u64>(),
                    "append":latency(&mut appends), "rename":latency(&mut renames)}));
            }
        }
    }
}

