//! Delayed append with continuous conflicting file renames. One renamer per
//! writer; each stream must follow its file across both names.
use super::*;
use std::sync::atomic::AtomicBool;

// A host pause can jump past the watchdog before it is polled. Keep late
// completions in a bounded overflow bucket; the elapsed-time check still fails.
fn record_progress(bins: &mut [u64], elapsed: Duration) {
    let index = (elapsed.as_secs() / 5).min((bins.len() - 1) as u64) as usize;
    bins[index] += 1;
}

#[test]
fn progress_clock_jump_uses_overflow_bucket() {
    let mut bins = [0; 3];
    record_progress(&mut bins, Duration::ZERO);
    record_progress(&mut bins, Duration::from_secs(5));
    record_progress(&mut bins, Duration::from_secs(u64::MAX));
    assert_eq!(bins, [1, 1, 1]);
}

fn setting(name: &str, default: u64, min: u64, max: u64) -> u64 {
    let value = std::env::var(name).map(|v| v.parse::<u64>().expect("integer benchmark setting")).unwrap_or(default);
    assert!((min..=max).contains(&value), "{name} outside {min}..={max}");
    value
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "duration-based delayed append and rename stress; release only, run in isolation"]
async fn benchmark_append_mixed() {
    let seconds = setting("OBS_APPEND_STRESS_SECONDS", 60, 1, 3600);
    let workers = setting("OBS_APPEND_STRESS_WORKERS", 4, 1, 16) as usize;
    let batch = setting("OBS_APPEND_STRESS_BATCH", 16, 1, 64) as usize;
    let concurrency = setting("OBS_APPEND_BENCH_CONCURRENCY", 4, 1, 256) as usize;
    let budget = setting("OBS_APPEND_BENCH_TOTAL_BYTES", 67108864, 4096, u32::MAX as u64);
    let delay = setting("OBS_APPEND_BENCH_DELAY_MS", 2, 1, 1000);
    let store = Arc::new(BenchmarkStore::new(false, false, None).with_append_delay(Duration::from_millis(delay)).with_metadata_only_head());
    let fs = Arc::new(ObsFileSystem::with_store(ObsConfig {
        pfs_append_concurrency: concurrency, pfs_append_total_bytes: budget, ..Default::default()
    }, store.clone()));
    let barrier = Arc::new(Barrier::new(workers + 1));
    let mut tasks = Vec::new();
    for worker in 0..workers {
        let (store, fs, barrier) = (store.clone(), fs.clone(), barrier.clone());
        tasks.push(tokio::spawn(async move {
            let names = [format!("mixed-{worker}-a"), format!("mixed-{worker}-b")];
            let mut current = 0usize;
            let payload = vec![(worker + 1) as u8; 4096];
            let mut samples = Vec::new();
            let mut rename_times = Vec::new();
            let mut append_bins = vec![0u64; seconds as usize / 5 + 26];
            let mut rename_bins = append_bins.clone();
            let mut append_gap_ns = 0u64;
            let mut rename_gap_ns = 0u64;
            let mut cancelled_flushes = 0u64;
            let mut epochs = 0;
            let mut maintenance_ns = 0u64;
            barrier.wait().await;
            let start = Instant::now();
            let mut last_append = start;
            let mut last_rename = start;
            loop {
                let maintenance = Instant::now();
                let key = &names[current];
                store.put(key, bytes::BytesMut::with_capacity(64 * batch * 4096).freeze(), PutHeaders::default()).await.unwrap();
                let mut stream = fs.append_stream(&format!("/{key}"), 0).await.unwrap();
                maintenance_ns += maintenance.elapsed().as_nanos() as u64;
                let stop = Arc::new(AtomicBool::new(false));
                let (rename_fs, rename_stop, rename_names) = (fs.clone(), stop.clone(), names.clone());
                let renamer = tokio::spawn(async move {
                    let mut times = Vec::new();
                    let mut bins = vec![0u64; seconds as usize / 5 + 26];
                    let mut gap_ns = 0;
                    let mut last = last_rename;
                    let mut index = current;
                    while !rename_stop.load(Ordering::Relaxed) {
                        let operation = Instant::now();
                        assert!(rename_fs.rename(&format!("/{}", rename_names[index]),
                            &format!("/{}", rename_names[1 - index]), false).await.unwrap());
                        index = 1 - index;
                        let ended = Instant::now();
                        assert!(times.len() < 1_000_000, "rename sample safety limit");
                        times.push(ended.duration_since(operation).as_nanos() as u64);
                        gap_ns = gap_ns.max(ended.duration_since(last).as_nanos() as u64);
                        last = ended;
                        record_progress(&mut bins, start.elapsed());
                        tokio::task::yield_now().await;
                    }
                    (index, times, bins, gap_ns, last)
                });
                let mut completed = 0;
                for _ in 0..64 {
                    let operation = Instant::now();
                    for _ in 0..batch { stream.write(&payload).await.unwrap(); }
                    // Poll, then drop a pending flush without dropping its stream.
                    // Subsequent flush must still own and join every append task.
                    if samples.len() % 17 == 0 {
                        let mut flush = Box::pin(stream.flush());
                        if futures::poll!(&mut flush).is_pending() { cancelled_flushes += 1; }
                    }
                    stream.flush().await.unwrap();
                    assert!(samples.len() < 1_000_000, "sample safety limit reached");
                    let ended = Instant::now();
                    samples.push(ended.duration_since(operation).as_nanos() as u64);
                    append_gap_ns = append_gap_ns.max(ended.duration_since(last_append).as_nanos() as u64);
                    last_append = ended;
                    record_progress(&mut append_bins, start.elapsed());
                    completed += 1;
                    if start.elapsed() >= Duration::from_secs(seconds) { break; }
                }
                let maintenance = Instant::now();
                stop.store(true, Ordering::Relaxed);
                let (index, times, bins, gap, last) = renamer.await.unwrap();
                current = index;
                assert!(rename_times.len() + times.len() <= 1_000_000, "total rename sample safety limit");
                rename_times.extend(times);
                for (total, count) in rename_bins.iter_mut().zip(bins) { *total += count; }
                rename_gap_ns = rename_gap_ns.max(gap);
                last_rename = last;
                stream.close().await.unwrap();
                assert!(store.snapshot(&names[1 - current]).is_err());
                let data = store.snapshot(&names[current]).unwrap();
                assert_eq!(data.len(), completed * batch * 4096);
                assert!(data.iter().all(|byte| *byte == (worker + 1) as u8));
                drop(data);
                maintenance_ns += maintenance.elapsed().as_nanos() as u64;
                epochs += 1;
                if start.elapsed() >= Duration::from_secs(seconds) { break; }
            }
            (samples, rename_times, cancelled_flushes, epochs, maintenance_ns, append_bins, rename_bins, append_gap_ns, rename_gap_ns)
        }));
    }
    let start = Instant::now();
    barrier.wait().await;
    let results = tokio::time::timeout(Duration::from_secs(seconds + 120), futures::future::join_all(tasks))
        .await.expect("append stress did not drain before watchdog deadline");
    let elapsed = start.elapsed().as_secs_f64();
    assert!(elapsed < (seconds + 120) as f64, "elapsed time exceeded watchdog despite ready tasks");
    let mut samples = Vec::new();
    let mut worker_stats = Vec::new();
    let mut renames = Vec::new();
    for result in results {
        let (mut times, mut rename_times, cancelled, epochs, maintenance_ns, append_bins, rename_bins, append_gap_ns, rename_gap_ns) = result.unwrap();
        worker_stats.push(serde_json::json!({"batch_latency":latency(&mut times),
            "rename_latency":latency(&mut rename_times),"append_bins_5s":append_bins,"rename_bins_5s":rename_bins,
            "append_max_progress_gap_ns":append_gap_ns,"rename_max_progress_gap_ns":rename_gap_ns,
            "cancelled_flushes":cancelled,"epochs":epochs,"maintenance_ns":maintenance_ns}));
        samples.extend(times);
        renames.extend(rename_times);
    }
    let requests = samples.len() * batch;
    let stats = store.delay_stats();
    assert_eq!(stats["calls"].as_u64().unwrap(), requests as u64);
    assert_eq!(stats["active"].as_u64().unwrap(), 0);
    assert!(stats["peak"].as_u64().unwrap() <= (workers * concurrency).min((budget / 4096) as usize) as u64);
    println!("APPEND_MIXED {}", serde_json::json!({"seconds":seconds,"elapsed_s":elapsed,
        "workers":workers,"batch":batch,"concurrency":concurrency,"budget_bytes":budget,"delay_ms":delay,
        "renames":renames.len(),"rename_ops_s":renames.len() as f64 / elapsed,
        "rename_latency":latency(&mut renames),"requests":requests,"payload_mib_s":requests as f64 / 256.0 / elapsed,
        "batch_latency":latency(&mut samples),"worker_stats":worker_stats,"delay_stats":stats}));
}
