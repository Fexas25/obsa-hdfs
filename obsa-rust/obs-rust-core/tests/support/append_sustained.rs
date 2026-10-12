//! Duration-based, closed-loop delayed append stress. Storage is bounded by
//! reopening an empty object every 64 batches, after close and byte verification.
use super::*;

fn setting(name: &str, default: u64, min: u64, max: u64) -> u64 {
    let value = std::env::var(name).map(|v| v.parse::<u64>().expect("integer benchmark setting")).unwrap_or(default);
    assert!((min..=max).contains(&value), "{name} outside {min}..={max}");
    value
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "duration-based delayed append stress; release only, run in isolation"]
async fn benchmark_append_sustained() {
    let seconds = setting("OBS_APPEND_STRESS_SECONDS", 60, 1, 3600);
    let workers = setting("OBS_APPEND_STRESS_WORKERS", 4, 1, 16) as usize;
    let batch = setting("OBS_APPEND_STRESS_BATCH", 16, 1, 64) as usize;
    let concurrency = setting("OBS_APPEND_BENCH_CONCURRENCY", 4, 1, 256) as usize;
    let budget = setting("OBS_APPEND_BENCH_TOTAL_BYTES", 67108864, 4096, u32::MAX as u64);
    let delay = setting("OBS_APPEND_BENCH_DELAY_MS", 2, 1, 1000);
    let store = Arc::new(BenchmarkStore::new(false, false, None).with_append_delay(Duration::from_millis(delay)));
    let fs = Arc::new(ObsFileSystem::with_store(ObsConfig {
        pfs_append_concurrency: concurrency, pfs_append_total_bytes: budget, ..Default::default()
    }, store.clone()));
    let barrier = Arc::new(Barrier::new(workers + 1));
    let mut tasks = Vec::new();
    for worker in 0..workers {
        let (store, fs, barrier) = (store.clone(), fs.clone(), barrier.clone());
        tasks.push(tokio::spawn(async move {
            let key = format!("sustained-{worker}");
            let payload = vec![(worker + 1) as u8; 4096];
            let mut samples = Vec::new();
            let mut cancelled_flushes = 0u64;
            let mut epochs = 0;
            let mut maintenance_ns = 0u64;
            barrier.wait().await;
            let start = Instant::now();
            loop {
                let maintenance = Instant::now();
                store.put(&key, bytes::BytesMut::with_capacity(64 * batch * 4096).freeze(), PutHeaders::default()).await.unwrap();
                let mut stream = fs.append_stream(&format!("/{key}"), 0).await.unwrap();
                maintenance_ns += maintenance.elapsed().as_nanos() as u64;
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
                    samples.push(operation.elapsed().as_nanos() as u64);
                    completed += 1;
                    if start.elapsed() >= Duration::from_secs(seconds) { break; }
                }
                let maintenance = Instant::now();
                stream.close().await.unwrap();
                let data = store.snapshot(&key).unwrap();
                assert_eq!(data.len(), completed * batch * 4096);
                assert!(data.iter().all(|byte| *byte == (worker + 1) as u8));
                drop(data);
                maintenance_ns += maintenance.elapsed().as_nanos() as u64;
                epochs += 1;
                if start.elapsed() >= Duration::from_secs(seconds) { break; }
            }
            (samples, cancelled_flushes, epochs, maintenance_ns)
        }));
    }
    let start = Instant::now();
    barrier.wait().await;
    let results = tokio::time::timeout(Duration::from_secs(seconds + 120), futures::future::join_all(tasks))
        .await.expect("append stress did not drain before watchdog deadline");
    let elapsed = start.elapsed().as_secs_f64();
    let mut samples = Vec::new();
    let mut worker_stats = Vec::new();
    for result in results {
        let (mut times, cancelled, epochs, maintenance_ns) = result.unwrap();
        worker_stats.push(serde_json::json!({"batch_latency":latency(&mut times),
            "cancelled_flushes":cancelled,"epochs":epochs,"maintenance_ns":maintenance_ns}));
        samples.extend(times);
    }
    let requests = samples.len() * batch;
    let stats = store.delay_stats();
    assert_eq!(stats["calls"].as_u64().unwrap(), requests as u64);
    assert_eq!(stats["active"].as_u64().unwrap(), 0);
    assert!(stats["peak"].as_u64().unwrap() <= (workers * concurrency).min((budget / 4096) as usize) as u64);
    println!("APPEND_SUSTAINED {}", serde_json::json!({"seconds":seconds,"elapsed_s":elapsed,
        "workers":workers,"batch":batch,"concurrency":concurrency,"budget_bytes":budget,"delay_ms":delay,
        "requests":requests,"payload_mib_s":requests as f64 / 256.0 / elapsed,
        "batch_latency":latency(&mut samples),"worker_stats":worker_stats,"delay_stats":stats}));
}
