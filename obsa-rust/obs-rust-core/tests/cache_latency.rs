//! Explicit local microbenchmark; never a CI timing threshold or cloud claim.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Barrier, OnceLock};
use std::time::{Duration, Instant};

use obs_rust_core::{cache::StatusCache, FileStatus};
use serde_json::{json, Value};

fn latency_summary(samples: &mut [u64]) -> Value {
    if samples.is_empty() {
        return Value::Null;
    }
    samples.sort_unstable();
    // Nearest-rank percentiles of individual operation durations, in us.
    let percentile = |p: usize| samples[(samples.len() * p).div_ceil(100) - 1] as f64 / 1000.0;
    json!({
        "count": samples.len(), "min_us": samples[0] as f64 / 1000.0,
        "p50_us": percentile(50), "p95_us": percentile(95),
        "p99_us": percentile(99), "max_us": *samples.last().unwrap() as f64 / 1000.0,
    })
}

#[test]
fn latency_summary_uses_nearest_rank() {
    let mut samples: Vec<_> = (1..=100).rev().map(|n| n * 1000).collect();
    let result = latency_summary(&mut samples);
    assert_eq!(result["p50_us"], 50.0);
    assert_eq!(result["p95_us"], 95.0);
    assert_eq!(result["p99_us"], 99.0);
    assert_eq!(result["max_us"], 100.0);
    assert_eq!(latency_summary(&mut [1234])["p99_us"], 1.234);
    assert!(latency_summary(&mut []).is_null());
}

#[test]
#[ignore = "release cache latency benchmark; run explicitly in isolation"]
fn benchmark_cache_latency() {
    const CAPACITY: usize = 4096;
    const WRITES: usize = 20_000;
    const HOT_KEYS: usize = 128;
    let scenarios = [("serial", 0, 1, false), ("mixed", 4, 2, false), ("mixed_clear", 4, 2, true)];

    // Rotate scenario order per trial; trial zero warms up and is not reported.
    for trial in 0..=5 {
        for offset in 0..scenarios.len() {
            let (name, readers, writers, clear) = scenarios[(trial + offset) % scenarios.len()];
            let cache = StatusCache::new(Duration::from_secs(600), Duration::from_secs(600), CAPACITY);
            let hot: Vec<_> = (0..HOT_KEYS).map(|i| format!("hot/{i}")).collect();
            for i in 0..CAPACITY {
                let key = if i < HOT_KEYS { hot[i].clone() } else { format!("cold/{i}") };
                cache.put_positive(&key, FileStatus::file(&key, 1, 0o644));
            }
            // Exclude fixture/string creation from put latency and run duration.
            let workloads: Vec<Vec<_>> = (0..writers).map(|worker| {
                (0..WRITES / writers).map(|i| {
                    let key = if i % 8 == 0 {
                        hot[(i / 8 + worker) % HOT_KEYS].clone()
                    } else {
                        format!("new/{worker}/{i}")
                    };
                    let status = FileStatus::file(&key, i as u64, 0o644);
                    (key, status)
                }).collect()
            }).collect();
            let ready = Barrier::new(readers + writers + 1);
            let go = Barrier::new(readers + writers + 1);
            let remaining = AtomicUsize::new(writers);
            let started = OnceLock::<Instant>::new();
            let (mut puts, mut clears, read_count, read_hits, elapsed_ms) = std::thread::scope(|scope| {
                let mut reader_threads = Vec::new();
                for worker in 0..readers {
                    let (cache, hot, ready, go, remaining) = (&cache, &hot, &ready, &go, &remaining);
                    reader_threads.push(scope.spawn(move || {
                        let (mut count, mut hits) = (0u64, 0u64);
                        ready.wait();
                        go.wait();
                        while remaining.load(Ordering::Acquire) != 0 {
                            for _ in 0..64 {
                                hits += u64::from(cache.get(&hot[(count as usize + worker) % HOT_KEYS]).is_some());
                                count += 1;
                            }
                        }
                        (count, hits)
                    }));
                }
                let mut writer_threads = Vec::new();
                for (worker, workload) in workloads.into_iter().enumerate() {
                    let (cache, ready, go, remaining, started) = (&cache, &ready, &go, &remaining, &started);
                    writer_threads.push(scope.spawn(move || {
                        let mut puts = Vec::with_capacity(workload.len());
                        let mut clears = Vec::with_capacity(workload.len() / 1024);
                        ready.wait();
                        go.wait();
                        for (i, (key, status)) in workload.into_iter().enumerate() {
                            let before = Instant::now();
                            cache.put_positive(key, status);
                            puts.push(before.elapsed().as_nanos() as u64);
                            if clear && worker == 0 && (i + 1) % 1024 == 0 {
                                let before = Instant::now();
                                cache.invalidate_prefix("/");
                                clears.push(before.elapsed().as_nanos() as u64);
                            }
                        }
                        let elapsed_ms = started.get().unwrap().elapsed().as_secs_f64() * 1000.0;
                        remaining.fetch_sub(1, Ordering::Release);
                        (puts, clears, elapsed_ms)
                    }));
                }
                ready.wait();
                started.set(Instant::now()).unwrap();
                go.wait();
                let (mut puts, mut clears, mut elapsed_ms) = (Vec::new(), Vec::new(), 0.0f64);
                for writer in writer_threads {
                    let (p, c, elapsed) = writer.join().unwrap();
                    puts.extend(p);
                    clears.extend(c);
                    elapsed_ms = elapsed_ms.max(elapsed);
                }
                let (mut read_count, mut read_hits) = (0, 0);
                for reader in reader_threads {
                    let (count, hits) = reader.join().unwrap();
                    read_count += count;
                    read_hits += hits;
                }
                (puts, clears, read_count, read_hits, elapsed_ms)
            });
            assert_eq!(puts.len(), WRITES);
            assert_eq!(clears.len(), if clear { WRITES / writers / 1024 } else { 0 });
            assert!(cache.len() <= CAPACITY);
            let put_summary = latency_summary(&mut puts);
            let clear_summary = latency_summary(&mut clears);
            if trial > 0 {
                println!("CACHE_BENCH_JSON {}", json!({
                    "scenario": name, "trial": trial, "capacity": CAPACITY,
                    "readers": readers, "writers": writers, "writes": WRITES,
                    "hot_keys": HOT_KEYS, "hot_write_every": 8, "ttl_seconds": 600,
                    "writer_elapsed_ms": elapsed_ms, "puts": put_summary, "clears": clear_summary,
                    "reads": read_count, "read_hits": read_hits, "final_entries": cache.len(),
                }));
            }
        }
    }
}

// Force the lazy index to refresh every live entry before one admission.
// This isolates an algorithmic cost; it does not explain OS scheduling outliers.
#[test]
#[ignore = "release promoted-cache benchmark; run explicitly in isolation"]
fn benchmark_promoted_cache_admission() {
    use std::collections::VecDeque;

    for trial in 0..=5 {
        for capacity in [128, 4096] {
            let cache = StatusCache::new(
                Duration::from_secs(600), Duration::from_secs(600), capacity,
            );
            let mut keys = VecDeque::new();
            for i in 0..capacity {
                let key = format!("initial/{i}");
                cache.put_positive(&key, FileStatus::file(&key, 1, 0o644));
                keys.push_back(key);
            }
            let mut samples = Vec::with_capacity(50);
            for round in 0..50 {
                let key = format!("new/{round}");
                let status = FileStatus::file(&key, 1, 0o644);
                for existing in &keys {
                    assert!(cache.get_positive(existing).is_some());
                }
                let before = Instant::now();
                cache.put_positive(&key, status);
                samples.push(before.elapsed().as_nanos() as u64);
                let oldest = keys.pop_front().unwrap();
                assert!(cache.get(&oldest).is_none());
                keys.push_back(key);
                assert_eq!(cache.len(), capacity);
            }
            let puts = latency_summary(&mut samples);
            if trial > 0 {
                println!("CACHE_PROMOTION_JSON {}", json!({
                    "capacity": capacity, "trial": trial, "puts": puts,
                    "promotions_before_each_put": capacity,
                }));
            }
        }
    }
}
