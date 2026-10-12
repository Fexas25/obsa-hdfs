//! Isolated release benchmark. Coordinator service work is synthetic, not PFS I/O.
use super::*;
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use tokio::sync::Barrier;

fn latency(samples: &mut [u64]) -> Value {
    samples.sort_unstable();
    let rank = |p: usize| samples[(samples.len() * p).div_ceil(100) - 1] as f64 / 1000.0;
    json!({"count": samples.len(), "p50_us": rank(50), "p95_us": rank(95),
        "p99_us": rank(99), "max_us": *samples.last().unwrap() as f64 / 1000.0})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "release append/rename coordinator benchmark; run explicitly in isolation"]
async fn benchmark_append_coordination() {
    let workers = std::env::var("OBS_APPEND_BENCH_WORKERS").map_or(4, |value| value.parse::<usize>().expect("worker count must be an integer"));
    assert!((1..=256).contains(&workers));
    // One unreported warm-up, then three trials; rotate scenario ordering.
    let scenarios = [(32, false, false, 0), (1024, false, false, 0),
        (8192, false, false, 0), (8192, true, false, 0),
        (8192, false, true, 0), (1024, false, false, 1)];
    for trial in 0..=3 {
        for offset in 0..scenarios.len() {
            let (streams, same_file, directory, delay_ms) = scenarios[(trial + offset) % scenarios.len()];
            let coordinator = Arc::new(AppendCoordinator::default());
            let mut targets = Vec::with_capacity(streams);
            let mut registration = Vec::with_capacity(streams);
            let setup = Instant::now();
            for index in 0..streams {
                let key = format!("files/{}", if same_file { 0 } else { index });
                let started = Instant::now();
                targets.push(coordinator.register(key).await);
                registration.push(started.elapsed().as_nanos() as u64);
            }
            let setup_ms = setup.elapsed().as_secs_f64() * 1000.0;
            let targets = Arc::new(targets);
            let barrier = Arc::new(Barrier::new(workers + 2));
            let mut tasks = Vec::new();
            for worker in 0..workers {
                let (coordinator, targets, barrier) = (coordinator.clone(), targets.clone(), barrier.clone());
                tasks.push(tokio::spawn(async move {
                    let mut samples = Vec::with_capacity(1000);
                    barrier.wait().await;
                    for operation in 0..1000 {
                        let target = &targets[(operation * workers + worker) % targets.len()];
                        let started = Instant::now();
                        let (permit, key) = coordinator.append(target).await.unwrap();
                        assert!(key.starts_with("files/") || key.starts_with("moved"));
                        if delay_ms == 0 { tokio::task::yield_now().await; }
                        else { tokio::time::sleep(Duration::from_millis(delay_ms)).await; }
                        drop(permit);
                        samples.push(started.elapsed().as_nanos() as u64);
                    }
                    samples
                }));
            }
            let (rename_coordinator, rename_barrier) = (coordinator.clone(), barrier.clone());
            let rename = tokio::spawn(async move {
                let mut samples = Vec::with_capacity(200);
                let (a, b) = if directory { ("files", "moved") } else { ("files/0", "moved") };
                rename_barrier.wait().await;
                for operation in 0..200 {
                    let (source, destination) = if operation % 2 == 0 { (a, b) } else { (b, a) };
                    let started = Instant::now();
                    let guard = rename_coordinator.rename(source, destination, directory).await;
                    if delay_ms == 0 { tokio::task::yield_now().await; }
                    else { tokio::time::sleep(Duration::from_millis(delay_ms)).await; }
                    guard.finish(&Ok(()));
                    samples.push(started.elapsed().as_nanos() as u64);
                }
                samples
            });
            let started = Instant::now();
            barrier.wait().await;
            let mut appends = Vec::with_capacity(workers * 1000);
            for task in tasks { appends.extend(task.await.unwrap()); }
            let mut renames = rename.await.unwrap();
            let elapsed = started.elapsed().as_secs_f64();
            assert_eq!(appends.len(), workers * 1000);
            assert_eq!(renames.len(), 200);
            for (index, target) in targets.iter().enumerate() {
                assert_eq!(target.key().unwrap(), format!("files/{}", if same_file { 0 } else { index }));
            }
            if trial > 0 {
                println!("APPEND_BENCH {}", json!({"kind":"coordinator", "trial":trial,
                    "streams":streams, "files":if same_file {1} else {streams}, "directory":directory,
                    "service_delay_ms":delay_ms, "workers":workers, "registration_ms":setup_ms,
                    "registration":latency(&mut registration), "elapsed_s":elapsed,
                    "append_ops_s":(workers * 1000) as f64/elapsed, "rename_ops_s":200.0/elapsed,
                    "append":latency(&mut appends), "rename":latency(&mut renames)}));
            }
        }
    }
}
#[tokio::test]
#[ignore = "isolated exact-index construction cost; run release without competing work"]
async fn benchmark_append_index_build() {
    for streams in [32, 8192] {
        for streams_per_path in [1, 2, 4, streams] {
            let same_file = streams_per_path == streams;
            let coordinator = Arc::new(AppendCoordinator::default());
            let mut targets = Vec::with_capacity(streams);
            for index in 0..streams {
                targets.push(coordinator.register(format!("file-{}", index / streams_per_path)).await);
            }
            for trial in 0..=10 {
                coordinator.state.lock().unwrap().exact_index = None;
                let started = Instant::now();
                let mut state = coordinator.state.lock().unwrap();
                let acquired = Instant::now();
                state.ensure_exact_index();
                let built = Instant::now();
                assert_eq!(state.exact_index.as_ref().unwrap().len(), streams.div_ceil(streams_per_path));
                let index_capacity = state.exact_index.as_ref().unwrap().capacity();
                drop(state);
                if trial > 0 {
                    println!("APPEND_INDEX {}", json!({"streams":streams,"streams_per_path":streams_per_path,"same_file":same_file,"trial":trial,
                        "slot_value_bytes":std::mem::size_of::<TargetSlots>(),
                        "entry_payload_bytes":std::mem::size_of::<(String, TargetSlots)>(), "index_capacity":index_capacity,
                        "mutex_wait_ns":acquired.duration_since(started).as_nanos() as u64,
                        "build_under_lock_ns":built.duration_since(acquired).as_nanos() as u64}));
                }
            }
        }
    }
}

#[tokio::test]
#[ignore = "index component probes; non-additive costs, release only"]
async fn benchmark_append_index_phases() {
    use std::hint::black_box;
    use std::hash::BuildHasher;
    for streams in [32, 8192] {
        for group in [1, 2, 4, streams] {
            let coordinator = Arc::new(AppendCoordinator::default());
            let mut targets = Vec::with_capacity(streams);
            for index in 0..streams {
                targets.push(coordinator.register(format!("file-{}", index / group)).await);
            }
            let state = coordinator.state.lock().unwrap();
            for trial in 0..=10 {
                let start = Instant::now();
                let live = state.targets.iter().filter(|entry| black_box(entry).target.strong_count() > 0).count();
                let traversal_ns = start.elapsed().as_nanos() as u64;
                assert_eq!(live, streams);
                let mut keys = Vec::with_capacity(streams);
                let start = Instant::now();
                for entry in &state.targets { keys.push(black_box(&entry.key).clone()); }
                let clone_ns = start.elapsed().as_nanos() as u64;
                let hasher = std::collections::hash_map::RandomState::new();
                let start = Instant::now();
                for key in &keys { black_box(hasher.hash_one(black_box(key))); }
                let hash_ns = start.elapsed().as_nanos() as u64;
                // Prepared-key insertion excludes key allocation, but changes cache locality.
                let start = Instant::now();
                let mut prepared: HashMap<String, TargetSlots> = HashMap::new();
                for (slot, key) in keys.into_iter().enumerate() {
                    prepared.entry(key).or_default().push(slot);
                }
                let prepared_ns = start.elapsed().as_nanos() as u64;
                assert_eq!(prepared.len(), streams.div_ceil(group));
                drop(prepared);
                // Alternate order: capacity is the only difference in these full builds.
                for order in 0..2 {
                    let reserved = (trial + order) % 2 == 0;
                    let start = Instant::now();
                    let mut index: HashMap<String, TargetSlots> = if reserved {
                        HashMap::with_capacity(streams)
                    } else { HashMap::new() };
                    for (slot, entry) in state.targets.iter().enumerate() {
                        if entry.target.strong_count() > 0 {
                            index.entry(entry.key.clone()).or_default().push(slot);
                        }
                    }
                    let build_ns = start.elapsed().as_nanos() as u64;
                    assert_eq!(index.len(), streams.div_ceil(group));
                    if trial > 0 {
                        println!("APPEND_INDEX_PHASE {}", json!({"trial":trial,"streams":streams,"streams_per_path":group,
                            "traversal_ns":traversal_ns,"clone_ns":clone_ns,"hash_ns":hash_ns,"prepared_insert_ns":prepared_ns,
                            "reserved":reserved,"build_ns":build_ns,"capacity":index.capacity()}));
                    }
                }
            }
        }
    }
}

