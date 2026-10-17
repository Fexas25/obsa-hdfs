//! Component probe only: collection construction, key insertion and destruction.
//! This does not measure filesystem, HTTP, directory visibility or cursor overhead.
use std::collections::{BTreeSet, HashSet};
use std::hint::black_box;
use std::time::Instant;

fn measure(mode: usize, keys: &[String], iterations: usize) -> f64 {
    let started = Instant::now();
    let mut checksum = 0;
    for _ in 0..iterations {
        match mode {
            0 => {
                let mut seen = BTreeSet::new();
                for key in black_box(keys) { seen.insert(key.clone()); }
                checksum += black_box(&seen).len();
            }
            1 => {
                let mut seen = HashSet::with_capacity(keys.len());
                for key in black_box(keys) { seen.insert(key.clone()); }
                checksum += black_box(&seen).len();
            }
            2 => {
                let mut seen = BTreeSet::new();
                for key in black_box(keys) { seen.insert(key.as_str()); }
                checksum += black_box(&seen).len();
            }
            3 => {
                let mut seen = HashSet::with_capacity(keys.len());
                for key in black_box(keys) { seen.insert(key.as_str()); }
                checksum += black_box(&seen).len();
            }
            _ => unreachable!(),
        }
    }
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(checksum, iterations * keys.len());
    elapsed_ms
}

fn main() {
    println!("entries,round,variant,iterations,elapsed_ms,ns_per_key");
    let names = ["tree_owned", "hash_owned", "tree_borrowed", "hash_borrowed"];
    for count in [1, 4, 8, 16, 32, 64, 128, 1000] {
        let keys: Vec<String> = (0..count).map(|index| format!("bench/file{index:09}")).collect();
        let iterations = 1_000_000 / count;
        for round in 0..=5 {
            let order = if round % 2 == 0 { [0, 1, 2, 3] } else { [3, 2, 1, 0] };
            for mode in order {
                let ms = measure(mode, &keys, iterations);
                if round > 0 {
                    println!("{count},{round},{},{iterations},{ms:.6},{:.3}",
                        names[mode], ms * 1e6 / (iterations * count) as f64);
                }
            }
        }
    }
}
