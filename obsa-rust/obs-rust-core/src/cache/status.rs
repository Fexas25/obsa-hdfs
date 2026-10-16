use std::cmp::Reverse;
use std::collections::{binary_heap::PeekMut, BinaryHeap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

use dashmap::DashMap;

use crate::types::FileStatus;

// (last observed access order, admission generation, key)
type EvictionIndex = BinaryHeap<Reverse<(u64, u64, String)>>;

/// Sharded status cache with TTL and negative caching.
///
/// - Positive entries: status=Some(FileStatus), TTL 1000ms default
/// - Negative entries: status=None (confirmed missing), TTL 300ms default
/// - Max 4096 entries, evict oldest on overflow
/// - Back-fill from LIST populates all children + parent directory
pub struct StatusCache {
    entries: DashMap<String, CacheEntry>,
    positive_ttl: Duration,
    negative_ttl: Duration,
    max_size: usize,
    insertion: Mutex<EvictionIndex>,
    invalidation_epoch: RwLock<u64>,
    access_counter: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
}

struct CacheEntry {
    status: Option<FileStatus>,
    inserted_at: Instant,
    access_order: AtomicU64,
    generation: u64,
}

impl StatusCache {
    pub fn new(positive_ttl: Duration, negative_ttl: Duration, max_size: usize) -> Self {
        Self {
            entries: DashMap::new(),
            positive_ttl,
            negative_ttl,
            max_size,
            insertion: Mutex::new(BinaryHeap::new()),
            invalidation_epoch: RwLock::new(0),
            access_counter: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub(crate) fn snapshot_epoch(&self) -> u64 {
        *self.invalidation_epoch.read().unwrap_or_else(|e| e.into_inner())
    }

    /// Admit a remote query result only if no invalidation crossed its lifetime.
    /// Hold the read guard through put so invalidation cannot race admission.
    pub(crate) fn put_if_unchanged(&self, key: &str, status: Option<FileStatus>, epoch: u64) {
        let current = self.invalidation_epoch.read().unwrap_or_else(|e| e.into_inner());
        if *current == epoch {
            self.put(key, status);
        }
    }

    /// A listing proves existence, not replacement metadata. Keep a live
    /// positive entry without extending its TTL or changing its access order.
    pub(crate) fn put_directory_hint_if_unchanged(&self, key: &str, status: FileStatus, epoch: u64) {
        let current = self.invalidation_epoch.read().unwrap_or_else(|e| e.into_inner());
        if *current == epoch {
            self.put_entry(key, Some(status), true);
        }
    }

    /// Complete a mutation without admitting a response overtaken by invalidation.
    /// Invalidate even on conflict: the remote mutation may itself have committed
    /// after the competing operation. A fresh query must decide the current state.
    pub(crate) fn complete_mutation(&self, key: &str, status: FileStatus, epoch: u64) {
        let mut current = self.invalidation_epoch.write().unwrap_or_else(|e| e.into_inner());
        let unchanged = *current == epoch;
        *current = current.wrapping_add(1);
        self.entries.remove(key);
        self.remove_ancestors(key);
        if unchanged {
            self.put_positive(key, status);
        }
    }

    pub fn get(&self, key: &str) -> Option<Option<FileStatus>> {
        let Some(entry) = self.entries.get(key) else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        let ttl = if entry.status.is_some() { self.positive_ttl } else { self.negative_ttl };
        // Comparing age avoids overflowing Instant for a large configured TTL.
        if entry.inserted_at.elapsed() >= ttl {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        // LRU promotion
        let order = self.access_counter.fetch_add(1, Ordering::Relaxed);
        entry.access_order.fetch_max(order, Ordering::Relaxed);

        self.hits.fetch_add(1, Ordering::Relaxed);
        Some(entry.status.clone())
    }

    pub fn get_positive(&self, key: &str) -> Option<FileStatus> {
        match self.get(key)? {
            Some(status) => Some(status),
            None => None, // negative cache hit
        }
    }

    pub fn is_negative(&self, key: &str) -> bool {
        matches!(self.get(key), Some(None))
    }

    pub fn put_positive(&self, key: impl Into<String>, status: FileStatus) {
        self.put(key, Some(status));
    }

    pub fn put_negative(&self, key: impl Into<String>) {
        self.put(key, None);
    }

    fn put(&self, key: impl Into<String>, status: Option<FileStatus>) {
        self.put_entry(key, status, false);
    }

    fn put_entry(&self, key: impl Into<String>, status: Option<FileStatus>, preserve_positive: bool) {
        if self.max_size == 0 {
            return;
        }
        let key = key.into();
        let order = self.access_counter.fetch_add(1, Ordering::Relaxed);
        let entry = CacheEntry {
            status,
            inserted_at: Instant::now(),
            access_order: AtomicU64::new(order),
            generation: order,
        };
        // Repeated LIST/status backfills normally replace existing entries.
        // No capacity scan or unrelated eviction is needed on this hot path.
        if let Some(mut existing) = self.entries.get_mut(&key) {
            if preserve_positive && existing.status.is_some()
                && existing.inserted_at.elapsed() < self.positive_ttl {
                return;
            }
            existing.status = entry.status;
            existing.inserted_at = entry.inserted_at;
            existing.access_order.fetch_max(order, Ordering::Relaxed);
            return;
        }
        // Serialize only new-key admission, keeping concurrent inserts bounded.
        // Never acquire this mutex while holding a shard guard.
        let mut index = self.insertion.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(mut existing) = self.entries.get_mut(&key) {
            if preserve_positive && existing.status.is_some()
                && existing.inserted_at.elapsed() < self.positive_ttl {
                return;
            }
            existing.status = entry.status;
            existing.inserted_at = entry.inserted_at;
            existing.access_order.fetch_max(order, Ordering::Relaxed);
            return;
        }
        // Invalidations leave lazy tombstones. Bound their space even when
        // the cache never fills; rebuild only after a capacity of churn.
        if index.len() >= self.max_size.saturating_mul(2) {
            *index = self
                .entries
                .iter()
                .map(|e| {
                    Reverse((
                        e.access_order.load(Ordering::Relaxed),
                        e.generation,
                        e.key().clone(),
                    ))
                })
                .collect();
        }
        if self.entries.len() >= self.max_size {
            self.evict_oldest(&mut index);
        }
        index.push(Reverse((order, entry.generation, key.clone())));
        self.entries.insert(key, entry);
    }

    pub fn invalidate(&self, key: &str) {
        let mut epoch = self.invalidation_epoch.write().unwrap_or_else(|e| e.into_inner());
        *epoch = epoch.wrapping_add(1);
        self.entries.remove(key);
    }

    pub fn invalidate_prefix(&self, prefix: &str) {
        let mut epoch = self.invalidation_epoch.write().unwrap_or_else(|e| e.into_inner());
        *epoch = epoch.wrapping_add(1);
        let root = prefix.trim_end_matches('/');
        if root.is_empty() {
            // Freeze admission before taking shard locks, as in put/len.
            // Drop the index allocation too: clear() would retain its capacity
            // and lazy tombstones would otherwise retain path strings.
            let mut index = self.insertion.lock().unwrap_or_else(|e| e.into_inner());
            self.entries.clear();
            *index = EvictionIndex::new();
            return;
        }
        let children = format!("{root}/");
        self.entries
            .retain(|k, _| !(k == root || k.starts_with(&children)));
    }

    /// Clear negative entries for all ancestor paths of key.
    /// e.g. invalidate_ancestors("/a/b/c") clears "/a/", "/a/b/"
    pub fn invalidate_ancestors(&self, key: &str) {
        let mut epoch = self.invalidation_epoch.write().unwrap_or_else(|e| e.into_inner());
        *epoch = epoch.wrapping_add(1);
        self.remove_ancestors(key);
    }

    // Caller holds the invalidation write lock.
    fn remove_ancestors(&self, key: &str) {
        let key = key.trim_start_matches('/');
        let parts: Vec<&str> = key.split('/').filter(|p| !p.is_empty()).collect();
        for i in 1..parts.len() {
            let ancestor = parts[..i].join("/");
            self.entries.remove(&ancestor);
            self.entries.remove(&format!("{ancestor}/"));
        }
    }

    /// Back-fill all LIST entries and parent directory.
    pub fn backfill_from_list(&self, parent_path: &str, entries: &[FileStatus]) {
        for entry in entries {
            self.put_positive(normalize_cache_key(&entry.path), entry.clone());
        }
        let parent = normalize_cache_key(parent_path);
        if !parent.is_empty() {
            self.put_entry(parent.clone(), Some(FileStatus::dir(parent, 0o755)), true);
        }
    }

    fn evict_oldest(&self, index: &mut EvictionIndex) {
        // Heap timestamps are lower bounds: reads and replacements only raise
        // access_order. Refresh stale minima until the true oldest is found.
        // Clamp concurrent promotions to a snapshot so busy readers cannot
        // indefinitely delay admission. Reads never acquire the index mutex.
        let cutoff = self.access_counter.load(Ordering::Relaxed);
        while let Some(mut oldest) = index.peek_mut() {
            let Reverse((order, generation, key)) = &*oldest;
            let (order, generation) = (*order, *generation);
            let Some(entry) = self.entries.get(key) else {
                PeekMut::pop(oldest);
                continue;
            };
            if entry.generation != generation {
                drop(entry);
                PeekMut::pop(oldest);
                continue;
            }
            let current = entry.access_order.load(Ordering::Relaxed).min(cutoff);
            drop(entry);
            if current != order {
                // Refresh in place. Dropping PeekMut repairs the heap once,
                // instead of removing and then reinserting the same key.
                oldest.0.0 = current;
                continue;
            }
            if self
                .entries
                .remove_if(key, |_, entry| {
                    entry.generation == generation
                        && entry.access_order.load(Ordering::Relaxed).min(cutoff) == order
                })
                .is_some()
            {
                PeekMut::pop(oldest);
                return;
            }
            // A promotion or invalidation raced the lookup; revalidate the
            // unchanged heap root without popping and reinserting it.
        }
    }

    pub fn hit_ratio(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed) as f64;
        let misses = self.misses.load(Ordering::Relaxed) as f64;
        let total = hits + misses;
        if total == 0.0 {
            0.0
        } else {
            hits / total
        }
    }

    pub fn len(&self) -> usize {
        // DashMap sums shards independently. Freeze admission during the sum
        // so removed entries cannot be counted again as newly inserted keys.
        let _insertion = self.insertion.lock().unwrap_or_else(|e| e.into_inner());
        self.entries.len()
    }
}

fn normalize_cache_key(path: &str) -> String {
    let p = path.trim_start_matches('/');
    if p.is_empty() {
        return String::new();
    }
    p.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration as StdDuration;

    #[test]
    fn negative_cache_and_ancestor_invalidation() {
        let cache = StatusCache::new(Duration::from_secs(10), Duration::from_secs(10), 4096);

        cache.put_negative("a/b/");
        assert!(cache.is_negative("a/b/"));

        cache.invalidate_ancestors("a/b/c");
        assert!(!cache.is_negative("a/b/"));
    }

    #[test]
    fn mutation_completion_fences_queries_and_discards_conflicting_snapshots() {
        let cache = StatusCache::new(Duration::from_secs(60), Duration::from_secs(60), 8);
        cache.put_negative("dir");
        cache.put_negative("dir/file");
        let epoch = cache.snapshot_epoch();
        cache.complete_mutation("dir/file", FileStatus::file("dir/file", 3, 0o600), epoch);
        assert!(cache.get("dir").is_none());
        assert_eq!(cache.get_positive("dir/file").unwrap().length, 3);
        cache.put_if_unchanged("dir/file", None, epoch);
        assert_eq!(cache.get_positive("dir/file").unwrap().length, 3);

        let epoch = cache.snapshot_epoch();
        cache.invalidate("dir/file");
        cache.put_positive("dir/file", FileStatus::file("dir/file", 5, 0o644));
        cache.put_negative("dir");
        cache.complete_mutation("dir/file", FileStatus::file("dir/file", 3, 0o600), epoch);
        assert!(cache.get("dir").is_none());
        assert!(cache.get("dir/file").is_none());
    }

    #[test]
    fn invalidation_fences_old_positive_and_negative_query_results() {
        for kind in 0..3 {
            let cache = StatusCache::new(Duration::from_secs(60), Duration::from_secs(60), 8);
            cache.put_positive("keep", FileStatus::file("keep", 1, 0o644));
            let old = cache.snapshot_epoch();
            match kind {
                0 => cache.invalidate("dir/key"),
                1 => cache.invalidate_prefix("dir"),
                _ => cache.invalidate_ancestors("dir/key"),
            }
            cache.put_if_unchanged("dir/key", Some(FileStatus::file("dir/key", 2, 0o644)), old);
            cache.put_if_unchanged("missing", None, old);
            assert!(cache.get("dir/key").is_none());
            assert!(cache.get("missing").is_none());
            assert!(cache.get_positive("keep").is_some());
            let fresh = cache.snapshot_epoch();
            cache.put_if_unchanged("dir/key", Some(FileStatus::file("dir/key", 3, 0o644)), fresh);
            cache.put_if_unchanged("missing", None, fresh);
            assert_eq!(cache.get_positive("dir/key").unwrap().length, 3);
            assert!(cache.is_negative("missing"));
            cache.invalidate_prefix("");
            cache.put_if_unchanged("dir/key", None, fresh);
            assert_eq!(cache.len(), 0);
        }
    }

    #[test]
    fn ttl_expiry() {
        let cache = StatusCache::new(Duration::from_millis(1), Duration::from_millis(1), 4096);
        cache.put_positive("file", FileStatus::file("file", 100, 0o644));
        thread::sleep(StdDuration::from_millis(5));
        assert!(cache.get("file").is_none());
    }

    #[test]
    fn extreme_ttl_and_positive_negative_replacement_remain_usable() {
        let cache = StatusCache::new(Duration::MAX, Duration::ZERO, 2);
        cache.put_positive("key", FileStatus::file("key", 1, 0o644));
        assert!(cache.get_positive("key").is_some());
        cache.put_negative("key");
        assert!(cache.get("key").is_none());
        cache.put_positive("key", FileStatus::file("key", 2, 0o644));
        assert_eq!(cache.get_positive("key").unwrap().length, 2);

        let cache = StatusCache::new(Duration::ZERO, Duration::MAX, 2);
        cache.put_negative("key");
        assert!(cache.is_negative("key"));
        cache.put_positive("key", FileStatus::file("key", 1, 0o644));
        assert!(cache.get("key").is_none());
        cache.put_negative("key");
        assert!(cache.is_negative("key"));
        cache.invalidate("key");
        assert!(cache.get("key").is_none());
    }

    #[test]
    fn replacement_refreshes_age_and_uses_the_new_entry_kind_ttl() {
        let cache = StatusCache::new(Duration::from_secs(10), Duration::from_secs(2), 2);
        cache.put_positive("key", FileStatus::file("key", 1, 0o644));
        cache.entries.get_mut("key").unwrap().inserted_at =
            Instant::now().checked_sub(Duration::from_secs(3)).unwrap();
        assert!(cache.get_positive("key").is_some());
        cache.put_negative("key");
        assert!(cache.is_negative("key"));
        cache.entries.get_mut("key").unwrap().inserted_at =
            Instant::now().checked_sub(Duration::from_secs(3)).unwrap();
        assert!(cache.get("key").is_none());
        cache.put_positive("key", FileStatus::file("key", 2, 0o644));
        assert_eq!(cache.get_positive("key").unwrap().length, 2);
        assert!(cache.entries.get("key").unwrap().inserted_at.elapsed() < Duration::from_secs(2));
    }
}

#[cfg(test)]
mod performance_tests {
    use super::*;
    #[test]
    #[ignore = "local cache microbenchmark; run explicitly"]
    fn benchmark_full_cache_updates() {
        for trial in 1..=3 {
            let cache = StatusCache::new(Duration::from_secs(60), Duration::from_secs(60), 1024);
            let keys: Vec<_> = (0..1024).map(|i| format!("dir/file-{i}")).collect();
            for key in &keys {
                cache.put_negative(key);
            }
            let start = Instant::now();
            for i in 0..2000 {
                cache.put_negative(&keys[i % keys.len()]);
            }
            println!(
                "trial={trial} updates=2000 capacity=1024 elapsed_ms={:.3} size={}",
                start.elapsed().as_secs_f64() * 1000.0,
                cache.len()
            );
        }
    }
}

#[cfg(test)]
mod concurrency_tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    #[test]
    fn replacing_full_cache_preserves_other_entries_and_lru() {
        let cache = StatusCache::new(Duration::from_secs(60), Duration::from_secs(60), 2);
        cache.put_negative("a");
        cache.put_negative("b");
        cache.put_positive("b", FileStatus::file("b", 42, 0o644));
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get_positive("b").unwrap().length, 42);
        assert!(cache.is_negative("a")); // promotes a over b
        cache.put_negative("c");
        assert!(cache.get("b").is_none());
        assert!(cache.is_negative("a"));
        assert!(cache.is_negative("c"));
    }

    #[test]
    fn concurrent_admission_never_exceeds_capacity() {
        let cache = Arc::new(StatusCache::new(
            Duration::from_secs(60),
            Duration::from_secs(60),
            4,
        ));
        let start = Arc::new(Barrier::new(8));
        std::thread::scope(|scope| {
            for worker in 0..8 {
                let cache = cache.clone();
                let start = start.clone();
                scope.spawn(move || {
                    start.wait();
                    for i in 0..100 {
                        let key = format!("{worker}/{i}");
                        cache.put_negative(&key);
                        assert!(cache.len() <= 4);
                        cache.put_positive(&key, FileStatus::file(&key, i, 0o644));
                        let _ = cache.get(&key);
                        if i % 3 == 0 {
                            cache.invalidate(&key);
                        }
                    }
                });
            }
        });
        assert!(cache.len() <= 4);
    }

    #[test]
    fn lazy_index_matches_lru_through_updates_and_invalidations() {
        let cache = StatusCache::new(Duration::from_secs(60), Duration::from_secs(60), 7);
        let mut lru = Vec::<String>::new();
        let mut seed = 19u64;
        for _ in 0..5000 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let key = format!("k{}", (seed >> 32) % 13);
            match seed % 5 {
                0 => {
                    cache.invalidate(&key);
                    lru.retain(|k| k != &key);
                }
                1 => {
                    let present = lru.contains(&key);
                    assert_eq!(cache.is_negative(&key), present);
                    if present {
                        lru.retain(|k| k != &key);
                        lru.push(key);
                    }
                }
                _ => {
                    cache.put_negative(&key);
                    lru.retain(|k| k != &key);
                    lru.push(key);
                    if lru.len() > 7 {
                        lru.remove(0);
                    }
                }
            }
            let mut actual: Vec<_> =
                cache.entries.iter().map(|e| e.key().clone()).collect();
            let mut expected = lru.clone();
            actual.sort();
            expected.sort();
            assert_eq!(actual, expected);
            assert!(cache.insertion.lock().unwrap().len() <= 14);
        }
    }

    #[test]
    fn lazy_index_bounds_tombstones_and_distinguishes_reinsertions() {
        let cache = StatusCache::new(Duration::from_secs(60), Duration::from_secs(60), 2);
        for _ in 0..1000 {
            cache.put_negative("a/b");
            cache.invalidate_prefix("a");
            assert!(cache.insertion.lock().unwrap().len() <= 4);
        }
        cache.put_negative("a/b");
        cache.put_negative("c");
        cache.invalidate("a/b");
        cache.put_negative("a/b");
        cache.put_negative("d");
        assert!(cache.get("c").is_none());
        assert!(cache.is_negative("a/b"));
        assert!(cache.is_negative("d"));
    }

    #[test]
    fn clearing_root_releases_index_and_supports_reuse() {
        let cache = StatusCache::new(Duration::from_secs(60), Duration::from_secs(60), 32);
        for root in ["", "/", "///"] {
            for i in 0..32 {
                cache.put_negative(format!("directory/{i}"));
            }
            cache.invalidate("directory/0"); // leave an index tombstone
            assert!(cache.insertion.lock().unwrap().capacity() > 0);
            cache.invalidate_prefix(root);
            assert_eq!(cache.len(), 0);
            let index = cache.insertion.lock().unwrap();
            assert!(index.is_empty());
            assert_eq!(index.capacity(), 0);
            drop(index);
            cache.put_positive("directory/0", FileStatus::file("directory/0", 42, 0o644));
            assert_eq!(cache.get_positive("directory/0").unwrap().length, 42);
        }
    }

    #[test]
    fn concurrent_read_update_evict_and_clear_preserve_index_invariants() {
        let cache = Arc::new(StatusCache::new(
            Duration::from_secs(60),
            Duration::from_secs(60),
            64,
        ));
        let keys: Vec<_> = (0..64).map(|i| format!("hot/{i}")).collect();
        for key in &keys {
            cache.put_negative(key);
        }
        let start = Barrier::new(8);
        std::thread::scope(|scope| {
            for worker in 0..8 {
                let cache = &cache;
                let keys = &keys;
                let start = &start;
                scope.spawn(move || {
                    start.wait();
                    if worker < 4 {
                        for i in 0..16000 {
                            let _ = cache.get(&keys[(i + worker) % keys.len()]);
                        }
                    } else {
                        for i in 0..1000 {
                            let hot = &keys[i % keys.len()];
                            cache.put_positive(hot, FileStatus::file(hot, i as u64, 0o644));
                            cache.put_negative(format!("new/{worker}/{i}"));
                            if i % 17 == 0 {
                                cache.invalidate(hot);
                            }
                            if i % 71 == 0 {
                                cache.invalidate_prefix("new");
                            }
                            if i % 251 == 0 {
                                cache.invalidate_prefix("/");
                            }
                            assert!(cache.len() <= 64);
                        }
                    }
                });
            }
        });
        // No live entry may lose its index record during concurrent clearing.
        let index = cache.insertion.lock().unwrap();
        assert!(index.len() <= 128);
        for entry in cache.entries.iter() {
            let matching: Vec<_> = index.iter().filter(|Reverse((_, generation, key))| {
                *generation == entry.generation && key == entry.key()
            }).collect();
            assert_eq!(matching.len(), 1);
            assert!(matching[0].0.0 <= entry.access_order.load(Ordering::Relaxed));
        }
        drop(index);
        // Continue beyond capacity to exercise every surviving index record.
        for i in 0..128 {
            cache.put_negative(format!("final/{i}"));
        }
        assert_eq!(cache.len(), 64);
        for i in 64..128 {
            assert!(cache.is_negative(&format!("final/{i}")));
        }
    }

    #[test]
    fn zero_capacity_never_admits_entries() {
        let cache = StatusCache::new(Duration::from_secs(1), Duration::from_secs(1), 0);
        cache.put_negative("a");
        cache.put_positive("b", FileStatus::file("b", 0, 0o644));
        assert_eq!(cache.len(), 0);
    }
}

#[cfg(test)]
mod directory_hint_tests {
    use super::*;

    #[test]
    fn directory_hint_keeps_live_metadata_and_original_ttl() {
        let cache = StatusCache::new(Duration::from_secs(60), Duration::from_secs(1), 8);
        let mut status = FileStatus::dir("/dir", 0o700);
        status.owner = "owner".into();
        status.modification_time = 1234;
        cache.put_positive("dir", status);
        let (inserted, order) = {
            let entry = cache.entries.get("dir").unwrap();
            (entry.inserted_at, entry.access_order.load(Ordering::Relaxed))
        };
        cache.put_directory_hint_if_unchanged("dir", FileStatus::dir("/dir", 0o755), cache.snapshot_epoch());
        cache.backfill_from_list("/dir", &[]);
        let entry = cache.entries.get("dir").unwrap();
        let status = entry.status.as_ref().unwrap();
        assert_eq!(status.permission, 0o700);
        assert_eq!(status.owner, "owner");
        assert_eq!(status.modification_time, 1234);
        assert_eq!(entry.inserted_at, inserted);
        assert_eq!(entry.access_order.load(Ordering::Relaxed), order);
    }

    #[test]
    fn directory_hint_replaces_expired_or_negative_entries_but_not_after_invalidation() {
        let cache = StatusCache::new(Duration::from_secs(1), Duration::from_secs(1), 8);
        for negative in [false, true] {
            if negative { cache.put_negative("dir"); }
            else {
                cache.put_positive("dir", FileStatus::dir("/dir", 0o700));
                cache.entries.get_mut("dir").unwrap().inserted_at = Instant::now() - Duration::from_secs(2);
            }
            cache.put_directory_hint_if_unchanged("dir", FileStatus::dir("/dir", 0o755), cache.snapshot_epoch());
            assert_eq!(cache.get_positive("dir").unwrap().permission, 0o755);
        }
        let epoch = cache.snapshot_epoch();
        cache.invalidate("dir");
        cache.put_directory_hint_if_unchanged("dir", FileStatus::dir("/dir", 0o755), epoch);
        assert!(cache.get("dir").is_none());
    }
}
