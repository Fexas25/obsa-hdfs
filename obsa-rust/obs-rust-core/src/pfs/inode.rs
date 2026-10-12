use std::sync::atomic::{AtomicU64, Ordering};

/// PFS inode fast-path state: DHT view version (max-seen, never decrease).
pub struct InodeState {
    dht_id: AtomicU64,
}

impl InodeState {
    pub fn new() -> Self {
        Self {
            dht_id: AtomicU64::new(0),
        }
    }

    pub fn dht_id(&self) -> Option<u64> {
        let v = self.dht_id.load(Ordering::Relaxed);
        if v == 0 {
            None
        } else {
            Some(v)
        }
    }

    /// Update DHT with max-seen CAS (unsigned, never decrease).
    pub fn update_dht(&self, seen: u64) {
        loop {
            let current = self.dht_id.load(Ordering::Relaxed);
            if seen <= current {
                return;
            }
            if self
                .dht_id
                .compare_exchange(current, seen, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }
}

impl Default for InodeState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dht_never_decreases() {
        let state = InodeState::new();
        state.update_dht(10);
        state.update_dht(5);
        assert_eq!(state.dht_id(), Some(10));
        state.update_dht(20);
        assert_eq!(state.dht_id(), Some(20));
    }
}
