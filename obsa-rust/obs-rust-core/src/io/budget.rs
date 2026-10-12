//! Shared buffer budget. Managed Vec allocations are charged by capacity;
//! opaque external Bytes frames by visible length (their capacity is unavailable).
use crate::error::{ObsError, ObsResult};
use bytes::Bytes;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};

pub struct BufferBudget {
    limit: u64,
    used: AtomicU64,
}

enum Storage {
    Mutable { data: Vec<u8>, lease: BufferLease },
    Frozen(Bytes),
}

/// Fallible allocation with leases attached to the complete backing allocation.
/// Growth reserves old + new capacity before copying; slices retain the whole lease.
pub(crate) struct BudgetBuffer {
    budget: Arc<BufferBudget>,
    storage: Storage,
}
impl std::ops::Deref for BudgetBuffer {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match &self.storage {
            Storage::Mutable { data, .. } => data,
            Storage::Frozen(data) => data,
        }
    }
}
impl BudgetBuffer {
    pub(crate) fn new(budget: Arc<BufferBudget>) -> Self {
        Self { budget, storage: Storage::Frozen(Bytes::new()) }
    }
    pub(crate) fn capacity(&self) -> usize {
        match &self.storage {
            Storage::Mutable { data, .. } => data.capacity(),
            Storage::Frozen(data) => data.len(),
        }
    }
    pub(crate) fn clear(&mut self) { self.storage = Storage::Frozen(Bytes::new()); }
    pub(crate) fn append(&mut self, bytes: &[u8], hint: usize) -> ObsResult<()> {
        if bytes.is_empty() { return Ok(()); }
        let needed = self.len().checked_add(bytes.len())
            .filter(|n| *n <= isize::MAX as usize)
            .ok_or_else(|| ObsError::Io("buffer exceeds addressable capacity".into()))?;
        if let Storage::Mutable { data, .. } = &mut self.storage {
            if data.capacity() >= needed {
                data.extend_from_slice(bytes);
                return Ok(());
            }
            // Preserve realloc's in-place opportunity when accounting is disabled.
            if self.budget.limit == 0 {
                let capacity = needed.max(hint).min(isize::MAX as usize);
                data.try_reserve_exact(capacity - data.len())
                    .map_err(|e| ObsError::Io(format!("cannot allocate buffer: {e}")))?;
                data.extend_from_slice(bytes);
                return Ok(());
            }
        }
        let capacity = needed.max(hint).min(isize::MAX as usize);
        // A speculative growth hint must not reject a request that fits exactly.
        let (capacity, mut lease) = match self.budget.reserve(capacity as u64) {
            Ok(lease) => (capacity, lease),
            Err(_) if capacity > needed => (needed, self.budget.reserve(needed as u64)?),
            Err(error) => return Err(error),
        };
        let mut data = Vec::new();
        data.try_reserve_exact(capacity)
            .map_err(|e| ObsError::Io(format!("cannot allocate buffer: {e}")))?;
        // Vec may legally grant more than requested. Account the actual capacity
        // before publishing it; release the new allocation if reconciliation fails.
        lease.grow((data.capacity() - capacity) as u64)?;
        data.extend_from_slice(self);
        data.extend_from_slice(bytes);
        self.storage = Storage::Mutable { data, lease };
        Ok(())
    }
    pub(crate) fn take_bytes(&mut self) -> Bytes {
        match std::mem::replace(&mut self.storage, Storage::Frozen(Bytes::new())) {
            Storage::Frozen(bytes) => bytes,
            Storage::Mutable { data, lease } => {
                if lease.len() == 0 { return Bytes::from(data); }
                struct Owner { data: Vec<u8>, _lease: BufferLease }
                impl AsRef<[u8]> for Owner {
                    fn as_ref(&self) -> &[u8] { &self.data }
                }
                Bytes::from_owner(Owner { data, _lease: lease })
            }
        }
    }
    pub(crate) fn split_to(&mut self, n: usize) -> Bytes {
        let mut bytes = self.take_bytes();
        let prefix = bytes.split_to(n);
        if !bytes.is_empty() { self.storage = Storage::Frozen(bytes); }
        prefix
    }
}

impl BufferBudget {
    pub(crate) fn new(limit: u64) -> Arc<Self> {
        Arc::new(Self { limit, used: AtomicU64::new(0) })
    }
    pub(crate) fn reserve(self: &Arc<Self>, bytes: u64) -> ObsResult<BufferLease> {
        let mut lease = BufferLease { budget: self.clone(), bytes: 0 };
        lease.grow(bytes)?;
        Ok(lease)
    }
}

pub(crate) struct BufferLease {
    budget: Arc<BufferBudget>,
    bytes: u64,
}
impl BufferLease {
    pub(crate) fn len(&self) -> u64 { self.bytes }
    pub(crate) fn grow(&mut self, bytes: u64) -> ObsResult<()> {
        if self.budget.limit == 0 || bytes == 0 { return Ok(()); }
        self.budget.used.fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
            used.checked_add(bytes).filter(|next| *next <= self.budget.limit)
        }).map_err(|_| ObsError::Io(format!(
            "shared buffer budget exhausted (limit {} bytes, requested {bytes}); release other streams or increase fs.obs.buffer.total.bytes",
            self.budget.limit
        )))?;
        self.bytes += bytes; // bounded by the successfully reserved total
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn split(&mut self, bytes: u64) -> Self {
        let bytes = if self.budget.limit == 0 { 0 } else { bytes };
        assert!(bytes <= self.bytes);
        self.bytes -= bytes;
        Self { budget: self.budget.clone(), bytes }
    }
    pub(crate) fn attach(self, data: Bytes) -> Bytes {
        if self.bytes == 0 { return data; }
        // Owner follows every Bytes clone/slice, including transport retries.
        // Release only when the last reference to the body disappears.
        struct Owner { data: Bytes, _lease: BufferLease }
        impl AsRef<[u8]> for Owner {
            fn as_ref(&self) -> &[u8] { &self.data }
        }
        Bytes::from_owner(Owner { data, _lease: self })
    }
}
impl Drop for BufferLease {
    fn drop(&mut self) {
        if self.bytes != 0 { self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel); }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn slices_hold_entire_allocation_including_unused_capacity() {
        let budget = BufferBudget::new(8);
        let mut buffer = BudgetBuffer::new(budget.clone());
        buffer.append(b"ab", 8).unwrap();
        assert_eq!(buffer.capacity(), 8);
        assert!(budget.reserve(1).is_err());
        let slice = buffer.split_to(1);
        buffer.clear();
        assert!(budget.reserve(1).is_err());
        drop(slice);
        assert!(budget.reserve(8).is_ok());
    }
    #[test]
    fn growth_accounts_transient_old_and_new_capacity_and_preserves_data_on_failure() {
        let budget = BufferBudget::new(6);
        let mut buffer = BudgetBuffer::new(budget.clone());
        buffer.append(b"ab", 2).unwrap();
        buffer.append(b"cd", 8).unwrap(); // hint falls back to exact allocation
        assert_eq!(&buffer[..], b"abcd");
        assert_eq!(budget.used.load(Ordering::Acquire), 4);
        assert!(buffer.append(b"ef", 6).is_err()); // final 6 fits, peak 4 + 6 does not
        assert_eq!(&buffer[..], b"abcd");
        drop(buffer);
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
    }
    #[test]
    fn leases_follow_body_clones_and_failed_reservation_is_atomic() {
        let budget = BufferBudget::new(7);
        let mut staging = budget.reserve(5).unwrap();
        assert!(staging.grow(3).is_err());
        let body = staging.split(3).attach(Bytes::from_static(b"abc"));
        let clone = body.slice(1..);
        drop(body);
        drop(staging);
        assert!(budget.reserve(5).is_err());
        drop(clone);
        assert!(budget.reserve(7).is_ok());
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
    }
    #[test]
    fn zero_disables_and_large_reservations_cannot_overflow() {
        let unlimited = BufferBudget::new(0);
        let mut lease = unlimited.reserve(u64::MAX).unwrap();
        lease.grow(u64::MAX).unwrap();
        let budget = BufferBudget::new(u64::MAX);
        let lease = budget.reserve(u64::MAX).unwrap();
        assert!(budget.reserve(1).is_err());
        drop(lease);
        assert!(budget.reserve(1).is_ok());
    }
}
