//! Binary-safe in-memory key/value store with per-key expiry.
//!
//! - one `RwLock<Inner>`: the value map plus a deadline heap, shareable as `Arc<Store>`
//! - passive expiry on access; [`Store::sweep_expired`] reclaims untouched keys
//! - heap tuples are lazily invalidated: the map is authoritative
//! - no persistence or value types
//! - a poisoned lock panics

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

/// Most deadlines one sweep pops, bounding how long it holds the write lock.
const SWEEP_BATCH: usize = 1024;

/// A value plus its optional deadline.
struct Entry {
    value: Arc<[u8]>,
    expires_at: Option<Instant>,
}

impl Entry {
    /// A key counts as expired from its deadline onwards.
    fn is_due(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|at| at <= now)
    }

    fn ttl(&self, now: Instant) -> Ttl {
        match self.expires_at {
            Some(at) => Ttl::Remaining(at.saturating_duration_since(now)),
            None => Ttl::Persistent,
        }
    }
}

/// Everything one lock protects: lookups and the deadline heap move together.
#[derive(Default)]
struct Inner {
    map: HashMap<Vec<u8>, Entry>,
    /// `(deadline, key)`, smallest deadline first; may hold stale tuples.
    deadlines: BinaryHeap<Reverse<(Instant, Vec<u8>)>>,
}

impl Inner {
    fn is_due(&self, key: &[u8], now: Instant) -> bool {
        self.map.get(key).is_some_and(|entry| entry.is_due(now))
    }

    /// Returns the live entry for `key`, dropping it first if expired.
    fn live(&mut self, key: &[u8], now: Instant) -> Option<&Entry> {
        if self.is_due(key, now) {
            self.map.remove(key);
            return None;
        }
        self.map.get(key)
    }

    /// Queues a deadline; the sweeper ignores it if the entry moves on.
    fn queue_deadline(&mut self, key: &[u8], at: Instant) {
        self.deadlines.push(Reverse((at, key.to_vec())));
    }
}

/// How long a key lives, as reported by `TTL`/`PTTL`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ttl {
    /// absent, or already expired
    Missing,
    /// present with no deadline
    Persistent,
    /// present with this much time left
    Remaining(Duration),
}

/// Shared map from byte keys to byte values with optional deadlines.
#[derive(Default)]
pub struct Store {
    inner: RwLock<Inner>,
}

impl Store {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a snapshot of the value for `key`, or `None` if it is absent or expired.
    pub fn get(&self, key: &[u8], now: Instant) -> Option<Arc<[u8]>> {
        self.read_entry(key, now, |entry| Arc::clone(&entry.value))
    }

    /// Inserts or replaces the value for `key`, discarding any previous deadline.
    pub fn set(&self, key: &[u8], value: &[u8], expires_at: Option<Instant>) {
        let mut inner = self.write();
        if let Some(at) = expires_at {
            inner.queue_deadline(key, at);
        }
        inner.map.insert(
            key.to_vec(),
            Entry {
                value: Arc::from(value),
                expires_at,
            },
        );
    }

    /// Removes `key`, returning whether it was present and unexpired.
    pub fn del(&self, key: &[u8], now: Instant) -> bool {
        self.del_many([key], now) == 1
    }

    /// Removes `keys`, returning how many were present and unexpired.
    ///
    /// - one write lock for the whole batch, so no other thread sees it half-applied
    /// - expired entries are removed but not counted
    /// - duplicate keys count once: the second removal finds nothing
    pub fn del_many<'a>(&self, keys: impl IntoIterator<Item = &'a [u8]>, now: Instant) -> usize {
        let mut inner = self.write();
        keys.into_iter()
            .filter_map(|key| inner.map.remove(key))
            .filter(|entry| !entry.is_due(now))
            .count()
    }

    /// Returns whether `key` is present and unexpired.
    pub fn exists(&self, key: &[u8], now: Instant) -> bool {
        self.exists_many([key], now) == 1
    }

    /// Counts how many of `keys` are present and unexpired.
    ///
    /// - one guard for the whole batch: the count is a snapshot
    /// - duplicate keys count once per occurrence
    pub fn exists_many<'a, I>(&self, keys: I, now: Instant) -> usize
    where
        I: IntoIterator<Item = &'a [u8]>,
        I::IntoIter: Clone,
    {
        let keys = keys.into_iter();
        {
            let inner = self.read();
            let mut count = 0;
            let mut any_due = false;
            for key in keys.clone() {
                match inner.map.get(key) {
                    Some(entry) if entry.is_due(now) => any_due = true,
                    Some(_) => count += 1,
                    None => {}
                }
            }
            if !any_due {
                return count;
            }
        }

        // Escalate once to purge; count the batch under the write lock.
        let mut inner = self.write();
        keys.filter(|key| inner.live(key, now).is_some()).count()
    }

    /// Reports `key`'s remaining lifetime, removing it first if it has expired.
    pub fn ttl(&self, key: &[u8], now: Instant) -> Ttl {
        self.read_entry(key, now, |entry| entry.ttl(now))
            .unwrap_or(Ttl::Missing)
    }

    /// Pops up to [`SWEEP_BATCH`] due deadlines, returning how many keys it removed.
    ///
    /// - stale tuples are discarded without counting against `removed`
    /// - stops at the first future deadline
    pub(crate) fn sweep_expired(&self, now: Instant) -> usize {
        let mut inner = self.write();
        let mut popped = 0;
        let mut removed = 0;

        while popped < SWEEP_BATCH {
            let Some(Reverse((at, _))) = inner.deadlines.peek() else {
                break;
            };
            if *at > now {
                break;
            }
            let Reverse((at, key)) = inner.deadlines.pop().expect("peeked a deadline");
            popped += 1;
            if inner
                .map
                .get(&key)
                .is_some_and(|entry| entry.expires_at == Some(at))
            {
                inner.map.remove(&key);
                removed += 1;
            }
        }

        removed
    }

    /// Reads a live entry, purging it first if expired.
    ///
    /// - fast path: shared lock when the entry is live or absent
    /// - slow path: exclusive lock to purge, then re-check
    fn read_entry<T>(
        &self,
        key: &[u8],
        now: Instant,
        project: impl FnOnce(&Entry) -> T,
    ) -> Option<T> {
        {
            let inner = self.read();
            match inner.map.get(key) {
                Some(entry) if !entry.is_due(now) => return Some(project(entry)),
                Some(_) => {}
                None => return None,
            }
        }

        // Expired on the fast path: retry under the write lock.
        let mut inner = self.write();
        inner.live(key, now).map(project)
    }

    // A panic while the lock is held poisons it: another thread may have died
    // mid-write, so recovering could serve corruption silently.
    fn read(&self) -> RwLockReadGuard<'_, Inner> {
        self.inner.read().expect("store lock poisoned")
    }

    fn write(&self) -> RwLockWriteGuard<'_, Inner> {
        self.inner.write().expect("store lock poisoned")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overwriting_a_deadline_leaves_a_harmless_stale_tuple() {
        let store = Store::new();
        let now = Instant::now();
        let at = now + Duration::from_secs(10);

        store.set(b"k", b"v", Some(at));
        assert_eq!(store.read().deadlines.len(), 1);

        // Persistent write: old tuple stays but must not expire the new value.
        store.set(b"k", b"v2", None);
        assert_eq!(store.read().deadlines.len(), 1);
        assert_eq!(store.get(b"k", at).as_deref(), Some(b"v2".as_slice()));
        assert_eq!(store.sweep_expired(at), 0);
        assert!(store.read().deadlines.is_empty());
        assert!(store.exists(b"k", at));

        // New deadlines queue new tuples; earlier ones stay stale.
        store.set(b"k", b"v3", Some(at));
        store.set(b"k", b"v4", Some(at));
        assert_eq!(store.read().deadlines.len(), 2);
        assert_eq!(store.get(b"k", at + Duration::from_secs(1)), None);
    }

    #[test]
    fn sweep_removes_only_due_keys() {
        let store = Store::new();
        let now = Instant::now();
        store.set(b"due", b"v", Some(now + Duration::from_secs(1)));
        store.set(b"later", b"v", Some(now + Duration::from_secs(10)));
        store.set(b"forever", b"v", None);

        assert_eq!(store.sweep_expired(now), 0);
        assert_eq!(store.sweep_expired(now + Duration::from_secs(5)), 1);
        assert_eq!(store.sweep_expired(now + Duration::from_secs(5)), 0);

        let inner = store.read();
        assert!(!inner.map.contains_key(&b"due"[..]));
        assert!(inner.map.contains_key(&b"later"[..]));
        assert!(inner.map.contains_key(&b"forever"[..]));
        assert_eq!(inner.deadlines.len(), 1);
    }

    #[test]
    fn sweep_is_capped_per_call() {
        let store = Store::new();
        let now = Instant::now();
        for i in 0..SWEEP_BATCH + 3 {
            store.set(&i.to_be_bytes(), b"", Some(now));
        }

        assert_eq!(store.sweep_expired(now), SWEEP_BATCH);
        assert_eq!(store.sweep_expired(now), 3);
        assert_eq!(store.sweep_expired(now), 0);
        assert!(store.read().map.is_empty());
    }

    #[test]
    fn stale_deadline_never_removes_a_live_entry() {
        let store = Store::new();
        let now = Instant::now();
        store.set(b"k", b"v", None);
        store.write().deadlines.push(Reverse((now, b"k".to_vec())));

        assert_eq!(store.sweep_expired(now + Duration::from_secs(1)), 0);
        assert!(store.exists(b"k", now));
        assert!(store.read().deadlines.is_empty());
    }
}
