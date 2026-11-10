//! Binary-safe in-memory key/value store with lazy expiry.
//!
//! - one `RwLock<HashMap<Vec<u8>, Entry>>`, shareable as `Arc<Store>`
//! - a key is dead once `now` reaches its deadline
//! - expired entries are removed only when accessed
//! - conditional writes take one lock and report the previous value
//! - a poisoned lock panics

use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockWriteGuard};
use std::time::{Duration, Instant};

type Map = HashMap<Vec<u8>, Entry>;

/// A value plus its optional deadline.
struct Entry {
    value: Arc<[u8]>,
    expires_at: Option<Instant>,
}

impl Entry {
    /// A key is dead once its deadline is reached.
    fn is_expired(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|deadline| deadline <= now)
    }

    /// Remaining time; callers only pass live entries, so it is positive.
    fn ttl(&self, now: Instant) -> Ttl {
        match self.expires_at {
            None => Ttl::Persistent,
            Some(deadline) => Ttl::Expiring(deadline - now),
        }
    }
}

/// Returns the live entry for `key`, evicting it when its deadline has passed.
fn live<'a>(map: &'a mut Map, key: &[u8], now: Instant) -> Option<&'a mut Entry> {
    if map.get(key).is_some_and(|entry| entry.is_expired(now)) {
        map.remove(key);
        return None;
    }
    map.get_mut(key)
}

/// Whether a key is absent, persistent, or expiring.
#[derive(Debug, PartialEq, Eq)]
pub enum Ttl {
    /// The key is absent or already expired.
    Missing,
    /// The key exists with no deadline.
    Persistent,
    /// The key exists and expires after the remaining duration.
    Expiring(Duration),
}

/// Whether a write applies unconditionally or only when the key is absent/present.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Condition {
    Always,
    Nx,
    Xx,
}

/// What a write does with an existing deadline.
#[derive(Clone, Copy)]
pub(crate) enum TtlPolicy {
    /// Drop any deadline.
    Clear,
    /// Keep the surviving entry's deadline; absent keys stay persistent.
    Keep,
    /// Install a new deadline.
    Expire(Instant),
}

/// Result of a conditional write.
pub(crate) struct SetOutcome {
    /// Whether the condition allowed the write.
    pub applied: bool,
    /// Value before the write; `None` when it was absent or expired.
    pub previous: Option<Arc<[u8]>>,
}

/// Shared map from byte keys to byte values and optional deadlines.
#[derive(Default)]
pub struct Store {
    inner: RwLock<Map>,
}

impl Store {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a snapshot of the value for `key`, or `None` if it is absent or expired.
    pub fn get(&self, key: &[u8], now: Instant) -> Option<Arc<[u8]>> {
        let mut map = self.write();
        live(&mut map, key, now).map(|entry| Arc::clone(&entry.value))
    }

    /// Inserts or replaces the value for `key`, with an optional deadline.
    pub fn set(&self, key: &[u8], value: &[u8], expires_at: Option<Instant>) {
        self.write().insert(
            key.to_vec(),
            Entry {
                value: Arc::from(value),
                expires_at,
            },
        );
    }

    /// Inserts `value` when `condition` allows it.
    ///
    /// - one write lock for the eviction, decision, and insert
    /// - `previous` is the live value before the decision, so `GET` can report it
    /// - a rejected condition leaves value and deadline untouched
    /// - `TtlPolicy::Keep` keeps the surviving deadline
    pub(crate) fn set_if(
        &self,
        key: &[u8],
        value: &[u8],
        ttl: TtlPolicy,
        condition: Condition,
        now: Instant,
    ) -> SetOutcome {
        let mut map = self.write();
        let previous = live(&mut map, key, now).map(|entry| Arc::clone(&entry.value));
        let found = previous.is_some();
        let allowed = match condition {
            Condition::Always => true,
            Condition::Nx => !found,
            Condition::Xx => found,
        };
        if !allowed {
            return SetOutcome {
                applied: false,
                previous,
            };
        }

        let expires_at = match ttl {
            TtlPolicy::Clear => None,
            TtlPolicy::Keep => map.get(key).and_then(|entry| entry.expires_at),
            TtlPolicy::Expire(deadline) => Some(deadline),
        };
        map.insert(
            key.to_vec(),
            Entry {
                value: Arc::from(value),
                expires_at,
            },
        );
        SetOutcome {
            applied: true,
            previous,
        }
    }

    /// Removes `key`, returning whether a live entry was present.
    pub fn del(&self, key: &[u8], now: Instant) -> bool {
        self.del_many([key], now) == 1
    }

    /// Removes `keys`, returning how many live entries were present.
    ///
    /// - one write lock for the whole batch
    /// - expired entries are removed but not counted
    /// - duplicate keys count once: the second removal finds nothing
    pub fn del_many<'a>(&self, keys: impl IntoIterator<Item = &'a [u8]>, now: Instant) -> usize {
        let mut map = self.write();
        keys.into_iter()
            .filter(|&key| map.remove(key).is_some_and(|entry| !entry.is_expired(now)))
            .count()
    }

    /// Returns whether `key` is present and live.
    pub fn exists(&self, key: &[u8], now: Instant) -> bool {
        self.exists_many([key], now) == 1
    }

    /// Counts how many of `keys` are present and live.
    ///
    /// - one write lock for the whole batch
    /// - expired entries are removed and not counted
    /// - duplicate keys count once per occurrence
    pub fn exists_many<'a>(&self, keys: impl IntoIterator<Item = &'a [u8]>, now: Instant) -> usize {
        let mut map = self.write();
        keys.into_iter()
            .filter(|&key| live(&mut map, key, now).is_some())
            .count()
    }

    /// Reports whether `key` is absent, persistent, or expiring.
    pub fn ttl(&self, key: &[u8], now: Instant) -> Ttl {
        let mut map = self.write();
        live(&mut map, key, now).map_or(Ttl::Missing, |entry| entry.ttl(now))
    }

    /// Sets the deadline for a live `key`.
    ///
    /// - `false` when the key is absent or already expired
    /// - a deadline at or before `now` removes the key and reports `true`
    pub fn set_expiry(&self, key: &[u8], deadline: Instant, now: Instant) -> bool {
        let mut map = self.write();
        if live(&mut map, key, now).is_none() {
            return false;
        }
        if deadline <= now {
            map.remove(key);
        } else {
            map.get_mut(key).expect("live entry disappeared").expires_at = Some(deadline);
        }
        true
    }

    /// Removes the deadline from a live `key`, returning whether one was set.
    pub fn persist(&self, key: &[u8], now: Instant) -> bool {
        let mut map = self.write();
        live(&mut map, key, now).is_some_and(|entry| entry.expires_at.take().is_some())
    }

    // A panic while the lock is held poisons it: another thread may have died
    // mid-write, so recovering could serve corruption silently.
    fn write(&self) -> RwLockWriteGuard<'_, Map> {
        self.inner.write().expect("store lock poisoned")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::thread;

    use super::*;

    /// Workers in `shared_handle_across_threads`, one disjoint key each.
    const THREADS: usize = 8;

    fn map_len(store: &Store) -> usize {
        store.inner.read().expect("lock").len()
    }

    fn value_of(store: &Store, key: &[u8]) -> Option<Vec<u8>> {
        store.get(key, Instant::now()).map(|value| value.to_vec())
    }

    #[test]
    fn expired_entry_is_removed_on_get() {
        let store = Store::new();
        let now = Instant::now();
        store.set(b"k", b"v", Some(now));

        assert_eq!(map_len(&store), 1);
        assert_eq!(store.get(b"k", now), None);
        assert_eq!(map_len(&store), 0);
    }

    #[test]
    fn live_entry_survives_before_its_deadline() {
        let store = Store::new();
        let now = Instant::now();
        let deadline = now + Duration::from_secs(10);
        store.set(b"k", b"v", Some(deadline));

        assert_eq!(store.get(b"k", now).as_deref(), Some(b"v".as_slice()));
        assert_eq!(store.ttl(b"k", now), Ttl::Expiring(Duration::from_secs(10)));
        assert_eq!(store.ttl(b"k", deadline), Ttl::Missing);
        assert_eq!(map_len(&store), 0);
    }

    #[test]
    fn plain_set_clears_a_deadline() {
        let store = Store::new();
        let now = Instant::now();
        store.set(b"k", b"v", Some(now + Duration::from_secs(10)));
        store.set(b"k", b"other", None);

        assert_eq!(store.ttl(b"k", now), Ttl::Persistent);
        assert_eq!(store.get(b"k", now).as_deref(), Some(b"other".as_slice()));
    }

    #[test]
    fn set_if_conditions_gate_writes() {
        let store = Store::new();
        let now = Instant::now();

        let outcome = store.set_if(b"k", b"first", TtlPolicy::Clear, Condition::Nx, now);
        assert!(outcome.applied);
        assert_eq!(outcome.previous, None);

        let outcome = store.set_if(b"k", b"second", TtlPolicy::Clear, Condition::Nx, now);
        assert!(!outcome.applied);
        assert_eq!(outcome.previous.as_deref(), Some(b"first".as_slice()));
        assert_eq!(value_of(&store, b"k").as_deref(), Some(b"first".as_slice()));

        let outcome = store.set_if(b"k", b"third", TtlPolicy::Clear, Condition::Xx, now);
        assert!(outcome.applied);
        assert_eq!(outcome.previous.as_deref(), Some(b"first".as_slice()));

        let outcome = store.set_if(b"missing", b"v", TtlPolicy::Clear, Condition::Xx, now);
        assert!(!outcome.applied);
        assert_eq!(outcome.previous, None);
        assert!(!store.exists(b"missing", now));
    }

    #[test]
    fn set_if_treats_expired_entries_as_absent() {
        let store = Store::new();
        let now = Instant::now();
        store.set(b"k", b"dead", Some(now));

        let outcome = store.set_if(b"k", b"fresh", TtlPolicy::Clear, Condition::Nx, now);
        assert!(outcome.applied);
        assert_eq!(outcome.previous, None, "an expired entry has no value");
        assert_eq!(value_of(&store, b"k").as_deref(), Some(b"fresh".as_slice()));
    }

    #[test]
    fn set_if_keep_preserves_the_deadline_and_clear_removes_it() {
        let store = Store::new();
        let now = Instant::now();
        let deadline = now + Duration::from_secs(10);

        store.set(b"k", b"v", Some(deadline));
        let outcome = store.set_if(b"k", b"other", TtlPolicy::Keep, Condition::Always, now);
        assert!(outcome.applied);
        assert_eq!(store.ttl(b"k", now), Ttl::Expiring(Duration::from_secs(10)));

        let outcome = store.set_if(b"k", b"fresh", TtlPolicy::Clear, Condition::Always, now);
        assert!(outcome.applied);
        assert_eq!(store.ttl(b"k", now), Ttl::Persistent);

        // Keep on an absent key leaves it persistent.
        let outcome = store.set_if(b"new", b"v", TtlPolicy::Keep, Condition::Always, now);
        assert!(outcome.applied);
        assert_eq!(store.ttl(b"new", now), Ttl::Persistent);
    }

    #[test]
    fn set_if_expire_installs_a_deadline() {
        let store = Store::new();
        let now = Instant::now();
        let deadline = now + Duration::from_millis(1500);

        let outcome = store.set_if(
            b"k",
            b"v",
            TtlPolicy::Expire(deadline),
            Condition::Always,
            now,
        );
        assert!(outcome.applied);
        assert_eq!(
            store.ttl(b"k", now),
            Ttl::Expiring(Duration::from_millis(1500))
        );
    }

    #[test]
    fn rejected_set_if_changes_nothing() {
        let store = Store::new();
        let now = Instant::now();
        store.set(b"k", b"v", Some(now + Duration::from_secs(10)));

        let outcome = store.set_if(
            b"k",
            b"new",
            TtlPolicy::Expire(now + Duration::from_secs(99)),
            Condition::Nx,
            now,
        );
        assert!(!outcome.applied);
        assert_eq!(value_of(&store, b"k").as_deref(), Some(b"v".as_slice()));
        assert_eq!(store.ttl(b"k", now), Ttl::Expiring(Duration::from_secs(10)));
    }

    #[test]
    fn set_expiry_replaces_or_deletes() {
        let store = Store::new();
        let now = Instant::now();
        store.set(b"k", b"v", None);

        assert!(store.set_expiry(b"k", now + Duration::from_secs(5), now));
        assert_eq!(store.ttl(b"k", now), Ttl::Expiring(Duration::from_secs(5)));

        // A deadline at `now` deletes immediately.
        assert!(store.set_expiry(b"k", now, now));
        assert!(!store.exists(b"k", now));
        assert!(!store.set_expiry(b"missing", now, now));
    }

    #[test]
    fn persist_reports_whether_a_deadline_was_removed() {
        let store = Store::new();
        let now = Instant::now();
        store.set(b"k", b"v", Some(now + Duration::from_secs(5)));

        assert!(store.persist(b"k", now));
        assert_eq!(store.ttl(b"k", now), Ttl::Persistent);
        assert!(!store.persist(b"k", now));

        store.set(b"expired", b"v", Some(now));
        assert!(!store.persist(b"expired", now));
        assert_eq!(map_len(&store), 1, "only the persistent key remains");
    }

    #[test]
    fn del_counts_only_live_entries() {
        let store = Store::new();
        let now = Instant::now();
        store.set(b"live", b"v", None);
        store.set(b"dead", b"v", Some(now));

        assert_eq!(store.del_many([b"dead".as_slice()], now), 0);
        assert_eq!(store.del_many([b"live".as_slice()], now), 1);
        assert_eq!(store.del_many([b"live".as_slice()], now), 0);
        assert_eq!(map_len(&store), 0);
    }

    #[test]
    fn exists_counts_live_duplicates_and_prunes_expired() {
        let store = Store::new();
        let now = Instant::now();
        store.set(b"live", b"v", None);
        store.set(b"dead", b"v", Some(now));

        assert_eq!(
            store.exists_many([b"live".as_slice(), b"live", b"dead"], now),
            2
        );
        assert_eq!(store.exists_many([b"dead".as_slice()], now), 0);
        assert_eq!(map_len(&store), 1);
    }

    #[test]
    fn binary_keys_with_deadlines_round_trip() {
        let store = Store::new();
        let now = Instant::now();
        let key: &[u8] = &[0x00, 0x80, 0xFF];
        let value: &[u8] = &[0xFF, b'\r', b'\n', 0x00];

        store.set(key, value, Some(now + Duration::from_secs(1)));
        assert_eq!(store.get(key, now).as_deref(), Some(value));
        assert_eq!(store.ttl(key, now), Ttl::Expiring(Duration::from_secs(1)));
    }

    /// Covers miss, hit, overwrite, delete, and delete-again.
    #[test]
    fn crud_lifecycle() {
        let store = Store::new();
        let now = Instant::now();

        // A miss is `None`, and `exists` agrees.
        assert_eq!(value_of(&store, b"k"), None);
        assert!(!store.exists(b"k", now));

        // After a write it is a hit.
        store.set(b"k", b"first", None);
        assert_eq!(value_of(&store, b"k").as_deref(), Some(b"first".as_slice()));
        assert!(store.exists(b"k", now));

        // A second write replaces the value: exactly one entry, not a list.
        store.set(b"k", b"second", None);
        assert_eq!(
            value_of(&store, b"k").as_deref(),
            Some(b"second".as_slice())
        );

        // A previously returned value is a snapshot, unaffected by a later delete.
        let snapshot = store.get(b"k", now).expect("key is present");
        assert!(store.del(b"k", now), "deleting a present key reports true");
        assert_eq!(&snapshot[..], b"second".as_slice());

        // Deleting again reports that there was nothing to delete.
        assert_eq!(value_of(&store, b"k"), None);
        assert!(
            !store.del(b"k", now),
            "deleting an absent key reports false"
        );

        // `exists` and `get` agree in every state, including the empty-value one.
        assert_eq!(store.exists(b"k", now), value_of(&store, b"k").is_some());
        store.set(b"k", b"", None);
        assert_eq!(store.exists(b"k", now), value_of(&store, b"k").is_some());
    }

    /// Keys and values round-trip bytes that are invalid UTF-8.
    #[test]
    fn binary_keys_and_values_round_trip() {
        let store = Store::new();
        let now = Instant::now();

        // NUL, a lone continuation byte, 0xFF, and an encoded surrogate.
        let key: &[u8] = &[0x00, 0x80, 0xFF, 0xFE, 0xED, 0xA0, 0x80];
        let value: &[u8] = &[0xFF, 0x00, 0xC3, 0x28];

        store.set(key, value, None);
        assert_eq!(value_of(&store, key).as_deref(), Some(value));
        assert!(store.exists(key, now));

        assert!(store.del(key, now));
        assert_eq!(value_of(&store, key), None);
    }

    /// Empty keys and empty values are present, not absent.
    #[test]
    fn empty_key_and_empty_value_are_present() {
        let store = Store::new();

        store.set(b"present", b"", None);
        assert_eq!(value_of(&store, b"present"), Some(Vec::new()));
        assert!(store.exists(b"present", Instant::now()));

        store.set(b"", b"empty-key-value", None);
        assert_eq!(
            value_of(&store, b"").as_deref(),
            Some(b"empty-key-value".as_slice())
        );

        // The empty key did not overwrite the other key nor alias it.
        assert_eq!(value_of(&store, b"present"), Some(Vec::new()));
    }

    /// A deadline hides the value and the first access removes it.
    #[test]
    fn expiry_is_lazy_and_removes_on_access() {
        let store = Store::new();
        let now = Instant::now();
        let deadline = now + Duration::from_secs(10);

        store.set(b"k", b"v", Some(deadline));
        assert_eq!(store.get(b"k", now).as_deref(), Some(b"v".as_slice()));
        assert_eq!(store.ttl(b"k", now), Ttl::Expiring(Duration::from_secs(10)));

        // The deadline itself is already expired.
        assert_eq!(store.get(b"k", deadline), None);
        assert!(!store.exists(b"k", now));
        assert_eq!(store.ttl(b"k", now), Ttl::Missing);
    }

    /// `set`, `set_expiry`, and `persist` manage deadlines.
    #[test]
    fn deadline_lifecycle() {
        let store = Store::new();
        let now = Instant::now();
        let deadline = now + Duration::from_secs(5);

        store.set(b"k", b"v", None);
        assert_eq!(store.ttl(b"k", now), Ttl::Persistent);

        assert!(store.set_expiry(b"k", deadline, now));
        assert_eq!(store.ttl(b"k", now), Ttl::Expiring(Duration::from_secs(5)));

        assert!(store.persist(b"k", now));
        assert_eq!(store.ttl(b"k", now), Ttl::Persistent);
        assert!(!store.persist(b"k", now));

        store.set(b"k", b"other", Some(deadline));
        store.set(b"k", b"fresh", None);
        assert_eq!(store.ttl(b"k", now), Ttl::Persistent);
        assert_eq!(store.get(b"k", now).as_deref(), Some(b"fresh".as_slice()));
    }

    /// A non-positive deadline deletes, and absent keys report no deadline.
    #[test]
    fn set_expiry_on_past_deadline_deletes() {
        let store = Store::new();
        let now = Instant::now();

        store.set(b"k", b"v", None);
        assert!(store.set_expiry(b"k", now, now));
        assert!(!store.exists(b"k", now));
        assert!(!store.set_expiry(b"missing", now, now));
        assert!(!store.persist(b"missing", now));
    }

    /// Expired entries are removed but not counted.
    #[test]
    fn batch_helpers_ignore_expired_entries() {
        let store = Store::new();
        let now = Instant::now();

        store.set(b"live", b"1", None);
        store.set(b"dead", b"2", Some(now));

        assert_eq!(
            store.exists_many([b"live".as_slice(), b"live", b"dead"], now),
            2
        );
        assert_eq!(store.del_many([b"dead".as_slice()], now), 0);
        assert_eq!(store.del_many([b"live".as_slice(), b"live"], now), 1);
        assert_eq!(store.exists_many([b"live".as_slice()], now), 0);
    }

    /// Disjoint keys written concurrently: no cross-talk, no deadlock.
    #[test]
    fn shared_handle_across_threads() {
        let store = Arc::new(Store::new());
        let barrier = Arc::new(Barrier::new(THREADS));

        let workers: Vec<_> = (0..THREADS)
            .map(|i| {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let key = format!("key-{i}");
                    let value = format!("value-{i}");
                    barrier.wait();
                    for _ in 0..100 {
                        store.set(key.as_bytes(), value.as_bytes(), None);
                    }
                })
            })
            .collect();

        for worker in workers {
            worker.join().expect("worker thread panicked");
        }

        // Every key landed with its own value: no cross-talk, no lost writes.
        for i in 0..THREADS {
            let key = format!("key-{i}");
            let expected = format!("value-{i}");
            assert_eq!(
                value_of(&store, key.as_bytes()).as_deref(),
                Some(expected.as_bytes()),
                "key-{i} is missing or wrong after concurrent writes"
            );
        }
    }

    /// Batch helpers take any iterator of key slices and keep duplicate semantics.
    #[test]
    fn batch_helpers_accept_any_key_iterator() {
        let store = Store::new();
        let now = Instant::now();
        store.set(b"a", b"1", None);
        store.set(b"b", b"2", None);

        let keys: [&[u8]; 3] = [b"a", b"a", b"missing"];
        assert_eq!(store.exists_many(keys, now), 2);
        assert_eq!(store.exists_many(keys.iter().copied(), now), 2);

        assert_eq!(store.del_many(keys.iter().copied(), now), 1);
        assert!(!store.exists(b"a", now));
        assert!(store.exists(b"b", now));
    }

    /// Concurrent readers see whole snapshots while a writer replaces the value.
    #[test]
    fn readers_never_observe_torn_values() {
        const READERS: usize = 4;
        const READS: usize = 1_000;
        const WRITES: usize = 1_000;

        let store = Arc::new(Store::new());
        store.set(b"k", &[0u8; 64], None);
        let barrier = Arc::new(Barrier::new(READERS + 1));

        let readers: Vec<_> = (0..READERS)
            .map(|_| {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let now = Instant::now();
                    barrier.wait();
                    for _ in 0..READS {
                        let value = store.get(b"k", now).expect("key is present");
                        assert_eq!(value.len(), 64);
                        assert!(value.iter().all(|&b| b == value[0]));
                    }
                })
            })
            .collect();

        barrier.wait();
        for i in 0..WRITES {
            store.set(b"k", &[i as u8 % 2; 64], None);
        }

        for reader in readers {
            reader.join().expect("reader thread panicked");
        }
    }
}
