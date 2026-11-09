use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use rust_redis::{Store, Ttl};

/// Workers in [`shared_handle_across_threads`], one disjoint key each.
const THREADS: usize = 8;

/// Real time is fine here: these tests set no deadline unless they say so.
fn now() -> Instant {
    Instant::now()
}

fn value_of(store: &Store, key: &[u8]) -> Option<Vec<u8>> {
    store.get(key, now()).map(|value| value.to_vec())
}

/// Covers miss, hit, overwrite, delete, and delete-again.
#[test]
fn crud_lifecycle() {
    let store = Store::new();

    // A miss is `None`, and `exists` agrees.
    assert_eq!(value_of(&store, b"k"), None);
    assert!(!store.exists(b"k", now()));

    // After a write it is a hit.
    store.set(b"k", b"first", None);
    assert_eq!(value_of(&store, b"k").as_deref(), Some(b"first".as_slice()));
    assert!(store.exists(b"k", now()));

    // A second write replaces the value: exactly one entry, not a list.
    store.set(b"k", b"second", None);
    assert_eq!(
        value_of(&store, b"k").as_deref(),
        Some(b"second".as_slice())
    );

    // A previously returned value is a snapshot, unaffected by a later delete.
    let snapshot = store.get(b"k", now()).expect("key is present");
    assert!(
        store.del(b"k", now()),
        "deleting a present key reports true"
    );
    assert_eq!(&snapshot[..], b"second".as_slice());

    // Deleting again reports that there was nothing to delete.
    assert_eq!(value_of(&store, b"k"), None);
    assert!(
        !store.del(b"k", now()),
        "deleting an absent key reports false"
    );

    // `exists` and `get` agree in every state, including the empty-value one.
    assert_eq!(store.exists(b"k", now()), value_of(&store, b"k").is_some());
    store.set(b"k", b"", None);
    assert_eq!(store.exists(b"k", now()), value_of(&store, b"k").is_some());
}

/// Keys and values round-trip bytes that are invalid UTF-8.
#[test]
fn binary_keys_and_values_round_trip() {
    let store = Store::new();

    // NUL, a lone continuation byte, 0xFF, and an encoded surrogate.
    let key: &[u8] = &[0x00, 0x80, 0xFF, 0xFE, 0xED, 0xA0, 0x80];
    let value: &[u8] = &[0xFF, 0x00, 0xC3, 0x28];

    store.set(key, value, None);
    assert_eq!(value_of(&store, key).as_deref(), Some(value));
    assert!(store.exists(key, now()));

    assert!(store.del(key, now()));
    assert_eq!(value_of(&store, key), None);
}

/// Empty keys and empty values are present, not absent.
#[test]
fn empty_key_and_empty_value_are_present() {
    let store = Store::new();

    store.set(b"present", b"", None);
    assert_eq!(value_of(&store, b"present"), Some(Vec::new()));
    assert!(store.exists(b"present", now()));

    store.set(b"", b"empty-key-value", None);
    assert_eq!(
        value_of(&store, b"").as_deref(),
        Some(b"empty-key-value".as_slice())
    );

    // The empty key did not overwrite the other key nor alias it.
    assert_eq!(value_of(&store, b"present"), Some(Vec::new()));
}

/// A deadline hides the key from every read path once it passes.
#[test]
fn expiry_hides_and_reclaims_on_access() {
    let store = Store::new();
    let now = Instant::now();
    let deadline = now + Duration::from_secs(5);
    store.set(b"k", b"v", Some(deadline));

    assert_eq!(store.get(b"k", now).as_deref(), Some(b"v".as_slice()));
    assert!(store.exists(b"k", now));
    assert_eq!(store.ttl(b"k", now), Ttl::Remaining(Duration::from_secs(5)));

    assert_eq!(store.get(b"k", deadline), None);
    assert!(!store.exists(b"k", deadline));
    assert_eq!(store.ttl(b"k", deadline), Ttl::Missing);
}

/// A plain `set` clears the deadline; `del` removes value and deadline together.
#[test]
fn expiry_is_cleared_by_set_and_removed_with_del() {
    let store = Store::new();
    let now = Instant::now();
    let deadline = now + Duration::from_secs(60);

    store.set(b"k", b"v", Some(deadline));
    store.set(b"k", b"v2", None);
    assert_eq!(store.ttl(b"k", now), Ttl::Persistent);
    assert_eq!(
        store
            .get(b"k", deadline + Duration::from_secs(1))
            .as_deref(),
        Some(b"v2".as_slice())
    );

    store.set(b"k", b"v3", Some(deadline));
    assert!(store.del(b"k", now));
    assert_eq!(store.ttl(b"k", now), Ttl::Missing);
}

/// Batch helpers treat an expired key as absent but still reclaim it.
#[test]
fn expired_keys_count_as_absent_in_batches() {
    let store = Store::new();
    let now = Instant::now();
    store.set(b"live", b"1", None);
    store.set(b"gone", b"2", Some(now + Duration::from_secs(1)));

    let later = now + Duration::from_secs(2);
    let keys = [&b"live"[..], &b"gone"[..]];
    assert_eq!(store.exists_many(keys, later), 1);
    assert_eq!(store.del_many(keys, later), 1);
    assert!(!store.exists(b"live", later));
    assert_eq!(store.ttl(b"live", later), Ttl::Missing);
}

/// Deadlines apply to binary keys like any other key.
#[test]
fn expiry_applies_to_binary_keys() {
    let store = Store::new();
    let now = Instant::now();
    let key: &[u8] = &[0x00, 0x80, 0xFF];

    store.set(key, b"v", Some(now + Duration::from_secs(1)));
    assert_eq!(store.get(key, now).as_deref(), Some(b"v".as_slice()));
    assert_eq!(store.get(key, now + Duration::from_secs(1)), None);
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
    store.set(b"a", b"1", None);
    store.set(b"b", b"2", None);

    let keys: [&[u8]; 3] = [b"a", b"a", b"missing"];
    assert_eq!(store.exists_many(keys, now()), 2);
    assert_eq!(store.exists_many(keys.iter().copied(), now()), 2);

    assert_eq!(store.del_many(keys.iter().copied(), now()), 1);
    assert!(!store.exists(b"a", now()));
    assert!(store.exists(b"b", now()));
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
                barrier.wait();
                for _ in 0..READS {
                    let value = store.get(b"k", Instant::now()).expect("key is present");
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
