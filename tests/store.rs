use std::sync::{Arc, Barrier};
use std::thread;

use rust_redis::Store;

/// Workers in [`shared_handle_across_threads`], one disjoint key each.
const THREADS: usize = 8;

fn value_of(store: &Store, key: &[u8]) -> Option<Vec<u8>> {
    store.get(key).map(|value| value.to_vec())
}

/// Covers miss, hit, overwrite, delete, and delete-again.
#[test]
fn crud_lifecycle() {
    let store = Store::new();

    // A miss is `None`, and `exists` agrees.
    assert_eq!(value_of(&store, b"k"), None);
    assert!(!store.exists(b"k"));

    // After a write it is a hit.
    store.set(b"k", b"first");
    assert_eq!(value_of(&store, b"k").as_deref(), Some(b"first".as_slice()));
    assert!(store.exists(b"k"));

    // A second write replaces the value: exactly one entry, not a list.
    store.set(b"k", b"second");
    assert_eq!(
        value_of(&store, b"k").as_deref(),
        Some(b"second".as_slice())
    );

    // A previously returned value is a snapshot, unaffected by a later delete.
    let snapshot = store.get(b"k").expect("key is present");
    assert!(store.del(b"k"), "deleting a present key reports true");
    assert_eq!(&snapshot[..], b"second".as_slice());

    // Deleting again reports that there was nothing to delete.
    assert_eq!(value_of(&store, b"k"), None);
    assert!(!store.del(b"k"), "deleting an absent key reports false");

    // `exists` and `get` agree in every state, including the empty-value one.
    assert_eq!(store.exists(b"k"), value_of(&store, b"k").is_some());
    store.set(b"k", b"");
    assert_eq!(store.exists(b"k"), value_of(&store, b"k").is_some());
}

/// Keys and values round-trip bytes that are invalid UTF-8.
#[test]
fn binary_keys_and_values_round_trip() {
    let store = Store::new();

    // NUL, a lone continuation byte, 0xFF, and an encoded surrogate.
    let key: &[u8] = &[0x00, 0x80, 0xFF, 0xFE, 0xED, 0xA0, 0x80];
    let value: &[u8] = &[0xFF, 0x00, 0xC3, 0x28];

    store.set(key, value);
    assert_eq!(value_of(&store, key).as_deref(), Some(value));
    assert!(store.exists(key));

    assert!(store.del(key));
    assert_eq!(value_of(&store, key), None);
}

/// Empty keys and empty values are present, not absent.
#[test]
fn empty_key_and_empty_value_are_present() {
    let store = Store::new();

    store.set(b"present", b"");
    assert_eq!(value_of(&store, b"present"), Some(Vec::new()));
    assert!(store.exists(b"present"));

    store.set(b"", b"empty-key-value");
    assert_eq!(
        value_of(&store, b"").as_deref(),
        Some(b"empty-key-value".as_slice())
    );

    // The empty key did not overwrite the other key nor alias it.
    assert_eq!(value_of(&store, b"present"), Some(Vec::new()));
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
                    store.set(key.as_bytes(), value.as_bytes());
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
    store.set(b"a", b"1");
    store.set(b"b", b"2");

    let keys: [&[u8]; 3] = [b"a", b"a", b"missing"];
    assert_eq!(store.exists_many(keys), 2);
    assert_eq!(store.exists_many(keys.iter().copied()), 2);

    assert_eq!(store.del_many(keys.iter().copied()), 1);
    assert!(!store.exists(b"a"));
    assert!(store.exists(b"b"));
}

/// Concurrent readers see whole snapshots while a writer replaces the value.
#[test]
fn readers_never_observe_torn_values() {
    const READERS: usize = 4;
    const READS: usize = 1_000;
    const WRITES: usize = 1_000;

    let store = Arc::new(Store::new());
    store.set(b"k", &[0u8; 64]);
    let barrier = Arc::new(Barrier::new(READERS + 1));

    let readers: Vec<_> = (0..READERS)
        .map(|_| {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..READS {
                    let value = store.get(b"k").expect("key is present");
                    assert_eq!(value.len(), 64);
                    assert!(value.iter().all(|&b| b == value[0]));
                }
            })
        })
        .collect();

    barrier.wait();
    for i in 0..WRITES {
        store.set(b"k", &[i as u8 % 2; 64]);
    }

    for reader in readers {
        reader.join().expect("reader thread panicked");
    }
}
