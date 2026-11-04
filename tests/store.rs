use std::sync::{Arc, Barrier};
use std::thread;

use rust_redis::Store;

/// Workers in [`shared_handle_across_threads`], one disjoint key each.
const THREADS: usize = 8;

fn value_of(store: &Store, key: &[u8]) -> Option<Vec<u8>> {
    store.get(key).map(|value| value.to_vec())
}

/// The four operations as one state machine: miss, hit, overwrite, delete,
/// and the second delete that has nothing to remove.
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

/// Keys and values are bytes, not text. Every byte here is either illegal in or
/// lossy under UTF-8, so a `String`-keyed store would have to reject or rewrite
/// it -- which is precisely the case this design exists to keep working.
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

/// An empty key is a real key and an empty value is a real value. The design
/// keeps them distinguishable from absence because `Option` wraps presence,
/// not length.
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

/// One store, shared by threads that write simultaneously. `Store` is not
/// `Clone`; the `Arc` is the sharing, and the `RwLock` inside is what makes it
/// safe. A barrier maximises overlap so the writers genuinely contend.
///
/// This cannot prove read-modify-write atomicity, because the layer has no
/// read-modify-write operation: every interleaving of `set` on disjoint keys is
/// legal. What it does prove is that the shared handle works and that nothing
/// deadlocks or panics under the write lock.
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
