//! In-memory, binary-safe key/value storage: no TTL, no value types, no
//! persistence.
//!
//! One `RwLock<HashMap<Vec<u8>, Arc<[u8]>>>` is shared by every connection
//! thread, so readers run concurrently and writers are exclusive. [`Store`] is
//! not `Clone`; callers hold `Arc<Store>` and clone that.
//!
//! Keys and values are unbounded, an empty key is valid, empty value is present value.
//! 
//! [`Store::get`] hands back an owned `Arc<[u8]>` instead of a borrow
//! because the read lock cannot outlive the call, result is a snapshot
//! and stays valid if the key is deleted or overwritten

use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// A shared, in-memory map from byte keys to byte values.
pub struct Store {
    inner: RwLock<HashMap<Vec<u8>, Arc<[u8]>>>,
}

impl Store {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Returns the value for `key`, or `None` if it is absent.
    pub fn get(&self, key: &[u8]) -> Option<Arc<[u8]>> {
        self.read().get(key).cloned()
    }

    /// Inserts or replaces the value for `key`.
    ///
    /// Replacing reports nothing
    pub fn set(&self, key: &[u8], value: &[u8]) {
        self.write().insert(key.to_vec(), Arc::from(value));
    }

    /// Removes `key`, returning whether it was present.
    pub fn del(&self, key: &[u8]) -> bool {
        self.write().remove(key).is_some()
    }

    /// Returns whether `key` is present.
    pub fn exists(&self, key: &[u8]) -> bool {
        self.read().contains_key(key)
    }

    // A panic while the lock was held poisons it: another thread died
    // mid-write, so the map may be inconsistent. Recovering would serve that
    // corruption silently, which is worse than dying loudly here.
    fn read(&self) -> RwLockReadGuard<'_, HashMap<Vec<u8>, Arc<[u8]>>> {
        self.inner.read().expect("store lock poisoned")
    }

    fn write(&self) -> RwLockWriteGuard<'_, HashMap<Vec<u8>, Arc<[u8]>>> {
        self.inner.write().expect("store lock poisoned")
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}
