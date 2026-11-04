//! Binary-safe in-memory key/value store.
//!
//! - one `RwLock<HashMap<Vec<u8>, Arc<[u8]>>>`, shareable as `Arc<Store>`
//! - no TTL, persistence, or value types
//! - a poisoned lock panics

use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

type Map = HashMap<Vec<u8>, Arc<[u8]>>;

/// Shared map from byte keys to byte values.
#[derive(Default)]
pub struct Store {
    inner: RwLock<Map>,
}

impl Store {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a snapshot of the value for `key`, or `None` if it is absent.
    pub fn get(&self, key: &[u8]) -> Option<Arc<[u8]>> {
        self.read().get(key).cloned()
    }

    /// Inserts or replaces the value for `key`.
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

    // A panic while the lock is held poisons it: another thread may have died
    // mid-write, so recovering could serve corruption silently.
    fn read(&self) -> RwLockReadGuard<'_, Map> {
        self.inner.read().expect("store lock poisoned")
    }

    fn write(&self) -> RwLockWriteGuard<'_, Map> {
        self.inner.write().expect("store lock poisoned")
    }
}
