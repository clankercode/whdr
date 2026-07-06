//! Cursor persistence hook.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;

use crate::error::Error;

/// A hook for loading and persisting the resume cursor across sessions.
///
/// Implement this to make at-least-once delivery survive process restarts:
/// `load` is called once at [`run`](crate::Client::run) start, and `save` is
/// called after each event is successfully handled. If you only need
/// not-missing-while-briefly-disconnected, the default in-memory store
/// ([`MemoryCursorStore`]) is enough.
#[async_trait]
pub trait CursorStore: Send + Sync {
    /// Load the last persisted cursor (0 to replay from the start of
    /// retention).
    async fn load(&self) -> Result<u64, Error>;

    /// Persist a cursor value. Called after each successfully-handled event.
    async fn save(&self, cursor: u64) -> Result<(), Error>;
}

/// In-memory cursor store seeded from an initial value. The default when no
/// persistence hook is configured; does not survive process restarts.
#[derive(Debug, Default)]
pub struct MemoryCursorStore {
    cursor: AtomicU64,
}

impl MemoryCursorStore {
    /// Create a store seeded with `initial` (the resume cursor).
    pub fn new(initial: u64) -> Self {
        Self {
            cursor: AtomicU64::new(initial),
        }
    }

    /// Current cursor value.
    pub fn get(&self) -> u64 {
        self.cursor.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl CursorStore for MemoryCursorStore {
    async fn load(&self) -> Result<u64, Error> {
        Ok(self.cursor.load(Ordering::Relaxed))
    }

    async fn save(&self, cursor: u64) -> Result<(), Error> {
        self.cursor.store(cursor, Ordering::Relaxed);
        Ok(())
    }
}

/// Blanket impl so `Arc<S>` is itself a `CursorStore` (lets callers keep a
/// handle to their store while handing a clone to the client).
#[async_trait]
impl<S: CursorStore + ?Sized> CursorStore for Arc<S> {
    async fn load(&self) -> Result<u64, Error> {
        (**self).load().await
    }

    async fn save(&self, cursor: u64) -> Result<(), Error> {
        (**self).save(cursor).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_store_round_trips() {
        let store = MemoryCursorStore::new(42);
        assert_eq!(store.load().await.unwrap(), 42);
        store.save(100).await.unwrap();
        assert_eq!(store.load().await.unwrap(), 100);
        assert_eq!(store.get(), 100);
    }
}
