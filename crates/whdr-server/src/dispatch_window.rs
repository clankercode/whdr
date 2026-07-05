use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::time;
use uuid::Uuid;

pub(crate) enum DispatchWait<T> {
    Result(T),
    Dead,
    Timeout,
}

pub(crate) struct DispatchWindow<T> {
    pending: Arc<Mutex<HashMap<Uuid, PendingDispatch<T>>>>,
    in_flight: Arc<AtomicUsize>,
}

impl<T> DispatchWindow<T> {
    pub(crate) fn new() -> Self {
        Self {
            pending: Arc::new(Mutex::new(HashMap::new())),
            in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(crate) fn reserve(&self, max: usize) -> Option<DispatchReservation<T>> {
        if !reserve_in_flight(&self.in_flight, max) {
            return None;
        }

        let req_id = Uuid::new_v4();
        let (tx, rx) = oneshot::channel();
        let permit = InFlightPermit::new(self.in_flight.clone());
        lock_pending(&self.pending).insert(
            req_id,
            PendingDispatch {
                tx,
                _permit: permit,
            },
        );

        Some(DispatchReservation {
            pending: self.pending.clone(),
            req_id,
            rx,
            armed: true,
        })
    }

    pub(crate) fn remove(&self, req_id: &Uuid) -> Option<oneshot::Sender<T>> {
        lock_pending(&self.pending)
            .remove(req_id)
            .map(|pending| pending.tx)
    }

    pub(crate) fn clear(&self) {
        lock_pending(&self.pending).clear();
    }

    pub(crate) fn is_idle(&self) -> bool {
        self.pending_len() == 0 && self.in_flight() == 0
    }

    pub(crate) fn pending_len(&self) -> usize {
        lock_pending(&self.pending).len()
    }

    pub(crate) fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Relaxed)
    }
}

impl<T> Clone for DispatchWindow<T> {
    fn clone(&self) -> Self {
        Self {
            pending: self.pending.clone(),
            in_flight: self.in_flight.clone(),
        }
    }
}

pub(crate) struct DispatchReservation<T> {
    pending: Arc<Mutex<HashMap<Uuid, PendingDispatch<T>>>>,
    req_id: Uuid,
    rx: oneshot::Receiver<T>,
    armed: bool,
}

impl<T> DispatchReservation<T> {
    pub(crate) fn req_id(&self) -> Uuid {
        self.req_id
    }

    pub(crate) async fn wait(&mut self, timeout: Duration) -> DispatchWait<T> {
        match time::timeout(timeout, &mut self.rx).await {
            Ok(Ok(result)) => {
                self.disarm();
                DispatchWait::Result(result)
            }
            Ok(Err(_)) => {
                self.disarm();
                DispatchWait::Dead
            }
            Err(_) => {
                self.remove_pending();
                DispatchWait::Timeout
            }
        }
    }

    pub(crate) fn remove_pending(&mut self) -> bool {
        if !self.armed {
            return false;
        }
        let removed = lock_pending(&self.pending).remove(&self.req_id).is_some();
        self.armed = false;
        removed
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl<T> Drop for DispatchReservation<T> {
    fn drop(&mut self) {
        if self.armed {
            lock_pending(&self.pending).remove(&self.req_id);
        }
    }
}

struct PendingDispatch<T> {
    tx: oneshot::Sender<T>,
    _permit: InFlightPermit,
}

struct InFlightPermit {
    counter: Arc<AtomicUsize>,
}

impl InFlightPermit {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        Self { counter }
    }
}

impl Drop for InFlightPermit {
    fn drop(&mut self) {
        release_in_flight(&self.counter);
    }
}

fn reserve_in_flight(counter: &AtomicUsize, max: usize) -> bool {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        if current >= max {
            return false;
        }
        match counter.compare_exchange_weak(
            current,
            current + 1,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return true,
            Err(observed) => current = observed,
        }
    }
}

fn release_in_flight(counter: &AtomicUsize) {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
            Some(v.saturating_sub(1))
        })
        .ok();
}

fn lock_pending<T>(
    pending: &Mutex<HashMap<Uuid, PendingDispatch<T>>>,
) -> MutexGuard<'_, HashMap<Uuid, PendingDispatch<T>>> {
    pending
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
