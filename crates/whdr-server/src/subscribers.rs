use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::{RwLock, watch};
use uuid::Uuid;
use whdr_proto::{ClosingReason, Event, Pattern, SubServerMsg, validate_pattern};

use crate::outbound_queue::OutboundQueue;

pub(crate) fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

pub(crate) struct SubscriberRegistry {
    subscribers: RwLock<HashMap<u64, Subscriber>>,
    next_id: AtomicU64,
}

pub(crate) struct SubscriberRegistration {
    pub(crate) name: String,
    pub(crate) remote_addr: Option<String>,
    pub(crate) queue: Arc<OutboundQueue>,
    pub(crate) close_tx: watch::Sender<Option<ClosingReason>>,
}

pub(crate) struct SubscriberSnapshot {
    pub(crate) name: String,
    pub(crate) remote_addr: Option<String>,
    pub(crate) patterns: Vec<String>,
    pub(crate) delivered: usize,
    pub(crate) dropped: usize,
}

struct Subscriber {
    name: String,
    remote_addr: Option<String>,
    patterns: Vec<Pattern>,
    queue: Arc<OutboundQueue>,
    close_tx: watch::Sender<Option<ClosingReason>>,
}

impl SubscriberRegistry {
    pub(crate) fn new() -> Self {
        Self {
            subscribers: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    pub(crate) async fn insert(&self, registration: SubscriberRegistration) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.subscribers.write().await.insert(
            id,
            Subscriber {
                name: registration.name,
                remote_addr: registration.remote_addr,
                patterns: Vec::new(),
                queue: registration.queue,
                close_tx: registration.close_tx,
            },
        );
        id
    }

    pub(crate) async fn remove(&self, id: u64) {
        self.subscribers.write().await.remove(&id);
    }

    #[cfg(test)]
    pub(crate) async fn is_empty(&self) -> bool {
        self.subscribers.read().await.is_empty()
    }

    pub(crate) async fn subscribe(&self, id: u64, patterns: Vec<String>) -> Result<(), String> {
        let mut parsed = Vec::new();
        for pattern in patterns {
            validate_pattern(&pattern).map_err(|err| format!("invalid pattern: {err}"))?;
            parsed.push(Pattern::new(pattern).map_err(|err| err.to_string())?);
        }

        if let Some(subscriber) = self.subscribers.write().await.get_mut(&id) {
            for pattern in parsed {
                if !subscriber
                    .patterns
                    .iter()
                    .any(|existing| existing == &pattern)
                {
                    subscriber.patterns.push(pattern);
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn unsubscribe(&self, id: u64, patterns: &[String]) {
        if let Some(subscriber) = self.subscribers.write().await.get_mut(&id) {
            subscriber
                .patterns
                .retain(|pattern| !patterns.iter().any(|p| p == pattern.as_str()));
        }
    }

    pub(crate) async fn fanout(&self, events: Vec<Event>) {
        if events.is_empty() {
            return;
        }
        let subscribers = self.subscribers.read().await;
        for event in events {
            // The server stamps identity/time once per event, so every
            // subscriber sees the same id — the future replay/dedup key.
            // The frame is serialized once and shared across all queues.
            let msg = SubServerMsg::Event {
                id: Uuid::new_v4(),
                ts_ms: now_unix_ms(),
                channel: event.channel.clone(),
                payload_b64: event.payload_b64,
            };
            let Ok(text) = serde_json::to_string(&msg) else {
                continue;
            };
            let text: Arc<str> = text.into();
            for subscriber in subscribers.values() {
                let matches = subscriber
                    .patterns
                    .iter()
                    .any(|pattern| pattern.matches(&event.channel).unwrap_or(false));
                if !matches {
                    continue;
                }
                subscriber.queue.push_event(text.clone());
            }
        }
    }

    pub(crate) async fn close_named(&self, name: &str, reason: ClosingReason) {
        let subscribers = self.subscribers.read().await;
        for subscriber in subscribers.values() {
            if subscriber.name == name {
                let _ = subscriber.close_tx.send(Some(reason.clone()));
            }
        }
    }

    pub(crate) async fn close_all(&self, reason: ClosingReason) {
        let subscribers = self.subscribers.read().await;
        for subscriber in subscribers.values() {
            let _ = subscriber.close_tx.send(Some(reason.clone()));
        }
    }

    pub(crate) async fn active_connection_counts(&self) -> BTreeMap<String, usize> {
        let subscribers = self.subscribers.read().await;
        let mut counts = BTreeMap::new();
        for subscriber in subscribers.values() {
            *counts.entry(subscriber.name.clone()).or_insert(0) += 1;
        }
        counts
    }

    pub(crate) async fn snapshots(&self) -> Vec<SubscriberSnapshot> {
        self.subscribers
            .read()
            .await
            .values()
            .map(|subscriber| SubscriberSnapshot {
                name: subscriber.name.clone(),
                remote_addr: subscriber.remote_addr.clone(),
                patterns: subscriber
                    .patterns
                    .iter()
                    .map(|pattern| pattern.as_str().to_string())
                    .collect(),
                delivered: subscriber.queue.delivered(),
                dropped: subscriber.queue.dropped(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::watch;

    use super::*;

    async fn insert_subscriber(
        registry: &SubscriberRegistry,
        name: &str,
        queue: Arc<OutboundQueue>,
    ) -> u64 {
        let (close_tx, _close_rx) = watch::channel(None);
        registry
            .insert(SubscriberRegistration {
                name: name.to_string(),
                remote_addr: None,
                queue,
                close_tx,
            })
            .await
    }

    fn event(channel: &str, payload_b64: &str) -> Event {
        Event {
            channel: channel.to_string(),
            payload_b64: payload_b64.to_string(),
        }
    }

    fn decode(frame: &crate::outbound_queue::Frame) -> SubServerMsg {
        serde_json::from_str(&frame.text).expect("queued frame is valid json")
    }

    #[tokio::test]
    async fn subscribe_deduplicates_and_unsubscribe_removes_patterns() {
        let registry = SubscriberRegistry::new();
        let queue = Arc::new(OutboundQueue::new(4, 1024));
        let id = insert_subscriber(&registry, "project", queue).await;

        registry
            .subscribe(id, vec!["github.>".to_string(), "github.>".to_string()])
            .await
            .unwrap();

        let snapshots = registry.snapshots().await;
        assert_eq!(snapshots[0].patterns, vec!["github.>".to_string()]);

        registry.unsubscribe(id, &["github.>".to_string()]).await;

        let snapshots = registry.snapshots().await;
        assert_eq!(snapshots[0].patterns, Vec::<String>::new());
    }

    #[tokio::test]
    async fn fanout_drops_oldest_event_and_counts_it() {
        let registry = SubscriberRegistry::new();
        let queue = Arc::new(OutboundQueue::new(1, 8192));
        let id = insert_subscriber(&registry, "project", queue.clone()).await;
        registry
            .subscribe(id, vec!["dev.>".to_string()])
            .await
            .unwrap();

        registry
            .fanout(vec![event("dev.one", "MQ=="), event("dev.two", "Mg==")])
            .await;

        // Drop-oldest: dev.one was evicted, dev.two survives.
        let frame = queue.pop().await;
        match decode(&frame) {
            SubServerMsg::Event { channel, .. } => assert_eq!(channel, "dev.two"),
            other => panic!("expected event frame, got {other:?}"),
        }
        let snapshots = registry.snapshots().await;
        assert_eq!(snapshots[0].delivered, 1);
        assert_eq!(snapshots[0].dropped, 1);
    }

    #[tokio::test]
    async fn fanout_stamps_one_id_shared_by_all_subscribers() {
        let registry = SubscriberRegistry::new();
        let mut queues = Vec::new();
        for name in ["a", "b"] {
            let queue = Arc::new(OutboundQueue::new(4, 8192));
            let id = insert_subscriber(&registry, name, queue.clone()).await;
            registry
                .subscribe(id, vec!["dev.>".to_string()])
                .await
                .unwrap();
            queues.push(queue);
        }

        registry.fanout(vec![event("dev.one", "MQ==")]).await;

        let mut ids = Vec::new();
        for queue in &queues {
            let frame = queue.pop().await;
            match decode(&frame) {
                SubServerMsg::Event { id, ts_ms, .. } => {
                    assert!(ts_ms > 0);
                    ids.push(id);
                }
                other => panic!("expected event frame, got {other:?}"),
            }
        }
        assert_eq!(ids[0], ids[1]);
    }

    #[tokio::test]
    async fn active_connection_counts_groups_by_name() {
        let registry = SubscriberRegistry::new();
        for name in ["project", "project", "other"] {
            let queue = Arc::new(OutboundQueue::new(4, 1024));
            insert_subscriber(&registry, name, queue).await;
        }

        let counts = registry.active_connection_counts().await;
        assert_eq!(counts.get("project"), Some(&2));
        assert_eq!(counts.get("other"), Some(&1));
    }
}
