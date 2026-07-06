use tokio::sync::RwLock;
use whdr_proto::{ClosingReason, ControlResponse};

use crate::TokenStore;
use crate::subscribers::SubscriberRegistry;

pub(crate) struct TokenControl<'a> {
    store: &'a RwLock<TokenStore>,
    subscribers: &'a SubscriberRegistry,
}

impl<'a> TokenControl<'a> {
    pub(crate) fn new(store: &'a RwLock<TokenStore>, subscribers: &'a SubscriberRegistry) -> Self {
        Self { store, subscribers }
    }

    pub(crate) async fn add(&self, name: String) -> ControlResponse {
        let mut store = self.store.write().await;
        match store.add(&name) {
            Ok(token) => ControlResponse::Token { name, token },
            Err(err) => ControlResponse::Error {
                msg: err.to_string(),
            },
        }
    }

    pub(crate) async fn rotate(&self, name: String) -> ControlResponse {
        let mut store = self.store.write().await;
        match store.rotate(&name) {
            Ok(token) => {
                drop(store);
                self.subscribers
                    .close_named(&name, ClosingReason::Revoked)
                    .await;
                ControlResponse::Token { name, token }
            }
            Err(err) => ControlResponse::Error {
                msg: err.to_string(),
            },
        }
    }

    pub(crate) async fn revoke(&self, name: String) -> ControlResponse {
        let mut store = self.store.write().await;
        match store.revoke(&name) {
            Ok(()) => {
                drop(store);
                self.subscribers
                    .close_named(&name, ClosingReason::Revoked)
                    .await;
                ControlResponse::Ok
            }
            Err(err) => ControlResponse::Error {
                msg: err.to_string(),
            },
        }
    }

    pub(crate) async fn list(&self) -> ControlResponse {
        let active = self.subscribers.active_connection_counts().await;
        ControlResponse::Tokens {
            tokens: self.store.read().await.list(&active),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::{RwLock, watch};

    use crate::outbound_queue::OutboundQueue;
    use crate::subscribers::SubscriberRegistration;

    use super::*;

    #[tokio::test]
    async fn rotate_closes_active_subscribers_for_the_token_name() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = TokenStore::load_or_empty(temp.path().join("tokens.toml")).unwrap();
        store.add("project").unwrap();
        let store = RwLock::new(store);
        let subscribers = SubscriberRegistry::new();
        let (close_tx, mut close_rx) = watch::channel(None);
        subscribers
            .insert(SubscriberRegistration {
                name: "project".to_string(),
                remote_addr: None,
                queue: Arc::new(OutboundQueue::new(1, 1024)),
                close_tx,
            })
            .await;

        let response = TokenControl::new(&store, &subscribers)
            .rotate("project".to_string())
            .await;

        assert!(matches!(response, ControlResponse::Token { .. }));
        close_rx.changed().await.unwrap();
        assert_eq!(*close_rx.borrow(), Some(ClosingReason::Revoked));
    }

    #[tokio::test]
    async fn list_includes_active_subscriber_counts() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = TokenStore::load_or_empty(temp.path().join("tokens.toml")).unwrap();
        store.add("project").unwrap();
        let store = RwLock::new(store);
        let subscribers = SubscriberRegistry::new();
        let (close_tx, _close_rx) = watch::channel(None);
        subscribers
            .insert(SubscriberRegistration {
                name: "project".to_string(),
                remote_addr: None,
                queue: Arc::new(OutboundQueue::new(1, 1024)),
                close_tx,
            })
            .await;

        let response = TokenControl::new(&store, &subscribers).list().await;

        let ControlResponse::Tokens { tokens } = response else {
            panic!("expected token list");
        };
        assert_eq!(tokens[0].name, "project");
        assert_eq!(tokens[0].active_conns, 1);
    }
}
