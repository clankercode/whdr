use std::collections::{BTreeMap, HashMap, VecDeque};
use std::env;
use std::fs;
#[cfg(test)]
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use axum::body::Bytes;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{OriginalUri, State};
use axum::http::header::{AUTHORIZATION, HeaderName, HeaderValue, RETRY_AFTER};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Router, routing::any};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, Lines};
use tokio::net::{TcpListener, UnixListener, UnixStream};
use tokio::process::{ChildStdin, ChildStdout};
use tokio::sync::{Mutex as AsyncMutex, RwLock, mpsc, oneshot, watch};
use tokio::time;
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use whdr_proto::{
    ClosingReason, ControlRequest, ControlResponse, Event, ExtMsg, HttpReply, SrvMsg, SubClientMsg,
    SubServerMsg, decode_line, encode_line, validate_channel,
};

use crate::dispatch_window::{DispatchWait, DispatchWindow};
use crate::extension_process::{
    ExtensionProcess, kill_child_wait, spawn_extension_process, wait_for_child_shutdown,
};
use crate::extension_registration::read_registration;
use crate::subscribers::{SubscriberRegistration, SubscriberRegistry};
use crate::{Config, TokenStore};

const INITIAL_RESPAWN_BACKOFF_MS: u64 = 250;
const MAX_RESPAWN_BACKOFF_MS: u64 = 30_000;

#[derive(Clone)]
pub struct AppState {
    inner: Arc<Inner>,
}

struct Inner {
    config: RwLock<Config>,
    token_store: RwLock<TokenStore>,
    routes: RwLock<HashMap<String, String>>,
    unavailable_routes: RwLock<HashMap<String, String>>,
    channel_prefixes: RwLock<HashMap<String, String>>,
    extensions: RwLock<HashMap<String, ExtHandle>>,
    failed_extensions: RwLock<HashMap<String, String>>,
    subscribers: SubscriberRegistry,
    supervisor: AsyncMutex<HashMap<String, SupervisorState>>,
    shutting_down: AtomicBool,
    started: Instant,
}

#[derive(Clone)]
struct ExtHandle {
    candidate_id: String,
    id: String,
    generation: Uuid,
    path: PathBuf,
    paths: Vec<String>,
    channels: Vec<String>,
    tx: mpsc::Sender<SrvMsg>,
    supervisor_tx: mpsc::Sender<SupervisorCommand>,
    dispatches: DispatchWindow<ExtResult>,
    protocol_errors: Arc<AtomicUsize>,
    namespace_violations: Arc<AtomicUsize>,
    consecutive_timeouts: Arc<AtomicUsize>,
    restarts: Arc<AtomicUsize>,
    pid: Option<u32>,
}

struct ExtResult {
    http: HttpReply,
    events: Vec<Event>,
}

#[derive(Debug)]
enum SupervisorCommand {
    DrainStop {
        reason: String,
        done: Option<oneshot::Sender<()>>,
    },
    KillAndRespawn {
        reason: String,
    },
}

#[derive(Default)]
struct SupervisorState {
    restarts: usize,
    recent_exits: VecDeque<Instant>,
}

#[derive(Debug)]
enum DispatchError {
    Busy,
    Starting,
    Timeout,
    Dead,
    NotFound,
}

pub fn route_key_from_path(path: &str) -> Option<String> {
    path.trim_start_matches('/')
        .split('/')
        .find(|segment| !segment.is_empty())
        .map(str::to_string)
}

impl AppState {
    pub async fn new(config: Config) -> Result<Self> {
        config.validate()?;
        let token_store = TokenStore::load_or_empty(config.token_store_path())?;
        Ok(Self {
            inner: Arc::new(Inner {
                config: RwLock::new(config),
                token_store: RwLock::new(token_store),
                routes: RwLock::new(HashMap::new()),
                unavailable_routes: RwLock::new(HashMap::new()),
                channel_prefixes: RwLock::new(HashMap::new()),
                extensions: RwLock::new(HashMap::new()),
                failed_extensions: RwLock::new(HashMap::new()),
                subscribers: SubscriberRegistry::new(),
                supervisor: AsyncMutex::new(HashMap::new()),
                shutting_down: AtomicBool::new(false),
                started: Instant::now(),
            }),
        })
    }

    pub async fn start_extensions(&self) -> Result<()> {
        let config = self.inner.config.read().await.clone();
        let candidates = discover_extensions()?;
        let mut candidate_map: HashMap<String, PathBuf> = candidates.into_iter().collect();
        let desired: HashMap<String, PathBuf> = if config.extensions.autostart_all {
            candidate_map.clone()
        } else {
            config
                .extensions
                .enabled
                .iter()
                .filter_map(|id| candidate_map.remove(id).map(|path| (id.clone(), path)))
                .collect()
        };

        self.stop_removed_extensions(&desired).await;

        for wanted in &config.extensions.enabled {
            if self.extension_candidate_is_active(wanted).await {
                continue;
            }
            if !desired.contains_key(wanted) {
                self.inner.failed_extensions.write().await.insert(
                    wanted.clone(),
                    "extension binary not found on PATH".to_string(),
                );
                self.inner.unavailable_routes.write().await.insert(
                    wanted.clone(),
                    "extension binary not found on PATH".to_string(),
                );
            }
        }

        for (candidate_id, path) in desired {
            if self.extension_candidate_is_active(&candidate_id).await {
                continue;
            }
            match self.start_extension(candidate_id.clone(), path).await {
                Ok(()) => {
                    self.inner
                        .failed_extensions
                        .write()
                        .await
                        .remove(&candidate_id);
                }
                Err(err) => {
                    warn!(ext = candidate_id, error = %err, "extension failed to start");
                    self.inner
                        .failed_extensions
                        .write()
                        .await
                        .insert(candidate_id.clone(), err.to_string());
                    self.inner
                        .unavailable_routes
                        .write()
                        .await
                        .insert(candidate_id, err.to_string());
                }
            }
        }
        Ok(())
    }

    async fn start_extension(&self, candidate_id: String, path: PathBuf) -> Result<()> {
        let config = self.inner.config.read().await.clone();
        let ExtensionProcess {
            mut child,
            stdin,
            stdout,
            pid,
        } = spawn_extension_process(&candidate_id, &path).await?;

        let mut lines = BufReader::new(stdout).lines();
        let registration =
            match read_registration(&candidate_id, &mut lines, config.timeouts.register_ms).await {
                Ok(registration) => registration,
                Err(err) => {
                    kill_child_wait(&mut child, &candidate_id, "register failed").await;
                    return Err(err);
                }
            };
        let id = registration.id;
        let aliases = registration.paths;
        let channels = registration.channels;

        let mut claims = vec![id.clone()];
        claims.extend(aliases);

        let (tx, rx) = mpsc::channel::<SrvMsg>(config.limits.max_in_flight.max(1) * 2);
        let (supervisor_tx, supervisor_rx) = mpsc::channel::<SupervisorCommand>(4);
        let dispatches = DispatchWindow::new();
        let protocol_errors = Arc::new(AtomicUsize::new(0));
        let namespace_violations = Arc::new(AtomicUsize::new(0));
        let consecutive_timeouts = Arc::new(AtomicUsize::new(0));
        let restarts = Arc::new(AtomicUsize::new(self.restart_count(&candidate_id).await));
        let generation = Uuid::new_v4();

        let handle = ExtHandle {
            candidate_id: candidate_id.clone(),
            id: id.clone(),
            generation,
            path: path.clone(),
            paths: claims.clone(),
            channels: channels.clone(),
            tx: tx.clone(),
            supervisor_tx: supervisor_tx.clone(),
            dispatches: dispatches.clone(),
            protocol_errors: protocol_errors.clone(),
            namespace_violations: namespace_violations.clone(),
            consecutive_timeouts: consecutive_timeouts.clone(),
            restarts: restarts.clone(),
            pid,
        };

        let registration_result = {
            let (mut extensions, mut routes, mut prefixes) = tokio::join!(
                self.inner.extensions.write(),
                self.inner.routes.write(),
                self.inner.channel_prefixes.write()
            );
            if extensions.contains_key(&id) {
                bail!("extension collision: {id} is already running");
            }
            if extensions
                .values()
                .any(|handle| handle.candidate_id == candidate_id)
            {
                bail!("extension collision: {candidate_id} is already running");
            }
            validate_registration_claims(&id, &claims, &channels, &routes, &prefixes)?;

            extensions.insert(id.clone(), handle);
            for claim in &claims {
                routes.insert(claim.clone(), id.clone());
            }
            for channel in &channels {
                prefixes.insert(channel.clone(), id.clone());
            }
            Ok::<(), anyhow::Error>(())
        };

        if let Err(err) = registration_result {
            kill_child_wait(&mut child, &id, "registration rejected").await;
            return Err(err);
        }

        {
            let mut failed = self.inner.failed_extensions.write().await;
            failed.remove(&candidate_id);
            failed.remove(&id);
        }
        {
            let mut unavailable = self.inner.unavailable_routes.write().await;
            unavailable.remove(&candidate_id);
            for claim in &claims {
                unavailable.remove(claim);
            }
        }

        spawn_extension_writer(stdin, rx);
        ExtensionReaderTask {
            state: self.clone(),
            dispatches: dispatches.clone(),
            protocol_errors: protocol_errors.clone(),
            namespace_violations: namespace_violations.clone(),
            consecutive_timeouts: consecutive_timeouts.clone(),
            supervisor_tx: supervisor_tx.clone(),
            id: id.clone(),
            generation,
            paths: claims.clone(),
            channels: channels.clone(),
        }
        .spawn(lines);

        tokio::spawn(supervise_extension(
            self.clone(),
            candidate_id.clone(),
            id.clone(),
            path,
            generation,
            child,
            supervisor_rx,
        ));

        info!(ext = id, "extension ready");
        Ok(())
    }

    async fn dispatch(
        &self,
        route_key: &str,
        method: Method,
        path: String,
        query: Option<String>,
        headers: BTreeMap<String, String>,
        body: Bytes,
    ) -> Result<ExtResult, DispatchError> {
        let ext_id = {
            let routes = self.inner.routes.read().await;
            routes.get(route_key).cloned()
        };
        let Some(ext_id) = ext_id else {
            let config = self.inner.config.read().await;
            if config.extensions.enabled.iter().any(|id| id == route_key)
                || self
                    .inner
                    .unavailable_routes
                    .read()
                    .await
                    .contains_key(route_key)
                || self
                    .inner
                    .failed_extensions
                    .read()
                    .await
                    .contains_key(route_key)
            {
                return Err(DispatchError::Starting);
            }
            return Err(DispatchError::NotFound);
        };

        let handle = {
            let exts = self.inner.extensions.read().await;
            exts.get(&ext_id).cloned()
        }
        .ok_or(DispatchError::Starting)?;
        handle
            .dispatch(self, method, path, query, headers, body)
            .await
    }

    async fn fanout(&self, events: Vec<Event>) {
        self.inner.subscribers.fanout(events).await;
    }

    async fn close_subscribers_named(&self, name: &str, reason: ClosingReason) {
        self.inner.subscribers.close_named(name, reason).await;
    }

    async fn stop_removed_extensions(&self, desired: &HashMap<String, PathBuf>) {
        let current: Vec<ExtHandle> = self
            .inner
            .extensions
            .read()
            .await
            .values()
            .cloned()
            .collect();
        for handle in current {
            if desired.contains_key(&handle.candidate_id) {
                continue;
            }
            info!(
                ext = handle.id,
                candidate = handle.candidate_id,
                "extension removed from config; draining"
            );
            self.unroute_extension(&handle).await;
            let _ = handle
                .supervisor_tx
                .send(SupervisorCommand::DrainStop {
                    reason: "removed from config".to_string(),
                    done: None,
                })
                .await;
        }
    }

    async fn unroute_extension(&self, handle: &ExtHandle) {
        {
            let mut routes = self.inner.routes.write().await;
            routes.retain(|_, owner| owner != &handle.id);
        }
        {
            let mut prefixes = self.inner.channel_prefixes.write().await;
            prefixes.retain(|_, owner| owner != &handle.id);
        }
        {
            let mut unavailable = self.inner.unavailable_routes.write().await;
            unavailable.remove(&handle.candidate_id);
            for claim in &handle.paths {
                unavailable.remove(claim);
            }
        }
    }

    async fn cleanup_extension_generation(
        &self,
        id: &str,
        generation: Uuid,
        reason: &str,
        mark_unavailable: bool,
    ) -> Option<ExtHandle> {
        let removed = {
            let mut extensions = self.inner.extensions.write().await;
            if extensions
                .get(id)
                .is_some_and(|handle| handle.generation == generation)
            {
                extensions.remove(id)
            } else {
                None
            }
        };

        let handle = removed?;

        self.unroute_extension(&handle).await;
        handle.dispatches.clear();
        {
            let mut unavailable = self.inner.unavailable_routes.write().await;
            if mark_unavailable {
                unavailable.insert(handle.candidate_id.clone(), reason.to_string());
                for claim in &handle.paths {
                    unavailable.insert(claim.clone(), reason.to_string());
                }
            } else {
                unavailable.remove(&handle.candidate_id);
                for claim in &handle.paths {
                    unavailable.remove(claim);
                }
            }
        }
        Some(handle)
    }

    async fn extension_is_active(&self, id: &str, generation: Uuid) -> bool {
        self.inner
            .extensions
            .read()
            .await
            .get(id)
            .is_some_and(|handle| handle.generation == generation)
    }

    async fn extension_candidate_is_active(&self, candidate_id: &str) -> bool {
        self.inner
            .extensions
            .read()
            .await
            .values()
            .any(|handle| handle.candidate_id == candidate_id)
    }

    async fn extension_handle(&self, id: &str, generation: Uuid) -> Option<ExtHandle> {
        self.inner
            .extensions
            .read()
            .await
            .get(id)
            .filter(|handle| handle.generation == generation)
            .cloned()
    }

    async fn wait_for_extension_drain(
        &self,
        id: &str,
        generation: Uuid,
        timeout: Duration,
    ) -> bool {
        let wait = async {
            loop {
                let Some(handle) = self.extension_handle(id, generation).await else {
                    return true;
                };
                if handle.dispatches.is_idle() {
                    return true;
                }
                time::sleep(Duration::from_millis(5)).await;
            }
        };
        time::timeout(timeout, wait).await.unwrap_or(false)
    }

    async fn should_respawn(&self, id: &str) -> bool {
        if self.inner.shutting_down.load(Ordering::Relaxed) {
            return false;
        }
        if self.inner.failed_extensions.read().await.contains_key(id) {
            return false;
        }
        let config = self.inner.config.read().await;
        config.extensions.autostart_all
            || config.extensions.enabled.iter().any(|wanted| wanted == id)
    }

    async fn restart_count(&self, id: &str) -> usize {
        self.inner
            .supervisor
            .lock()
            .await
            .get(id)
            .map(|state| state.restarts)
            .unwrap_or_default()
    }

    async fn next_respawn_delay(&self, id: &str) -> Option<Duration> {
        let config = self.inner.config.read().await.clone();
        let threshold = config.limits.crashloop_threshold.max(1);
        let window = Duration::from_millis(config.limits.crashloop_window_ms.max(1));
        let now = Instant::now();
        let mut supervisor = self.inner.supervisor.lock().await;
        let state = supervisor.entry(id.to_string()).or_default();
        while state
            .recent_exits
            .front()
            .is_some_and(|exit| now.duration_since(*exit) > window)
        {
            state.recent_exits.pop_front();
        }
        state.recent_exits.push_back(now);
        if state.recent_exits.len() >= threshold {
            let reason = format!(
                "extension entered crashloop: {threshold} exits within {}ms",
                window.as_millis()
            );
            drop(supervisor);
            self.inner
                .failed_extensions
                .write()
                .await
                .insert(id.to_string(), reason.clone());
            self.inner
                .unavailable_routes
                .write()
                .await
                .entry(id.to_string())
                .or_insert(reason);
            return None;
        }
        let shift = state.restarts.min(7) as u32;
        state.restarts += 1;
        let delay_ms = INITIAL_RESPAWN_BACKOFF_MS
            .saturating_mul(1u64 << shift)
            .min(MAX_RESPAWN_BACKOFF_MS);
        Some(Duration::from_millis(delay_ms))
    }

    async fn status_json(&self) -> Value {
        let extensions = self.inner.extensions.read().await;
        let failed = self.inner.failed_extensions.read().await;
        let mut ext_rows = Vec::new();
        for handle in extensions.values() {
            ext_rows.push(json!({
                "candidate_id": handle.candidate_id,
                "id": handle.id,
                "state": "Ready",
                "pid": handle.pid,
                "path": handle.path,
                "restarts": handle.restarts.load(Ordering::Relaxed),
                "paths": handle.paths,
                "channels": handle.channels,
                "in_flight": handle.dispatches.in_flight(),
                "protocol_errors": handle.protocol_errors.load(Ordering::Relaxed),
                "namespace_violations": handle.namespace_violations.load(Ordering::Relaxed),
                "consecutive_timeouts": handle.consecutive_timeouts.load(Ordering::Relaxed),
            }));
        }
        for (id, reason) in failed.iter() {
            ext_rows.push(json!({
                "id": id,
                "state": "Failed",
                "reason": reason,
            }));
        }
        let sub_rows: Vec<Value> = self
            .inner
            .subscribers
            .snapshots()
            .await
            .into_iter()
            .map(|subscriber| {
                json!({
                    "name": subscriber.name,
                    "remote_addr": subscriber.remote_addr,
                    "patterns": subscriber.patterns,
                    "delivered": subscriber.delivered,
                    "dropped": subscriber.dropped,
                })
            })
            .collect();
        let subscriber_count = sub_rows.len();
        json!({
            "uptime_ms": self.inner.started.elapsed().as_millis(),
            "extensions": ext_rows,
            "subscribers": sub_rows,
            "global": {
                "routes": self.inner.routes.read().await.len(),
                "unavailable_routes": self.inner.unavailable_routes.read().await.len(),
                "channel_prefixes": self.inner.channel_prefixes.read().await.len(),
                "subscriber_count": subscriber_count,
            }
        })
    }
}

impl ExtHandle {
    async fn dispatch(
        &self,
        state: &AppState,
        method: Method,
        path: String,
        query: Option<String>,
        headers: BTreeMap<String, String>,
        body: Bytes,
    ) -> Result<ExtResult, DispatchError> {
        let config = state.inner.config.read().await.clone();
        let Some(mut reservation) = self.dispatches.reserve(config.limits.max_in_flight) else {
            return Err(DispatchError::Busy);
        };
        let secret = config.secrets.get(&self.id).cloned();
        let msg = SrvMsg::Dispatch {
            req_id: reservation.req_id(),
            method: method.to_string(),
            path,
            query,
            headers,
            body_b64: STANDARD.encode(body),
            secret,
        };
        if self.tx.send(msg).await.is_err() {
            reservation.remove_pending();
            return Err(DispatchError::Dead);
        }
        match reservation
            .wait(Duration::from_millis(config.limits.dispatch_timeout_ms))
            .await
        {
            DispatchWait::Result(mut result) => {
                result.events = filter_owned_events(
                    &self.id,
                    &self.paths,
                    &self.channels,
                    &self.namespace_violations,
                    result.events,
                );
                state.fanout(result.events.clone()).await;
                Ok(result)
            }
            DispatchWait::Dead => Err(DispatchError::Dead),
            DispatchWait::Timeout => {
                let timeouts = self.consecutive_timeouts.fetch_add(1, Ordering::Relaxed) + 1;
                let threshold = config.limits.hang_kill_threshold.max(1);
                if timeouts == threshold {
                    warn!(
                        ext = self.id,
                        timeouts,
                        "extension exceeded consecutive dispatch timeout threshold; killing"
                    );
                    let _ = self
                        .supervisor_tx
                        .try_send(SupervisorCommand::KillAndRespawn {
                            reason: format!("{timeouts} consecutive dispatch timeouts"),
                        });
                }
                Err(DispatchError::Timeout)
            }
        }
    }
}

fn spawn_extension_writer(stdin: ChildStdin, mut rx: mpsc::Receiver<SrvMsg>) {
    tokio::spawn(async move {
        let mut writer = BufWriter::new(stdin);
        while let Some(msg) = rx.recv().await {
            match encode_line(&msg) {
                Ok(line) => {
                    if writer.write_all(line.as_bytes()).await.is_err() {
                        break;
                    }
                    if writer.flush().await.is_err() {
                        break;
                    }
                }
                Err(err) => {
                    error!(error = %err, "failed to encode server message");
                }
            }
        }
    });
}

struct ExtensionReaderTask {
    state: AppState,
    dispatches: DispatchWindow<ExtResult>,
    protocol_errors: Arc<AtomicUsize>,
    namespace_violations: Arc<AtomicUsize>,
    consecutive_timeouts: Arc<AtomicUsize>,
    supervisor_tx: mpsc::Sender<SupervisorCommand>,
    id: String,
    generation: Uuid,
    paths: Vec<String>,
    channels: Vec<String>,
}

impl ExtensionReaderTask {
    fn spawn(self, lines: Lines<BufReader<ChildStdout>>) {
        tokio::spawn(self.run(lines));
    }

    async fn run(self, mut lines: Lines<BufReader<ChildStdout>>) {
        while let Ok(Some(line)) = lines.next_line().await {
            if !self
                .state
                .extension_is_active(&self.id, self.generation)
                .await
            {
                break;
            }
            match decode_line::<ExtMsg>(&line) {
                Ok(Some(ExtMsg::Result {
                    req_id,
                    http,
                    mut events,
                })) => {
                    if let Some(tx) = self.dispatches.remove(&req_id) {
                        self.consecutive_timeouts.store(0, Ordering::Relaxed);
                        events = filter_owned_events(
                            &self.id,
                            &self.paths,
                            &self.channels,
                            &self.namespace_violations,
                            events,
                        );
                        let _ = tx.send(ExtResult { http, events });
                    } else {
                        warn!(ext = self.id, %req_id, "late or unknown result dropped");
                    }
                }
                Ok(Some(ExtMsg::Event { ev })) => {
                    let events = filter_owned_events(
                        &self.id,
                        &self.paths,
                        &self.channels,
                        &self.namespace_violations,
                        vec![ev],
                    );
                    self.state.fanout(events).await;
                }
                Ok(Some(ExtMsg::Log { level, msg })) => {
                    info!(ext = self.id, ?level, "{msg}");
                }
                Ok(Some(ExtMsg::Register { .. })) => {
                    warn!(ext = self.id, "duplicate register ignored");
                }
                Ok(None) => {}
                Err(err) => {
                    if self.handle_protocol_error(err.into()).await {
                        break;
                    }
                }
            }
        }
    }

    async fn handle_protocol_error(&self, err: anyhow::Error) -> bool {
        let count = self.protocol_errors.fetch_add(1, Ordering::Relaxed) + 1;
        let threshold = self
            .state
            .inner
            .config
            .read()
            .await
            .limits
            .max_protocol_errors
            .max(1);
        warn!(
            ext = self.id,
            error = %err,
            count,
            threshold,
            "protocol error"
        );
        if count >= threshold {
            let _ = self
                .supervisor_tx
                .send(SupervisorCommand::KillAndRespawn {
                    reason: format!("{count} protocol errors"),
                })
                .await;
            return true;
        }
        false
    }
}

async fn supervise_extension(
    state: AppState,
    candidate_id: String,
    id: String,
    path: PathBuf,
    generation: Uuid,
    mut child: tokio::process::Child,
    mut commands: mpsc::Receiver<SupervisorCommand>,
) {
    let (reason, should_consider_respawn, done) = tokio::select! {
            status = child.wait() => {
                let reason = match status {
                    Ok(status) => format!("extension exited with {status}"),
                    Err(err) => format!("extension wait failed: {err}"),
                };
                debug!(ext = id, %reason);
                (reason, true, None)
            }
            command = commands.recv() => {
                match command {
                    Some(SupervisorCommand::DrainStop { reason, done }) => {
                        let timeouts = state.inner.config.read().await.timeouts.clone();
                        let drain_ms = timeouts.drain_ms;
                        let drained = state
                            .wait_for_extension_drain(&id, generation, Duration::from_millis(drain_ms))
                            .await;
                        if !drained {
                            warn!(ext = id, drain_ms, "extension drain timed out");
                        }
                        if let Some(handle) = state.extension_handle(&id, generation).await {
                            let _ = handle.tx.send(SrvMsg::Shutdown).await;
                        }
                        wait_for_child_shutdown(&mut child, &id, timeouts.term_grace_ms).await;
                        (reason, false, done)
                    }
                    Some(SupervisorCommand::KillAndRespawn { reason }) => {
                        let _ = child.start_kill();
                        let _ = child.wait().await;
                        (reason, true, None)
                    }
                    None => {
                        let _ = child.start_kill();
                        let _ = child.wait().await;
                        ("extension supervisor command channel closed".to_string(), false, None)
                    }
                }
            }
    };

    let removed = state
        .cleanup_extension_generation(&id, generation, &reason, should_consider_respawn)
        .await;
    if let Some(done) = done {
        let _ = done.send(());
    }
    if removed.is_none() || !should_consider_respawn || !state.should_respawn(&candidate_id).await {
        return;
    }

    let Some(delay) = state.next_respawn_delay(&candidate_id).await else {
        warn!(
            ext = id,
            candidate = candidate_id,
            "extension crashloop threshold reached; not respawning"
        );
        return;
    };
    time::sleep(delay).await;
    if !state.should_respawn(&candidate_id).await
        || state.extension_candidate_is_active(&candidate_id).await
    {
        return;
    }
    let respawn_state = state.clone();
    let respawn_id = candidate_id.clone();
    let respawn_path = path;
    let handle = tokio::runtime::Handle::current();
    let start_result = tokio::task::spawn_blocking(move || {
        handle.block_on(respawn_state.start_extension(respawn_id, respawn_path))
    })
    .await;
    match start_result {
        Ok(Ok(())) => {
            state
                .inner
                .failed_extensions
                .write()
                .await
                .remove(&candidate_id);
        }
        Ok(Err(err)) => {
            warn!(ext = id, candidate = candidate_id, error = %err, "extension respawn failed");
            state
                .inner
                .failed_extensions
                .write()
                .await
                .insert(candidate_id.clone(), err.to_string());
            state
                .inner
                .unavailable_routes
                .write()
                .await
                .insert(candidate_id, err.to_string());
        }
        Err(err) => {
            warn!(ext = id, candidate = candidate_id, error = %err, "extension respawn task failed");
            state
                .inner
                .failed_extensions
                .write()
                .await
                .insert(candidate_id.clone(), err.to_string());
            state
                .inner
                .unavailable_routes
                .write()
                .await
                .insert(candidate_id, err.to_string());
        }
    }
}

pub async fn run_with_signals(config_path: PathBuf) -> Result<()> {
    let config = Config::load(&config_path)?;
    let state = AppState::new(config).await?;
    state.start_extensions().await?;
    let servers = start_servers(state.clone()).await?;
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let mut interrupt =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        let mut hangup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;
        loop {
            tokio::select! {
                _ = term.recv() => break,
                _ = interrupt.recv() => break,
                _ = hangup.recv() => {
                    if let Err(err) = reload_from_path(&state, &config_path).await {
                        error!(error = %err, "SIGHUP reload failed");
                    }
                },
            }
        }
        shutdown_state(&state, servers).await;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = config_path;
        tokio::signal::ctrl_c().await?;
        shutdown_state(&state, servers).await;
        Ok(())
    }
}

pub async fn run_until_shutdown(
    config: Config,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let state = AppState::new(config).await?;
    state.start_extensions().await?;
    let servers = start_servers(state.clone()).await?;

    shutdown.await;
    shutdown_state(&state, servers).await;
    Ok(())
}

struct RunningServers {
    #[cfg(test)]
    sub_addr: SocketAddr,
    ingest_task: tokio::task::JoinHandle<std::io::Result<()>>,
    sub_task: tokio::task::JoinHandle<std::io::Result<()>>,
    control_task: tokio::task::JoinHandle<Result<()>>,
}

async fn start_servers(state: AppState) -> Result<RunningServers> {
    let config = state.inner.config.read().await.clone();
    let ingest_addr = config.server.listen_addr;
    let sub_addr = config.server.sub_addr;
    let control_socket = config.server.control_socket.clone();
    let ingest_listener = TcpListener::bind(ingest_addr)
        .await
        .with_context(|| format!("bind ingest listener {ingest_addr}"))?;
    let sub_listener = TcpListener::bind(sub_addr)
        .await
        .with_context(|| format!("bind subscriber listener {sub_addr}"))?;
    #[cfg(test)]
    let bound_sub_addr = sub_listener
        .local_addr()
        .context("read subscriber listener address")?;
    let ingest_router = Router::new()
        .fallback(any(ingest_handler))
        .with_state(state.clone());
    let sub_router = Router::new()
        .route("/subscribe", get(subscribe_handler))
        .with_state(state.clone());

    let ingest_task =
        tokio::spawn(async move { axum::serve(ingest_listener, ingest_router).await });
    let sub_task = tokio::spawn(async move { axum::serve(sub_listener, sub_router).await });
    let control_task = tokio::spawn(control_loop(state.clone(), control_socket));
    Ok(RunningServers {
        #[cfg(test)]
        sub_addr: bound_sub_addr,
        ingest_task,
        sub_task,
        control_task,
    })
}

async fn shutdown_state(state: &AppState, servers: RunningServers) {
    state.inner.shutting_down.store(true, Ordering::Relaxed);
    send_shutdown_to_subscribers(state).await;
    send_shutdown_to_extensions(state).await;
    servers.ingest_task.abort();
    servers.sub_task.abort();
    servers.control_task.abort();
}

async fn reload_from_path(state: &AppState, config_path: &Path) -> Result<()> {
    let config = Config::load(config_path)?;
    let token_store = TokenStore::load_or_empty(config.token_store_path())?;
    let invalidated = state
        .inner
        .token_store
        .read()
        .await
        .invalidated_names(&token_store);
    *state.inner.config.write().await = config;
    *state.inner.token_store.write().await = token_store;
    for name in invalidated {
        state
            .close_subscribers_named(&name, ClosingReason::Revoked)
            .await;
    }
    state.start_extensions().await?;
    Ok(())
}

async fn ingest_handler(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let config = state.inner.config.read().await.clone();
    if body.len() > config.limits.max_body_bytes {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    }
    let Some(route_key) = route_key_from_path(uri.path()) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let headers = header_map_to_btree(&headers);
    let query = uri.query().map(ToString::to_string);
    match state
        .dispatch(
            &route_key,
            method,
            uri.path().to_string(),
            query,
            headers,
            body,
        )
        .await
    {
        Ok(result) => http_reply_to_response(result.http),
        Err(DispatchError::Busy) => StatusCode::TOO_MANY_REQUESTS.into_response(),
        Err(DispatchError::Starting) => {
            let mut response = StatusCode::SERVICE_UNAVAILABLE.into_response();
            response
                .headers_mut()
                .insert(RETRY_AFTER, HeaderValue::from_static("1"));
            response
        }
        Err(DispatchError::Timeout) => StatusCode::GATEWAY_TIMEOUT.into_response(),
        Err(DispatchError::Dead) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        Err(DispatchError::NotFound) => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn subscribe_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let token = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::to_string);
    let Some(token) = token else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let name = {
        let store = state.inner.token_store.read().await;
        store.authenticate(&token)
    };
    let Some(name) = name else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    ws.on_upgrade(move |socket| subscriber_socket(state, name, socket))
        .into_response()
}

async fn subscriber_socket(state: AppState, name: String, socket: WebSocket) {
    let config = state.inner.config.read().await.clone();
    let queue_len = config.limits.sub_queue_len.max(1);
    let ws_idle_timeout = Duration::from_millis(config.subscribers.ws_idle_timeout_ms.max(1));
    let (tx, mut rx) = mpsc::channel::<SubServerMsg>(queue_len);
    let (close_tx, mut close_rx) = watch::channel::<Option<ClosingReason>>(None);
    let id = state
        .inner
        .subscribers
        .insert(SubscriberRegistration {
            name: name.clone(),
            remote_addr: None,
            tx: tx.clone(),
            close_tx,
        })
        .await;

    let (mut sink, mut stream) = socket.split();
    let _ = sink
        .send(Message::Text(
            serde_json::to_string(&SubServerMsg::Welcome { name })
                .unwrap()
                .into(),
        ))
        .await;

    let mut ping_interval = time::interval(ws_idle_timeout);
    ping_interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    ping_interval.tick().await;
    let mut awaiting_pong = false;

    loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                if awaiting_pong {
                    warn!(subscriber = id, "subscriber missed websocket pong; closing");
                    break;
                }
                if sink.send(Message::Ping(Bytes::new())).await.is_err() {
                    break;
                }
                awaiting_pong = true;
            }
            Some(outgoing) = rx.recv() => {
                let closing = matches!(outgoing, SubServerMsg::Closing { .. });
                let text = match serde_json::to_string(&outgoing) {
                    Ok(text) => text,
                    Err(_) => continue,
                };
                if sink.send(Message::Text(text.into())).await.is_err() {
                    break;
                }
                if closing {
                    break;
                }
            }
            changed = close_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                let Some(reason) = close_rx.borrow().clone() else {
                    continue;
                };
                if let Ok(text) = serde_json::to_string(&SubServerMsg::Closing { reason }) {
                    let _ = sink.send(Message::Text(text.into())).await;
                }
                break;
            }
            incoming = stream.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        if handle_subscriber_text(&state, id, text.as_str(), &tx).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        let _ = sink.send(Message::Pong(payload)).await;
                    }
                    Some(Ok(Message::Pong(_))) => {
                        awaiting_pong = false;
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
        }
    }

    state.inner.subscribers.remove(id).await;
}

async fn handle_subscriber_text(
    state: &AppState,
    id: u64,
    text: &str,
    tx: &mpsc::Sender<SubServerMsg>,
) -> Result<()> {
    let msg: SubClientMsg = serde_json::from_str(text)?;
    match msg {
        SubClientMsg::Subscribe { patterns } => {
            if let Err(msg) = state.inner.subscribers.subscribe(id, patterns).await {
                let _ = tx
                    .send(SubServerMsg::Error {
                        op: "subscribe".to_string(),
                        msg,
                    })
                    .await;
                return Ok(());
            }
            let _ = tx
                .send(SubServerMsg::Ok {
                    op: "subscribe".to_string(),
                })
                .await;
        }
        SubClientMsg::Unsubscribe { patterns } => {
            state.inner.subscribers.unsubscribe(id, &patterns).await;
            let _ = tx
                .send(SubServerMsg::Ok {
                    op: "unsubscribe".to_string(),
                })
                .await;
        }
        SubClientMsg::Ping => {
            let _ = tx.send(SubServerMsg::Pong).await;
        }
    }
    Ok(())
}

async fn control_loop(state: AppState, path: PathBuf) -> Result<()> {
    if path.exists() {
        fs::remove_file(&path).with_context(|| format!("remove old {}", path.display()))?;
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let listener = UnixListener::bind(&path).with_context(|| format!("bind {}", path.display()))?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o660))
        .with_context(|| format!("chmod {}", path.display()))?;
    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_control_stream(state, stream).await {
                debug!(error = %err, "control stream ended");
            }
        });
    }
}

async fn handle_control_stream(state: AppState, stream: UnixStream) -> Result<()> {
    let (read, write) = stream.into_split();
    let mut lines = BufReader::new(read).lines();
    let mut writer = BufWriter::new(write);
    while let Some(line) = lines.next_line().await? {
        let response = match decode_line::<ControlRequest>(&line) {
            Ok(Some(request)) => handle_control_request(&state, request).await,
            Ok(None) => continue,
            Err(err) => ControlResponse::Error {
                msg: err.to_string(),
            },
        };
        writer.write_all(encode_line(&response)?.as_bytes()).await?;
        writer.flush().await?;
    }
    Ok(())
}

async fn handle_control_request(state: &AppState, request: ControlRequest) -> ControlResponse {
    match request {
        ControlRequest::Status => ControlResponse::Status {
            status: state.status_json().await,
        },
        ControlRequest::TokenAdd { name } => {
            let mut store = state.inner.token_store.write().await;
            match store.add(&name) {
                Ok(token) => ControlResponse::Token { name, token },
                Err(err) => ControlResponse::Error {
                    msg: err.to_string(),
                },
            }
        }
        ControlRequest::TokenRotate { name } => {
            let mut store = state.inner.token_store.write().await;
            match store.rotate(&name) {
                Ok(token) => {
                    drop(store);
                    state
                        .close_subscribers_named(&name, ClosingReason::Revoked)
                        .await;
                    ControlResponse::Token { name, token }
                }
                Err(err) => ControlResponse::Error {
                    msg: err.to_string(),
                },
            }
        }
        ControlRequest::TokenRevoke { name } => {
            let mut store = state.inner.token_store.write().await;
            match store.revoke(&name) {
                Ok(()) => {
                    drop(store);
                    state
                        .close_subscribers_named(&name, ClosingReason::Revoked)
                        .await;
                    ControlResponse::Ok
                }
                Err(err) => ControlResponse::Error {
                    msg: err.to_string(),
                },
            }
        }
        ControlRequest::TokenList => {
            let active = state.inner.subscribers.active_connection_counts().await;
            ControlResponse::Tokens {
                tokens: state.inner.token_store.read().await.list(&active),
            }
        }
    }
}

async fn send_shutdown_to_subscribers(state: &AppState) {
    state
        .inner
        .subscribers
        .close_all(ClosingReason::Shutdown)
        .await;
}

async fn send_shutdown_to_extensions(state: &AppState) {
    let extensions: Vec<ExtHandle> = state
        .inner
        .extensions
        .read()
        .await
        .values()
        .cloned()
        .collect();
    let timeouts = state.inner.config.read().await.timeouts.clone();
    let wait_ms = timeouts
        .drain_ms
        .saturating_add(timeouts.term_grace_ms.saturating_mul(2))
        .saturating_add(100)
        .max(1);
    let mut done_receivers = Vec::new();
    for handle in extensions {
        let (done_tx, done_rx) = oneshot::channel();
        let _ = handle
            .supervisor_tx
            .send(SupervisorCommand::DrainStop {
                reason: "server shutdown".to_string(),
                done: Some(done_tx),
            })
            .await;
        done_receivers.push(done_rx);
    }
    for done in done_receivers {
        if time::timeout(Duration::from_millis(wait_ms), done)
            .await
            .is_err()
        {
            warn!(wait_ms, "timed out waiting for extension shutdown drain");
        }
    }
}

fn header_map_to_btree(headers: &HeaderMap) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_ascii_lowercase(), value.to_string()))
        })
        .collect()
}

fn http_reply_to_response(reply: HttpReply) -> Response {
    let status = StatusCode::from_u16(reply.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut response = (status, reply.body).into_response();
    for (name, value) in reply.headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            response.headers_mut().insert(name, value);
        }
    }
    response
}

fn filter_owned_events(
    ext_id: &str,
    paths: &[String],
    registered_prefixes: &[String],
    violations: &AtomicUsize,
    events: Vec<Event>,
) -> Vec<Event> {
    events
        .into_iter()
        .filter_map(|event| {
            match validate_emitted_channel(&event.channel, ext_id, paths, registered_prefixes) {
                Ok(()) => Some(event),
                Err(err) => {
                    let count = violations.fetch_add(1, Ordering::Relaxed) + 1;
                    warn!(
                        ext = ext_id,
                        channel = event.channel,
                        count,
                        error = %err,
                        "extension emitted unauthorized channel"
                    );
                    None
                }
            }
        })
        .collect()
}

fn validate_registration_claims(
    id: &str,
    claims: &[String],
    channel_prefixes: &[String],
    routes: &HashMap<String, String>,
    existing_prefixes: &HashMap<String, String>,
) -> Result<()> {
    let mut seen_claims = HashMap::new();
    for claim in claims {
        validate_path_claim(claim)?;
        if seen_claims.insert(claim.as_str(), ()).is_some() {
            bail!("duplicate path claim: {claim}");
        }
        if let Some(owner) = routes.get(claim) {
            bail!("path collision: {claim} already claimed by {owner}");
        }
        for (prefix, owner) in existing_prefixes {
            if owner != id && first_channel_segment(prefix) == claim {
                bail!("path claim {claim} collides with channel prefix {prefix} owned by {owner}");
            }
        }
    }

    let mut seen_prefixes: Vec<&str> = Vec::new();
    for prefix in channel_prefixes {
        validate_channel(prefix).with_context(|| format!("invalid channel prefix: {prefix}"))?;
        for seen in &seen_prefixes {
            if channel_prefixes_overlap(prefix, seen) {
                bail!("channel prefix {prefix} collides with channel prefix {seen}");
            }
        }
        seen_prefixes.push(prefix.as_str());

        let first = first_channel_segment(prefix);
        if let Some(owner) = routes.get(first)
            && owner != id
        {
            bail!("channel prefix {prefix} collides with route {first} owned by {owner}");
        }
        for (existing, owner) in existing_prefixes {
            if owner != id && channel_prefixes_overlap(prefix, existing) {
                bail!(
                    "channel prefix {prefix} collides with channel prefix {existing} owned by {owner}"
                );
            }
        }
    }
    Ok(())
}

fn validate_emitted_channel(
    channel: &str,
    ext_id: &str,
    claims: &[String],
    registered_prefixes: &[String],
) -> Result<()> {
    validate_channel(channel).with_context(|| format!("invalid event channel: {channel}"))?;
    let first = first_channel_segment(channel);
    if claims.iter().any(|claim| claim == first)
        || registered_prefixes
            .iter()
            .any(|prefix| channel_is_in_prefix(channel, prefix))
    {
        return Ok(());
    }
    bail!("channel {channel} is not owned by extension {ext_id}");
}

fn first_channel_segment(channel: &str) -> &str {
    channel.split('.').next().unwrap_or_default()
}

fn channel_is_in_prefix(channel: &str, prefix: &str) -> bool {
    channel == prefix
        || channel
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('.'))
}

fn channel_prefixes_overlap(left: &str, right: &str) -> bool {
    channel_is_in_prefix(left, right) || channel_is_in_prefix(right, left)
}

fn discover_extensions() -> Result<Vec<(String, PathBuf)>> {
    let mut found = HashMap::new();
    let Some(path_var) = env::var_os("PATH") else {
        return Ok(Vec::new());
    };
    for dir in env::split_paths(&path_var) {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let file_name = entry.file_name();
            let Some(file_name) = file_name.to_str() else {
                continue;
            };
            let Some(id) = file_name.strip_prefix("whdr-ext-") else {
                continue;
            };
            if id.is_empty() || !is_executable(&entry.path()) {
                continue;
            }
            found.entry(id.to_string()).or_insert(entry.path());
        }
    }
    Ok(found.into_iter().collect())
}

fn is_executable(path: &Path) -> bool {
    fs::metadata(path)
        .map(|meta| meta.is_file() && (meta.permissions().mode() & 0o111 != 0))
        .unwrap_or(false)
}

fn validate_path_claim(claim: &str) -> Result<()> {
    if claim.is_empty()
        || claim.contains('/')
        || !claim
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
    {
        bail!("invalid path claim: {claim}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use crate::{ExtensionsConfig, LimitsConfig, ServerConfig, SubscribersConfig, TimeoutsConfig};
    use tokio::sync::Barrier;

    fn test_config(root: &Path) -> Config {
        Config {
            server: ServerConfig {
                listen_addr: loopback_addr(),
                sub_addr: loopback_addr(),
                control_socket: root.join("ctl.sock"),
            },
            subscribers: SubscribersConfig {
                token_store: Some(root.join("tokens.toml")),
                allow_plaintext_lan: false,
                ws_idle_timeout_ms: 30_000,
                tls: None,
            },
            extensions: ExtensionsConfig::default(),
            limits: LimitsConfig {
                dispatch_timeout_ms: 250,
                crashloop_threshold: 20,
                ..LimitsConfig::default()
            },
            timeouts: TimeoutsConfig {
                register_ms: 500,
                drain_ms: 100,
                term_grace_ms: 50,
            },
            secrets_file: None,
            secrets: BTreeMap::new(),
        }
    }

    fn loopback_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
    }

    fn write_executable_script(root: &Path, name: &str, body: String) -> PathBuf {
        let path = root.join(name);
        fs::write(&path, body).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    async fn shutdown_test_extensions(state: &AppState) {
        send_shutdown_to_extensions(state).await;
        time::sleep(Duration::from_millis(75)).await;
    }

    #[tokio::test]
    async fn removed_extension_is_unrouted_without_dropping_in_flight_dispatch() {
        let temp = tempfile::tempdir().unwrap();
        let log_path = temp.path().join("drain.log");
        let script = write_executable_script(
            temp.path(),
            "whdr-ext-drain",
            format!(
                r#"#!/bin/sh
printf '%s\n' '{{"type":"register","protocol":1}}'
while IFS= read -r line; do
  printf '%s\n' "$line" >> '{}'
done
"#,
                log_path.display()
            ),
        );
        let state = AppState::new(test_config(temp.path())).await.unwrap();
        state
            .start_extension("drain".to_string(), script)
            .await
            .unwrap();

        let dispatch_state = state.clone();
        let dispatch = tokio::spawn(async move {
            dispatch_state
                .dispatch(
                    "drain",
                    Method::POST,
                    "/drain".to_string(),
                    None,
                    BTreeMap::new(),
                    Bytes::new(),
                )
                .await
        });

        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let pending = {
                let extensions = state.inner.extensions.read().await;
                let handle = extensions.get("drain").unwrap();
                handle.dispatches.pending_len()
            };
            if pending == 1 {
                break;
            }
            assert!(Instant::now() < deadline, "dispatch never became in-flight");
            time::sleep(Duration::from_millis(10)).await;
        }

        state.stop_removed_extensions(&HashMap::new()).await;

        let handle = {
            let extensions = state.inner.extensions.read().await;
            extensions
                .get("drain")
                .cloned()
                .expect("draining extension should remain active")
        };
        assert_eq!(handle.dispatches.pending_len(), 1);
        assert_eq!(handle.dispatches.in_flight(), 1);
        assert!(!state.inner.routes.read().await.contains_key("drain"));

        time::sleep(Duration::from_millis(25)).await;
        let log = fs::read_to_string(&log_path).unwrap_or_default();
        assert!(
            !log.contains(r#""type":"shutdown""#),
            "removed extension should not receive Shutdown before drain deadline"
        );

        shutdown_test_extensions(&state).await;
        let _ = dispatch.await;
    }

    #[tokio::test]
    async fn concurrent_dispatches_reserve_max_in_flight_atomically() {
        const DISPATCHES: usize = 16;

        let temp = tempfile::tempdir().unwrap();
        let script = write_executable_script(
            temp.path(),
            "whdr-ext-slow",
            r#"#!/bin/sh
printf '%s\n' '{"type":"register","protocol":1}'
while IFS= read -r _line; do
  :
done
"#
            .to_string(),
        );
        let mut config = test_config(temp.path());
        config.limits.max_in_flight = 1;
        config.limits.dispatch_timeout_ms = 100;
        let state = AppState::new(config).await.unwrap();
        state
            .start_extension("slow".to_string(), script)
            .await
            .unwrap();

        let handle = {
            let extensions = state.inner.extensions.read().await;
            extensions.get("slow").cloned().unwrap()
        };
        let barrier = Arc::new(Barrier::new(DISPATCHES + 1));
        let mut tasks = Vec::new();
        for _ in 0..DISPATCHES {
            let state = state.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                state
                    .dispatch(
                        "slow",
                        Method::POST,
                        "/slow".to_string(),
                        None,
                        BTreeMap::new(),
                        Bytes::new(),
                    )
                    .await
            }));
        }

        barrier.wait().await;
        time::sleep(Duration::from_millis(50)).await;

        let mut busy = 0;
        let mut timed_out = 0;
        for task in tasks {
            match task.await.unwrap() {
                Err(DispatchError::Busy) => busy += 1,
                Err(DispatchError::Timeout) => timed_out += 1,
                Err(err) => panic!("unexpected dispatch error: {err:?}"),
                Ok(_) => panic!("unexpected dispatch success"),
            }
        }

        assert_eq!(timed_out, 1, "only one dispatch should reserve capacity");
        assert_eq!(busy, DISPATCHES - 1);
        assert_eq!(handle.dispatches.in_flight(), 0);

        shutdown_test_extensions(&state).await;
    }

    #[tokio::test]
    async fn cancelled_dispatch_releases_in_flight_capacity() {
        let temp = tempfile::tempdir().unwrap();
        let script = write_executable_script(
            temp.path(),
            "whdr-ext-ignore",
            r#"#!/bin/sh
printf '%s\n' '{"type":"register","protocol":1}'
while IFS= read -r _line; do
  :
done
"#
            .to_string(),
        );
        let mut config = test_config(temp.path());
        config.limits.max_in_flight = 1;
        config.limits.dispatch_timeout_ms = 250;
        let state = AppState::new(config).await.unwrap();
        state
            .start_extension("ignore".to_string(), script)
            .await
            .unwrap();

        let handle = {
            let extensions = state.inner.extensions.read().await;
            extensions.get("ignore").cloned().unwrap()
        };
        let first_state = state.clone();
        let first = tokio::spawn(async move {
            first_state
                .dispatch(
                    "ignore",
                    Method::POST,
                    "/ignore".to_string(),
                    None,
                    BTreeMap::new(),
                    Bytes::new(),
                )
                .await
        });

        let pending_deadline = Instant::now() + Duration::from_millis(100);
        loop {
            let pending = handle.dispatches.pending_len();
            if pending == 1 && handle.dispatches.in_flight() == 1 {
                break;
            }
            assert!(
                Instant::now() < pending_deadline,
                "first dispatch never became in-flight"
            );
            time::sleep(Duration::from_millis(10)).await;
        }

        first.abort();
        match first.await {
            Err(err) => assert!(err.is_cancelled()),
            Ok(_) => panic!("first dispatch completed before cancellation"),
        }

        let second = state
            .dispatch(
                "ignore",
                Method::POST,
                "/ignore".to_string(),
                None,
                BTreeMap::new(),
                Bytes::new(),
            )
            .await;
        assert!(
            matches!(second, Err(DispatchError::Timeout)),
            "capacity leaked after cancellation"
        );
        assert_eq!(handle.dispatches.pending_len(), 0);
        assert_eq!(handle.dispatches.in_flight(), 0);

        shutdown_test_extensions(&state).await;
    }

    #[tokio::test]
    async fn late_timed_out_result_does_not_release_active_dispatch_capacity() {
        let temp = tempfile::tempdir().unwrap();
        let marker_path = temp.path().join("late-result-sent");
        let script = write_executable_script(
            temp.path(),
            "whdr-ext-late",
            format!(
                r#"#!/bin/sh
printf '%s\n' '{{"type":"register","protocol":1}}'
count=0
while IFS= read -r line; do
  req_id=$(printf '%s\n' "$line" | sed -n 's/.*"req_id":"\([^"]*\)".*/\1/p')
  count=$((count + 1))
  if [ "$count" -eq 1 ]; then
    sleep 0.65
    printf '{{"type":"result","req_id":"%s","http":{{"status":204,"headers":{{}},"body":""}},"events":[]}}\n' "$req_id"
    printf 'sent\n' > '{}'
  else
    while :; do sleep 1; done
  fi
done
"#,
                marker_path.display()
            ),
        );
        let mut config = test_config(temp.path());
        config.limits.max_in_flight = 1;
        config.limits.dispatch_timeout_ms = 400;
        let state = AppState::new(config).await.unwrap();
        state
            .start_extension("late".to_string(), script)
            .await
            .unwrap();

        let handle = {
            let extensions = state.inner.extensions.read().await;
            extensions.get("late").cloned().unwrap()
        };

        let first = state
            .dispatch(
                "late",
                Method::POST,
                "/late".to_string(),
                None,
                BTreeMap::new(),
                Bytes::new(),
            )
            .await;
        assert!(matches!(first, Err(DispatchError::Timeout)));
        state.inner.config.write().await.limits.dispatch_timeout_ms = 1_000;

        let second_state = state.clone();
        let second = tokio::spawn(async move {
            second_state
                .dispatch(
                    "late",
                    Method::POST,
                    "/late".to_string(),
                    None,
                    BTreeMap::new(),
                    Bytes::new(),
                )
                .await
        });

        let pending_deadline = Instant::now() + Duration::from_millis(250);
        loop {
            let pending = handle.dispatches.pending_len();
            if pending == 1 && handle.dispatches.in_flight() == 1 {
                break;
            }
            assert!(
                Instant::now() < pending_deadline,
                "second dispatch never became in-flight"
            );
            time::sleep(Duration::from_millis(10)).await;
        }

        let late_result_deadline = Instant::now() + Duration::from_millis(500);
        loop {
            if marker_path.exists() {
                break;
            }
            assert!(
                Instant::now() < late_result_deadline,
                "late result was not emitted while second dispatch was active"
            );
            time::sleep(Duration::from_millis(10)).await;
        }
        time::sleep(Duration::from_millis(75)).await;

        let third = state
            .dispatch(
                "late",
                Method::POST,
                "/late".to_string(),
                None,
                BTreeMap::new(),
                Bytes::new(),
            )
            .await;
        match third {
            Err(DispatchError::Busy) => {}
            Err(err) => {
                panic!("late result must not release the second dispatch's capacity: {err:?}")
            }
            Ok(_) => panic!("late result must not release the second dispatch's capacity: success"),
        }

        assert!(matches!(second.await.unwrap(), Err(DispatchError::Timeout)));
        shutdown_test_extensions(&state).await;
    }

    #[tokio::test]
    async fn sighup_diff_keeps_registered_id_override_when_candidate_is_still_desired() {
        let temp = tempfile::tempdir().unwrap();
        let script = write_executable_script(
            temp.path(),
            "whdr-ext-candidate",
            r#"#!/bin/sh
printf '%s\n' '{"type":"register","protocol":1,"id":"registered"}'
while IFS= read -r _line; do :; done
"#
            .to_string(),
        );
        let state = AppState::new(test_config(temp.path())).await.unwrap();
        state
            .start_extension("candidate".to_string(), script.clone())
            .await
            .unwrap();

        let desired = HashMap::from([("candidate".to_string(), script)]);
        state.stop_removed_extensions(&desired).await;

        assert!(
            state
                .inner
                .extensions
                .read()
                .await
                .contains_key("registered")
        );
        assert_eq!(
            state.inner.routes.read().await.get("registered"),
            Some(&"registered".to_string())
        );

        shutdown_test_extensions(&state).await;
    }

    #[tokio::test]
    async fn protocol_error_limit_kills_and_restarts_extension() {
        let temp = tempfile::tempdir().unwrap();
        let script = write_executable_script(
            temp.path(),
            "whdr-ext-bad",
            r#"#!/bin/sh
printf '%s\n' '{"type":"register","protocol":1}'
printf '%s\n' '{not-json}'
sleep 5
"#
            .to_string(),
        );
        let mut config = test_config(temp.path());
        config.extensions.enabled = vec!["bad".to_string()];
        config.limits.max_protocol_errors = 1;
        config.timeouts.drain_ms = 20;
        config.timeouts.term_grace_ms = 20;
        let state = AppState::new(config).await.unwrap();
        state
            .start_extension("bad".to_string(), script)
            .await
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if state.restart_count("bad").await > 0 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "protocol errors did not trigger supervisor restart"
            );
            time::sleep(Duration::from_millis(25)).await;
        }

        shutdown_test_extensions(&state).await;
    }

    #[tokio::test]
    async fn subscriber_liveness_drops_connections_that_miss_ws_pongs() {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;

        let temp = tempfile::tempdir().unwrap();
        let token_path = temp.path().join("tokens.toml");
        let mut store = TokenStore::load_or_empty(token_path.clone()).unwrap();
        let token = store.add("project-a").unwrap();

        let mut config = test_config(temp.path());
        config.subscribers.token_store = Some(token_path);
        config.subscribers.ws_idle_timeout_ms = 20;
        let state = AppState::new(config.clone()).await.unwrap();
        let servers = start_servers(state.clone()).await.unwrap();

        let mut request = format!("ws://{}/subscribe", servers.sub_addr)
            .into_client_request()
            .unwrap();
        request.headers_mut().insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let welcome = time::timeout(Duration::from_secs(1), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(welcome.is_text());

        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if state.inner.subscribers.is_empty().await {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "subscriber was not dropped after missing websocket pongs"
            );
            time::sleep(Duration::from_millis(25)).await;
        }

        drop(socket);
        shutdown_state(&state, servers).await;
    }

    #[tokio::test]
    async fn constructed_config_rejects_non_loopback_plaintext_subscriber_bind() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = test_config(temp.path());
        config.server.sub_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
        config.subscribers.allow_plaintext_lan = false;

        let err = match AppState::new(config).await {
            Ok(_) => panic!("constructed non-loopback plaintext config should be rejected"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("refusing non-loopback subscriber bind")
        );
    }

    #[test]
    fn registration_rejects_channel_prefix_that_collides_with_existing_route() {
        let mut routes = HashMap::new();
        routes.insert("github".to_string(), "github".to_string());
        let prefixes = HashMap::new();

        let err = validate_registration_claims(
            "teams",
            &["teams".to_string()],
            &["github.notifications".to_string()],
            &routes,
            &prefixes,
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("channel prefix github.notifications collides with route github")
        );
    }

    #[test]
    fn emitted_event_channel_must_be_valid_and_owned() {
        let registered_prefixes = vec!["alerts.ops".to_string()];

        assert!(
            validate_emitted_channel(
                "teams.message",
                "teams",
                &["teams".to_string()],
                &registered_prefixes
            )
            .is_ok()
        );
        assert!(
            validate_emitted_channel(
                "alerts.ops.high",
                "teams",
                &["teams".to_string()],
                &registered_prefixes
            )
            .is_ok()
        );

        let err = validate_emitted_channel(
            "github.push",
            "teams",
            &["teams".to_string()],
            &registered_prefixes,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("channel github.push is not owned by extension teams")
        );
    }
}
