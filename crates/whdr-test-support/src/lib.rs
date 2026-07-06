//! Whole-system test harness for whdr.
//!
//! Spawns the real `whdr-server` binary against temp dirs, with a per-child
//! PATH pointing at a directory of scriptable fake extensions (copies of the
//! `whdr-ext-fake` example plus a `whdr-ext-<id>.toml` behavior file). No
//! global environment mutation, so tests parallelize safely.
//!
//! Binary locations are supplied by the calling test crate (via
//! `env!("CARGO_BIN_EXE_whdr-server")` and the example path) because Cargo
//! only exposes those paths inside the owning crate.

use std::fs;
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpStream, UnixStream};
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};
use whdr_proto::{ControlRequest, ControlResponse, decode_line, encode_line};

pub use futures_util::{SinkExt, StreamExt};

const READY_TIMEOUT: Duration = Duration::from_secs(10);

/// Builder for a spawned whdr-server with scripted fake extensions.
pub struct ServerBuilder {
    server_bin: PathBuf,
    fake_ext_bin: PathBuf,
    enabled: Vec<String>,
    limits: String,
    timeouts: String,
    delivery: Option<String>,
    temp: tempfile::TempDir,
}

impl ServerBuilder {
    pub fn new(server_bin: impl Into<PathBuf>, fake_ext_bin: impl Into<PathBuf>) -> Result<Self> {
        let temp = tempfile::tempdir().context("create temp dir")?;
        fs::create_dir(temp.path().join("exts")).context("create ext dir")?;
        Ok(Self {
            server_bin: server_bin.into(),
            fake_ext_bin: fake_ext_bin.into(),
            enabled: Vec::new(),
            limits: String::new(),
            timeouts: String::new(),
            delivery: None,
            temp,
        })
    }

    /// Install a fake extension under `whdr-ext-<id>` with the given
    /// behavior TOML (empty string = well-behaved echo) and enable it.
    pub fn with_fake_ext(self, id: &str, behavior_toml: &str) -> Result<Self> {
        let ext_dir = self.temp.path().join("exts");
        let bin = ext_dir.join(format!("whdr-ext-{id}"));
        fs::copy(&self.fake_ext_bin, &bin)
            .with_context(|| format!("copy fake ext to {}", bin.display()))?;
        fs::write(ext_dir.join(format!("whdr-ext-{id}.toml")), behavior_toml)
            .context("write behavior file")?;
        let mut this = self;
        this.enabled.push(id.to_string());
        Ok(this)
    }

    /// Extra lines for the `[limits]` section, e.g. `"max_in_flight = 1"`.
    pub fn with_limits(mut self, lines: &str) -> Self {
        self.limits = lines.to_string();
        self
    }

    /// Extra lines for the `[timeouts]` section.
    pub fn with_timeouts(mut self, lines: &str) -> Self {
        self.timeouts = lines.to_string();
        self
    }

    /// Enable durable delivery. `store_path` is set under the temp dir
    /// automatically; `extra_lines` supplies any other `[delivery]` knobs
    /// (e.g. `"retention_secs = 3600"`).
    pub fn with_delivery(mut self, extra_lines: &str) -> Self {
        self.delivery = Some(extra_lines.to_string());
        self
    }

    pub async fn start(self) -> Result<ServerHandle> {
        let ingest_addr = free_port().await?;
        let sub_addr = free_port().await?;
        let metrics_addr = free_port().await?;
        let root = self.temp.path().to_path_buf();
        let control_socket = root.join("ctl.sock");
        if control_socket.as_os_str().len() > 100 {
            bail!(
                "temp dir yields a control socket path too long for a UDS: {}",
                control_socket.display()
            );
        }

        let secrets_path = root.join("secrets.toml");
        let secrets_body: String = self
            .enabled
            .iter()
            .map(|id| format!("{id} = \"secret-{id}\"\n"))
            .collect();
        fs::write(&secrets_path, secrets_body)?;
        fs::set_permissions(&secrets_path, fs::Permissions::from_mode(0o600))?;

        let delivery = self.delivery.as_ref().map(|extra| {
            format!(
                "[delivery]\nenabled = true\nstore_path = \"{}\"\n{extra}\n",
                root.join("delivery.redb").display()
            )
        });

        let config_path = root.join("config.toml");
        write_config(WriteConfig {
            path: &config_path,
            ingest_addr,
            sub_addr,
            metrics_addr,
            control_socket: &control_socket,
            token_store: &root.join("tokens.toml"),
            enabled: &self.enabled,
            secrets_path: &secrets_path,
            limits: &self.limits,
            timeouts: &self.timeouts,
            delivery: delivery.as_deref(),
        })?;

        let handle = ServerHandle {
            ingest_addr,
            sub_addr,
            metrics_addr,
            control_socket,
            config_path,
            ext_dir: root.join("exts"),
            log_path: root.join("server.log"),
            server_bin: self.server_bin,
            child: None,
            _temp: self.temp,
        };
        handle.spawn_and_wait().await
    }
}

struct WriteConfig<'a> {
    path: &'a Path,
    ingest_addr: SocketAddr,
    sub_addr: SocketAddr,
    metrics_addr: SocketAddr,
    control_socket: &'a Path,
    token_store: &'a Path,
    enabled: &'a [String],
    secrets_path: &'a Path,
    limits: &'a str,
    timeouts: &'a str,
    delivery: Option<&'a str>,
}

fn write_config(cfg: WriteConfig<'_>) -> Result<()> {
    let enabled = cfg
        .enabled
        .iter()
        .map(|id| format!("\"{id}\""))
        .collect::<Vec<_>>()
        .join(", ");
    fs::write(
        cfg.path,
        format!(
            r#"[server]
listen_addr = "{ingest}"
sub_addr = "{sub}"
metrics_addr = "{metrics}"
control_socket = "{ctl}"

[subscribers]
token_store = "{tokens}"

[extensions]
enabled = [{enabled}]

[limits]
{limits}

[timeouts]
{timeouts}

{delivery}[secrets]
file = "{secrets}"
"#,
            ingest = cfg.ingest_addr,
            sub = cfg.sub_addr,
            metrics = cfg.metrics_addr,
            ctl = cfg.control_socket.display(),
            tokens = cfg.token_store.display(),
            limits = cfg.limits,
            timeouts = cfg.timeouts,
            delivery = cfg.delivery.unwrap_or(""),
            secrets = cfg.secrets_path.display(),
        ),
    )?;
    Ok(())
}

/// A running whdr-server child plus everything needed to talk to it.
pub struct ServerHandle {
    pub ingest_addr: SocketAddr,
    pub sub_addr: SocketAddr,
    pub metrics_addr: SocketAddr,
    pub control_socket: PathBuf,
    pub config_path: PathBuf,
    pub ext_dir: PathBuf,
    log_path: PathBuf,
    server_bin: PathBuf,
    child: Option<Child>,
    _temp: tempfile::TempDir,
}

impl ServerHandle {
    async fn spawn_and_wait(mut self) -> Result<Self> {
        let orig_path = std::env::var_os("PATH").unwrap_or_default();
        let mut path = self.ext_dir.clone().into_os_string();
        path.push(":");
        path.push(orig_path);
        let log = fs::File::options()
            .create(true)
            .append(true)
            .open(&self.log_path)?;
        let child = Command::new(&self.server_bin)
            .arg("--config")
            .arg(&self.config_path)
            .env("PATH", path)
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("spawn {}", self.server_bin.display()))?;
        self.child = Some(child);
        self.wait_control_ready().await?;
        Ok(self)
    }

    async fn wait_control_ready(&self) -> Result<()> {
        let deadline = tokio::time::Instant::now() + READY_TIMEOUT;
        loop {
            if let Ok(ControlResponse::Status { .. }) = self.control(ControlRequest::Status).await {
                return Ok(());
            }
            if tokio::time::Instant::now() > deadline {
                bail!("server did not become ready; log:\n{}", self.logs());
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    /// Wait until the extension shows up in status with the given state.
    pub async fn wait_ext_state(&self, id: &str, state: &str) -> Result<Value> {
        let deadline = tokio::time::Instant::now() + READY_TIMEOUT;
        loop {
            let status = self.status().await?;
            if let Some(ext) = ext_row(&status, id)
                && ext["state"].as_str() == Some(state)
            {
                return Ok(ext.clone());
            }
            if tokio::time::Instant::now() > deadline {
                bail!("ext {id} never reached state {state}; last status: {status}");
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    /// Wait until `predicate` holds on the status document.
    pub async fn wait_status(&self, predicate: impl Fn(&Value) -> bool) -> Result<Value> {
        let deadline = tokio::time::Instant::now() + READY_TIMEOUT;
        loop {
            let status = self.status().await?;
            if predicate(&status) {
                return Ok(status);
            }
            if tokio::time::Instant::now() > deadline {
                bail!("status predicate never satisfied; last status: {status}");
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    pub async fn control(&self, request: ControlRequest) -> Result<ControlResponse> {
        let stream = UnixStream::connect(&self.control_socket).await?;
        let (read, write) = stream.into_split();
        let mut writer = BufWriter::new(write);
        writer.write_all(encode_line(&request)?.as_bytes()).await?;
        writer.flush().await?;
        drop(writer);
        let mut lines = BufReader::new(read).lines();
        let line = lines
            .next_line()
            .await?
            .ok_or_else(|| anyhow!("control socket closed without response"))?;
        decode_line::<ControlResponse>(&line)?.ok_or_else(|| anyhow!("blank control response"))
    }

    pub async fn status(&self) -> Result<Value> {
        match self.control(ControlRequest::Status).await? {
            ControlResponse::Status { status } => Ok(status),
            other => bail!("unexpected control response: {other:?}"),
        }
    }

    pub async fn token_add(&self, name: &str) -> Result<String> {
        match self
            .control(ControlRequest::TokenAdd {
                name: name.to_string(),
            })
            .await?
        {
            ControlResponse::Token { token, .. } => Ok(token),
            other => bail!("unexpected control response: {other:?}"),
        }
    }

    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().and_then(|child| child.id())
    }

    pub fn sighup(&self) -> Result<()> {
        self.signal("HUP")
    }

    fn signal(&self, sig: &str) -> Result<()> {
        let pid = self.pid().ok_or_else(|| anyhow!("server not running"))?;
        let status = std::process::Command::new("kill")
            .arg(format!("-{sig}"))
            .arg(pid.to_string())
            .status()?;
        if !status.success() {
            bail!("kill -{sig} {pid} failed");
        }
        Ok(())
    }

    /// Rewrite the `enabled` list in the config file (for SIGHUP rescans).
    pub fn set_enabled(&self, ids: &[&str]) -> Result<()> {
        let text = fs::read_to_string(&self.config_path)?;
        let enabled = ids
            .iter()
            .map(|id| format!("\"{id}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let mut out = String::new();
        for line in text.lines() {
            if line.starts_with("enabled = ") {
                out.push_str(&format!("enabled = [{enabled}]\n"));
            } else {
                out.push_str(line);
                out.push('\n');
            }
        }
        fs::write(&self.config_path, out)?;
        Ok(())
    }

    /// Install another fake extension binary + behavior (hot-add flow).
    pub fn install_fake_ext(&self, fake_ext_bin: &Path, id: &str, behavior: &str) -> Result<()> {
        let bin = self.ext_dir.join(format!("whdr-ext-{id}"));
        fs::copy(fake_ext_bin, &bin)?;
        fs::write(self.ext_dir.join(format!("whdr-ext-{id}.toml")), behavior)?;
        Ok(())
    }

    /// SIGTERM and wait for exit (graceful shutdown path).
    pub async fn stop(&mut self) -> Result<()> {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        std::process::Command::new("kill")
            .arg("-TERM")
            .arg(child.id().unwrap_or_default().to_string())
            .status()?;
        if timeout(Duration::from_secs(10), child.wait())
            .await
            .is_err()
        {
            child.kill().await.ok();
            bail!(
                "server did not exit on SIGTERM; killed. log:\n{}",
                self.logs()
            );
        }
        Ok(())
    }

    /// Kill hard (crash simulation), then restart on the same config/state.
    pub async fn kill_and_restart(mut self) -> Result<Self> {
        if let Some(mut child) = self.child.take() {
            child.kill().await.ok();
            child.wait().await.ok();
        }
        // The stale control socket file lingers; the server unlinks it on boot.
        self.spawn_and_wait().await
    }

    pub fn logs(&self) -> String {
        fs::read_to_string(&self.log_path).unwrap_or_default()
    }
}

pub fn ext_row<'a>(status: &'a Value, id: &str) -> Option<&'a Value> {
    status["extensions"]
        .as_array()?
        .iter()
        .find(|ext| ext["id"].as_str() == Some(id))
}

pub fn subscriber_row<'a>(status: &'a Value, name: &str) -> Option<&'a Value> {
    status["subscribers"]
        .as_array()?
        .iter()
        .find(|sub| sub["name"].as_str() == Some(name))
}

async fn free_port() -> Result<SocketAddr> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    Ok(listener.local_addr()?)
}

/// Minimal HTTP/1.1 request against the ingest or metrics listener.
pub async fn http_request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: &[u8],
) -> Result<(u16, String)> {
    let mut stream = TcpStream::connect(addr).await?;
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: whdr-test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body).await?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    let status: u16 = response
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .ok_or_else(|| anyhow!("malformed http response: {response:?}"))?;
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, tail)| tail.to_string())
        .unwrap_or_default();
    Ok((status, decode_chunked_if_needed(&response, body)))
}

fn decode_chunked_if_needed(full: &str, body: String) -> String {
    if !full
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        return body;
    }
    let mut out = String::new();
    let mut rest = body.as_str();
    while let Some((size_line, tail)) = rest.split_once("\r\n") {
        let Ok(size) = usize::from_str_radix(size_line.trim(), 16) else {
            break;
        };
        if size == 0 {
            break;
        }
        out.push_str(&tail[..size.min(tail.len())]);
        rest = tail.get(size + 2..).unwrap_or("");
    }
    out
}

pub type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// WebSocket subscriber client speaking the SPEC §9.2 protocol.
pub struct WsSubscriber {
    ws: WsStream,
}

impl WsSubscriber {
    /// Connect + authenticate; returns the client and the welcome frame.
    pub async fn connect(sub_addr: SocketAddr, token: &str) -> Result<(Self, Value)> {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut request = format!("ws://{sub_addr}/subscribe").into_client_request()?;
        request.headers_mut().insert(
            "Authorization",
            format!("Bearer {token}").parse().context("token header")?,
        );
        let (ws, _) = tokio_tungstenite::connect_async(request).await?;
        let mut client = Self { ws };
        let welcome = client.recv(Duration::from_secs(5)).await?;
        Ok((client, welcome))
    }

    /// Attempt to connect with a bad token; returns the HTTP status code.
    pub async fn connect_expect_reject(sub_addr: SocketAddr, token: &str) -> Result<u16> {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut request = format!("ws://{sub_addr}/subscribe").into_client_request()?;
        request.headers_mut().insert(
            "Authorization",
            format!("Bearer {token}").parse().context("token header")?,
        );
        match tokio_tungstenite::connect_async(request).await {
            Ok(_) => bail!("connection unexpectedly accepted"),
            Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
                Ok(response.status().as_u16())
            }
            Err(other) => Err(other.into()),
        }
    }

    pub async fn subscribe(&mut self, patterns: &[&str]) -> Result<Value> {
        self.send_json(&serde_json::json!({"type": "subscribe", "patterns": patterns}))
            .await?;
        self.recv(Duration::from_secs(5)).await
    }

    pub async fn send_json(&mut self, value: &Value) -> Result<()> {
        self.ws
            .send(tokio_tungstenite::tungstenite::Message::Text(
                value.to_string().into(),
            ))
            .await?;
        Ok(())
    }

    /// Next text frame as JSON (answers WS-level pings along the way).
    pub async fn recv(&mut self, wait: Duration) -> Result<Value> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let remaining = deadline
                .checked_duration_since(tokio::time::Instant::now())
                .ok_or_else(|| anyhow!("timed out waiting for frame"))?;
            let msg = timeout(remaining, self.ws.next())
                .await
                .map_err(|_| anyhow!("timed out waiting for frame"))?
                .ok_or_else(|| anyhow!("websocket closed"))??;
            match msg {
                tokio_tungstenite::tungstenite::Message::Text(text) => {
                    return Ok(serde_json::from_str(&text)?);
                }
                tokio_tungstenite::tungstenite::Message::Ping(payload) => {
                    self.ws
                        .send(tokio_tungstenite::tungstenite::Message::Pong(payload))
                        .await?;
                }
                tokio_tungstenite::tungstenite::Message::Close(_) => {
                    bail!("websocket closed");
                }
                _ => {}
            }
        }
    }

    /// Next `event` frame, skipping ok/pong frames.
    pub async fn recv_event(&mut self, wait: Duration) -> Result<Value> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let remaining = deadline
                .checked_duration_since(tokio::time::Instant::now())
                .ok_or_else(|| anyhow!("timed out waiting for event"))?;
            let frame = self.recv(remaining).await?;
            if frame["type"] == "event" {
                return Ok(frame);
            }
        }
    }

    /// Assert no event arrives within `wait`.
    pub async fn expect_silence(&mut self, wait: Duration) -> Result<()> {
        match self.recv_event(wait).await {
            Ok(frame) => bail!("expected silence, got {frame}"),
            Err(_) => Ok(()),
        }
    }

    pub fn into_inner(self) -> WsStream {
        self.ws
    }
}

pub fn b64(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}
