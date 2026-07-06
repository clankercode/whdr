use std::collections::BTreeMap;
use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Clone, Debug)]
pub struct Config {
    pub server: ServerConfig,
    pub subscribers: SubscribersConfig,
    pub extensions: ExtensionsConfig,
    pub limits: LimitsConfig,
    pub timeouts: TimeoutsConfig,
    pub secrets_file: Option<PathBuf>,
    pub secrets: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub listen_addr: SocketAddr,
    pub sub_addr: SocketAddr,
    pub control_socket: PathBuf,
    /// Optional Prometheus text-format listener. Loopback only: metrics stay
    /// on the admin plane; scrape locally or relay via a reverse proxy.
    pub metrics_addr: Option<SocketAddr>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8787),
            sub_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8788),
            control_socket: PathBuf::from("/run/whdr/ctl.sock"),
            metrics_addr: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct SubscribersConfig {
    pub token_store: Option<PathBuf>,
    pub allow_plaintext_lan: bool,
    pub ws_idle_timeout_ms: u64,
    pub tls: Option<TlsConfig>,
}

impl Default for SubscribersConfig {
    fn default() -> Self {
        Self {
            token_store: None,
            allow_plaintext_lan: false,
            ws_idle_timeout_ms: 30_000,
            tls: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct TlsConfig {
    pub cert: PathBuf,
    pub key: PathBuf,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            cert: PathBuf::new(),
            key: PathBuf::new(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ExtensionsConfig {
    pub enabled: Vec<String>,
    pub autostart_all: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct LimitsConfig {
    pub max_body_bytes: usize,
    pub max_in_flight: usize,
    pub sub_queue_len: usize,
    pub sub_queue_bytes: usize,
    pub dispatch_timeout_ms: u64,
    pub max_protocol_errors: usize,
    pub hang_kill_threshold: usize,
    pub crashloop_threshold: usize,
    pub crashloop_window_ms: u64,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_body_bytes: 1_048_576,
            max_in_flight: 64,
            sub_queue_len: 1024,
            sub_queue_bytes: 8_388_608,
            dispatch_timeout_ms: 10_000,
            max_protocol_errors: 3,
            hang_kill_threshold: 3,
            crashloop_threshold: 5,
            crashloop_window_ms: 60_000,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct TimeoutsConfig {
    pub register_ms: u64,
    pub drain_ms: u64,
    pub term_grace_ms: u64,
}

impl Default for TimeoutsConfig {
    fn default() -> Self {
        Self {
            register_ms: 5_000,
            drain_ms: 5_000,
            term_grace_ms: 3_000,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct RawConfig {
    server: ServerConfig,
    subscribers: SubscribersConfig,
    extensions: ExtensionsConfig,
    limits: LimitsConfig,
    timeouts: TimeoutsConfig,
    secrets: Option<SecretsSection>,
}

#[derive(Debug, Deserialize)]
struct SecretsSection {
    file: PathBuf,
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let text = fs::read_to_string(path.as_ref())
            .with_context(|| format!("read config {}", path.as_ref().display()))?;
        let raw: RawConfig = toml::from_str(&text).context("parse config toml")?;

        let (secrets_file, secrets) = match raw.secrets {
            Some(section) => {
                enforce_0600(&section.file, "secrets file")?;
                let secrets_text = fs::read_to_string(&section.file)
                    .with_context(|| format!("read secrets file {}", section.file.display()))?;
                let secrets = toml::from_str(&secrets_text).context("parse secrets toml")?;
                (Some(section.file), secrets)
            }
            None => (None, BTreeMap::new()),
        };

        let config = Self {
            server: raw.server,
            subscribers: raw.subscribers,
            extensions: raw.extensions,
            limits: raw.limits,
            timeouts: raw.timeouts,
            secrets_file,
            secrets,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn token_store_path(&self) -> PathBuf {
        self.subscribers
            .token_store
            .clone()
            .unwrap_or_else(|| PathBuf::from("/var/lib/whdr/tokens.toml"))
    }

    pub fn validate(&self) -> Result<()> {
        if self.subscribers.tls.is_some() {
            bail!("subscriber TLS is configured but native TLS is not implemented");
        }
        if let Some(metrics_addr) = self.server.metrics_addr
            && !metrics_addr.ip().is_loopback()
        {
            bail!(
                "metrics_addr must bind a loopback address; scrape locally or front with a proxy"
            );
        }
        if !self.server.sub_addr.ip().is_loopback()
            && self.subscribers.tls.is_none()
            && !self.subscribers.allow_plaintext_lan
        {
            bail!(
                "refusing non-loopback subscriber bind without TLS or allow_plaintext_lan = true"
            );
        }
        Ok(())
    }
}

pub(crate) fn enforce_0600(path: &Path, label: &str) -> Result<()> {
    let meta = fs::metadata(path).with_context(|| format!("stat {label} {}", path.display()))?;
    let mode = meta.permissions().mode() & 0o777;
    if mode != 0o600 {
        bail!(
            "{label} {} must have mode 0600, got {mode:o}",
            path.display()
        );
    }
    Ok(())
}
