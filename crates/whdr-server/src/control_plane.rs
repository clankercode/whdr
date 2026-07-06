//! Admin plane: request/response ndjson over a local UDS (SPEC §13).
//! Reachability to this socket IS the admin capability — it stays a local
//! UDS gated by filesystem permissions, never network-exposed.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use anyhow::{Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{UnixListener, UnixStream};
use tracing::debug;
use whdr_proto::{ControlRequest, ControlResponse, decode_line, encode_line};

use crate::daemon::AppState;

pub(crate) async fn control_loop(state: AppState, path: PathBuf) -> Result<()> {
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
        ControlRequest::TokenAdd { name } => state.token_control().add(name).await,
        ControlRequest::TokenRotate { name } => state.token_control().rotate(name).await,
        ControlRequest::TokenRevoke { name } => state.token_control().revoke(name).await,
        ControlRequest::TokenList => state.token_control().list().await,
    }
}
