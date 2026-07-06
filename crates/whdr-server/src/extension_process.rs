use std::env;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::time;
use tracing::{debug, warn};

/// Scan PATH for `whdr-ext-*` executables; the suffix is the candidate id.
pub(crate) fn discover_extensions() -> Result<Vec<(String, PathBuf)>> {
    let mut found = std::collections::HashMap::new();
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

pub(crate) struct ExtensionProcess {
    pub(crate) child: Child,
    pub(crate) stdin: ChildStdin,
    pub(crate) stdout: ChildStdout,
    pub(crate) pid: Option<u32>,
}

pub(crate) async fn spawn_extension_process(
    candidate_id: &str,
    path: &Path,
) -> Result<ExtensionProcess> {
    let mut child = extension_command(path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("spawn extension {}", path.display()))?;

    let pid = child.id();
    let Some(stdin) = child.stdin.take() else {
        kill_child_wait(&mut child, candidate_id, "stdin unavailable").await;
        bail!("extension stdin unavailable");
    };
    let Some(stdout) = child.stdout.take() else {
        kill_child_wait(&mut child, candidate_id, "stdout unavailable").await;
        bail!("extension stdout unavailable");
    };
    forward_stderr(candidate_id, child.stderr.take());

    Ok(ExtensionProcess {
        child,
        stdin,
        stdout,
        pid,
    })
}

fn forward_stderr(candidate_id: &str, stderr: Option<tokio::process::ChildStderr>) {
    if let Some(stderr) = stderr {
        let stderr_id = candidate_id.to_string();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                warn!(ext = stderr_id, "{line}");
            }
        });
    }
}

#[cfg(not(test))]
fn extension_command(path: &Path) -> Command {
    Command::new(path)
}

#[cfg(test)]
fn extension_command(path: &Path) -> Command {
    if is_test_shell_script(path) {
        let mut command = Command::new("/bin/sh");
        command.arg(path);
        command
    } else {
        Command::new(path)
    }
}

#[cfg(test)]
fn is_test_shell_script(path: &Path) -> bool {
    let Ok(contents) = fs::read(path) else {
        return false;
    };
    contents
        .split(|byte| *byte == b'\n')
        .next()
        .is_some_and(|line| line == b"#!/bin/sh")
}

pub(crate) async fn wait_for_child_shutdown(child: &mut Child, ext: &str, term_grace_ms: u64) {
    let grace = Duration::from_millis(term_grace_ms.max(1));
    match time::timeout(grace, child.wait()).await {
        Ok(Ok(status)) => {
            debug!(ext, ?status, "extension exited after shutdown");
            return;
        }
        Ok(Err(err)) => {
            debug!(ext, error = %err, "extension wait failed after shutdown");
            return;
        }
        Err(_) => {}
    }

    warn!(
        ext,
        term_grace_ms, "extension did not exit after shutdown; terminating"
    );
    terminate_child(child, ext);
    match time::timeout(grace, child.wait()).await {
        Ok(Ok(status)) => {
            debug!(ext, ?status, "extension exited after terminate");
        }
        Ok(Err(err)) => {
            debug!(ext, error = %err, "extension wait failed after terminate");
        }
        Err(_) => {
            warn!(
                ext,
                term_grace_ms, "extension did not exit after terminate; killing"
            );
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
    }
}

fn terminate_child(child: &mut Child, ext: &str) {
    #[cfg(unix)]
    {
        if let Some(pid) = child.id() {
            let rc = unsafe { libc::kill(pid as i32, libc::SIGTERM) };
            if rc == 0 {
                return;
            }
            debug!(ext, pid, "failed to send SIGTERM; falling back to kill");
        }
    }
    let _ = child.start_kill();
}

pub(crate) async fn kill_child_wait(child: &mut Child, ext: &str, reason: &str) {
    match child.kill().await {
        Ok(()) => debug!(ext, reason, "extension child killed before ready"),
        Err(err) => {
            debug!(ext, reason, error = %err, "failed to kill extension child before ready")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use tokio::io::{AsyncBufReadExt, BufReader};

    use super::*;

    #[tokio::test]
    async fn spawn_extension_process_runs_test_shell_script_with_piped_stdio() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ext.sh");
        fs::write(
            &path,
            "#!/bin/sh\necho ready\nprintf 'err-line\\n' >&2\nsleep 30\n",
        )
        .unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();

        let ExtensionProcess {
            mut child,
            stdin,
            stdout,
            pid,
        } = spawn_extension_process("candidate", &path).await.unwrap();

        assert!(pid.is_some());
        drop(stdin);
        let mut lines = BufReader::new(stdout).lines();
        assert_eq!(lines.next_line().await.unwrap().as_deref(), Some("ready"));

        kill_child_wait(&mut child, "candidate", "test complete").await;
    }
}
