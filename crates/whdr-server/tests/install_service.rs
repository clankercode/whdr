use std::path::Path;
use std::process::Command;

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate is under crates/whdr-server")
}

fn run_install_service(args: &[&str]) -> String {
    let script = repo_root().join("scripts/install-service.sh");

    let output = Command::new("bash")
        .arg(script)
        .args(args)
        .output()
        .expect("run install-service dry-run");

    assert!(
        output.status.success(),
        "dry-run failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8(output.stdout).expect("stdout is utf8")
}

#[test]
fn install_service_dry_run_uses_default_layout_without_overrides() {
    let stdout = run_install_service(&["--dry-run", "--no-start"]);

    assert!(stdout.contains("install whdr-server -> /usr/local/bin/whdr-server"));
    assert!(stdout.contains("install whdr -> /usr/local/bin/whdr"));
    assert!(stdout.contains("write config -> /etc/whdr/config.toml"));
    assert!(stdout.contains("write secrets -> /etc/whdr/secrets.toml"));
    assert!(stdout.contains("token_store = \"/var/lib/whdr/tokens.toml\""));
    assert!(stdout.contains("control_socket = \"/run/whdr/ctl.sock\""));
    assert!(stdout.contains("install systemd unit -> /etc/systemd/system/whdr.service"));
}

#[test]
fn install_service_dry_run_uses_overridable_default_layout() {
    let stdout = run_install_service(&[
        "--dry-run",
        "--prefix",
        "/opt/whdr",
        "--config-dir",
        "/tmp/whdr-test/etc",
        "--state-dir",
        "/tmp/whdr-test/state",
        "--service-dir",
        "/tmp/whdr-test/systemd",
        "--no-start",
    ]);

    assert!(stdout.contains("install whdr-server -> /opt/whdr/bin/whdr-server"));
    assert!(stdout.contains("write config -> /tmp/whdr-test/etc/config.toml"));
    assert!(stdout.contains("write secrets -> /tmp/whdr-test/etc/secrets.toml"));
    assert!(stdout.contains("token_store = \"/tmp/whdr-test/state/tokens.toml\""));
    assert!(stdout.contains("control_socket = \"/run/whdr/ctl.sock\""));
    assert!(stdout.contains("file = \"/tmp/whdr-test/etc/secrets.toml\""));
    assert!(stdout.contains("install systemd unit -> /tmp/whdr-test/systemd/whdr.service"));
    assert!(
        stdout.contains(
            "Environment=PATH=/opt/whdr/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin"
        )
    );
    assert!(
        stdout.contains(
            "ExecStart=/opt/whdr/bin/whdr-server --config /tmp/whdr-test/etc/config.toml"
        )
    );
    assert!(stdout.contains("RuntimeDirectory=whdr"));
    assert!(stdout.contains("StateDirectory=whdr"));
}
