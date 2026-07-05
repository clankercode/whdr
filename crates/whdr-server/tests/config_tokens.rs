use std::fs;
use std::os::unix::fs::PermissionsExt;

use std::collections::BTreeMap;

use whdr_server::{Config, TokenStore};

#[test]
fn config_loads_defaults_and_enforces_secret_file_0600() {
    let temp = tempfile::tempdir().unwrap();
    let secrets = temp.path().join("secrets.toml");
    fs::write(&secrets, "github = \"whsec_test\"\n").unwrap();
    fs::set_permissions(&secrets, fs::Permissions::from_mode(0o644)).unwrap();

    let config = temp.path().join("whdr.toml");
    fs::write(
        &config,
        format!(
            r#"
[server]
control_socket = "{}"

[secrets]
file = "{}"
"#,
            temp.path().join("ctl.sock").display(),
            secrets.display()
        ),
    )
    .unwrap();

    let err = Config::load(&config).unwrap_err();
    assert!(err.to_string().contains("mode 0600"));

    fs::set_permissions(&secrets, fs::Permissions::from_mode(0o600)).unwrap();
    let loaded = Config::load(&config).unwrap();
    assert_eq!(loaded.server.listen_addr.to_string(), "127.0.0.1:8787");
    assert_eq!(loaded.subscribers.ws_idle_timeout_ms, 30_000);
    assert_eq!(loaded.secrets.get("github").unwrap(), "whsec_test");
}

#[test]
fn config_rejects_tls_until_native_tls_is_supported() {
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("whdr.toml");
    fs::write(
        &config,
        format!(
            r#"
[server]
control_socket = "{}"

[subscribers.tls]
cert = "/tmp/cert.pem"
key = "/tmp/key.pem"
"#,
            temp.path().join("ctl.sock").display()
        ),
    )
    .unwrap();

    let err = Config::load(&config).unwrap_err();
    assert!(
        err.to_string()
            .contains("subscriber TLS is configured but native TLS is not implemented")
    );
}

#[test]
fn token_store_hashes_tokens_persists_and_authenticates() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("tokens.toml");

    let mut store = TokenStore::load_or_empty(path.clone()).unwrap();
    let token = store.add("project-a").unwrap();
    assert!(token.starts_with("tok_"));
    assert!(!fs::read_to_string(&path).unwrap().contains(&token));
    assert_eq!(store.authenticate(&token).unwrap(), "project-a");

    let old = token;
    let rotated = store.rotate("project-a").unwrap();
    assert_ne!(old, rotated);
    assert!(store.authenticate(&old).is_none());
    assert_eq!(store.authenticate(&rotated).unwrap(), "project-a");

    let reloaded = TokenStore::load_or_empty(path).unwrap();
    assert_eq!(reloaded.authenticate(&rotated).unwrap(), "project-a");
    let mut active = BTreeMap::new();
    active.insert("project-a".to_string(), 2);
    active.insert("other".to_string(), 7);
    assert_eq!(reloaded.list(&active)[0].name, "project-a");
    assert_eq!(reloaded.list(&active)[0].active_conns, 2);
}
