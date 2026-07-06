# whdr Operations Guide

End-to-end operational guide for running `whdr` on a Linux/systemd host: from a
fresh checkout to receiving provider webhooks, then day-two operations
(tokens, upgrades, backups, troubleshooting).

This guide only documents commands and flags that exist in the code and the
installer as of writing. For extension authoring see
[EXTENSIONS.md](EXTENSIONS.md); for public exposure via a tunnel/reverse proxy
see [TUNNELS.md](TUNNELS.md); for the full design see [SPEC.md](SPEC.md).

## Architecture in one paragraph

`whdr-server` is the daemon. It exposes three planes:

- **Ingest** — HTTP listener (`[server].listen_addr`, default `127.0.0.1:8787`)
  that receives provider webhooks and routes each to a supervised
  `whdr-ext-*` extension process over NDJSON stdio.
- **Subscriber** — WebSocket listener (`[server].sub_addr`, default
  `127.0.0.1:8788`) that fans emitted events out to token-authenticated
  subscribers.
- **Admin** — a local Unix domain socket (`[server].control_socket`, default
  `/run/whdr/ctl.sock`, mode `0660`). Reachability to this socket *is* the
  admin capability. It is never network-exposed. The `whdr` CLI talks to it.

All three default to loopback. Publishing ingest to the internet is done with a
separate tunnel/reverse proxy — see [TUNNELS.md](TUNNELS.md).

---

## 1. Build

Limit builds/tests to 2 threads on shared machines (the `justfile` already does
this):

```bash
just build          # cargo build --workspace -j 2
just build-release  # cargo build --workspace --bins --release -j 2
just test           # cargo test --workspace --all-features -j 2
just ci             # fmt-check + clippy + test (mirrors .github/workflows/ci.yml)
```

Without `just`:

```bash
cargo build --workspace --bins --release -j 2
```

The workspace produces these binaries: `whdr-server` (daemon), `whdr` (CLI),
`whdr-ext-dev`, `whdr-ext-github`, `whdr-ext-teams`.

## 2. Install as a systemd service

The installer builds release binaries, creates the `whdr` system user/group,
installs the binaries and a systemd unit, and writes a default config, secrets
file, and service layout. It must run as root.

**Always preview first** — `--dry-run` prints the full plan plus the generated
`config.toml` and unit without touching the machine:

```bash
scripts/install-service.sh --dry-run
```

Then install:

```bash
sudo scripts/install-service.sh
```

### Default layout

| What          | Path                          | Mode / owner        |
|---------------|-------------------------------|---------------------|
| Binaries      | `/usr/local/bin/`             | `0755`              |
| Config        | `/etc/whdr/config.toml`       | `0644`              |
| Provider secrets | `/etc/whdr/secrets.toml`   | `0600 whdr:whdr`    |
| Token store   | `/var/lib/whdr/tokens.toml`   | dir `0750 whdr:whdr`|
| Control socket| `/run/whdr/ctl.sock`          | `0660 whdr:whdr`    |

### Installer options

All flags (from `scripts/install-service.sh --help`):

| Flag | Effect |
|------|--------|
| `--dry-run` | Print the install plan and generated config/unit; change nothing. |
| `--prefix DIR` | Install binaries under `DIR/bin` (default `/usr/local`). |
| `--config-dir DIR` | Config directory (default `/etc/whdr`). |
| `--state-dir DIR` | State directory (default `/var/lib/whdr`). |
| `--service-dir DIR` | systemd unit directory (default `/etc/systemd/system`). |
| `--user USER` / `--group GROUP` | Service identity (default `whdr` / `whdr`). |
| `--listen-addr ADDR` | Ingest listen address written into `config.toml` (default `127.0.0.1:8787`). Must contain a `:port`. |
| `--debug` | Install debug-profile binaries from `target/debug`. |
| `--skip-build` | Do not run `cargo build` first (use already-built binaries). |
| `--no-enable` | Do not `systemctl enable` the service. |
| `--no-start` | Do not restart the service after install. |
| `--tunnel-provider PROVIDER` | Optional tunnel companion: `none` or `cloudflare` (default `none`). |
| `--public-host HOST` | Public hostname routed to ingest when using a tunnel. |
| `--cloudflare-tunnel NAME` | Cloudflare Tunnel name (with `--tunnel-provider cloudflare`). |
| `--cloudflare-credentials-file FILE` | Cloudflare Tunnel credentials file. |
| `--cloudflared-bin FILE` | `cloudflared` binary path (default `/usr/bin/cloudflared`). |
| `--tunnel-config-dir DIR` | Tunnel config directory (default `/etc/cloudflared`). |
| `--tunnel-service-name NAME` | Companion systemd service name (default `whdr-tunnel-cloudflare`). |

The installer **preserves existing** `config.toml` and `secrets.toml` on
re-run (it prints `keeping existing …`), so it is safe to re-run for upgrades.

### After install: enable extensions and set secrets

The installed config ships with `enabled = []` — no extensions are routed until
you list them. Edit `/etc/whdr/config.toml`:

```toml
[extensions]
enabled = ["github", "teams"]
```

Then put the real provider signing secrets (keyed by extension id) in
`/etc/whdr/secrets.toml` (must be mode `0600`):

```toml
github = "whsec_…"
teams  = "…"
```

Apply the changes (see [§5 Reloading](#5-reloading-config)):

```bash
sudo systemctl restart whdr.service
```

## 3. First webhook (smoke test)

With the `dev` extension enabled you can prove the ingest path end-to-end:

```bash
curl -X POST http://127.0.0.1:8787/dev -d 'hello'
sudo whdr status
```

The path segment (`/dev`) selects the extension by id. A provider integration
points its webhook URL at `https://<public-host>/<ext-id>` (via the tunnel).

---

## 4. Subscriber tokens

Tokens are minted, rotated, revoked and listed **at runtime** over the control
socket — no restart, no editing the store by hand. Every change is persisted
(hashed) to the token store atomically before the command returns, and survives
restarts. Token values are shown **once**, at mint/rotate time.

The CLI global flag `--socket` defaults to `/run/whdr/ctl.sock`; on a default
install you need `sudo` (or membership in the `whdr` group) to reach it.

```bash
sudo whdr token add project-a       # mint; prints "project-a: tok_…" ONCE
sudo whdr token rotate project-a    # new value, invalidates old, drops its live conns
sudo whdr token revoke project-a    # remove; closes its live connections
sudo whdr token list                # NAME  FINGERPRINT  CREATED  ACTIVE
```

`token list` shows a short non-reversible fingerprint (never the token value),
creation time, and current live connection count.

Typical flow: `whdr token add project-a` → copy the `tok_…` into that
subscriber's config → it connects to the WebSocket plane with
`Authorization: Bearer tok_…`. Lost a token? `rotate`. Decommissioning a
consumer? `revoke`.

---

## 5. Reloading config

The daemon handles **SIGHUP** as a config reload. Reload re-reads
`config.toml`, the token store, and the secrets file; it restarts/starts
extensions per the new `enabled` list, and closes subscriber connections whose
tokens are no longer valid.

The unit defines `ExecReload` (SIGHUP), so a reload is just:

```bash
sudo systemctl reload whdr.service
```

Sending the signal directly (`sudo systemctl kill -s HUP whdr.service`) is
equivalent.

**Reload does not rebind listeners.** Changes to `listen_addr`, `sub_addr`, or
`control_socket` require a full restart:

```bash
sudo systemctl restart whdr.service
```

---

## 6. systemd unit management

```bash
sudo systemctl start whdr.service
sudo systemctl stop whdr.service
sudo systemctl restart whdr.service
sudo systemctl enable whdr.service     # start on boot (installer does this by default)
sudo systemctl disable whdr.service
systemctl status whdr.service
systemctl is-active whdr.service
```

If you installed a Cloudflare tunnel companion, it is a second unit (default
name `whdr-tunnel-cloudflare.service`) managed the same way. See
[TUNNELS.md](TUNNELS.md).

---

## 7. Status, health, and metrics

There is no separate health CLI — **`whdr status` is the health surface.**
It reflects the same status document the metrics endpoint renders, so the two
never disagree.

```bash
sudo whdr status          # human-readable table
sudo whdr status --json   # raw status document (pretty JSON)
```

The status document reports:

- `uptime_ms`, plus global counters.
- Per extension: `id`, `state`, `pid`, `restarts`, `paths`, `channels`,
  `in_flight`, `protocol_errors`, `consecutive_timeouts`, `events_emitted`,
  `last_event_at_ms`. (`last_event_at_ms` makes silence from a pure poller
  visible — such extensions can't be hang-detected via dispatch timeouts.)
- Per subscriber: `name`, `remote_addr`, `patterns`, `delivered`, `dropped`.
  `delivered` + `dropped` together account for every event that matched the
  subscriber's patterns.

Quick liveness check for scripts:

```bash
systemctl is-active whdr.service && sudo whdr status --json | jq .uptime_ms
```

### Prometheus metrics (optional, off by default)

Set `metrics_addr` under `[server]` to serve `GET /metrics` in Prometheus text
format (0.0.4). The bind **must be loopback** — the daemon refuses a
non-loopback `metrics_addr` at startup; scrape locally or front it with a proxy.

```toml
[server]
metrics_addr = "127.0.0.1:9184"
```

The installer does not set `metrics_addr`; add it yourself and restart.

```bash
curl -s http://127.0.0.1:9184/metrics
```

---

## 8. Logs

The daemon logs via `tracing` to stderr, captured by journald. Extension stderr
lines are ingested and tagged `ext=<id>`.

```bash
journalctl -u whdr.service -f              # follow
journalctl -u whdr.service --since "1h ago"
journalctl -u whdr.service -p err          # errors only
```

Log verbosity is controlled by the `RUST_LOG` env filter; the default when
unset is `whdr_server=info,whdr=info`. To change it, add a drop-in override:

```bash
sudo systemctl edit whdr.service
# [Service]
# Environment=RUST_LOG=whdr_server=debug
sudo systemctl restart whdr.service
```

---

## 9. Upgrading in place

Re-running the installer is the supported upgrade path. It rebuilds release
binaries, reinstalls them, **keeps** the existing `config.toml` and
`secrets.toml`, refreshes the unit, and restarts the service. Tokens persist in
the store across the upgrade.

```bash
git pull
scripts/install-service.sh --dry-run       # confirm nothing unexpected changes
sudo scripts/install-service.sh
```

If you build binaries separately (e.g. on a build host) use `--skip-build`.
To install a new binary without an automatic restart, add `--no-start` and
restart during a maintenance window yourself.

---

## 10. Backup and restore of the persistent store

The daemon's durable state is three files. Back up all three:

| File | Contents |
|------|----------|
| `/var/lib/whdr/tokens.toml` | Subscriber tokens (stored **hashed**). |
| `/etc/whdr/secrets.toml` | Provider signing secrets (plaintext, `0600`). |
| `/etc/whdr/config.toml` | Server configuration. |

Backup (as root):

```bash
sudo tar czf whdr-backup-$(date +%F).tar.gz \
  /etc/whdr/config.toml /etc/whdr/secrets.toml /var/lib/whdr/tokens.toml
```

Restore:

```bash
sudo systemctl stop whdr.service
sudo tar xzf whdr-backup-YYYY-MM-DD.tar.gz -C /
# Re-assert ownership/modes the daemon expects:
sudo chown whdr:whdr /etc/whdr/secrets.toml /var/lib/whdr/tokens.toml
sudo chmod 0600 /etc/whdr/secrets.toml
sudo systemctl start whdr.service
```

Because tokens are stored hashed, a restore preserves working subscriber tokens
(the plaintext values were only ever shown once at mint time — they are not
recoverable from a backup, only usable).

---

## 11. Troubleshooting

**`whdr status` fails with permission denied / connection refused.**
The control socket is `0660 whdr:whdr`. Run under `sudo`, or add the operator to
the `whdr` group. "Connection refused" instead means the daemon isn't running —
check `systemctl status whdr.service`.

**Service won't start.** Check `journalctl -u whdr.service -e`. Common config
validation errors that abort startup:
- `subscriber TLS is configured but native TLS is not implemented` — native
  `[subscribers.tls]` is rejected; terminate TLS at a proxy/tunnel instead.
- `refusing non-loopback subscriber bind without TLS or allow_plaintext_lan` —
  binding `sub_addr` off loopback requires `allow_plaintext_lan = true` (LAN
  opt-in) since tokens cross the wire in the clear.
- `metrics_addr must bind a loopback address` — see §7.

**A webhook returns 404.** The extension for that path isn't routed. Confirm the
id is in `[extensions].enabled`, that you reloaded/restarted, and that
`whdr status` shows it in a running state.

**An extension keeps restarting.** `whdr status` shows its `state`, `restarts`,
`protocol_errors`, and `consecutive_timeouts`. Repeated timeouts (default
threshold 3 consecutive) trigger a kill + backoff restart; a crash-loop
(5 exits / 60 s) parks it in `Failed` until the next SIGHUP. Check the tagged
`ext=<id>` log lines.

**Subscriber can't connect / keeps dropping.** Verify the token with
`whdr token list` (fingerprint + active count). A rotated/revoked token closes
live connections immediately. Idle connections that miss WebSocket pings are
dropped after `ws_idle_timeout_ms` (default 30 s).

**Provider signatures failing.** Signatures are verified by the extension
against the exact raw request body delivered through whdr; a tunnel/proxy in
front does not change that. Confirm the secret in `/etc/whdr/secrets.toml` is
keyed by the correct extension id and the file is `0600`.
