# whdr

whdr is a single-node Webhook Dynamic Router. It accepts provider webhooks over HTTP, dispatches each request to a supervised `whdr-ext-*` process over NDJSON stdio, and fans emitted events out to token-authenticated WebSocket subscribers.

## Crates

- `whdr-proto`: shared wire types, NDJSON helpers, channel/pattern validation.
- `whdr-server`: daemon, extension supervisor, HTTP ingest, WebSocket subscriber plane, admin UDS.
- `whdr-cli`: local control client, installed as `whdr`.
- `whdr-ext-kit`: helper library for extension binaries.
- `whdr-ext-dev`: development echo extension.
- `whdr-ext-github`: GitHub webhook extension.
- `whdr-ext-teams`: Microsoft Teams/Graph webhook extension.

## Quick Start

```bash
cargo build --workspace
cp examples/whdr.toml /tmp/whdr.toml
cp examples/secrets.toml /tmp/whdr-secrets.toml
chmod 600 /tmp/whdr-secrets.toml
PATH="$PWD/target/debug:$PATH" target/debug/whdr-server --config /tmp/whdr.toml
```

In another terminal:

```bash
curl -X POST http://127.0.0.1:8787/dev -d 'hello'
target/debug/whdr --socket /tmp/whdr/ctl.sock status
```

Issue subscriber tokens with:

```bash
target/debug/whdr --socket /tmp/whdr/ctl.sock token add project-a
```

## Observability

`whdr status` over the control socket is the primary admin surface. Optionally set
`metrics_addr = "127.0.0.1:9598"` under `[server]` to serve Prometheus text metrics at
`GET /metrics` — loopback only, rendered from the same data as `whdr status`.

## Install As A Service

On a Linux host with systemd:

```bash
sudo scripts/install-service.sh
```

The installer builds release binaries, installs them to `/usr/local/bin`, writes a systemd unit, and creates the default service layout:

- Config: `/etc/whdr/config.toml`
- Provider secrets: `/etc/whdr/secrets.toml`
- Token store: `/var/lib/whdr/tokens.toml`
- Control socket: `/run/whdr/ctl.sock`

Preview the exact files and commands without changing the machine:

```bash
scripts/install-service.sh --dry-run
```

Useful overrides:

```bash
sudo scripts/install-service.sh --prefix /opt/whdr --config-dir /etc/whdr --state-dir /var/lib/whdr
```

After install:

```bash
systemctl status whdr.service
journalctl -u whdr.service -f
sudo whdr --socket /run/whdr/ctl.sock status
```

The service owns the control socket as `whdr:whdr` with mode `0660`. Use `sudo`
for one-off admin commands, or add trusted administrators to the `whdr` group.

## Documentation

- [Specification](docs/SPEC.md)
- [Implementation Plan](docs/PLAN.md)
- [Extension Authoring Guide](docs/EXTENSIONS.md)

## Verification

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

## License

Released under your choice of the Unlicense or CC0 1.0 Universal. See [LICENSE](LICENSE).
