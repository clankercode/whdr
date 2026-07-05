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
