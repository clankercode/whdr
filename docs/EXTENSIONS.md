# Writing whdr Extensions

An extension is a native executable named `whdr-ext-<id>` on `PATH`. The server starts enabled extensions, waits for a `register` message on stdout, then sends `dispatch` messages on stdin.

Stdout is protocol-only NDJSON. Human logs go to stderr.

```rust
use anyhow::Result;
use async_trait::async_trait;
use whdr_ext_kit::{DispatchResult, Extension, run_extension};
use whdr_proto::SrvMsg;

struct MyExt;

#[async_trait]
impl Extension for MyExt {
    async fn handle_dispatch(&self, dispatch: SrvMsg) -> DispatchResult {
        // Verify the raw body, parse the provider payload, then return the HTTP reply
        // and zero or more whdr events.
        todo!()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    run_extension("myext", vec![], vec![], serde_json::json!({}), MyExt).await
}
```

The `Dispatch` body is base64 so signature verification can use the exact raw bytes. Secrets arrive on stdin per request and should never be logged or cached.

Channels use NATS-style grammar. Emit under your own namespace, for example `myext.created` or a channel prefix registered at startup.
