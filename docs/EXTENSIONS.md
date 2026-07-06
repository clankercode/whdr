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

## The generic HMAC extension (`whdr-ext-hmac`)

`whdr-ext-hmac` verifies an HMAC signature over the raw request body so whdr can
ingest webhooks from any provider whose scheme is "HMAC of the body, encoded in a
header" (Stripe, Linear, Shopify, ...) without writing a bespoke Rust extension.

**Secret.** The provider secret is supplied exactly like every other extension:
per request on stdin, keyed by the ext id `hmac` in `secrets.toml`
(`hmac = "whsec_..."`). It is never on argv and never logged (SPEC §12).

**Non-secret configuration** is read once at startup from environment variables
(the extension inherits the server process environment; set them in the systemd
unit or the server's shell). All are optional:

| Variable | Default | Meaning |
| --- | --- | --- |
| `WHDR_HMAC_HEADER` | `X-Signature` | Header carrying the signature (matched case-insensitively). |
| `WHDR_HMAC_ALGORITHM` | `sha256` | Digest: `sha1`, `sha256`, or `sha512`. |
| `WHDR_HMAC_ENCODING` | `hex` | Signature encoding in the header: `hex` or `base64`. |
| `WHDR_HMAC_PREFIX` | *(none)* | Literal prefix stripped before decoding, e.g. `sha256=`. If set and absent from the header, the request is rejected. |
| `WHDR_HMAC_CHANNEL_PREFIX` | `hmac` | First channel segment for emitted events. Declared as the ext's channel namespace at register (SPEC §5.3). |

An unparseable algorithm or encoding makes the extension exit at startup with a
clear message rather than silently misbehave.

**Verification.** The extension recomputes the HMAC over the exact raw body,
decodes the provided signature to raw bytes, and compares in constant time
(`subtle::ConstantTimeEq`). Every failure — missing secret, missing header,
missing/mismatched prefix, malformed encoding, wrong length, or value mismatch —
returns `401` with no events, matching the github/teams rejection semantics.

**Channel derivation.** On success the extension emits one event whose payload is
the raw body and whose channel is `WHDR_HMAC_CHANNEL_PREFIX` plus any request-path
segments beyond the routing mount, each tokenised to the channel grammar. With the
defaults, `POST /hmac/stripe/foo` emits on `hmac.stripe.foo`; `POST /hmac` emits on
`hmac`. Point multiple providers at distinct sub-paths to fan them onto distinct
channels under the same extension.
