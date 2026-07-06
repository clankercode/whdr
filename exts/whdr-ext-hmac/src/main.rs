use anyhow::Result;
use async_trait::async_trait;
use whdr_ext_hmac::{HmacConfig, handle_hmac_dispatch};
use whdr_ext_kit::{DispatchResult, Extension, run_extension};
use whdr_proto::SrvMsg;

struct Hmac {
    config: HmacConfig,
}

#[async_trait]
impl Extension for Hmac {
    async fn handle_dispatch(&self, dispatch: SrvMsg) -> DispatchResult {
        handle_hmac_dispatch(&self.config, dispatch)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Read non-secret config from the environment once at startup. A bad value
    // fails here (before register) so the operator sees the error, rather than
    // the ext silently accepting or rejecting everything.
    let config = HmacConfig::from_env()?;
    eprintln!(
        "whdr-ext-hmac: header={:?} algorithm={:?} encoding={:?} prefix={:?} channel_prefix={:?}",
        config.header, config.algorithm, config.encoding, config.prefix, config.channel_prefix
    );
    let channel_prefix = config.channel_prefix.clone();
    run_extension(
        "hmac",
        vec![],
        vec![channel_prefix],
        serde_json::json!({"description":"Generic HMAC signature-verification webhook extension"}),
        Hmac { config },
    )
    .await
}
