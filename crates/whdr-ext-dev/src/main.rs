use anyhow::Result;
use async_trait::async_trait;
use whdr_ext_dev::handle_dev_dispatch;
use whdr_ext_kit::{DispatchResult, Extension, run_extension};
use whdr_proto::SrvMsg;

struct Dev;

#[async_trait]
impl Extension for Dev {
    async fn handle_dispatch(&self, dispatch: SrvMsg) -> DispatchResult {
        handle_dev_dispatch(dispatch)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    run_extension(
        "dev",
        vec![],
        vec![],
        serde_json::json!({"description":"development echo extension"}),
        Dev,
    )
    .await
}
