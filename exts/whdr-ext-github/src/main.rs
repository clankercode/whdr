use anyhow::Result;
use async_trait::async_trait;
use whdr_ext_github::handle_github_dispatch;
use whdr_ext_kit::{DispatchResult, Extension, run_extension};
use whdr_proto::SrvMsg;

struct Github;

#[async_trait]
impl Extension for Github {
    async fn handle_dispatch(&self, dispatch: SrvMsg) -> DispatchResult {
        handle_github_dispatch(dispatch)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    run_extension(
        "github",
        vec!["gh".to_string()],
        vec![],
        serde_json::json!({"description":"GitHub webhook extension"}),
        Github,
    )
    .await
}
