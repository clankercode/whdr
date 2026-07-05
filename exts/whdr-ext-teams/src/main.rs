use anyhow::Result;
use async_trait::async_trait;
use whdr_ext_kit::{DispatchResult, Extension, run_extension};
use whdr_ext_teams::handle_teams_dispatch;
use whdr_proto::SrvMsg;

struct Teams;

#[async_trait]
impl Extension for Teams {
    async fn handle_dispatch(&self, dispatch: SrvMsg) -> DispatchResult {
        handle_teams_dispatch(dispatch)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    run_extension(
        "teams",
        vec![],
        vec![],
        serde_json::json!({"description":"Microsoft Teams webhook extension"}),
        Teams,
    )
    .await
}
