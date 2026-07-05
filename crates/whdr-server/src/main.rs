use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use whdr_server::run_with_signals;

#[derive(Debug, Parser)]
#[command(name = "whdr-server", about = "Webhook Dynamic Router daemon")]
struct Args {
    #[arg(long, default_value = "/etc/whdr/config.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "whdr_server=info,whdr=info".into()),
        )
        .init();
    let args = Args::parse();
    run_with_signals(args.config).await
}
