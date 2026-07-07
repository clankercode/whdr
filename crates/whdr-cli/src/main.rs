use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::{Parser, Subcommand};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::UnixStream;
use whdr_proto::{ControlRequest, ControlResponse, decode_line, encode_line};

#[derive(Debug, Parser)]
#[command(name = "whdr", about = "Control a local whdr daemon")]
struct Args {
    #[arg(long, default_value = "/run/whdr/ctl.sock")]
    socket: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Status {
        #[arg(long)]
        json: bool,
    },
    Token {
        #[command(subcommand)]
        command: TokenCommand,
    },
}

#[derive(Debug, Subcommand)]
enum TokenCommand {
    Add { name: String },
    Rotate { name: String },
    Revoke { name: String },
    List,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let request = match &args.command {
        Command::Status { .. } => ControlRequest::Status,
        Command::Token { command } => match command {
            TokenCommand::Add { name } => ControlRequest::TokenAdd { name: name.clone() },
            TokenCommand::Rotate { name } => ControlRequest::TokenRotate { name: name.clone() },
            TokenCommand::Revoke { name } => ControlRequest::TokenRevoke { name: name.clone() },
            TokenCommand::List => ControlRequest::TokenList,
        },
    };

    let response = send_request(&args.socket, request).await?;
    match (&args.command, response) {
        (Command::Status { json: true }, ControlResponse::Status { status }) => {
            println!("{}", serde_json::to_string_pretty(&status)?);
        }
        (Command::Status { json: false }, ControlResponse::Status { status }) => {
            print_status(&status);
        }
        (_, ControlResponse::Token { name, token }) => {
            println!("{name}: {token}");
        }
        (_, ControlResponse::Tokens { tokens }) => {
            println!("NAME\tFINGERPRINT\tCREATED\tACTIVE");
            for token in tokens {
                println!(
                    "{}\t{}\t{}\t{}",
                    token.name, token.fingerprint, token.created, token.active_conns
                );
            }
        }
        (_, ControlResponse::Ok) => println!("ok"),
        (_, ControlResponse::Error { msg }) => bail!("{msg}"),
        (_, other) => bail!("unexpected response: {other:?}"),
    }
    Ok(())
}

async fn send_request(path: &PathBuf, request: ControlRequest) -> Result<ControlResponse> {
    let stream = UnixStream::connect(path).await?;
    let (read, write) = stream.into_split();
    let mut writer = BufWriter::new(write);
    writer.write_all(encode_line(&request)?.as_bytes()).await?;
    writer.flush().await?;
    drop(writer);

    let mut lines = BufReader::new(read).lines();
    let Some(line) = lines.next_line().await? else {
        bail!("daemon closed control socket without a response");
    };
    Ok(decode_line::<ControlResponse>(&line)?.expect("response line is not blank"))
}

/// Format a millisecond duration as a compact string using the largest two
/// non-zero units (d/h/m/s), e.g. `10h 32m`. Below a minute, only seconds are
/// shown.
fn humanize_ms(ms: u64) -> String {
    let total_secs = ms / 1000;
    let days = total_secs / 86_400;
    let hours = (total_secs % 86_400) / 3_600;
    let mins = (total_secs % 3_600) / 60;
    let secs = total_secs % 60;

    if days > 0 {
        format!("{days}d {hours}h")
    } else if hours > 0 {
        format!("{hours}h {mins}m")
    } else if mins > 0 {
        format!("{mins}m {secs}s")
    } else {
        format!("{secs}s")
    }
}

fn print_status(status: &serde_json::Value) {
    let uptime_ms = status["uptime_ms"].as_u64().unwrap_or(0);
    println!("uptime_ms: {uptime_ms} ({})", humanize_ms(uptime_ms));
    println!("extensions:");
    if let Some(exts) = status["extensions"].as_array() {
        for ext in exts {
            println!(
                "  {}\t{}\tpid={}\tevents={}\tlast_event_at_ms={}",
                ext["id"].as_str().unwrap_or("?"),
                ext["state"].as_str().unwrap_or("?"),
                ext["pid"],
                ext["events_emitted"],
                ext["last_event_at_ms"]
            );
        }
    }
    println!("subscribers:");
    if let Some(subs) = status["subscribers"].as_array() {
        for sub in subs {
            println!(
                "  {}\tpatterns={}\tdelivered={}\tdropped={}",
                sub["name"].as_str().unwrap_or("?"),
                sub["patterns"],
                sub["delivered"],
                sub["dropped"]
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::humanize_ms;

    #[test]
    fn humanize_ms_examples() {
        assert_eq!(humanize_ms(0), "0s");
        assert_eq!(humanize_ms(5_000), "5s");
        assert_eq!(humanize_ms(90_000), "1m 30s");
        assert_eq!(humanize_ms(37_946_866), "10h 32m");
        assert_eq!(humanize_ms(172_800_000), "2d 0h");
    }

    #[test]
    fn humanize_ms_boundaries() {
        // Just under a minute stays in seconds.
        assert_eq!(humanize_ms(59_999), "59s");
        // Exactly one minute.
        assert_eq!(humanize_ms(60_000), "1m 0s");
        // Just under an hour stays in minutes+seconds.
        assert_eq!(humanize_ms(3_599_000), "59m 59s");
        // Exactly one hour.
        assert_eq!(humanize_ms(3_600_000), "1h 0m");
        // Just under a day stays in hours+minutes.
        assert_eq!(humanize_ms(86_399_000), "23h 59m");
        // Exactly one day.
        assert_eq!(humanize_ms(86_400_000), "1d 0h");
    }
}
