use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncBufRead, Lines};
use tokio::time;
use whdr_proto::{ExtMsg, PROTOCOL_VERSION, decode_line};

#[derive(Debug)]
pub(crate) struct ExtensionRegistration {
    pub(crate) id: String,
    pub(crate) paths: Vec<String>,
    pub(crate) channels: Vec<String>,
}

pub(crate) async fn read_registration<R>(
    candidate_id: &str,
    lines: &mut Lines<R>,
    timeout_ms: u64,
) -> Result<ExtensionRegistration>
where
    R: AsyncBufRead + Unpin,
{
    let first_line = time::timeout(Duration::from_millis(timeout_ms), lines.next_line())
        .await
        .context("extension register timeout")??
        .context("extension exited before register")?;

    match decode_line::<ExtMsg>(&first_line)? {
        Some(ExtMsg::Register {
            protocol,
            id,
            paths,
            channels,
            meta: _,
        }) => {
            if protocol != PROTOCOL_VERSION {
                bail!("unsupported protocol version {protocol}");
            }
            Ok(ExtensionRegistration {
                id: id.unwrap_or_else(|| candidate_id.to_string()),
                paths,
                channels,
            })
        }
        Some(other) => bail!("expected register, got {other:?}"),
        None => bail!("blank register line"),
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncBufReadExt, BufReader};

    use super::*;

    #[tokio::test]
    async fn read_registration_applies_candidate_id_fallback() {
        let input = br#"{"type":"register","protocol":1,"paths":["dev"],"channels":["dev.>"]}
"#;
        let mut lines = BufReader::new(&input[..]).lines();

        let registration = read_registration("candidate", &mut lines, 100)
            .await
            .unwrap();

        assert_eq!(registration.id, "candidate");
        assert_eq!(registration.paths, ["dev"]);
        assert_eq!(registration.channels, ["dev.>"]);
    }

    #[tokio::test]
    async fn read_registration_rejects_non_register_first_frame() {
        let input = br#"{"type":"log","level":"info","msg":"hello"}
"#;
        let mut lines = BufReader::new(&input[..]).lines();

        let err = read_registration("candidate", &mut lines, 100)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("expected register"));
    }

    #[tokio::test]
    async fn read_registration_reports_timeout() {
        let (_writer, reader) = tokio::io::duplex(64);
        let mut lines = BufReader::new(reader).lines();

        let err = read_registration("candidate", &mut lines, 1)
            .await
            .unwrap_err();

        assert_eq!(err.to_string(), "extension register timeout");
    }

    #[tokio::test]
    async fn read_registration_reports_eof_before_register() {
        let input = b"";
        let mut lines = BufReader::new(&input[..]).lines();

        let err = read_registration("candidate", &mut lines, 100)
            .await
            .unwrap_err();

        assert_eq!(err.to_string(), "extension exited before register");
    }

    #[tokio::test]
    async fn read_registration_rejects_blank_first_line() {
        let input = b"\n";
        let mut lines = BufReader::new(&input[..]).lines();

        let err = read_registration("candidate", &mut lines, 100)
            .await
            .unwrap_err();

        assert_eq!(err.to_string(), "blank register line");
    }

    #[tokio::test]
    async fn read_registration_rejects_protocol_mismatch() {
        let input = br#"{"type":"register","protocol":999}
"#;
        let mut lines = BufReader::new(&input[..]).lines();

        let err = read_registration("candidate", &mut lines, 100)
            .await
            .unwrap_err();

        assert_eq!(err.to_string(), "unsupported protocol version 999");
    }
}
