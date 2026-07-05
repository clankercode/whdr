use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum NdjsonError {
    #[error("malformed json: {0}")]
    MalformedJson(#[from] serde_json::Error),
}

pub fn encode_line<T: Serialize>(value: &T) -> Result<String, NdjsonError> {
    let mut line = serde_json::to_string(value)?;
    line.push('\n');
    Ok(line)
}

pub fn decode_line<T: DeserializeOwned>(line: &str) -> Result<Option<T>, NdjsonError> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    Ok(Some(serde_json::from_str(trimmed)?))
}
