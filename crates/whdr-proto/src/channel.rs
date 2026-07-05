use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ChannelError {
    #[error("empty channel")]
    Empty,
    #[error("empty token")]
    EmptyToken,
    #[error("invalid token: {0}")]
    InvalidToken(String),
    #[error("wildcard is not valid in channels")]
    WildcardInChannel,
    #[error("'>' wildcard must be the final pattern token")]
    TailWildcardNotFinal,
}

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Pattern(String);

impl Pattern {
    pub fn new(value: impl Into<String>) -> Result<Self, ChannelError> {
        let value = value.into();
        validate_pattern(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn matches(&self, channel: &str) -> Result<bool, ChannelError> {
        channel_matches(channel, &self.0)
    }
}

impl fmt::Debug for Pattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Pattern").field(&self.0).finish()
    }
}

fn valid_plain_token(token: &str) -> bool {
    !token.is_empty()
        && token
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
}

pub fn validate_channel(channel: &str) -> Result<(), ChannelError> {
    if channel.is_empty() {
        return Err(ChannelError::Empty);
    }
    for token in channel.split('.') {
        if token.is_empty() {
            return Err(ChannelError::EmptyToken);
        }
        if token == "*" || token == ">" {
            return Err(ChannelError::WildcardInChannel);
        }
        if !valid_plain_token(token) {
            return Err(ChannelError::InvalidToken(token.to_string()));
        }
    }
    Ok(())
}

pub fn validate_pattern(pattern: &str) -> Result<(), ChannelError> {
    if pattern.is_empty() {
        return Err(ChannelError::Empty);
    }
    let tokens: Vec<&str> = pattern.split('.').collect();
    for (idx, token) in tokens.iter().enumerate() {
        if token.is_empty() {
            return Err(ChannelError::EmptyToken);
        }
        if *token == ">" {
            if idx + 1 != tokens.len() {
                return Err(ChannelError::TailWildcardNotFinal);
            }
            continue;
        }
        if *token == "*" {
            continue;
        }
        if !valid_plain_token(token) {
            return Err(ChannelError::InvalidToken((*token).to_string()));
        }
    }
    Ok(())
}

pub fn channel_matches(channel: &str, pattern: &str) -> Result<bool, ChannelError> {
    validate_channel(channel)?;
    validate_pattern(pattern)?;

    let channel_tokens: Vec<&str> = channel.split('.').collect();
    let pattern_tokens: Vec<&str> = pattern.split('.').collect();
    let mut ci = 0usize;

    for (pi, pattern_token) in pattern_tokens.iter().enumerate() {
        match *pattern_token {
            ">" => return Ok(ci < channel_tokens.len() || pi == 0),
            "*" => {
                if ci >= channel_tokens.len() {
                    return Ok(false);
                }
                ci += 1;
            }
            exact => {
                if channel_tokens.get(ci) != Some(&exact) {
                    return Ok(false);
                }
                ci += 1;
            }
        }
    }

    Ok(ci == channel_tokens.len())
}
